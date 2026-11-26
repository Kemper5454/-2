"""Streaming-часть Lambda: один процесс Spark Structured Streaming, два запроса.

1. bronze — сырые сообщения Kafka «как есть» в Iceberg (append-only). Это вход
   batch-слоя: сюда попадает всё, включая дубли и сколь угодно поздние события.
2. speed  — живой мониторинг: event-time, watermark, дедупликация по event_id,
   минутные окна -> upsert в PostgreSQL.

Без потерь и задвоений:
  * offsets Kafka и состояние хранятся в checkpoint — после падения запрос
    продолжает с того же места (at-least-once на чтении);
  * приёмники идемпотентны: Iceberg не коммитит один и тот же микробатч дважды,
    PostgreSQL принимает upsert по ключу (окно, датчик).
"""
import os
import time

from psycopg2.extras import execute_values
from pyspark.sql import functions as F

from common import parse_events, pg_connect, spark_session

WATERMARK = os.getenv("WATERMARK", "2 minutes")

spark = spark_session("iot-stream")
spark.sparkContext.setLogLevel("WARN")

spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.bronze")
spark.sql("""
    CREATE TABLE IF NOT EXISTS lake.bronze.events (
        payload         string,
        kafka_partition int,
        kafka_offset    bigint,
        kafka_ts        timestamp,
        ingest_ts       timestamp
    ) USING iceberg
    PARTITIONED BY (days(kafka_ts))
""")

raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", os.environ["KAFKA_BOOTSTRAP"])
    .option("subscribe", os.environ["KAFKA_TOPIC"])
    .option("startingOffsets", "earliest")
    .option("maxOffsetsPerTrigger", 50000)
    .load()
    .select(
        F.col("value").cast("string").alias("payload"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_ts"),
    )
)

# ---------- запрос 1: bronze ----------
bronze_query = (
    raw.withColumn("ingest_ts", F.current_timestamp())
    .writeStream.format("iceberg")
    .queryName("bronze")
    .outputMode("append")
    .option("checkpointLocation", "/checkpoints/bronze")
    .option("fanout-enabled", "true")
    .trigger(processingTime="30 seconds")
    .toTable("lake.bronze.events")
)

# ---------- запрос 2: speed ----------
speed = (
    parse_events(raw)
    # Время берём из самого события (event-time), а не момент прихода.
    # Watermark: ждём опоздавших 2 минуты, потом окно закрывается и состояние чистится.
    .withWatermark("event_time", WATERMARK)
    # Дубли: одинаковый event_id в пределах watermark пропускается один раз.
    .dropDuplicatesWithinWatermark(["event_id"])
    .groupBy(F.window("event_time", "1 minute"), "sensor_id")
    .agg(
        F.count("*").alias("events_cnt"),
        F.avg("temperature").alias("avg_temp"),
        F.min("temperature").alias("min_temp"),
        F.max("temperature").alias("max_temp"),
    )
    .select(F.col("window.start").alias("window_start"), "sensor_id",
            "events_cnt", "avg_temp", "min_temp", "max_temp")
)


def upsert_live(batch_df, batch_id):
    """Микробатч -> PostgreSQL. В режиме update приходит полное новое значение окна,
    поэтому повтор того же батча даёт тот же результат (идемпотентно)."""
    rows = [tuple(r) for r in batch_df.collect()]
    if not rows:
        return
    with pg_connect() as conn, conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO live_sensor_1m
                (window_start, sensor_id, events_cnt, avg_temp, min_temp, max_temp)
            VALUES %s
            ON CONFLICT (window_start, sensor_id) DO UPDATE SET
                events_cnt = EXCLUDED.events_cnt,
                avg_temp   = EXCLUDED.avg_temp,
                min_temp   = EXCLUDED.min_temp,
                max_temp   = EXCLUDED.max_temp,
                updated_at = now()
        """, rows)
    conn.close()


speed_query = (
    speed.writeStream.queryName("speed")
    .outputMode("update")
    .foreachBatch(upsert_live)
    .option("checkpointLocation", "/checkpoints/speed")
    .trigger(processingTime="10 seconds")
    .start()
)

# Раз в 30 с печатаем прогресс: сколько прочитано и сколько строк отброшено
# watermark-ом (это «очень поздние» — их подберёт batch-слой из bronze).
dropped_total = 0
last_batch = -1
while bronze_query.isActive and speed_query.isActive:
    time.sleep(30)
    p = speed_query.lastProgress
    if p and p["batchId"] != last_batch:
        last_batch = p["batchId"]
        ops = p.get("stateOperators", [])
        dropped = sum(o.get("numRowsDroppedByWatermark", 0) for o in ops)
        dup_dropped = sum(o.get("customMetrics", {}).get("numDroppedDuplicateRows", 0) for o in ops)
        wm = p.get("eventTime", {}).get("watermark")
        print(f"[speed] batch={p['batchId']} input={p['numInputRows']} "
              f"watermark={wm} late_dropped={dropped} dup_dropped={dup_dropped}", flush=True)
    b = bronze_query.lastProgress
    if b:
        print(f"[bronze] batch={b['batchId']} input={b['numInputRows']}", flush=True)

for q in (bronze_query, speed_query):
    if q.exception():
        raise q.exception()
