"""Batch-слой Lambda: точный суточный отчёт из сырых данных bronze.

Шаги (в проде запускается раз в сутки планировщиком, на стенде — вручную):
  1. bronze -> silver: разбор JSON и дедупликация по event_id через MERGE.
     Берём сообщения, пришедшие за последние LOOKBACK_DAYS дней, — так в отчёт
     попадают и события, опоздавшие сильнее, чем ждёт streaming.
  2. silver -> gold: пересчёт суточных агрегатов только за затронутые даты.
  3. gold -> PostgreSQL daily_report (upsert) — витрина для отчётов.
  4. Сверка с эталоном генератора.

Задача идемпотентна: повторный запуск на тех же данных ничего не меняет.
"""
import os

from psycopg2.extras import execute_values
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from common import parse_events, pg_connect, spark_session

LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "2"))

spark = spark_session("iot-batch")
spark.sparkContext.setLogLevel("WARN")

spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.silver")
spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.gold")
spark.sql("""
    CREATE TABLE IF NOT EXISTS lake.silver.events (
        event_id    string,
        sensor_id   string,
        event_time  timestamp,
        temperature double,
        humidity    double,
        event_date  date,
        kafka_ts    timestamp
    ) USING iceberg
    PARTITIONED BY (event_date)
""")
spark.sql("""
    CREATE TABLE IF NOT EXISTS lake.gold.daily_report (
        event_date date,
        sensor_id  string,
        events_cnt bigint,
        avg_temp   double,
        min_temp   double,
        max_temp   double
    ) USING iceberg
""")

# ---------- 1. bronze -> silver ----------
bronze_new = spark.table("lake.bronze.events").where(
    F.col("kafka_ts") >= F.date_sub(F.current_date(), LOOKBACK_DAYS)
)
first_copy = Window.partitionBy("event_id").orderBy("kafka_ts", "kafka_partition", "kafka_offset")
src = (
    parse_events(bronze_new)
    .withColumn("rn", F.row_number().over(first_copy))   # дубли внутри пачки
    .where("rn = 1")
    .withColumn("event_date", F.to_date("event_time"))
    .select("event_id", "sensor_id", "event_time", "temperature", "humidity",
            "event_date", "kafka_ts")
    .cache()
)
src.createOrReplaceTempView("src")
affected = [r["event_date"] for r in src.select("event_date").distinct().collect()]
print(f"bronze rows in lookback window: {bronze_new.count()}, "
      f"unique events: {src.count()}, affected dates: {sorted(map(str, affected))}")

if not affected:
    print("nothing to process")
    spark.stop()
    raise SystemExit(0)

# Дубли относительно уже загруженного: вставляем только те event_id, которых ещё нет.
spark.sql("""
    MERGE INTO lake.silver.events t
    USING src s
    ON t.event_id = s.event_id AND t.event_date = s.event_date
    WHEN NOT MATCHED THEN INSERT *
""")

# ---------- 2. silver -> gold (только затронутые даты) ----------
dates_sql = ", ".join(f"DATE '{d}'" for d in affected)
spark.sql(f"""
    MERGE INTO lake.gold.daily_report t
    USING (
        SELECT event_date, sensor_id,
               count(*)         AS events_cnt,
               avg(temperature) AS avg_temp,
               min(temperature) AS min_temp,
               max(temperature) AS max_temp
        FROM lake.silver.events
        WHERE event_date IN ({dates_sql})
        GROUP BY event_date, sensor_id
    ) s
    ON t.event_date = s.event_date AND t.sensor_id = s.sensor_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

# ---------- 3. gold -> PostgreSQL ----------
gold_rows = [tuple(r) for r in spark.sql(f"""
    SELECT event_date, sensor_id, events_cnt, avg_temp, min_temp, max_temp
    FROM lake.gold.daily_report WHERE event_date IN ({dates_sql})
""").collect()]
conn = pg_connect()
with conn, conn.cursor() as cur:
    execute_values(cur, """
        INSERT INTO daily_report (event_date, sensor_id, events_cnt, avg_temp, min_temp, max_temp)
        VALUES %s
        ON CONFLICT (event_date, sensor_id) DO UPDATE SET
            events_cnt = EXCLUDED.events_cnt,
            avg_temp   = EXCLUDED.avg_temp,
            min_temp   = EXCLUDED.min_temp,
            max_temp   = EXCLUDED.max_temp,
            loaded_at  = now()
    """, gold_rows)

# ---------- 4. сверка ----------
with conn, conn.cursor() as cur:
    cur.execute("SELECT event_date, unique_events, sent_messages FROM generator_control")
    control = {r[0]: (r[1], r[2]) for r in cur.fetchall()}
    cur.execute("""
        SELECT (window_start AT TIME ZONE 'UTC')::date, sum(events_cnt)
        FROM live_sensor_1m GROUP BY 1
    """)
    live = {r[0]: int(r[1]) for r in cur.fetchall()}
conn.close()

silver = {r["event_date"]: r["cnt"] for r in spark.sql(f"""
    SELECT event_date, count(*) AS cnt FROM lake.silver.events
    WHERE event_date IN ({dates_sql}) GROUP BY event_date
""").collect()}
gold = {r["event_date"]: r["cnt"] for r in spark.sql(f"""
    SELECT event_date, sum(events_cnt) AS cnt FROM lake.gold.daily_report
    WHERE event_date IN ({dates_sql}) GROUP BY event_date
""").collect()}
bronze_total = spark.table("lake.bronze.events").count()

print("\n================ СВЕРКА ================")
print(f"bronze, всего сообщений (с дублями): {bronze_total}")
print(f"{'дата':<12}{'эталон уник.':>14}{'отправлено':>12}{'silver':>10}{'gold':>10}{'speed':>10}  итог")
for d in sorted(affected):
    uniq, sent = control.get(d, (None, None))
    s, g = silver.get(d, 0), gold.get(d, 0)
    if uniq is None:
        verdict = "нет эталона"
    elif s == g == uniq:
        verdict = "OK"
    elif s == g < uniq:
        # Генератор ещё работает: последние события лежат в Kafka и не дошли до bronze.
        verdict = f"в пути {uniq - s} (для точной сверки остановить генератор)"
    else:
        verdict = "РАСХОЖДЕНИЕ"
    print(f"{str(d):<12}{str(uniq):>14}{str(sent):>12}{s:>10}{g:>10}{live.get(d, 0):>10}  {verdict}")
print("speed < эталона — это нормально: streaming не учитывает события, опоздавшие за watermark.")
print("========================================\n")

spark.stop()
