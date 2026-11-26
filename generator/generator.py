"""Имитация IoT-датчиков: события с дублями, задержками и вразнобой по времени.

Каждое событие — JSON: event_id, sensor_id, event_time (UTC), temperature, humidity.
«Грязь» в потоке:
  * P_DUP       — то же сообщение отправляется второй раз (через 0–20 с);
  * P_DELAYED   — событие приходит с опозданием 5–90 с (перемешивает порядок);
  * P_VERY_LATE — событие приходит с опозданием 5–30 мин (за пределами watermark).
Опоздание имитируется тем, что event_time ставится в прошлое: для потребителя
это то же самое, что датчик долго был без связи.

Параллельно генератор ведёт «эталон» — сколько уникальных событий он отправил
за каждую дату (таблица generator_control). Это только для проверки стенда.
"""
import json
import os
import random
import signal
import time
import uuid
from collections import defaultdict
from datetime import datetime, timedelta, timezone

import psycopg2
from confluent_kafka import Producer

BOOTSTRAP = os.environ["KAFKA_BOOTSTRAP"]
TOPIC = os.environ["KAFKA_TOPIC"]
PG_DSN = os.environ["PG_DSN"]
RATE = float(os.getenv("RATE", "20"))
SENSORS = int(os.getenv("SENSORS", "20"))
P_DUP = float(os.getenv("P_DUP", "0.05"))
P_DELAYED = float(os.getenv("P_DELAYED", "0.15"))
P_VERY_LATE = float(os.getenv("P_VERY_LATE", "0.02"))

running = True


def stop(*_):
    global running
    running = False


signal.signal(signal.SIGTERM, stop)
signal.signal(signal.SIGINT, stop)

# Надёжная доставка: брокер подтверждает запись (acks=all), а idempotence
# не даёт самому продюсеру задвоить сообщение при сетевом ретрае.
producer = Producer({
    "bootstrap.servers": BOOTSTRAP,
    "enable.idempotence": True,
    "acks": "all",
    "linger.ms": 50,
})

# Счётчики подтверждённых брокером сообщений: дата -> [unique, sent, dups, very_late]
stats = defaultdict(lambda: [0, 0, 0, 0])
failed = 0


def on_delivery(meta):
    def cb(err, _msg):
        global failed
        if err is not None:
            failed += 1
            print(f"DELIVERY FAILED: {err}", flush=True)
            return
        day, is_dup, is_very_late = meta
        s = stats[day]
        s[1] += 1
        if is_dup:
            s[2] += 1
        else:
            s[0] += 1
            if is_very_late:
                s[3] += 1
    return cb


def send(event, is_dup, is_very_late):
    day = event["event_time"][:10]
    while True:
        try:
            producer.produce(
                TOPIC,
                key=event["sensor_id"],
                value=json.dumps(event),
                on_delivery=on_delivery((day, is_dup, is_very_late)),
            )
            return
        except BufferError:  # локальная очередь заполнена — ждём, не теряем
            producer.poll(0.5)


def flush_control(conn):
    """Прибавляет накопленные счётчики к таблице-эталону."""
    if not stats:
        return
    with conn.cursor() as cur:
        for day, (uniq, sent, dups, late) in list(stats.items()):
            cur.execute(
                """
                INSERT INTO generator_control
                    (event_date, unique_events, sent_messages, duplicates_sent, very_late_sent)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (event_date) DO UPDATE SET
                    unique_events   = generator_control.unique_events   + EXCLUDED.unique_events,
                    sent_messages   = generator_control.sent_messages   + EXCLUDED.sent_messages,
                    duplicates_sent = generator_control.duplicates_sent + EXCLUDED.duplicates_sent,
                    very_late_sent  = generator_control.very_late_sent  + EXCLUDED.very_late_sent,
                    updated_at      = now()
                """,
                (day, uniq, sent, dups, late),
            )
    conn.commit()
    stats.clear()


def main():
    conn = psycopg2.connect(PG_DSN)
    sensors = [f"sensor-{i:02d}" for i in range(1, SENSORS + 1)]
    base_temp = {s: random.uniform(18, 26) for s in sensors}
    pending_dups = []  # (когда отправить, событие)
    total = 0
    last_flush = time.time()
    print(f"generator started: rate={RATE}/s sensors={SENSORS} "
          f"dup={P_DUP} delayed={P_DELAYED} very_late={P_VERY_LATE}", flush=True)

    while running:
        now = datetime.now(timezone.utc)
        r = random.random()
        very_late = r < P_VERY_LATE
        if very_late:
            lag = random.uniform(300, 1800)
        elif r < P_VERY_LATE + P_DELAYED:
            lag = random.uniform(5, 90)
        else:
            lag = 0.0
        sensor = random.choice(sensors)
        event = {
            "event_id": str(uuid.uuid4()),
            "sensor_id": sensor,
            "event_time": (now - timedelta(seconds=lag)).isoformat(timespec="milliseconds"),
            "temperature": round(base_temp[sensor] + random.gauss(0, 0.8), 2),
            "humidity": round(random.uniform(30, 70), 1),
        }
        send(event, is_dup=False, is_very_late=very_late)
        total += 1
        if random.random() < P_DUP:
            pending_dups.append((time.time() + random.uniform(0, 20), event))

        due = [d for d in pending_dups if d[0] <= time.time()]
        if due:
            pending_dups = [d for d in pending_dups if d[0] > time.time()]
            for _, ev in due:
                send(ev, is_dup=True, is_very_late=False)

        producer.poll(0)
        if time.time() - last_flush >= 5:
            flush_control(conn)
            last_flush = time.time()
            print(f"generated unique events: {total}", flush=True)
        time.sleep(1.0 / RATE)

    # Аккуратная остановка: досылаем отложенные дубли и ждём подтверждений брокера.
    for _, ev in pending_dups:
        send(ev, is_dup=True, is_very_late=False)
    left = producer.flush(15)
    flush_control(conn)
    conn.close()
    print(f"generator stopped: unique={total}, not confirmed={left}, failed={failed}", flush=True)


if __name__ == "__main__":
    main()
