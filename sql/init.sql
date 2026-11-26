-- Выполняется один раз при первом старте PostgreSQL (БД serving).

-- Отдельная БД под JDBC-каталог Iceberg (таблицы каталога Iceberg создаст сам).
CREATE DATABASE iceberg_catalog;

-- Speed view: минутные агрегаты для живого мониторинга.
-- PK нужен для идемпотентного upsert: повтор микробатча не задваивает строки.
CREATE TABLE live_sensor_1m (
    window_start timestamptz NOT NULL,
    sensor_id    text        NOT NULL,
    events_cnt   bigint      NOT NULL,
    avg_temp     double precision,
    min_temp     double precision,
    max_temp     double precision,
    updated_at   timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (window_start, sensor_id)
);

-- Batch view: точный суточный отчёт, пересчитывается batch-слоем из Lakehouse.
CREATE TABLE daily_report (
    event_date date   NOT NULL,
    sensor_id  text   NOT NULL,
    events_cnt bigint NOT NULL,
    avg_temp   double precision,
    min_temp   double precision,
    max_temp   double precision,
    loaded_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (event_date, sensor_id)
);

-- Эталон от генератора (только для стенда): сколько уникальных событий реально
-- было отправлено за каждую дату. С ним сверяется суточный отчёт.
CREATE TABLE generator_control (
    event_date      date PRIMARY KEY,
    unique_events   bigint NOT NULL DEFAULT 0,
    sent_messages   bigint NOT NULL DEFAULT 0,
    duplicates_sent bigint NOT NULL DEFAULT 0,
    very_late_sent  bigint NOT NULL DEFAULT 0,
    updated_at      timestamptz NOT NULL DEFAULT now()
);
