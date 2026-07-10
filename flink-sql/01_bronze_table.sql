-- ============================================================
-- BRONZE LAYER: Ingesta de datos crudos desde Kafka
-- ============================================================

SET 'execution.attached' = 'false';
SET 'execution.checkpointing.interval' = '10s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
SET 'table.exec.sink.not-null-enforcer' = 'DROP';

-- Crear tabla fuente Kafka
CREATE TABLE IF NOT EXISTS kafka_source (
    `timestamp` BIGINT,
    voltage_v DOUBLE,
    current_a DOUBLE,
    power_kw DOUBLE,
    reactive_power_kvar DOUBLE,
    power_factor DOUBLE,
    solar_kw DOUBLE,
    wind_kw DOUBLE,
    grid_in_kw DOUBLE,
    grid_out_kw DOUBLE,
    `voltage_fluct_%` DOUBLE,
    fault_indicator STRING,
    fault_num INT,
    temperature_c DOUBLE,
    `humidity_%` DOUBLE,
    electricity_price_gbp_per_kwh DOUBLE,
    event_time AS TO_TIMESTAMP(FROM_UNIXTIME(`timestamp`)),
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'smartgrid',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'flink_bronze_group',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true'
);

-- Crear tabla Bronze (PostgreSQL)
CREATE TABLE IF NOT EXISTS bronze_sink (
    raw_data STRING,
    ingestion_timestamp TIMESTAMP(3),
    kafka_offset BIGINT,
    kafka_partition INT
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/smartgrid',
    'table-name' = 'bronze_raw_data',
    'username' = 'admin',
    'password' = 'admin',
    'sink.buffer-flush.max-rows' = '100',
    'sink.buffer-flush.interval' = '5s'
);

-- Insertar datos crudos en Bronze
INSERT INTO bronze_sink
SELECT 
    CAST(JSON_OBJECT(
        'timestamp' VALUE `timestamp`,
        'voltage_v' VALUE voltage_v,
        'current_a' VALUE current_a,
        'power_kw' VALUE power_kw,
        'solar_kw' VALUE solar_kw,
        'wind_kw' VALUE wind_kw,
        'fault_num' VALUE fault_num,
        'fault_indicator' VALUE fault_indicator,
        'temperature_c' VALUE temperature_c,
        'humidity_%' VALUE `humidity_%`,
        'price' VALUE electricity_price_gbp_per_kwh
    ) AS STRING) AS raw_data,
    event_time AS ingestion_timestamp,
    CAST(NULL AS BIGINT) AS kafka_offset,
    CAST(NULL AS INT) AS kafka_partition
FROM kafka_source;