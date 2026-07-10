-- ============================================================
-- SILVER LAYER: Limpieza y Validación de Datos
-- ============================================================

SET 'execution.attached' = 'false';
SET 'execution.checkpointing.interval' = '10s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';

-- ============================================================
-- FUENTE: KAFKA (Necesaria para Silver)
-- ============================================================
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

-- ============================================================
-- TABLA SILVER
-- ============================================================
CREATE TABLE IF NOT EXISTS silver_sink (
    event_timestamp TIMESTAMP(3),
    voltage_v DOUBLE,
    current_a DOUBLE,
    power_kw DOUBLE,
    solar_kw DOUBLE,
    wind_kw DOUBLE,
    fault_num INT,
    fault_indicator STRING,
    temperature_c DOUBLE,
    humidity_perc DOUBLE,
    price_gbp DOUBLE,
    data_quality_flag STRING,
    quality_score INT,
    processed_timestamp TIMESTAMP(3)
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/smartgrid',
    'table-name' = 'silver_validated_data',
    'username' = 'admin',
    'password' = 'admin',
    'sink.buffer-flush.max-rows' = '100',
    'sink.buffer-flush.interval' = '5s'
);

-- ============================================================
-- PROCESAR DATOS
-- ============================================================
INSERT INTO silver_sink
SELECT 
    event_time AS event_timestamp,
    voltage_v,
    current_a,
    power_kw,
    solar_kw,
    wind_kw,
    fault_num,
    fault_indicator,
    temperature_c,
    `humidity_%` AS humidity_perc,
    electricity_price_gbp_per_kwh AS price_gbp,
    CASE 
        WHEN power_kw IS NULL OR power_kw < 0 OR power_kw > 500 THEN 'INVALID_POWER'
        WHEN voltage_v IS NULL OR voltage_v < 100 OR voltage_v > 300 THEN 'INVALID_VOLTAGE'
        WHEN current_a IS NULL OR current_a < 0 OR current_a > 1000 THEN 'INVALID_CURRENT'
        ELSE 'OK'
    END AS data_quality_flag,
    CASE 
        WHEN power_kw IS NULL OR power_kw < 0 OR power_kw > 500 THEN 0
        WHEN voltage_v IS NULL OR voltage_v < 100 OR voltage_v > 300 THEN 0
        WHEN current_a IS NULL OR current_a < 0 OR current_a > 1000 THEN 0
        ELSE 100
    END AS quality_score,
    CURRENT_TIMESTAMP AS processed_timestamp
FROM kafka_source
WHERE 
    power_kw IS NOT NULL 
    AND voltage_v IS NOT NULL 
    AND current_a IS NOT NULL
    AND power_kw BETWEEN 0 AND 500
    AND voltage_v BETWEEN 100 AND 300
    AND current_a BETWEEN 0 AND 1000;