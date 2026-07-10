-- ============================================================
-- GOLD LAYER: Star Schema + SCD Type 2
-- ============================================================

SET 'execution.attached' = 'false';
SET 'execution.checkpointing.interval' = '10s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';

-- ============================================================
-- FUENTE: KAFKA (Necesaria para Gold)
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
-- DIMENSIÓN TIEMPO
-- ============================================================
CREATE TABLE IF NOT EXISTS dim_time_sink (
    time_id INT,
    `year` INT,
    `month` INT,
    `day` INT,
    `hour` INT,
    `minute` INT,
    day_of_week INT,
    is_weekend BOOLEAN,
    PRIMARY KEY (time_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/smartgrid',
    'table-name' = 'dim_time',
    'username' = 'admin',
    'password' = 'admin',
    'sink.buffer-flush.max-rows' = '1',
    'sink.buffer-flush.interval' = '5s'
);

-- CORREGIDO: Usamos una ventana de 1 minuto para crear registros de tiempo únicos
INSERT INTO dim_time_sink
SELECT DISTINCT
    CAST(DATE_FORMAT(window_start, 'yyyyMMddHHmm') AS INT) AS time_id,
    CAST(EXTRACT(YEAR FROM window_start) AS INT) AS `year`,
    CAST(EXTRACT(MONTH FROM window_start) AS INT) AS `month`,
    CAST(EXTRACT(DAY FROM window_start) AS INT) AS `day`,
    CAST(EXTRACT(HOUR FROM window_start) AS INT) AS `hour`,
    CAST(EXTRACT(MINUTE FROM window_start) AS INT) AS `minute`,
    CAST(EXTRACT(DOW FROM window_start) AS INT) AS day_of_week,
    CASE WHEN EXTRACT(DOW FROM window_start) IN (1, 7) THEN TRUE ELSE FALSE END AS is_weekend
FROM TABLE(
    TUMBLE(TABLE kafka_source, DESCRIPTOR(event_time), INTERVAL '1' MINUTE)
)
GROUP BY window_start;

-- ============================================================
-- SCD TYPE 2: DIMENSIÓN PRECIO (VERSIÓN SIMPLIFICADA)
-- ============================================================
CREATE TABLE IF NOT EXISTS dim_price_scd2 (
    price_value DOUBLE,
    effective_date TIMESTAMP(3),
    expiry_date TIMESTAMP(3),
    is_current BOOLEAN,
    PRIMARY KEY (price_value, effective_date) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/smartgrid',
    'table-name' = 'dim_price',
    'username' = 'admin',
    'password' = 'admin',
    'sink.buffer-flush.max-rows' = '1',
    'sink.buffer-flush.interval' = '1s'
);

-- NOTA: Para SCD Type 2 completo necesitarías una tabla temporal y lógica UPSERT
-- Esta versión simplificada inserta nuevos precios sin cerrar los anteriores
INSERT INTO dim_price_scd2
SELECT 
    electricity_price_gbp_per_kwh AS price_value,
    window_start AS effective_date,
    CAST(NULL AS TIMESTAMP(3)) AS expiry_date,
    TRUE AS is_current
FROM TABLE(
    TUMBLE(TABLE kafka_source, DESCRIPTOR(event_time), INTERVAL '1' MINUTE)
)
WHERE electricity_price_gbp_per_kwh IS NOT NULL
GROUP BY window_start, electricity_price_gbp_per_kwh;

-- ============================================================
-- FACT TABLE: Métricas Agregadas (CORREGIDO)
-- ============================================================
CREATE TABLE IF NOT EXISTS fact_sink (
    time_id INT,
    avg_power_kw DOUBLE,
    max_fault_num INT,
    total_solar_generated DOUBLE,
    avg_temperature_c DOUBLE,
    event_count BIGINT,  
    aggregated_timestamp TIMESTAMP(3),
    PRIMARY KEY (time_id, aggregated_timestamp) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/smartgrid',
    'table-name' = 'fact_grid_metrics',
    'username' = 'admin',
    'password' = 'admin',
    'sink.buffer-flush.max-rows' = '100',
    'sink.buffer-flush.interval' = '10s'
);

-- CORREGIDO: GROUP BY solo con window_start
INSERT INTO fact_sink
SELECT 
    CAST(DATE_FORMAT(window_start, 'yyyyMMddHHmm') AS INT) AS time_id,
    AVG(power_kw) AS avg_power_kw,
    MAX(fault_num) AS max_fault_num,
    SUM(solar_kw) AS total_solar_generated,
    AVG(temperature_c) AS avg_temperature_c,
    COUNT(*) AS event_count,  
    window_start AS aggregated_timestamp
FROM TABLE(
    TUMBLE(TABLE kafka_source, DESCRIPTOR(event_time), INTERVAL '1' MINUTE)
)
GROUP BY window_start;  