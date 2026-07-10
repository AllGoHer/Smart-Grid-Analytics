-- ============================================================
-- MEDALLION ARCHITECTURE - POSTGRESQL SCHEMA
-- ============================================================

-- ============================================================
-- BRONZE: Almacenamiento Raw
-- ============================================================
CREATE TABLE IF NOT EXISTS bronze_raw_data (
    id SERIAL PRIMARY KEY,
    raw_data JSONB NOT NULL,
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    kafka_offset BIGINT,
    kafka_partition INT
);

-- ============================================================
-- SILVER: Datos Limpios y Validados
-- ============================================================
CREATE TABLE IF NOT EXISTS silver_validated_data (
    id SERIAL PRIMARY KEY,
    event_timestamp TIMESTAMP(3),
    voltage_v DOUBLE PRECISION,
    current_a DOUBLE PRECISION,
    power_kw DOUBLE PRECISION,
    solar_kw DOUBLE PRECISION,
    wind_kw DOUBLE PRECISION,
    fault_num INT,
    fault_indicator VARCHAR(50),
    temperature_c DOUBLE PRECISION,
    humidity_perc DOUBLE PRECISION,
    price_gbp DOUBLE PRECISION,
    data_quality_flag VARCHAR(50),
    quality_score INT,
    processed_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================
-- GOLD: Star Schema (Fact & Dimensions)
-- ============================================================

-- DIMENSION: Tiempo
CREATE TABLE IF NOT EXISTS dim_time (
    time_id SERIAL PRIMARY KEY,
    year INT,
    month INT,
    day INT,
    hour INT,
    minute INT,
    day_of_week INT,
    is_weekend BOOLEAN
);

-- DIMENSION: Ubicación (SCD Type 2)
CREATE TABLE IF NOT EXISTS dim_location (
    location_id SERIAL PRIMARY KEY,
    grid_zone VARCHAR(50),
    location_name VARCHAR(100),
    effective_date TIMESTAMP(3),
    expiry_date TIMESTAMP(3),
    is_current BOOLEAN DEFAULT TRUE,
    UNIQUE(grid_zone, effective_date)
);

-- DIMENSION: Precio (SCD Type 2)
CREATE TABLE IF NOT EXISTS dim_price (
    price_id SERIAL PRIMARY KEY,
    price_value DOUBLE PRECISION,
    effective_date TIMESTAMP(3),
    expiry_date TIMESTAMP(3),
    is_current BOOLEAN DEFAULT TRUE,
    UNIQUE(price_value, effective_date)
);

-- FACT TABLE: Smart Grid Metrics
DROP TABLE IF EXISTS fact_grid_metrics CASCADE;
CREATE TABLE fact_grid_metrics (
    fact_id SERIAL PRIMARY KEY,
    avg_power_kw DOUBLE PRECISION,
    total_solar_generated DOUBLE PRECISION,
    max_fault_num INTEGER,
    event_count BIGINT,
    aggregated_timestamp TIMESTAMP(3) UNIQUE
);

-- ============================================================
-- ÍNDICES
-- ============================================================
CREATE INDEX idx_bronze_ingestion ON bronze_raw_data(ingestion_timestamp);
CREATE INDEX idx_silver_event ON silver_validated_data(event_timestamp);
CREATE INDEX idx_fact_time ON fact_grid_metrics(time_id);
CREATE INDEX idx_fact_location ON fact_grid_metrics(location_id);