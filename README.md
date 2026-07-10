# Smart-Grid-Analytics
________________________________________________________________________________________________________________________________________________________________________________________________________________

## ⚡Plataforma de Análisis de Datos Energéticos en Tiempo Real

![image](https://img.shields.io/badge/Apache%2520Flink-1.17.1-blue)
![image](https://img.shields.io/badge/Apache%2520Kafka-3.5.0-black)
![image](https://img.shields.io/badge/PostgreSQL-15-blue)
![image](https://img.shields.io/badge/Apache%2520Superset-2.1.0-orange)

Una solución end-to-end de procesamiento de datos para smart grids, diseñada para demostrar habilidades en arquitectura de datos moderna.

![image](https://github.com/user-attachments/assets/02dac6d2-ed17-40bc-b150-66808032a8c8)

________________________________________________________________________________________________________________________________________________________________________________________________________________
## 🎯 Visión General
________________________________________________________________________________________________________________________________________________________________________________________________________________
Smart Grid V2 es una plataforma completa de análisis de datos que simula el procesamiento de datos de una red eléctrica inteligente en tiempo real. El proyecto implementa una arquitectura de datos moderna que cubre todo el ciclo de vida de los datos: ingesta, procesamiento, almacenamiento y visualización. 

________________________________________________________________________________________________________________________________________________________________________________________________________________
## 🏗️ Arquitectura
________________________________________________________________________________________________________________________________________________________________________________________________________________
![image](https://github.com/user-attachments/assets/1f472e1a-c5c3-43e6-9259-a87133168394)
![image](https://github.com/user-attachments/assets/9e4fecc6-4b43-43a1-a23e-abec0ef9ad2c)

________________________________________________________________________________________________________________________________________________________________________________________________________________
## 🚀 El Producer Original: Corazón del Proyecto
________________________________________________________________________________________________________________________________________________________________________________________________________________
El Producer Original es el componente fundamental que simula una red de sensores IoT en una smart grid. Su diseño está inspirado en arquitecturas reales de ingestión de datos en el sector energético .
________________________________________________________________________________________________________________________________________________________________________________________________________________
## 🎯 Propósito y Diseño
________________________________________________________________________________________________________________________________________________________________________________________________________________
El producer simula el comportamiento de miles de medidores inteligentes, generando datos de red eléctrica de forma continua y realista . Fue desarrollado siguiendo el principio de mínima fricción: no realiza procesamiento, solo captura y publica datos brutos para mantener la fidelidad con un escenario de sensores reales .
________________________________________________________________________________________________________________________________________________________________________________________________________________
## 📊 Estructura de Datos Generados
________________________________________________________________________________________________________________________________________________________________________________________________________________
json:

      {
        "timestamp": 1700000000,              // Timestamp Unix en segundos
        "power_kw": 10.5,                     // Potencia demandada (kW)
        "solar_kw": 5.2,                      // Generación solar (kW)
        "fault_num": 0,                       // Número de fallas activas
        "temperature_c": 25.5,                // Temperatura (°C)
        "electricity_price_gbp_per_kwh": 0.15 // Precio de electricidad
      }

________________________________________________________________________________________________________________________________________________________________________________________________________________
## 🔧 Patrón de Producción
________________________________________________________________________________________________________________________________________________________________________________________________________________
El producer funciona con una lógica de generación periódica, típica en sistemas de smart metering :

python:

    def generate_meter_data(device_id):
        """Genera una lectura de medidor inteligente con variaciones realistas."""
        base_load = 10.0 + (device_id % 5) * 2  # Carga base por dispositivo
        load_variation = random.uniform(-0.5, 0.5)  # Variación simulada
        return {
            'device_id': device_id,
            'load': base_load + load_variation,
            'solar': 5.0 + random.uniform(-0.5, 0.5),
            'fault': random.choices([0, 1, 2], weights=[0.95, 0.04, 0.01])[0],
            'temperature': 25.0 + random.uniform(-2, 2)
        }
________________________________________________________________________________________________________________________________________________________________________________________________________________
## 📈 Escalabilidad y Realismo
________________________________________________________________________________________________________________________________________________________________________________________________________________
El diseño del producer permite :

Generación paralela: Múltiples workers para simular dispositivos a escala

Datos realistas: Variaciones temporales y estacionales

Comportamiento de sensores: Cada dispositivo tiene un comportamiento consistente

Frecuencia configurable: Tasa de producción ajustable
________________________________________________________________________________________________________________________________________________________________________________________________________________
## 🔄 Integración con Kafka
________________________________________________________________________________________________________________________________________________________________________________________________________________
El producer publica directamente en un tópico Kafka sin procesar los datos, manteniendo la separación de responsabilidades :

python:

    def publish_to_kafka(data):
        producer = KafkaProducer(
            bootstrap_servers='kafka:9092',
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        producer.send('smartgrid', data)
        producer.flush()

________________________________________________________________________________________________________________________________________________________________________________________________________________
## 📊 Capa de Procesamiento con Flink
________________________________________________________________________________________________________________________________________________________________________________________________________________

### Arquitectura Multi-Capa (Medallion)

El proyecto implementa la arquitectura Medallion (Bronce → Plata → Oro), un estándar en Data Engineering:

| Capa | Propósito | Operaciones |
|------|-----------|-------------|
| Bronze | Ingestion de datos crudos | Lectura desde Kafka, almacenamiento en tabla staging |
| Silver | Limpieza y validación | Filtrado de datos inválidos, tipado correcto, deduplicación |
| Gold | Agregaciones y star schema | Ventanas temporales, cálculos agregados, SCD Type 2 |

________________________________________________________________________________________________________________________________________________________________________________________________________________
## ⚡ Procesamiento en Tiempo Real
________________________________________________________________________________________________________________________________________________________________________________________________________________

**SQL de la capa Gold :**

SQL:

     INSERT INTO fact_sink
     SELECT
         AVG(power_kw) AS avg_power_kw,
         SUM(solar_kw) AS total_solar_generated,
         MAX(fault_num) AS max_fault_num,
         COUNT(*) AS event_count,
         window_start AS aggregated_timestamp,
         1 AS time_id,
         1 AS location_id,
         1 AS price_id
     FROM TABLE(
         TUMBLE(TABLE kafka_source, DESCRIPTOR(event_time), INTERVAL '1' MINUTE)
     )
     GROUP BY window_start;

____________________________________________________________________________________________________________________________________________________________________________________________________________________________
## 📈 Métricas Calculadas
____________________________________________________________________________________________________________________________________________________________________________________________________________________________

| KPI | Fórmula | Significado |
|-----|---------|-------------|
| avg_power_kw | AVG(power_kw) | Potencia promedio demandada |
| total_solar_generated | SUM(solar_kw) | Energía solar generada en el período |
| max_fault_num | MAX(fault_num) | Pico de fallas detectadas |
| event_count | COUNT(*) | Volumen de eventos procesados |

____________________________________________________________________________________________________________________________________________________________________________________________________________________________
## 🗄️ Modelo de Datos (Star Schema)
____________________________________________________________________________________________________________________________________________________________________________________________________________________________
**Dimensión Tiempo (SCD Type 0)**

SQL:

     CREATE TABLE dim_time (
         time_id SERIAL PRIMARY KEY,
         year INT,
         month INT,
         day INT,
         hour INT,
         minute INT,
         day_of_week INT,
         is_weekend BOOLEAN
     );

**Dimensión Precio (SCD Type 2)**

Mantiene historial completo de cambios de precio:

SQL:

     CREATE TABLE dim_price (
         price_value DOUBLE PRECISION,
         effective_date TIMESTAMP(3),
         expiry_date TIMESTAMP(3),
         is_current BOOLEAN
     );

**Tabla de Hechos**

SQL:

     CREATE TABLE fact_grid_metrics (
         fact_id SERIAL PRIMARY KEY,
         avg_power_kw DOUBLE PRECISION,
         total_solar_generated DOUBLE PRECISION,
         max_fault_num INTEGER,
         event_count BIGINT,
         aggregated_timestamp TIMESTAMP(3) UNIQUE,
         time_id INTEGER REFERENCES dim_time(time_id),
         price_id INTEGER REFERENCES dim_price(price_id)
     );

____________________________________________________________________________________________________________________________________________________________________________________________________________________________
## 📊 Visualización con Superset
____________________________________________________________________________________________________________________________________________________________________________________________________________________________
**KPIs de Alto Impacto**

| KPI | Métrica | Visualización |
|-----|---------|---------------|
| ⚡ Potencia Promedio | AVG(avg_power_kw) | Big Number con tendencia |
| ☀️ Energía Solar | SUM(total_solar_generated) | Big Number |
| 🔴 Pico de Fallas | MAX(max_fault_num) | Big Number con alerta |
| 🎯 Eficiencia | Índice compuesto | Gauge Chart |

**Dashboards Interactivos**

El dashboard incluye :

* Filtros por rango de tiempo

* Análisis de correlaciones (demanda vs temperatura)

* Mapa de calor de fallas por hora/día

* Tablas detalladas con datos históricos

____________________________________________________________________________________________________________________________________________________________________________________________________________________________
## 🚀 Cómo Ejecutar
____________________________________________________________________________________________________________________________________________________________________________________________________________________________

**Requisitos Previos**

bash:

      # Docker y Docker Compose
      docker --version
      docker-compose --version

      # Puerto 8088 para Superset
      # Puerto 5432 para PostgreSQL
      # Puerto 9092 para Kafka

      
**Levantar la Infraestructura**

Bash:

      # 1. Clonar el repositorio
      git clone https://github.com/tuusuario/Smart_Grid_V2.git
      cd Smart_Grid_V2

      # 2. Iniciar todos los servicios
      docker-compose up -d

      # 3. Verificar que todo está corriendo
      docker ps

      # 4. Ejecutar el pipeline Gold
      docker exec -it smart_grid_flink_jobmanager bash -c \
        "cd /opt/flink && ./bin/sql-client.sh \
        -f /opt/flink/usrlib-sql/03_gold_simple.sql \
        -j /opt/flink/lib/extra/flink-sql-connector-kafka-1.17.1.jar \
        -j /opt/flink/lib/extra/flink-connector-jdbc-1.16.0.jar \
        -j /opt/flink/lib/extra/postgresql-42.7.1.jar"


**Enviar Datos de Prueba**

bash:

      docker exec -it smart_grid_kafka bash -c \
        "cd /opt/kafka/bin && ./kafka-console-producer.sh \
        --bootstrap-server localhost:9092 --topic smartgrid"

Pega estos datos:

json:

      {"timestamp":1700000000,"power_kw":10.5,"solar_kw":5.2,"fault_num":0,"temperature_c":25.5,"electricity_price_gbp_per_kwh":0.15}
      {"timestamp":1700000060,"power_kw":12.3,"solar_kw":6.1,"fault_num":1,"temperature_c":26.0,"electricity_price_gbp_per_kwh":0.16}
      {"timestamp":1700000120,"power_kw":11.8,"solar_kw":5.8,"fault_num":0,"temperature_c":24.5,"electricity_price_gbp_per_kwh":0.15}

____________________________________________________________________________________________________________________________________________________________________________________________________________________________
**Ver Dashboard en Superset**

1. Abre http://localhost:8088

2. Usuario: admin / Contraseña: admin

3. Conecta a la base de datos PostgreSQL.

4. Crea los datasets desde fact_grid_metrics.

5. Construye los dashboards.

____________________________________________________________________________________________________________________________________________________________________________________________________________________________
## 🛠️ Tecnologías Utilizadas
____________________________________________________________________________________________________________________________________________________________________________________________________________________________

| Capa | Tecnología | Versión | Propósito |
|------|------------|---------|-----------|
| Ingesta | Apache Kafka | 3.5.0 | Buffer de mensajería y streaming |
| Procesamiento | Apache Flink | 1.17.1 | Procesamiento de streams y ETL |
| Almacenamiento | PostgreSQL | 15 |  Data warehouse y tablas dimensionales |
| Visualización | Apache Superset | 2.1.0 | Dashboards y análisis interactivo |
| Orquestación | Docker Compose | - | Contenedores y servicios |
| Lenguaje | Python | 3.11 | Simulador de sensores |
| Lenguaje | SQL | - | Procesamiento Flink y consultas |


![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()
