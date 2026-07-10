# Smart-Grid-Analytics
________________________________________________________________________________________________________________________________________________________________________________________________________________

## ⚡Plataforma de Análisis de Datos Energéticos en Tiempo Real

![image](https://img.shields.io/badge/Apache%2520Flink-1.17.1-blue)
![image](https://img.shields.io/badge/Apache%2520Kafka-3.5.0-black)
![image](https://img.shields.io/badge/PostgreSQL-15-blue)
![image](https://img.shields.io/badge/Apache%2520Superset-2.1.0-orange)

Una solución end-to-end de procesamiento de datos para smart grids, diseñada para demostrar habilidades en arquitectura de datos moderna.

![image](https://github.com/user-attachments/assets/02dac6d2-ed17-40bc-b150-66808032a8c8)

_______________________________________________________________________________________________________________________________________________________________________________________________________________
![image](https://github.com/user-attachments/assets/85d4c55d-4453-4561-ac24-c19175008faf)

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

____________________________________________________________________________________________________________________________________________________________________________________________________________________________
## 📁 Estructura del Proyecto
____________________________________________________________________________________________________________________________________________________________________________________________________________________________

![image](https://github.com/user-attachments/assets/6899e215-ff6d-47db-b4f9-5444cb96a8e3)
____________________________________________________________________________________________________________________________________________________________________________________________________________________________
## 🎯 Lo que este Proyecto Demuestra
____________________________________________________________________________________________________________________________________________________________________________________________________________________________

🧠 **Data Engineering**
__________________________________________________________________  
✅ Arquitectura de datos moderna con pipeline end-to-end 

✅ Procesamiento de streams en tiempo real con Flink

✅ Modelo de datos dimensional (Star Schema)

✅ SCD Type 2 para gestión de cambios históricos

✅ Visualización de alto impacto con KPIs y dashboards

✅ Infraestructura como código con Docker Compose

✅ Simulación de sensores realista a escala 
________________________________________________________________
📈 **Data Science**
________________________________________________________________  
✅ Datos estructurados listos para modelado

✅ Métricas de eficiencia y patrones de consumo

✅ Correlaciones temporales (demanda vs temperatura)

✅ Historial de precios para análisis predictivo

____________________________________________________________________________________________________________________________________________________________________________________________________________________________
## 🤝 Contribuciones
____________________________________________________________________________________________________________________________________________________________________________________________________________________________

**¡Las contribuciones son bienvenidas!** Áreas de mejora:

🔄 Más métricas en tiempo real

🧠 Modelos ML para predicción de demanda

🌐 Escalabilidad con Kubernetes

📈 Alertas y notificaciones

🔍 Data quality con Great Expectations

____________________________________________________________________________________________________________________________________________________________________________________________________________________________
## 📬 Contacto
____________________________________________________________________________________________________________________________________________________________________________________________________________________________
🎓 Autor: <MARK>**ALLAN GONZALES HEREDIA**</MARK>

📧 Correo: Allgoher007@gmail.com

📌 LinkedIn: https://www.linkedin.com/in/allan-gonzales-heredia-13a557b5/

____________________________________________________________________________________________________________________________________________________________________________________________________________________________

### **¡Gracias por visitar el proyecto! Si te gusta, dale ⭐ y compártelo. 🚀**
____________________________________________________________________________________________________________________________________________________________________________________________________________________________

## 🔥 DESARROLLO DEL PROYECTO 🔥
____________________________________________________________________________________________________________________________________________________________________________________________________________________________

1. Crear el proyecto y descargar JARs

  bash:

        # Crear estructura de carpetas
        mkdir -p Smart_Grid_V2/{producer,flink-sql,postgres,flink-jars}
        cd Smart_Grid_V2

  **Descargar JARs**

  bash:

        cd flink-jars
        curl -L -o flink-sql-connector-kafka-1.17.1.jar https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/1.17.1/flink-sql-connector-kafka-1.17.1.jar
        curl -L -o flink-connector-jdbc-1.16.0.jar https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc/1.16.0/flink-connector-jdbc-1.16.0.jar
        curl -L -o postgresql-42.7.1.jar https://jdbc.postgresql.org/download/postgresql-42.7.1.jar
        cd ..

  ![image](https://github.com/user-attachments/assets/38dbfc4c-cdb9-4717-86f4-b3f51a308c34)

____________________________________________________________________________________________________________________________________________________________________________________________________________________________
2. Instalar dependencias del productor

  bash:

        python -m pip install kafka-python six

  ![image](https://github.com/user-attachments/assets/76d4051c-56fe-4a34-8598-15369b465e7e)

___________________________________________________________________________________________________________________________________________________________________________________________________________________________
3. Levantar la infraestructura

    bash:

         docker-compose up -d

  ![image](https://github.com/user-attachments/assets/19339169-7576-44b9-97f6-b21ed44ab455)

  ![image](https://github.com/user-attachments/assets/36f205f8-abd1-438f-ac91-4d3f8f21a4e8)

____________________________________________________________________________________________________________________________________________________________________________________________________________________________
4. Crear el topic en Kafka
   
    bash:

          docker exec smart_grid_kafka /opt/kafka/bin/kafka-topics.sh --create --topic smartgrid --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1


    ![image](https://github.com/user-attachments/assets/a0df7582-fd8d-41e0-9868-9e2eaf394291)

____________________________________________________________________________________________________________________________________________________________________________________________________________________________
5. Ejecutar el productor (Terminal 1)
   
    bash:

          cd producer
          python smart_grid_producer.py


  ![image](https://github.com/user-attachments/assets/6bd46c42-61cd-4811-b54f-2109fa0eea77)

  ![image](https://github.com/user-attachments/assets/b8e558b7-caf5-40b6-9d92-6960d2b69b3a)

____________________________________________________________________________________________________________________________________________________________________________________________________________________________
### ![image](https://github.com/user-attachments/assets/2a4744d9-3b6c-4280-b1a9-c8bed7b89eda) 6. Ejecutar Flink SQL - Capa Bronze (Terminal 2)
____________________________________________________________________________________________________________________________________________________________________________________________________________________________   
   bash:

         docker exec -it smart_grid_flink_jobmanager bash -c "cd /opt/flink && ./bin/sql-client.sh -f /opt/flink/usrlib-sql/01_bronze_table.sql -j /opt/flink/lib/extra/flink-sql-connector-kafka-1.17.1.jar -j      /opt/flink/lib/extra/flink-connector-jdbc-1.16.0.jar -j /opt/flink/lib/extra/postgresql-42.7.1.jar"


  ![image](https://github.com/user-attachments/assets/3a5245e9-6a30-4654-addd-fb7078c6c96f)

  ![image](https://github.com/user-attachments/assets/3fe66de0-ebb3-42ea-81fe-22c23189fe5e)

____________________________________________________________________________________________________________________________________________________________________________________________________________________________
### ![image](https://github.com/user-attachments/assets/dc7a088c-7326-4080-bb4f-ae0e5fbd663c) 7. Ejecutar Flink SQL - Capa Silver (Terminal 3)
____________________________________________________________________________________________________________________________________________________________________________________________________________________________
   
bash:

      docker exec -it smart_grid_flink_jobmanager bash -c "cd /opt/flink && ./bin/sql-client.sh -f /opt/flink/usrlib-sql/02_silver_table.sql -j /opt/flink/lib/extra/flink-sql-connector-kafka-1.17.1.jar -j /opt/flink/lib/extra/flink-connector-jdbc-1.16.0.jar -j /opt/flink/lib/extra/postgresql-42.7.1.jar"


![image](https://github.com/user-attachments/assets/aa1b20e6-6e87-416b-9482-b1cd78eaee6a)

![image](https://github.com/user-attachments/assets/3a601b57-fa33-4415-b98f-2f658771e11c)

____________________________________________________________________________________________________________________________________________________________________________________________________________________________
### ![image](https://github.com/user-attachments/assets/0ce56bdd-6038-4c22-b6ae-6e714adb6c30) 8. Ejecutar Flink SQL - Capa Gold (Terminal 4)
____________________________________________________________________________________________________________________________________________________________________________________________________________________________

bash:

      docker exec -it smart_grid_flink_jobmanager bash -c "cd /opt/flink && ./bin/sql-client.sh -f /opt/flink/usrlib-sql/03_gold_table.sql -j /opt/flink/lib/extra/flink-sql-connector-kafka-1.17.1.jar -j /opt/flink/lib/extra/flink-connector-jdbc-1.16.0.jar -j /opt/flink/lib/extra/postgresql-42.7.1.jar"


![image](https://github.com/user-attachments/assets/42561884-70df-46f2-abac-c15a999fb6ab)

![image](https://github.com/user-attachments/assets/269b3504-c0a1-43a3-9450-ff64a4ee7743)

____________________________________________________________________________________________________________________________________________________________________________________________________________________________
**📊 PASO 9: Verificación de Datos**
____________________________________________________________________________________________________________________________________________________________________________________________________________________________

bash:

      docker exec -it smart_grid_postgres psql -U admin -d smartgrid


sql:

     -- Ver todas las tablas
      \dt

![image](https://github.com/user-attachments/assets/905f6f6a-735b-41be-a582-94aa0599fb23)

#### Capa Bronze

sql:

     SELECT COUNT(*) FROM bronze_raw_data;


![image](https://github.com/user-attachments/assets/fca39cae-850f-4943-bdea-6883bbe204a5)

#### Capa Silver

sql:

     SELECT COUNT(*) FROM silver_validated_data;


![image](https://github.com/user-attachments/assets/95d1d7d4-eae3-473c-b44d-d412881b9863)

#### Capa Gold (Fact Table)

sql:

     SELECT * FROM fact_grid_metrics ORDER BY aggregated_timestamp DESC LIMIT 5;

#### SCD Type 2 (Precios)

sql:

     SELECT * FROM dim_price WHERE is_current = TRUE;


![image](https://github.com/user-attachments/assets/06b3a976-173c-4b97-97db-8f80b07450da)

#### Dimensión Tiempo

sql:

     SELECT * FROM dim_time LIMIT 5;

#### Verificación en APACHE FLINK Web Dashboard.

![image](https://github.com/user-attachments/assets/f2d3b804-1d6b-4855-a903-05940b8606a8)

____________________________________________________________________________________________________________________________________________________________________________________________________________________________
## ![image](https://github.com/user-attachments/assets/215906b0-5f49-48b8-8101-585fc2967a44) Creación de Dashboard SUPERSET
____________________________________________________________________________________________________________________________________________________________________________________________________________________________
 Hacemos click en el puerto 8088 de docker desktop ó abrimos el navegador y escribimos localhost:8088, se abrira la vetana de superset y ahí, vamos al simbolo + en la esquina superior izquierda, hacemos click y, seleccionamos **database connections*
 
![image](https://github.com/user-attachments/assets/be1fbe81-c9ab-4812-b4ac-9ffcdfb81abd)

Luego en el mismo símbolo más de la esquina derecha has click en cocnnect a database, Y aparecerá una ventana emergente.

![image](https://github.com/user-attachments/assets/b81ea9ac-d868-4562-9160-dcf53abe37d7)

y tendrán que pasar los siguientes datos:

✅ Configuración CORRECTA para Conectar Superset a PostgreSQL

Llena los campos así:

| Campo | Valor CORRECTO |
|-------|----------------|
| Host | postgres |
| Port | 5432 |
| Database name |	smartgrid |
| Username | admin |
| Password | admin |
| Display Name | Smart Grid PostgreSQL |
| SSL | Desactivado/Disabled |


🔍 Explicación de cada campo

| Campo | Valor | Por qué |
|-------|-------|---------|
| Host | postgres | Es el nombre del servicio en Docker, NO localhost |
| Port | 5432 | Es el puerto por defecto de PostgreSQL (NO 54325432) |
| Database name |	smartgrid | Es la base de datos con tus datos (NO "Smart Grid PostgreSQL") |
| Username | admin | El usuario que creaste en docker-compose.yml |
| Password | admin | La contraseña del usuario admin |
| Display Name | Smart Grid PostgreSQL | El nombre que verás en Superset (puede ser cualquier cosa) |

Luego hacemos click en CREATE DATASET y, llenamos los datos correspondientes para crear el panel de control con indicadores claves de desempeño.

![image](https://github.com/user-attachments/assets/c90cddf6-082e-4cb5-89e7-6435a0a4214e)


