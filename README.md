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

SQL de la capa Gold :
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
