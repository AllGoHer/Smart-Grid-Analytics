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

Smart Grid V2 es una plataforma completa de análisis de datos que simula el procesamiento de datos de una red eléctrica inteligente en tiempo real. El proyecto implementa una arquitectura de datos moderna que cubre todo el ciclo de vida de los datos: ingesta, procesamiento, almacenamiento y visualización. 

________________________________________________________________________________________________________________________________________________________________________________________________________________
## 🏗️ Arquitectura
________________________________________________________________________________________________________________________________________________________________________________________________________________
![image](https://github.com/user-attachments/assets/1f472e1a-c5c3-43e6-9259-a87133168394)
![image](https://github.com/user-attachments/assets/9e4fecc6-4b43-43a1-a23e-abec0ef9ad2c)











┌────────────────────────────────────────────────────────────────────────────┐
│                           **DATA SOURCE LAYER**                            │
│  ┌──────────────────────────────────────────────────────────────────────┐  │
│  │                    Simulador de Sensores (Python)                    │  │
│  │  Genera datos mock de red eléctrica: potencia, solar, fallas, temp   │  │
│  └──────────────────────────────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌────────────────────────────────────────────────────────────────────────────┐
│                           **INGESTION LAYER**                              │
│  ┌──────────────────────────────────────────────────────────────────────┐  │
│  │                        Apache Kafka                                  │  │
│  │  • Topic: smartgrid                                                  │  │
│  │  • Broker: kafka:9092                                                │  │
│  │  • Formato: JSON                                                     │  │
│  │  • Rol: Buffer de mensajería y desacoplamiento                       │  │
│  └──────────────────────────────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌────────────────────────────────────────────────────────────────────────────┐
│                         **PROCESSING LAYER**                               │
│  ┌──────────────────────────────────────────────────────────────────────┐  │
│  │                    Apache Flink (SQL Client)                         │  │
│  │                                                                      │  │
│  │       ┌─────────────┐    ┌─────────────┐    ┌─────────────┐          │  │
│  │       │   BRONZE    │  → │   SILVER    │  → │    GOLD     │          │  │
│  │       │ (Raw Data)  │    │ (Validated) │    │(Star Schema)│          │  │
│  │       └─────────────┘    └─────────────┘    └─────────────┘          │  │
│  │                                                                      │  │
│  │  • Ventanas Tumbling de 1 minuto                                     │  │
│  │  • Agregaciones: AVG, SUM, MAX, COUNT                                │  │
│  │  • SCD Type 2 para dimensiones                                       │  │
│  └──────────────────────────────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌────────────────────────────────────────────────────────────────────────────┐
│                         **STORAGE LAYER**                                  │
│  ┌──────────────────────────────────────────────────────────────────────┐  │
│  │                       PostgreSQL 15                                  │  │
│  │                                                                      │  │
│  │  • dim_time (SCD Type 0)                                             │  │
│  │  • dim_price (SCD Type 2)                                            │  │
│  │  • fact_grid_metrics (Hechos agregados por minuto)                   │  │
│  └──────────────────────────────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌────────────────────────────────────────────────────────────────────────────┐
│                        **VISUALIZATION LAYER**                             │
│  ┌──────────────────────────────────────────────────────────────────────┐  │
│  │                     Apache Superset                                  │  │
│  │                                                                      │  │
│  │  • KPIs en tiempo real                                               │  │
│  │  • Dashboards interactivos                                           │  │
│  │  • Análisis de tendencias                                            │  │
│  │  • Alertas y monitoreo                                               │  │
│  └──────────────────────────────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────────────────────────────┘

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
