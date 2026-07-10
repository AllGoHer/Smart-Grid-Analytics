"""
Smart Grid Data Producer - Versión Senior con monitoreo integrado
Genera datos simulados de una red eléctrica inteligente en tiempo real
"""

import random
import json
import time
import math
import logging
import codecs
import sys
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError as NoBrokersAvailable

# ============================================================
# CONFIGURACIÓN DE LOGGING
# ============================================================
if sys.platform == 'win32':
    sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
    sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, 'strict')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('smart_grid_producer.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

# ============================================================
# CONFIGURACIÓN DEL SISTEMA
# ============================================================
NOMINAL_VOLTAGE = 230      # Volts
MAX_CURRENT = 500          # Amperes
MAX_SOLAR = 300            # kW
MAX_WIND = 200             # kW
EVENT_INTERVAL = 5         # Segundos entre eventos

KAFKA_BOOTSTRAP = "localhost:29092"
TOPIC = "smartgrid"

# Variables globales para métricas
event_counter = 0
error_counter = 0
error_rate = 0.0
producer = None

# ============================================================
# FUNCIONES DE GENERACIÓN DE DATOS
# ============================================================

def compute_fault(current, power_kw, voltage_fluct, prev_power_kw):
    """
    Determina el tipo de falla basado en reglas:
    0 → normal
    1 → voltage drop
    2 → overload
    3 → blackout risk
    """
    faults = ['normal', 'unstable', 'bad', 'blackout']
    
    if prev_power_kw is None:
        if current > MAX_CURRENT:
            return 2, faults[2]
        return 0, faults[0]
    
    power_spike = (abs(power_kw - prev_power_kw) / max(prev_power_kw, 1)) * 100
    fault_num = 0
    
    # Voltage drop
    if -10 <= voltage_fluct < -5:
        fault_num = 1
    # Current overload
    if current > MAX_CURRENT:
        fault_num = 2
    # Blackout / extreme power imbalance
    if power_spike > 50 or voltage_fluct < -10:
        fault_num = 3
    
    return fault_num, faults[fault_num]

def generate_voltage():
    """Genera voltaje con fluctuaciones realistas"""
    voltage = random.gauss(NOMINAL_VOLTAGE, 0.04 * NOMINAL_VOLTAGE)
    
    # Rare sag or surge
    if random.random() < 0.05:
        voltage *= random.uniform(0.75, 0.9)   # sag
    elif random.random() < 0.02:
        voltage *= random.uniform(1.1, 1.2)    # surge
    
    return round(voltage, 2)

def solar_output(prev_solar):
    """Simula la generación solar basada en la hora del día"""
    hour = time.localtime().tm_hour
    daylight = max(0, math.cos((hour - 12) * math.pi / 12))
    cloud = random.uniform(0.5, 1.0)
    base = MAX_SOLAR * daylight * cloud
    solar_kw = 0.6 * prev_solar + 0.4 * base
    return round(max(0, min(solar_kw, MAX_SOLAR)), 2)

def wind_output(prev_wind):
    """Simula la generación eólica con ráfagas"""
    mean = 0.6 * MAX_WIND
    volatility = 0.2 * MAX_WIND
    reversion = 0.1
    gust = random.gauss(0, volatility)
    wind_kw = prev_wind + reversion * (mean - prev_wind) + gust
    return round(max(0, min(wind_kw, MAX_WIND)), 2)

def temperature_output(prev_temp):
    """Simula temperatura con ciclo diario"""
    hour = time.localtime().tm_hour
    daily_cycle = math.sin(math.pi * (hour - 5) / 12)
    base_temp = 24 + 9 * daily_cycle
    drift = random.uniform(-0.5, 0.5)
    temp = 0.85 * prev_temp + 0.15 * (base_temp + drift)
    return round(temp, 1)

def humidity_output(prev_humidity, temp_c):
    """Simula humedad relativa"""
    base_humidity = 90 - (temp_c - 18) * 2.0
    noise = random.uniform(-4, 4)
    humidity = 0.8 * prev_humidity + 0.2 * (base_humidity + noise)
    return round(max(25, min(humidity, 98)), 1)

def price_output(prev_price, power_kw, solar_kw, wind_kw):
    """Simula precio de electricidad dinámico"""
    base_price = 0.11
    demand_factor = power_kw / 400
    renewable_factor = (solar_kw + wind_kw) / 250
    hour = time.localtime().tm_hour
    peak_factor = 0.04 if 17 <= hour <= 22 else 0
    market_noise = random.uniform(-0.015, 0.02)
    
    target_price = (
        base_price
        + 0.08 * demand_factor
        - 0.06 * renewable_factor
        + peak_factor
        + market_noise
    )
    
    price = 0.85 * prev_price + 0.15 * target_price
    return round(max(0.06, min(price, 0.35)), 3)

# ============================================================
# CONEXIÓN A KAFKA CON REINTENTOS
# ============================================================
def create_producer():
    """Crea el productor de Kafka con reintentos y monitoreo"""
    global producer
    
    max_retries = 5
    retry_delay = 3
    
    for attempt in range(max_retries):
        try:
            logger.info(f"Intentando conectar a Kafka (intento {attempt + 1}/{max_retries})...")
            
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda x: json.dumps(x).encode('utf-8'),
                max_block_ms=10000,
                request_timeout_ms=5000,
                retry_backoff_ms=500,
                reconnect_backoff_ms=1000
            )
            
            # Probar conexión
            producer.partitions_for(TOPIC)
            logger.info("✅ Conectado a Kafka exitosamente")
            return producer
            
        except NoBrokersAvailable:
            logger.warning(f"No se pudo conectar a Kafka en {KAFKA_BOOTSTRAP}. Reintentando en {retry_delay}s...")
            time.sleep(retry_delay)
            
        except Exception as e:
            logger.error(f"Error conectando a Kafka: {e}")
            time.sleep(retry_delay)
    
    logger.error("❌ No se pudo conectar a Kafka después de varios intentos")
    sys.exit(1)

# ============================================================
# INICIALIZACIÓN DE ESTADO (Variables de estado)
# ============================================================
prev_power_kw = None
prev_solar_kw = 0.0
prev_wind_kw = MAX_WIND * 0.5
prev_temp = 24.0
prev_humidity = 60.0
prev_price = 0.12

# ============================================================
# FUNCIÓN PRINCIPAL DE GENERACIÓN DE DATOS
# ============================================================
def generate_data():
    """Genera un evento de la red eléctrica y lo envía a Kafka"""
    global prev_power_kw, prev_solar_kw, prev_wind_kw, prev_temp, prev_humidity, prev_price
    
    # Generar datos
    voltage = generate_voltage()
    current = round(random.uniform(0.5*MAX_CURRENT, 1.1*MAX_CURRENT), 2)
    pf = round(random.uniform(0.8, 1.0), 2)
    power_kw = round(voltage * current * pf / 1000, 2)
    
    s = voltage * current / 1000
    reactive_power = round((s**2 - power_kw**2)**0.5, 2) if s > power_kw else 0.0
    
    solar_kw = solar_output(prev_solar_kw)
    wind_kw = wind_output(prev_wind_kw)
    
    grid_in = round(max(power_kw - (solar_kw + wind_kw), 0), 2)
    grid_out = round(max((solar_kw + wind_kw) - power_kw, 0), 2)
    
    voltage_fluct = round((voltage - NOMINAL_VOLTAGE) / NOMINAL_VOLTAGE * 100, 2)
    fault_num, fault = compute_fault(current, power_kw, voltage_fluct, prev_power_kw)
    
    temp_c = temperature_output(prev_temp)
    humidity = humidity_output(prev_humidity, temp_c)
    price = price_output(prev_price, power_kw, solar_kw, wind_kw)
    
    # Actualizar estado
    prev_power_kw = power_kw
    prev_solar_kw = solar_kw
    prev_wind_kw = wind_kw
    prev_temp = temp_c
    prev_humidity = humidity
    prev_price = price
    
    # Construir mensaje
    data = {
        "timestamp": time.time(),
        "voltage_v": voltage,
        "current_a": current,
        "power_kw": power_kw,
        "reactive_power_kvar": reactive_power,
        "power_factor": pf,
        "solar_kw": solar_kw,
        "wind_kw": wind_kw,
        "grid_in_kw": grid_in,
        "grid_out_kw": grid_out,
        "voltage_fluct_%": voltage_fluct,
        "fault_indicator": fault,
        "fault_num": fault_num,
        "temperature_c": temp_c,
        "humidity_%": humidity,
        "electricity_price_gbp_per_kwh": price
    }
    
    # Enviar a Kafka
    try:
        producer.send(TOPIC, value=data)
        producer.flush()
        logger.info(
            f"✅ Datos enviados: "
            f"Power={power_kw:.2f}kW, "
            f"Solar={solar_kw:.2f}kW, "
            f"Wind={wind_kw:.2f}kW, "
            f"Fault={fault} ({fault_num}), "
            f"Price=£{price:.3f}"
        )
        return data
        
    except Exception as e:
        logger.error(f"❌ Error enviando a Kafka: {e}")
        return None

# ============================================================
# FUNCIÓN DE MÉTRICAS DE MONITOREO (ÚNICA DEFINICIÓN)
# ============================================================
def print_metrics():
    """Imprime métricas de monitoreo"""
    global event_counter, error_rate, producer
    
    logger.info("=" * 60)
    logger.info("📊 MÉTRICAS DEL SISTEMA")
    logger.info(f"📈 Total de eventos enviados: {event_counter}")
    logger.info(f"📉 Tasa de error: {error_rate:.2f}%")
    logger.info(f"📊 Estado del productor: {'✅ Activo' if producer else '❌ Inactivo'}")
    logger.info("=" * 60)

# ============================================================
# EJECUCIÓN PRINCIPAL
# ============================================================
if __name__ == "__main__":
    # Inicializar contadores
    event_counter = 0
    error_counter = 0
    error_rate = 0.0
    
    logger.info("=" * 60)
    logger.info("🚀 INICIANDO PRODUCTOR DE SMART GRID - VERSIÓN SENIOR")
    logger.info(f"📤 Kafka Broker: {KAFKA_BOOTSTRAP}")
    logger.info(f"📡 Topic: {TOPIC}")
    logger.info(f"⏱️  Intervalo de eventos: {EVENT_INTERVAL}s")
    logger.info("=" * 60)
    
    # Crear productor
    producer = create_producer()
    
    try:
        while True:
            # Generar y enviar datos
            result = generate_data()
            event_counter += 1
            
            if result is None:
                error_counter += 1
                error_rate = (error_counter / event_counter) * 100
            
            # Mostrar métricas cada 10 eventos
            if event_counter % 10 == 0:
                print_metrics()
            
            # Esperar antes del siguiente evento
            time.sleep(EVENT_INTERVAL)
            
    except KeyboardInterrupt:
        logger.info("\n👋 Productor detenido por el usuario")
        print_metrics()
        
    except Exception as e:
        logger.error(f"❌ Error inesperado: {e}")
        print_metrics()
        
    finally:
        # Cerrar el productor correctamente
        if producer:
            producer.close()
            logger.info("✅ Productor cerrado correctamente")