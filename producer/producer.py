import json
import time
import random
from pathlib import Path
from datetime import datetime, timezone
from kafka import KafkaProducer

# CONFIGURACIÓN GENERAL

# Cantidad de vehículos a simular.
# Para prueba se usan 3 vehículos:
# EV-ACME-00001, EV-ACME-00002 y EV-ACME-00003.
TOTAL_CARROS = 3

# Intervalos de envío en segundos.
# GPS se genera cada 30 segundos.
# ESTADO se genera cada 60 segundos.
INTERVALO_GPS = 30
INTERVALO_ESTADO = 60

# Carpetas dentro del contenedor Docker.
# Estas rutas se conectan con la carpeta /data de tu proyecto usando volumes.
CARPETA_BASE = Path("/app/data")
CARPETA_GPS = CARPETA_BASE / "GPS"
CARPETA_ESTADO = CARPETA_BASE / "ESTADO"

# Crear carpetas si no existen.
CARPETA_GPS.mkdir(parents=True, exist_ok=True)
CARPETA_ESTADO.mkdir(parents=True, exist_ok=True)

# Crear lista de vehículos simulados.
vehiculos = [
    f"EV-ACME-{i:05d}"
    for i in range(1,TOTAL_CARROS+1)
]

# CONEXIÓN A KAFKA

# Se crea el productor de Kafka.
# "kafka:9092" se usa porque Kafka está en otro servicio del docker-compose.
producer_kafka = KafkaProducer(
    bootstrap_servers="kafka:9092",
    value_serializer=lambda data: json.dumps(data).encode("utf-8")
)

print("Conectado a Kafka")


# FUNCIONES DE TIEMPO

def timestamp_actual():
    """
    Devuelve la fecha y hora actual en formato ISO 8601.
    Este valor va dentro del JSON.
    """
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def timestamp_archivo():
    """
    Devuelve la fecha y hora actual en formato seguro para nombres de archivo.
    Este valor evita que los archivos se sobrescriban.
    """
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


# FUNCIONES PARA CREAR JSON

def crear_gps(id_vehiculo):
    """
    Crea un documento JSON de tipo GPS.
    Simula la ubicación del vehículo.
    """
    return {
        "id_vehiculo": id_vehiculo,
        "timestamp": timestamp_actual(),
        "tipo_trama": "GPS",
        "telemetria": {
            "latitud": round(random.uniform(14.60, 14.70), 6),
            "longitud": round(random.uniform(-90.55, -90.45), 6)
        }
    }


def crear_estado(id_vehiculo):
    """
    Crea un documento JSON de tipo ESTADO.
    Simula información operativa del vehículo.
    """
    return {
        "id_vehiculo": id_vehiculo,
        "timestamp": timestamp_actual(),
        "tipo_trama": "ESTADO",
        "telemetria": {
            "estado_bateria_porcentaje": random.randint(1, 100),
            "encendido": random.choice([True, False]),
            "codigo_problema": random.choice(["000", "101", "205", "310"]),
            "kilometraje": round(random.uniform(1000, 50000), 1)
        }
    }


# FUNCIÓN PARA GUARDAR ARCHIVOS

def guardar_json(ruta, data):
    """
    Guarda el JSON generado en un archivo físico.
    """
    with open(ruta, "w", encoding="utf-8") as archivo:
        json.dump(data, archivo, indent=4, ensure_ascii=False)

# BUCLE PRINCIPAL DEL PRODUCTOR

# Cada ciclo dura 30 segundos.
# Como ESTADO debe salir cada 60 segundos, se genera cada 2 ciclos.
contador_ciclos = 0

while True:
    contador_ciclos += 1

    # Se genera una marca de tiempo única para los nombres de archivos.
    marca_tiempo = timestamp_archivo()

    print(f"\n===== CICLO {contador_ciclos} =====")

    # GENERAR Y ENVIAR GPS
    print("Generando GPS...")

    for vehiculo in vehiculos:
        gps = crear_gps(vehiculo)

        # Guardar archivo JSON nuevo por cada vehículo.
        ruta_gps = CARPETA_GPS / f"{vehiculo}_GPS_{marca_tiempo}.json"
        guardar_json(ruta_gps, gps)

        # Enviar el mismo JSON al topic gps de Kafka.
        producer_kafka.send("gps", gps)

    print(f"GPS guardado y enviado para {len(vehiculos)} vehículos")

    # GENERAR Y ENVIAR ESTADO
    # En el ciclo 1, 3, 5, 7... también se genera ESTADO.
    # Eso equivale a cada 60 segundos, porque cada ciclo dura 30 segundos.
    if contador_ciclos % 2 == 1:
        print("Generando ESTADO...")

        for vehiculo in vehiculos:
            estado = crear_estado(vehiculo)

            # Guardar archivo JSON nuevo por cada vehículo.
            ruta_estado = CARPETA_ESTADO / f"{vehiculo}_ESTADO_{marca_tiempo}.json"
            guardar_json(ruta_estado, estado)

            # Enviar el mismo JSON al topic estado de Kafka.
            producer_kafka.send("estado", estado)

        print(f"ESTADO guardado y enviado para {len(vehiculos)} vehículos")
    else:
        print("En este ciclo no corresponde generar ESTADO")

    # Asegura que los mensajes pendientes se manden a Kafka.
    producer_kafka.flush()

    # Espera 30 segundos antes del siguiente ciclo.
    time.sleep(INTERVALO_GPS)