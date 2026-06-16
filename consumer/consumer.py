import json
import os
import time
from datetime import datetime, timezone

from pymongo import MongoClient
from pyspark.sql import SparkSession


# Configuracion del procesamiento
MODO_BATCH = True  # False = tiempo real, True = batch
BATCH_SIZE = 10  # Numero maximo de mensajes por micro-batch de Spark

RETENCION_GPS_DIAS = 30
RETENCION_ESTADO_DIAS = 365

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPICS = "gps,estado"
MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongodb:27017/")


def conectar_mongo():
    while True:
        try:
            client = MongoClient(MONGO_URI)
            client.admin.command("ping")
            return client
        except Exception as e:
            print(f"Esperando MongoDB en {MONGO_URI}: {e}")
            time.sleep(5)


# Conexion a MongoDB
mongo_client = conectar_mongo()
mongo_db = mongo_client["acme_ev"]

gps_raw = mongo_db["gps_raw"]
estado_raw = mongo_db["estado_raw"]
metricas_rendimiento = mongo_db["metricas_rendimiento"]

print("Conectado a MongoDB")

# Indices TTL para vigencia en MongoDB
gps_raw.create_index(
    "fecha_evento",
    expireAfterSeconds=RETENCION_GPS_DIAS * 24 * 60 * 60,
)

estado_raw.create_index(
    "fecha_evento",
    expireAfterSeconds=RETENCION_ESTADO_DIAS * 24 * 60 * 60,
)

print("Indices TTL creados en MongoDB")


# Preparar documento para MongoDB
def preparar_documento_mongo(data):
    documento = data.copy()

    fecha_evento = datetime.fromisoformat(
        data["timestamp"].replace("Z", "+00:00")
    )

    documento["fecha_evento"] = fecha_evento

    return documento


# Guardar metrica de rendimiento
def guardar_metrica(modo, tipo, registros, duracion):
    registros_por_segundo = registros / duracion if duracion > 0 else 0

    metricas_rendimiento.insert_one(
        {
            "fecha": datetime.now(timezone.utc),
            "modo": modo,
            "tipo": tipo,
            "registros": registros,
            "duracion_segundos": duracion,
            "registros_por_segundo": registros_por_segundo,
        }
    )


# Guardar GPS en tiempo real
def guardar_gps_tiempo_real(data):
    inicio = time.time()

    gps_raw.insert_one(preparar_documento_mongo(data))

    fin = time.time()
    duracion = fin - inicio

    guardar_metrica("Tiempo Real", "GPS", 1, duracion)

    id_vehiculo = data["id_vehiculo"]

    print(f"GPS MongoDB tiempo real: {id_vehiculo} | {duracion:.4f} segundos")


# Guardar ESTADO en tiempo real
def guardar_estado_tiempo_real(data):
    inicio = time.time()

    estado_raw.insert_one(preparar_documento_mongo(data))

    fin = time.time()
    duracion = fin - inicio

    guardar_metrica("Tiempo Real", "ESTADO", 1, duracion)

    id_vehiculo = data["id_vehiculo"]

    print(f"ESTADO MongoDB tiempo real: {id_vehiculo} | {duracion:.4f} segundos")


# Guardar lote GPS
def guardar_batch_gps(batch_gps):
    inicio = time.time()

    documentos_mongo = [
        preparar_documento_mongo(data)
        for data in batch_gps
    ]

    gps_raw.insert_many(documentos_mongo)

    fin = time.time()
    duracion = fin - inicio
    rendimiento = len(batch_gps) / duracion if duracion > 0 else 0

    guardar_metrica("Batch", "GPS", len(batch_gps), duracion)

    print(f"Batch GPS MongoDB guardado: {len(batch_gps)} registros | {duracion:.4f} segundos | {rendimiento:.2f} reg/s")


# Guardar lote ESTADO
def guardar_batch_estado(batch_estado):
    inicio = time.time()

    documentos_mongo = [
        preparar_documento_mongo(data)
        for data in batch_estado
    ]

    estado_raw.insert_many(documentos_mongo)

    fin = time.time()
    duracion = fin - inicio
    rendimiento = len(batch_estado) / duracion if duracion > 0 else 0

    guardar_metrica("Batch", "ESTADO", len(batch_estado), duracion)

    print(f"Batch ESTADO MongoDB guardado: {len(batch_estado)} registros | {duracion:.4f} segundos | {rendimiento:.2f} reg/s")


def procesar_micro_batch(batch_df, batch_id):
    filas = batch_df.collect()

    if not filas:
        return

    batch_gps = []
    batch_estado = []

    print(f"Procesando micro-batch Spark {batch_id} con {len(filas)} mensajes")

    for fila in filas:
        topic = fila["topic"]

        try:
            data = json.loads(fila["value"])

            if not MODO_BATCH:
                if topic == "gps":
                    guardar_gps_tiempo_real(data)

                elif topic == "estado":
                    guardar_estado_tiempo_real(data)

            else:
                if topic == "gps":
                    batch_gps.append(data)

                elif topic == "estado":
                    batch_estado.append(data)

        except Exception as e:
            print(f"Error procesando mensaje del topic {topic}: {e}")

    if MODO_BATCH:
        try:
            if batch_gps:
                guardar_batch_gps(batch_gps)

            if batch_estado:
                guardar_batch_estado(batch_estado)

        except Exception as e:
            print(f"Error guardando micro-batch Spark {batch_id} en MongoDB: {e}")


def crear_spark_session():
    return (
        SparkSession.builder
        .appName("ACME EV Spark Structured Streaming Consumer")
        .master("local[*]")
        .config("spark.ui.enabled", "true")
        .config("spark.ui.port", "4040")
        .config("spark.ui.bindAddress", "0.0.0.0")
        .config("spark.driver.bindAddress", "0.0.0.0")
        .getOrCreate()
    )


if __name__ == "__main__":
    spark = crear_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print("SparkSession iniciada")
    print(f"Spark app: {spark.sparkContext.appName}")
    print(f"Spark UI: {spark.sparkContext.uiWebUrl}")
    print(f"Kafka bootstrap servers: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Topics: {KAFKA_TOPICS}")

    kafka_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPICS)
        .option("startingOffsets", "latest")
        .option("maxOffsetsPerTrigger", BATCH_SIZE)
        .load()
    )

    mensajes = kafka_stream.selectExpr(
        "topic",
        "CAST(value AS STRING) AS value",
    )

    if MODO_BATCH:
        print(f"Escuchando mensajes en MODO BATCH con micro-batches Spark | maxOffsetsPerTrigger: {BATCH_SIZE}\n")
    else:
        print("Escuchando mensajes en MODO TIEMPO REAL con Spark Structured Streaming\n")

    query = (
        mensajes.writeStream
        .foreachBatch(procesar_micro_batch)
        .option("checkpointLocation", "/tmp/acme_ev_spark_checkpoint")
        .trigger(processingTime="30 seconds")
        .start()
    )

    query.awaitTermination()
