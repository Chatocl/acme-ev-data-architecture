import json
import os
import time
from datetime import datetime

import psycopg2
from pymongo import MongoClient
from pyspark.sql import SparkSession


# Configuracion del procesamiento
MODO_BATCH = True  # False = tiempo real, True = batch
BATCH_SIZE = 10  # Numero de mensajes por micro-batch de Spark

RETENCION_GPS_DIAS = 30
RETENCION_ESTADO_DIAS = 365

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPICS = "gps,estado"
MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongodb:27017/")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "acme_ev")
POSTGRES_USER = os.getenv("POSTGRES_USER", "admin")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "admin123")


def conectar_mongo():
    while True:
        try:
            client = MongoClient(MONGO_URI)
            client.admin.command("ping")
            return client
        except Exception as e:
            print(f"Esperando MongoDB en {MONGO_URI}: {e}")
            time.sleep(5)


def conectar_postgres():
    while True:
        try:
            return psycopg2.connect(
                host=POSTGRES_HOST,
                dbname=POSTGRES_DB,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD,
                port=POSTGRES_PORT,
            )
        except Exception as e:
            print(f"Esperando PostgreSQL en {POSTGRES_HOST}:{POSTGRES_PORT}: {e}")
            time.sleep(5)


# Conexion a MongoDB
mongo_client = conectar_mongo()
mongo_db = mongo_client["acme_ev"]

gps_raw = mongo_db["gps_raw"]
estado_raw = mongo_db["estado_raw"]

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

# Conexion a PostgreSQL
pg_conn = conectar_postgres()
pg_cursor = pg_conn.cursor()

print("Conectado a PostgreSQL")


# Preparar documento para MongoDB
def preparar_documento_mongo(data):
    documento = data.copy()

    fecha_evento = datetime.fromisoformat(
        data["timestamp"].replace("Z", "+00:00")
    )

    documento["fecha_evento"] = fecha_evento

    return documento


# Limpieza por vigencia en PostgreSQL
def limpiar_datos_vencidos():
    pg_cursor.execute("""
        DELETE FROM gps
        WHERE timestamp < NOW() - INTERVAL '30 days';
    """)

    pg_cursor.execute("""
        DELETE FROM estado
        WHERE timestamp < NOW() - INTERVAL '365 days';
    """)

    pg_conn.commit()
    print("Limpieza de datos vencidos ejecutada en PostgreSQL")


# Guardar GPS en tiempo real
def guardar_gps_tiempo_real(data):
    inicio = time.time()

    gps_raw.insert_one(preparar_documento_mongo(data))

    id_vehiculo = data["id_vehiculo"]
    timestamp = data["timestamp"]
    latitud = data["telemetria"]["latitud"]
    longitud = data["telemetria"]["longitud"]

    pg_cursor.execute("""
        INSERT INTO gps (
            id_vehiculo,
            timestamp,
            latitud,
            longitud
        )
        VALUES (%s, %s, %s, %s)
    """, (
        id_vehiculo,
        timestamp,
        latitud,
        longitud
    ))

    pg_conn.commit()

    fin = time.time()
    duracion = fin - inicio

    print(f"GPS tiempo real: {id_vehiculo} | {duracion:.4f} segundos")


# Guardar ESTADO en tiempo real
def guardar_estado_tiempo_real(data):
    inicio = time.time()

    estado_raw.insert_one(preparar_documento_mongo(data))

    id_vehiculo = data["id_vehiculo"]
    timestamp = data["timestamp"]
    telemetria = data["telemetria"]

    estado_bateria_porcentaje = telemetria["estado_bateria_porcentaje"]
    encendido = telemetria["encendido"]
    codigo_problema = telemetria["codigo_problema"]
    kilometraje = telemetria["kilometraje"]

    pg_cursor.execute("""
        INSERT INTO estado (
            id_vehiculo,
            timestamp,
            estado_bateria_porcentaje,
            encendido,
            codigo_problema,
            kilometraje
        )
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (
        id_vehiculo,
        timestamp,
        estado_bateria_porcentaje,
        encendido,
        codigo_problema,
        kilometraje
    ))

    pg_conn.commit()

    fin = time.time()
    duracion = fin - inicio

    print(f"ESTADO tiempo real: {id_vehiculo} | {duracion:.4f} segundos")


# Guardar lote GPS
def guardar_batch_gps(batch_gps):
    inicio = time.time()

    documentos_mongo = [
        preparar_documento_mongo(data)
        for data in batch_gps
    ]

    gps_raw.insert_many(documentos_mongo)

    registros_postgres = []

    for data in batch_gps:
        registros_postgres.append((
            data["id_vehiculo"],
            data["timestamp"],
            data["telemetria"]["latitud"],
            data["telemetria"]["longitud"]
        ))

    pg_cursor.executemany("""
        INSERT INTO gps (
            id_vehiculo,
            timestamp,
            latitud,
            longitud
        )
        VALUES (%s, %s, %s, %s)
    """, registros_postgres)

    pg_conn.commit()

    fin = time.time()
    duracion = fin - inicio
    rendimiento = len(batch_gps) / duracion if duracion > 0 else 0

    print(f"Batch GPS guardado: {len(batch_gps)} registros | {duracion:.4f} segundos | {rendimiento:.2f} reg/s")


# Guardar lote ESTADO
def guardar_batch_estado(batch_estado):
    inicio = time.time()

    documentos_mongo = [
        preparar_documento_mongo(data)
        for data in batch_estado
    ]

    estado_raw.insert_many(documentos_mongo)

    registros_postgres = []

    for data in batch_estado:
        telemetria = data["telemetria"]

        registros_postgres.append((
            data["id_vehiculo"],
            data["timestamp"],
            telemetria["estado_bateria_porcentaje"],
            telemetria["encendido"],
            telemetria["codigo_problema"],
            telemetria["kilometraje"]
        ))

    pg_cursor.executemany("""
        INSERT INTO estado (
            id_vehiculo,
            timestamp,
            estado_bateria_porcentaje,
            encendido,
            codigo_problema,
            kilometraje
        )
        VALUES (%s, %s, %s, %s, %s, %s)
    """, registros_postgres)

    pg_conn.commit()

    fin = time.time()
    duracion = fin - inicio
    rendimiento = len(batch_estado) / duracion if duracion > 0 else 0

    print(f"Batch ESTADO guardado: {len(batch_estado)} registros | {duracion:.4f} segundos | {rendimiento:.2f} reg/s")


def crear_tablas_postgres_si_no_existen():
    pg_cursor.execute("""
        CREATE TABLE IF NOT EXISTS gps (
            id SERIAL PRIMARY KEY,
            id_vehiculo VARCHAR(50) NOT NULL,
            timestamp TIMESTAMPTZ NOT NULL,
            latitud DOUBLE PRECISION NOT NULL,
            longitud DOUBLE PRECISION NOT NULL
        );
    """)

    pg_cursor.execute("""
        CREATE TABLE IF NOT EXISTS estado (
            id SERIAL PRIMARY KEY,
            id_vehiculo VARCHAR(50) NOT NULL,
            timestamp TIMESTAMPTZ NOT NULL,
            estado_bateria_porcentaje INTEGER NOT NULL,
            encendido BOOLEAN NOT NULL,
            codigo_problema VARCHAR(20) NOT NULL,
            kilometraje DOUBLE PRECISION NOT NULL
        );
    """)

    pg_conn.commit()
    print("Tablas PostgreSQL verificadas")


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
            pg_conn.rollback()
            print(f"Error procesando mensaje del topic {topic}: {e}")

    try:
        if MODO_BATCH:
            if batch_gps:
                guardar_batch_gps(batch_gps)

            if batch_estado:
                guardar_batch_estado(batch_estado)

    except Exception as e:
        pg_conn.rollback()
        print(f"Error guardando micro-batch Spark {batch_id}: {e}")


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
    crear_tablas_postgres_si_no_existen()

    # Ejecutar limpieza inicial en PostgreSQL
    limpiar_datos_vencidos()

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
