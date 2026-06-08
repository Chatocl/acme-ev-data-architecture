import csv
import os
import time

import psycopg2


POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "acme_ev")
POSTGRES_USER = os.getenv("POSTGRES_USER", "admin")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "admin123")


def conectar_postgres():
    print("Intentando conectar a PostgreSQL...")

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


conn = conectar_postgres()
cursor = conn.cursor()

print("Conectado a PostgreSQL")

cursor.execute("""
    CREATE TABLE IF NOT EXISTS clientes (
        id_cliente VARCHAR(50) PRIMARY KEY,
        nombre VARCHAR(150) NOT NULL,
        correo VARCHAR(150) NOT NULL,
        pais VARCHAR(80) NOT NULL
    );
""")

cursor.execute("""
    CREATE TABLE IF NOT EXISTS vehiculos (
        id_vehiculo VARCHAR(50) PRIMARY KEY,
        vin VARCHAR(17) NOT NULL,
        id_cliente VARCHAR(50) NOT NULL REFERENCES clientes(id_cliente),
        sucursal VARCHAR(20) NOT NULL,
        pais VARCHAR(80) NOT NULL,
        modelo VARCHAR(80) NOT NULL,
        anio INTEGER NOT NULL
    );
""")

conn.commit()
print("Tablas maestras verificadas")

# Cargar CLIENTES
clientes_insertados = 0

with open("clientes.csv", mode="r", encoding="utf-8") as archivo_clientes:
    lector = csv.DictReader(archivo_clientes)

    for fila in lector:
        cursor.execute("""
            INSERT INTO clientes (
                id_cliente,
                nombre,
                correo,
                pais
            )
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (id_cliente) DO NOTHING;
        """, (
            fila["id_cliente"],
            fila["nombre"],
            fila["correo"],
            fila["pais"],
        ))

        clientes_insertados += 1

print(f"Clientes procesados: {clientes_insertados}")

# Cargar VEHICULOS
vehiculos_insertados = 0

with open("vehiculos.csv", mode="r", encoding="utf-8") as archivo_vehiculos:
    lector = csv.DictReader(archivo_vehiculos)

    for fila in lector:
        cursor.execute("""
            INSERT INTO vehiculos (
                id_vehiculo,
                vin,
                id_cliente,
                sucursal,
                pais,
                modelo,
                anio
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id_vehiculo) DO NOTHING;
        """, (
            fila["id_vehiculo"],
            fila["VIN"],
            fila["id_cliente"],
            fila["sucursal"],
            fila["pais"],
            fila["modelo"],
            int(fila["anio"]),
        ))

        vehiculos_insertados += 1

print(f"Vehiculos procesados: {vehiculos_insertados}")

conn.commit()

cursor.close()
conn.close()

print("Carga de datos maestros finalizada")
