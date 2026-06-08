import csv
import random
import os

# Crear la carpeta master_data si no existe (Vehículos y clientes)
carpeta = "master_data"
os.makedirs(carpeta, exist_ok=True)

# Lista de países donde ACME EV tiene sucursales
paises = [
    "México", "Guatemala", "Honduras", "Costa Rica", "Colombia",
    "Argentina", "Canadá", "Chile", "Panamá", "Perú"
]

# Relación fija entre país y sucursal
sucursales_por_pais = {
    "México": "SUC-001",
    "Guatemala": "SUC-002",
    "Honduras": "SUC-003",
    "Costa Rica": "SUC-004",
    "Colombia": "SUC-005",
    "Argentina": "SUC-006",
    "Canadá": "SUC-007",
    "Chile": "SUC-008",
    "Panamá": "SUC-009",
    "Perú": "SUC-010"
}

# Datos para generar clientes aleatorios
nombres = [
    "Ana", "Luis", "Carlos", "María", "Paola",
    "Diego", "Sofía", "Juan", "Sebastian", "Valeria",
    "Ileana", "Miguel", "Fernanda", "Andrés", "Camila",
    "Ricardo", "Lucía", "Jorge", "Natalia", "Eduardo"
]

apellidos = [
    "García", "López", "Mérida", "Pérez", "Ramírez",
    "Torres", "Morales", "Sánchez", "Flores", "Gómez",
    "Díaz", "Vargas", "Rojas", "Castillo", "Ortiz",
    "Silva", "Mendoza", "Romero", "Alvarez", "Cruz"
]

# Modelos de vehículos disponibles
modelos = [
    "Model A",
    "Model B",
    "Model C",
    "Model D",
    "Model E",
    "Model F"
]

# Código que se utilizará dentro del VIN para identificar el modelo
codigos_modelo = {
    "Model A": "MDLA1",
    "Model B": "MDLB1",
    "Model C": "MDLC1",
    "Model D": "MDLD1",
    "Model E": "MDLE1",
    "Model F": "MDLF1"
}

# Códigos simplificados para representar el año dentro del VIN
codigos_anio = {
    2024: "R",
    2025: "S",
    2026: "T"
}

# Código de planta asociado a cada sucursal
plantas = {
    "SUC-001": "A",
    "SUC-002": "B",
    "SUC-003": "C",
    "SUC-004": "D",
    "SUC-005": "E",
    "SUC-006": "F",
    "SUC-007": "G",
    "SUC-008": "H",
    "SUC-009": "J",
    "SUC-010": "K"
}

# Función para eliminar acentos al generar correos electrónicos
def limpiar_texto(texto):
    return (
        texto.lower()
        .replace("á", "a")
        .replace("é", "e")
        .replace("í", "i")
        .replace("ó", "o")
        .replace("ú", "u")
        .replace("ñ", "n")
    )

# Función que construye un VIN de 17 caracteres (El número VIN es el Número de Identificación del Vehículoes un código único e intransferible compuesto por 17 caracteres alfanuméricos)
def generar_vin(numero_vehiculo, modelo, anio, sucursal):

    # Código del fabricante (ACME EV)
    wmi = "ACE"

    # Código correspondiente al modelo
    vds = codigos_modelo[modelo]

    # Dígito de control aleatorio
    check = str(random.randint(0, 9))

    # Código correspondiente al año
    year = codigos_anio[anio]

    # Código de planta según la sucursal
    plant = plantas[sucursal]

    # Número de serie único para cada vehículo
    serial = f"{numero_vehiculo:06d}"

    # VIN final de 17 caracteres
    return f"{wmi}{vds}{check}{year}{plant}{serial}"


# Diccionario temporal para almacenar la información de los clientes
clientes_info = {}

# GENERACIÓN DE CLIENTES (1000 CLIENTES) - relación de 1 cliente con 10 vehículos
with open(os.path.join(carpeta, "clientes.csv"), "w", newline="", encoding="utf-8") as file:

    writer = csv.writer(file)

    # Encabezados del archivo CSV
    writer.writerow([
        "id_cliente",
        "nombre",
        "correo",
        "pais"
    ])

    # Generar 1000 clientes
    for i in range(1, 1001):

        # ID único del cliente
        id_cliente = f"CLI-{i:05d}"

        # Nombre completo aleatorio
        nombre = random.choice(nombres)
        apellido = random.choice(apellidos)
        nombre_completo = f"{nombre} {apellido}"

        # País aleatorio
        pais = random.choice(paises)

        # Sucursal asociada automáticamente al país
        sucursal = sucursales_por_pais[pais]

        # Correo electrónico generado automáticamente
        correo = (
            f"{limpiar_texto(nombre)}."
            f"{limpiar_texto(apellido)}{i}@correo.com"
        )

        # Guardar información para utilizarla posteriormente
        clientes_info[id_cliente] = {
            "pais": pais,
            "sucursal": sucursal
        }

        # Escribir cliente en el CSV
        writer.writerow([
            id_cliente,
            nombre_completo,
            correo,
            pais
        ])

# GENERACIÓN DE VEHÍCULOS (10000 VEHÍCULOS)
with open(os.path.join(carpeta, "vehiculos.csv"), "w", newline="", encoding="utf-8") as file:

    writer = csv.writer(file)

    # Encabezados del archivo CSV
    writer.writerow([
        "id_vehiculo",
        "VIN",
        "id_cliente",
        "sucursal",
        "pais",
        "modelo",
        "anio"
    ])

    # Lista de IDs de clientes disponibles
    clientes_ids = list(clientes_info.keys())

    # Generar 10,000 vehículos
    for i in range(1, 10001):

        # ID único del vehículo
        id_vehiculo = f"EV-ACME-{i:05d}"

        # Seleccionar un cliente aleatorio
        id_cliente = random.choice(clientes_ids)

        # Obtener país y sucursal del cliente
        pais = clientes_info[id_cliente]["pais"]
        sucursal = clientes_info[id_cliente]["sucursal"]

        # Seleccionar modelo aleatorio
        modelo = random.choice(modelos)

        # Seleccionar año aleatorio
        anio = random.choice([2024, 2025, 2026])

        # Generar VIN siguiendo la estructura definida
        vin = generar_vin(
            i,
            modelo,
            anio,
            sucursal
        )

        # Guardar registro del vehículo
        writer.writerow([
            id_vehiculo,
            vin,
            id_cliente,
            sucursal,
            pais,
            modelo,
            anio
        ])

# Mensaje final
print("Archivos generados correctamente")
print("master_data/clientes.csv")
print("master_data/vehiculos.csv")