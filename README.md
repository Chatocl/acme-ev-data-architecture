# ACME EV - Arquitectura de Datos para la Movilidad Inteligente

## Descripción

Este proyecto implementa una arquitectura de datos para ACME EV, una empresa dedicada a la comercialización de vehículos eléctricos. La solución permite capturar, procesar, almacenar y visualizar información de telemetría generada por los vehículos, incluyendo datos GPS y estados operativos.

### Tecnologías utilizadas

- Apache Kafka
- Apache Spark Structured Streaming
- MongoDB
- PostgreSQL
- Metabase
- Docker Compose

---

## Arquitectura General

```text
Vehículos
    ↓
Kafka
    ↓
Spark Structured Streaming
    ↓
MongoDB
    ↓
Metabase

Clientes CSV / Vehículos CSV
            ↓
        PostgreSQL
            ↓
        Metabase
```

---

## Requisitos

- Docker Desktop
- Docker Compose
- 8 GB RAM mínimo recomendados

### Puertos utilizados

| Servicio | Puerto |
|-----------|---------|
| Metabase | 3000 |
| Spark UI | 4040 |
| PostgreSQL | 5433 |
| MongoDB | 27017 |
| Kafka | 29092 |
| Kafdrop | 9000 |

---

## Levantar el Proyecto

Ubicarse en la carpeta raíz:

```bash
cd acme-ev-data-architecture
```

Construir y levantar todos los servicios:

```bash
docker compose up --build -d
```

Verificar que los contenedores estén activos:

```bash
docker ps
```

Contenedores esperados:

```text
kafka
kafdrop
acme_producer
acme_consumer
mongodb
postgres
acme_master_data
metabase
```

---

## Verificación de Servicios

### Logs del Consumer

```bash
docker logs -f acme_consumer
```

### Logs del Producer

```bash
docker logs -f acme_producer
```

---

## Acceso a Herramientas

### Metabase

```text
http://localhost:3000
```

### Spark UI

```text
http://localhost:4040
```

### Kafdrop

```text
http://localhost:9000
```

---

## Acceso a MongoDB

Ingresar al contenedor:

```bash
docker exec -it mongodb mongosh
```

Seleccionar base de datos:

```javascript
use acme_ev
```

Ver colecciones:

```javascript
show collections
```

Colecciones esperadas:

```text
gps_raw
estado_raw
metricas_rendimiento
```

### Consultar GPS

```javascript
db.gps_raw.find().limit(5)
```

### Consultar Estados

```javascript
db.estado_raw.find().limit(5)
```

### Consultar Métricas

```javascript
db.metricas_rendimiento.find().limit(10)
```

---

## Acceso a PostgreSQL

Ingresar al contenedor:

```bash
docker exec -it postgres psql -U admin -d acme_ev
```

Listar tablas:

```sql
\dt
```

Consultar clientes:

```sql
SELECT * FROM clientes LIMIT 10;
```

Consultar vehículos:

```sql
SELECT * FROM vehiculos LIMIT 10;
```

---

## Retención de Datos

### GPS

Los registros GPS son almacenados durante:

```text
30 días
```

mediante índices TTL sobre:

```text
fecha_evento
```

### Estados

Los registros de estados son almacenados durante:

```text
365 días
```

mediante índices TTL sobre:

```text
fecha_evento
```

---

## Modos de Procesamiento

### Batch

Modificar en:

```python
consumer.py
```

```python
MODO_BATCH = True
```

Reconstruir el consumer:

```bash
docker compose up -d --build consumer
```

### Tiempo Real

Modificar en:

```python
consumer.py
```

```python
MODO_BATCH = False
```

Reconstruir el consumer:

```bash
docker compose up -d --build consumer
```

Verificar en logs:

```text
Escuchando mensajes en MODO TIEMPO REAL con Spark Structured Streaming
```

---

## Dashboards Implementados

### Portal Cliente - Seguimiento GPS

Funcionalidades:

- Visualización de ubicaciones GPS.
- Historial de posiciones.
- Visualización geográfica en mapa.
- Exportación de información.

---

### Centro de Monitoreo de Flota

Funcionalidades:

- Estado de batería.
- Detección de fallas.
- Seguimiento de kilometraje.
- Vehículos con batería baja.

---

### Dashboard de Rendimiento

Funcionalidades:

- Comparación Batch vs Tiempo Real.
- Tiempo promedio de procesamiento.
- Registros por segundo.
- Métricas de latencia.

---

### Resumen Ejecutivo

Funcionalidades:

- Indicadores clave del negocio.
- Monitoreo de vehículos.
- Métricas operativas.
- Visualización consolidada para gerencia.

---

## Detener el Proyecto

Sin perder información:

```bash
docker compose stop
```

---

## Reiniciar Servicios

```bash
docker compose start
```

---

## Eliminar Contenedores

Conservando datos:

```bash
docker compose down
```

---

## Eliminar Contenedores y Datos

⚠️ Elimina completamente las bases de datos.

```bash
docker compose down -v
```

---

## Integrantes

- Ana Paola Mérida Loy – 1084120
- Diego Andrés Gil Morales – 1084720
- María del Mar Rosado – 1070720
- Diego Alejandro Tobar Ochoa – 1229720
- Carlos Roberto Coronado Lazo – 1236020

---

## Universidad Rafael Landívar

Especialización en Data Science

Curso: Fundamentos de Arquitectura de Datos
