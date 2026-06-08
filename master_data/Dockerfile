FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir psycopg2-binary==2.9.9

COPY clientes.csv .
COPY vehiculos.csv .
COPY load_master_data.py .

CMD ["python", "load_master_data.py"]
