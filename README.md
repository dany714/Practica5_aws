# Práctica 5: Pipeline ETL de PostgreSQL a AWS S3

Extracción de clientes desde PostgreSQL, clasificación de edades,
normalización Min-Max de compras y cálculo de score promocional.
El resultado se serializa en memoria como Parquet con compresión Snappy
y se carga a la zona processed de Amazon S3.

## Entorno

- PostgreSQL en localhost:5432, base ventas y usuario etl_user.
- Tabla public.clientes con id, cliente, edad y monto_compras.
- Credenciales AWS activas con acceso de escritura al bucket configurado.

## Ejecución en Ubuntu

```bash
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install -r requirements.txt
python3 pipeline_etl.py
```

La contraseña de PostgreSQL se solicita al ejecutar y no se almacena.
Boto3 obtiene las credenciales mediante la configuración de AWS.

Destino: s3://aws-academy-datacenter-bucket-23-02-017/processed/etl_output.parquet
