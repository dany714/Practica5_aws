"""Pipeline ETL de PostgreSQL a Amazon S3 con Parquet en memoria."""
import io
from getpass import getpass

import boto3
import pandas as pd
from sklearn.preprocessing import MinMaxScaler
from sqlalchemy import URL, create_engine, text

BUCKET = "aws-academy-datacenter-bucket-23-02-017"
CLAVE = "processed/etl_output.parquet"


def extraer_clientes():
    """Consulta los registros de PostgreSQL sin guardar la contraseña."""
    url = URL.create(
        "postgresql+psycopg",
        username="etl_user",
        password=getpass("Contraseña de PostgreSQL: "),
        host="localhost",
        port=5432,
        database="ventas",
    )
    engine = create_engine(url)
    try:
        with engine.connect() as conexion:
            return pd.read_sql_query(
                text("""
                    SELECT cliente, edad, monto_compras
                    FROM public.clientes
                    ORDER BY id
                """),
                conexion,
            )
    finally:
        engine.dispose()


def transformar_clientes(df):
    """Generaliza edades, normaliza compras y construye un score."""
    df = df.copy()
    df["categoria_edad"] = pd.cut(
        df["edad"],
        bins=[0, 25, 50, 120],
        labels=["Joven", "Adulto", "Senior"],
        include_lowest=True,
    )
    scaler = MinMaxScaler()
    df["monto_normalizado"] = scaler.fit_transform(
        df[["monto_compras"]]
    ).ravel()
    df["score_promocional"] = df["monto_normalizado"] * 100
    return df


def cargar_parquet_s3(df):
    """Serializa en memoria con Snappy y realiza la carga efectiva a S3."""
    # Boto3 obtiene las credenciales de su cadena de configuración
    # (por ejemplo, rol de EC2 o credenciales temporales de AWS Academy).
    s3 = boto3.client("s3", region_name="us-east-1")
    with io.BytesIO() as buffer:
        df.to_parquet(
            buffer, engine="pyarrow", compression="snappy", index=False
        )
        buffer.seek(0)
        s3.upload_fileobj(buffer, BUCKET, CLAVE)


def pipeline_etl_completo():
    """Ejecuta secuencialmente las tres fases del pipeline."""
    print("=== 1. FASE DE EXTRACCIÓN (Extract) ===")
    df = extraer_clientes()
    if df.empty:
        print("No hay registros para procesar.")
        return
    print(f"Registros extraídos: {len(df)}")
    print("\n=== 2. FASE DE TRANSFORMACIÓN (Transform) ===")
    df = transformar_clientes(df)
    print(df.to_string(index=False))
    print("\n=== 3. FASE DE CARGA (Load) ===")
    cargar_parquet_s3(df)
    print(f"Éxito: archivo guardado en s3://{BUCKET}/{CLAVE}")


if __name__ == "__main__":
    pipeline_etl_completo()
