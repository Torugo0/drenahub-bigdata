# Databricks notebook source
"""Carga incremental dos JSONL enviados ao Unity Catalog Volume.

Execute depois de enviar os arquivos weather_*.jsonl e sensors_*.jsonl para
/Volumes/<catalog>/<schema>/<volume>/chegada/. Reexecutar COPY INTO sem FORCE
não recarrega os mesmos arquivos. O estado de arquivos é mantido pelo Delta.
"""

import re


def identifier(value: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z_0-9]*", value):
        raise ValueError(f"Identificador SQL inválido: {value!r}")
    return value


dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "default")
dbutils.widgets.text("volume", "drenahub_raw")
dbutils.widgets.text("arrival_subdir", "chegada")

catalog = identifier(dbutils.widgets.get("catalog"))
schema = identifier(dbutils.widgets.get("schema"))
volume = identifier(dbutils.widgets.get("volume"))
arrival_subdir = identifier(dbutils.widgets.get("arrival_subdir"))
namespace = f"`{catalog}`.`{schema}`"
arrival_path = f"/Volumes/{catalog}/{schema}/{volume}/{arrival_subdir}"

spark.sql("SET TIME ZONE 'UTC'")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {namespace}")

# Todos os campos são guardados como texto na Bronze: valores inválidos e nulos
# precisam chegar à Prata para que a rejeição seja contada e auditável.
spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {namespace}.bronze_weather (
      event_time STRING,
      latitude STRING,
      longitude STRING,
      precipitation_mm STRING,
      source STRING,
      collected_at STRING
    ) USING DELTA
    """
)
spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {namespace}.bronze_sensors (
      record_id STRING,
      point_id STRING,
      event_time STRING,
      water_level_cm STRING,
      sensor_status STRING,
      is_synthetic STRING,
      collected_at STRING,
      responsible_name_synthetic STRING
    ) USING DELTA
    """
)


def count(table: str) -> int:
    return spark.sql(f"SELECT COUNT(*) AS n FROM {namespace}.{table}").first().n


SOURCE_FIELDS = {
    "bronze_weather": (
        "event_time", "latitude", "longitude", "precipitation_mm",
        "source", "collected_at",
    ),
    "bronze_sensors": (
        "record_id", "point_id", "event_time", "water_level_cm",
        "sensor_status", "is_synthetic", "collected_at",
        "responsible_name_synthetic",
    ),
}


def load(table: str, pattern: str) -> None:
    before = count(table)
    print(f"{table}: antes={before}")
    # O leitor JSON pode inferir DOUBLE/BOOLEAN mesmo quando a Bronze é STRING.
    # Projetar e converter cada campo evita DELTA_FAILED_TO_MERGE_FIELDS e
    # preserva valores brutos para as regras de qualidade da Prata.
    projection = ",\n            ".join(
        f"CAST(`{field}` AS STRING) AS `{field}`" for field in SOURCE_FIELDS[table]
    )
    result = spark.sql(
        f"""
        COPY INTO {namespace}.{table}
        FROM (
          SELECT {projection}
          FROM '{arrival_path}'
        )
        FILEFORMAT = JSON
        PATTERN = '{pattern}'
        FORMAT_OPTIONS ('multiLine' = 'false')
        COPY_OPTIONS ('force' = 'false')
        """
    )
    result.show(truncate=False)
    after = count(table)
    print(f"{table}: depois={after}; incremento={after - before}")
    spark.sql(
        f"""
        SELECT collected_at, COUNT(*) AS linhas
        FROM {namespace}.{table}
        GROUP BY collected_at
        ORDER BY collected_at
        """
    ).show(100, truncate=False)


# PATTERN usa glob (não regex) e os arquivos ficam diretamente em chegada/.
load("bronze_weather", "weather_*.jsonl")
load("bronze_sensors", "sensors_*.jsonl")

print("Execute novamente este notebook sem novos arquivos: ambos os incrementos devem ser zero.")
