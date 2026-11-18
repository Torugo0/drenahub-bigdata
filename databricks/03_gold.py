# Databricks notebook source
"""Modelo dimensional para o piloto DrenaHub.

Grão de fato_monitoramento: uma leitura válida por record_id de sensor.
Precipitação tem grão de uma hora e fica em tabela própria; nunca é somada
diretamente da fato. O vínculo por hora usa a estação piloto de São Paulo
como contexto temporal, sem inferir chuva exata em cada ralo.
"""

import re


def identifier(value: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z_0-9]*", value):
        raise ValueError(f"Identificador SQL inválido: {value!r}")
    return value


dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "default")
dbutils.widgets.text("attention_cm", "20")
catalog = identifier(dbutils.widgets.get("catalog"))
schema = identifier(dbutils.widgets.get("schema"))
attention_cm = float(dbutils.widgets.get("attention_cm"))
if not 0 <= attention_cm <= 300:
    raise ValueError("attention_cm deve estar entre 0 e 300 cm")
namespace = f"`{catalog}`.`{schema}`"
spark.sql("SET TIME ZONE 'UTC'")

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {namespace}.dim_local (
      local_id STRING,
      ponto_id STRING,
      nome_local STRING,
      is_synthetic BOOLEAN
    ) USING DELTA
    """
)
spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {namespace}.dim_tempo (
      tempo_id STRING,
      data_hora TIMESTAMP,
      data DATE,
      hora INT,
      hora_inicio TIMESTAMP
    ) USING DELTA
    """
)
spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {namespace}.fato_monitoramento (
      leitura_id STRING,
      local_id STRING,
      tempo_id STRING,
      nivel_agua_cm DOUBLE,
      em_atencao BOOLEAN,
      sensor_status STRING,
      is_synthetic BOOLEAN,
      responsavel_hash STRING,
      collected_at TIMESTAMP
    ) USING DELTA
    """
)
spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {namespace}.precipitacao_horaria (
      data_hora TIMESTAMP,
      latitude DOUBLE,
      longitude DOUBLE,
      precipitacao_mm DOUBLE,
      source STRING,
      collected_at TIMESTAMP
    ) USING DELTA
    """
)

sensor_count = spark.sql(
    f"SELECT COUNT(*) AS n FROM {namespace}.silver_sensors"
).first().n
weather_count = spark.sql(
    f"""SELECT COUNT(*) AS n FROM {namespace}.silver_weather
        WHERE source = 'open-meteo-archive'"""
).first().n
if sensor_count == 0 or weather_count == 0:
    raise RuntimeError(
        "Prata vazia: execute 01_bronze.py e 02_silver.py com arquivos reais. "
        f"silver_sensors={sensor_count}, silver_weather_archive={weather_count}"
    )

spark.sql(
    f"""
    CREATE OR REPLACE TEMP VIEW weather_hourly_to_merge AS
    SELECT data_hora, latitude, longitude, precipitacao_mm,
           source, collected_at
    FROM (
      SELECT event_time AS data_hora, latitude, longitude,
             precipitation_mm AS precipitacao_mm, source, collected_at,
             ROW_NUMBER() OVER (
               PARTITION BY event_time
               ORDER BY collected_at DESC, weather_key DESC
             ) AS rn
      FROM {namespace}.silver_weather
      WHERE source = 'open-meteo-archive'
    ) WHERE rn = 1
    """
)

# O piloto tem uma única coordenada Open-Meteo. Se isso mudar, é necessário
# modelar explicitamente uma estação/local antes do vínculo temporal.
locations = spark.sql(
    f"""
    SELECT COUNT(DISTINCT concat_ws('|', cast(latitude AS STRING),
                                    cast(longitude AS STRING))) AS n
    FROM {namespace}.silver_weather
    WHERE source = 'open-meteo-archive'
    """
).first().n
if locations != 1:
    raise RuntimeError(
        f"Esperada uma coordenada meteorológica piloto; encontradas {locations}."
    )

spark.sql(
    f"""
    MERGE INTO {namespace}.dim_local AS target
    USING (
      SELECT DISTINCT point_id AS local_id, point_id AS ponto_id,
             point_id AS nome_local, true AS is_synthetic
      FROM {namespace}.silver_sensors
    ) AS source
      ON target.local_id = source.local_id
    WHEN MATCHED THEN UPDATE SET
      target.ponto_id = source.ponto_id,
      target.nome_local = source.nome_local,
      target.is_synthetic = source.is_synthetic
    WHEN NOT MATCHED THEN INSERT (
      local_id, ponto_id, nome_local, is_synthetic
    ) VALUES (
      source.local_id, source.ponto_id, source.nome_local,
      source.is_synthetic
    )
    """
)
spark.sql(
    f"""
    MERGE INTO {namespace}.dim_tempo AS target
    USING (
      SELECT DISTINCT
        cast(event_time AS STRING) AS tempo_id,
        event_time AS data_hora,
        to_date(event_time) AS data,
        hour(event_time) AS hora,
        date_trunc('hour', event_time) AS hora_inicio
      FROM {namespace}.silver_sensors
    ) AS source
      ON target.tempo_id = source.tempo_id
    WHEN MATCHED THEN UPDATE SET
      target.data_hora = source.data_hora,
      target.data = source.data,
      target.hora = source.hora,
      target.hora_inicio = source.hora_inicio
    WHEN NOT MATCHED THEN INSERT (
      tempo_id, data_hora, data, hora, hora_inicio
    ) VALUES (
      source.tempo_id, source.data_hora, source.data,
      source.hora, source.hora_inicio
    )
    """
)

# Os nomes de responsáveis são exclusivamente fictícios. A Ouro publica só
# SHA-256 do identificador fictício, nunca o texto original da Prata.
spark.sql(
    f"""
    MERGE INTO {namespace}.fato_monitoramento AS target
    USING (
      SELECT record_id AS leitura_id,
             point_id AS local_id,
             cast(event_time AS STRING) AS tempo_id,
             water_level_cm AS nivel_agua_cm,
             (water_level_cm >= {attention_cm}) AS em_atencao,
             sensor_status,
             is_synthetic,
             CASE WHEN responsible_name_synthetic IS NOT NULL
                  THEN sha2(concat('DrenaHub:synthetic:',
                                   responsible_name_synthetic), 256)
                  ELSE NULL END AS responsavel_hash,
             collected_at
      FROM {namespace}.silver_sensors
    ) AS source
      ON target.leitura_id = source.leitura_id
    WHEN MATCHED THEN UPDATE SET
      target.local_id = source.local_id,
      target.tempo_id = source.tempo_id,
      target.nivel_agua_cm = source.nivel_agua_cm,
      target.em_atencao = source.em_atencao,
      target.sensor_status = source.sensor_status,
      target.is_synthetic = source.is_synthetic,
      target.responsavel_hash = source.responsavel_hash,
      target.collected_at = source.collected_at
    WHEN NOT MATCHED THEN INSERT (
      leitura_id, local_id, tempo_id, nivel_agua_cm, em_atencao,
      sensor_status, is_synthetic, responsavel_hash, collected_at
    ) VALUES (
      source.leitura_id, source.local_id, source.tempo_id,
      source.nivel_agua_cm, source.em_atencao, source.sensor_status,
      source.is_synthetic, source.responsavel_hash, source.collected_at
    )
    """
)
spark.sql(
    f"""
    MERGE INTO {namespace}.precipitacao_horaria AS target
    USING weather_hourly_to_merge AS source
      ON target.data_hora = source.data_hora
    WHEN MATCHED THEN UPDATE SET
      target.latitude = source.latitude,
      target.longitude = source.longitude,
      target.precipitacao_mm = source.precipitacao_mm,
      target.source = source.source,
      target.collected_at = source.collected_at
    WHEN NOT MATCHED THEN INSERT (
      data_hora, latitude, longitude, precipitacao_mm,
      source, collected_at
    ) VALUES (
      source.data_hora, source.latitude, source.longitude,
      source.precipitacao_mm, source.source, source.collected_at
    )
    """
)

print("Contagens da Ouro:")
spark.sql(
    f"""
    SELECT 'dim_local' AS tabela, COUNT(*) AS linhas FROM {namespace}.dim_local
    UNION ALL
    SELECT 'dim_tempo', COUNT(*) FROM {namespace}.dim_tempo
    UNION ALL
    SELECT 'fato_monitoramento', COUNT(*) FROM {namespace}.fato_monitoramento
    UNION ALL
    SELECT 'precipitacao_horaria', COUNT(*) FROM {namespace}.precipitacao_horaria
    """
).show(truncate=False)

checks = spark.sql(
    f"""
    SELECT
      (SELECT COUNT(*) FROM {namespace}.fato_monitoramento f
       LEFT ANTI JOIN {namespace}.dim_local l ON f.local_id = l.local_id)
       AS fatos_sem_local,
      (SELECT COUNT(*) FROM {namespace}.fato_monitoramento f
       LEFT ANTI JOIN {namespace}.dim_tempo t ON f.tempo_id = t.tempo_id)
       AS fatos_sem_tempo,
      (SELECT COUNT(*) - COUNT(DISTINCT leitura_id)
       FROM {namespace}.fato_monitoramento) AS leituras_duplicadas,
      (SELECT COUNT(*) - COUNT(DISTINCT data_hora)
       FROM {namespace}.precipitacao_horaria) AS horas_chuva_duplicadas
    """
).first()
print(f"Integridade dimensional: {checks.asDict()}")
if any(value != 0 for value in checks):
    raise RuntimeError("Falha de integridade dimensional na Ouro")

fact_count = spark.sql(
    f"SELECT COUNT(*) AS n FROM {namespace}.fato_monitoramento"
).first().n
if fact_count != sensor_count:
    raise RuntimeError(
        f"Grão da fato inconsistente: {fact_count} fatos para "
        f"{sensor_count} leituras válidas"
    )
