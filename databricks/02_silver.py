# Databricks notebook source
"""Validação, rejeições auditáveis, deduplicação e MERGE da camada Prata.

Execute após 01_bronze.py. A sessão usa UTC. As contagens exibidas são reais
somente quando este notebook é executado no workspace Databricks.
"""

import re


def identifier(value: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z_0-9]*", value):
        raise ValueError(f"Identificador SQL inválido: {value!r}")
    return value


dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "default")
catalog = identifier(dbutils.widgets.get("catalog"))
schema = identifier(dbutils.widgets.get("schema"))
namespace = f"`{catalog}`.`{schema}`"
spark.sql("SET TIME ZONE 'UTC'")

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {namespace}.silver_weather (
      weather_key STRING,
      event_time TIMESTAMP,
      latitude DOUBLE,
      longitude DOUBLE,
      precipitation_mm DOUBLE,
      source STRING,
      collected_at TIMESTAMP
    ) USING DELTA
    """
)
spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {namespace}.silver_sensors (
      record_id STRING,
      point_id STRING,
      event_time TIMESTAMP,
      water_level_cm DOUBLE,
      sensor_status STRING,
      is_synthetic BOOLEAN,
      collected_at TIMESTAMP,
      responsible_name_synthetic STRING
    ) USING DELTA
    """
)
spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {namespace}.quality_rejections (
      source_name STRING,
      source_row_hash STRING,
      business_key STRING,
      rule_code STRING,
      collected_at_raw STRING,
      rule_description STRING,
      rejected_at TIMESTAMP
    ) USING DELTA
    """
)

# TRY_CAST preserva o pipeline mesmo quando o bruto contém texto inválido.
# O hash da linha bruta identifica rejeições em reexecuções e entre lotes.
spark.sql(
    f"""
    CREATE OR REPLACE TEMP VIEW weather_normalized AS
    SELECT
      event_time AS event_time_raw,
      try_cast(event_time AS TIMESTAMP) AS event_ts,
      try_cast(latitude AS DOUBLE) AS latitude_num,
      try_cast(longitude AS DOUBLE) AS longitude_num,
      try_cast(precipitation_mm AS DOUBLE) AS precipitation_num,
      trim(source) AS source_clean,
      collected_at AS collected_at_raw,
      try_cast(collected_at AS TIMESTAMP) AS collected_ts,
      sha2(to_json(named_struct(
        'event_time', event_time, 'latitude', latitude,
        'longitude', longitude, 'precipitation_mm', precipitation_mm,
        'source', source, 'collected_at', collected_at
      )), 256) AS source_row_hash
    FROM {namespace}.bronze_weather
    """
)
spark.sql(
    f"""
    CREATE OR REPLACE TEMP VIEW sensor_normalized AS
    SELECT
      record_id,
      point_id,
      event_time AS event_time_raw,
      try_cast(event_time AS TIMESTAMP) AS event_ts,
      try_cast(water_level_cm AS DOUBLE) AS water_level_num,
      lower(trim(sensor_status)) AS sensor_status_clean,
      try_cast(is_synthetic AS BOOLEAN) AS is_synthetic_bool,
      collected_at AS collected_at_raw,
      try_cast(collected_at AS TIMESTAMP) AS collected_ts,
      responsible_name_synthetic,
      sha2(to_json(named_struct(
        'record_id', record_id, 'point_id', point_id,
        'event_time', event_time, 'water_level_cm', water_level_cm,
        'sensor_status', sensor_status, 'is_synthetic', is_synthetic,
        'collected_at', collected_at,
        'responsible_name_synthetic', responsible_name_synthetic
      )), 256) AS source_row_hash
    FROM {namespace}.bronze_sensors
    """
)

# Cada regra produz sua própria linha: uma observação pode violar mais de uma.
# O limite físico adotado para o piloto é 0 a 300 cm; 20 cm é atenção.
spark.sql(
    """
    CREATE OR REPLACE TEMP VIEW current_rejections AS
    SELECT 'weather' AS source_name, source_row_hash,
           cast(event_ts AS STRING) AS business_key, 'W_REQUIRED' AS rule_code,
           collected_at_raw,
           'Horário, fonte ou coleta ausente/inválido' AS rule_description
    FROM weather_normalized
    WHERE event_ts IS NULL OR source_clean IS NULL OR source_clean = ''
       OR collected_ts IS NULL
    UNION ALL
    SELECT 'weather', source_row_hash, cast(event_ts AS STRING),
           'W_COORDINATES', collected_at_raw,
           'Latitude/longitude ausentes ou fora da faixa geográfica'
    FROM weather_normalized
    WHERE latitude_num IS NULL OR longitude_num IS NULL
       OR isnan(latitude_num) OR isnan(longitude_num)
       OR latitude_num NOT BETWEEN -90 AND 90
       OR longitude_num NOT BETWEEN -180 AND 180
    UNION ALL
    SELECT 'weather', source_row_hash, cast(event_ts AS STRING),
           'W_PRECIPITATION', collected_at_raw,
           'Precipitação ausente, negativa ou não numérica'
    FROM weather_normalized
    WHERE precipitation_num IS NULL OR isnan(precipitation_num)
       OR precipitation_num < 0
    UNION ALL
    SELECT 'sensors', source_row_hash, record_id,
           'S_REQUIRED', collected_at_raw,
           'ID, ponto, horário ou coleta ausente/inválido'
    FROM sensor_normalized
    WHERE record_id IS NULL OR trim(record_id) = ''
       OR point_id IS NULL OR trim(point_id) = ''
       OR event_ts IS NULL OR collected_ts IS NULL
    UNION ALL
    SELECT 'sensors', source_row_hash, record_id,
           'S_POINT', collected_at_raw,
           'Ponto fora dos três pontos fictícios do piloto'
    FROM sensor_normalized
    WHERE point_id IS NULL OR point_id NOT IN (
      'GARAGEM_A_RALO_01', 'GARAGEM_A_RALO_02', 'GARAGEM_B_RALO_01'
    )
    UNION ALL
    SELECT 'sensors', source_row_hash, record_id,
           'S_LEVEL', collected_at_raw,
           'Nível ausente, não numérico ou fora de 0 a 300 cm'
    FROM sensor_normalized
    WHERE water_level_num IS NULL OR isnan(water_level_num)
       OR water_level_num NOT BETWEEN 0 AND 300
    UNION ALL
    SELECT 'sensors', source_row_hash, record_id,
           'S_STATUS', collected_at_raw,
           'Leitura sem status operacional ok'
    FROM sensor_normalized
    WHERE sensor_status_clean IS NULL OR sensor_status_clean <> 'ok'
    UNION ALL
    SELECT 'sensors', source_row_hash, record_id,
           'S_SYNTHETIC', collected_at_raw,
           'Flag de dado sintético ausente ou falsa'
    FROM sensor_normalized
    WHERE is_synthetic_bool IS NULL OR is_synthetic_bool <> true
    """
)

print("Rejeições por regra no bruto atual (uma linha pode contar em várias regras):")
spark.sql(
    """
    SELECT source_name, rule_code,
           COUNT(*) AS ocorrencias_bronze,
           COUNT(DISTINCT source_row_hash) AS linhas_distintas_rejeitadas
    FROM current_rejections
    GROUP BY source_name, rule_code
    ORDER BY source_name, rule_code
    """
).show(100, truncate=False)

spark.sql(
    f"""
    MERGE INTO {namespace}.quality_rejections AS target
    USING (SELECT DISTINCT source_name, source_row_hash, business_key,
           rule_code, collected_at_raw, rule_description
           FROM current_rejections) AS source
      ON target.source_name = source.source_name
     AND target.source_row_hash = source.source_row_hash
     AND target.rule_code = source.rule_code
    WHEN NOT MATCHED THEN INSERT (
      source_name, source_row_hash, business_key, rule_code,
      collected_at_raw, rule_description, rejected_at
    ) VALUES (
      source.source_name, source.source_row_hash, source.business_key,
      source.rule_code, source.collected_at_raw,
      source.rule_description, current_timestamp()
    )
    """
)

spark.sql(
    """
    CREATE OR REPLACE TEMP VIEW valid_weather AS
    SELECT
      sha2(concat_ws('|', cast(n.event_ts AS STRING),
                     cast(n.latitude_num AS STRING),
                     cast(n.longitude_num AS STRING), n.source_clean), 256)
        AS weather_key,
      n.event_ts AS event_time,
      n.latitude_num AS latitude,
      n.longitude_num AS longitude,
      n.precipitation_num AS precipitation_mm,
      n.source_clean AS source,
      n.collected_ts AS collected_at,
      n.source_row_hash
    FROM weather_normalized n
    WHERE NOT EXISTS (
      SELECT 1 FROM current_rejections r
      WHERE r.source_name = 'weather'
        AND r.source_row_hash = n.source_row_hash
    )
    """
)
spark.sql(
    """
    CREATE OR REPLACE TEMP VIEW valid_sensors AS
    SELECT n.record_id, n.point_id, n.event_ts AS event_time,
           n.water_level_num AS water_level_cm,
           n.sensor_status_clean AS sensor_status,
           n.is_synthetic_bool AS is_synthetic,
           n.collected_ts AS collected_at,
           n.responsible_name_synthetic,
           n.source_row_hash
    FROM sensor_normalized n
    WHERE NOT EXISTS (
      SELECT 1 FROM current_rejections r
      WHERE r.source_name = 'sensors'
        AND r.source_row_hash = n.source_row_hash
    )
    """
)

print("Chaves duplicadas válidas no bruto, antes da deduplicação:")
spark.sql(
    """
    SELECT 'weather' AS source_name,
           COUNT(*) - COUNT(DISTINCT weather_key) AS duplicatas
    FROM valid_weather
    UNION ALL
    SELECT 'sensors', COUNT(*) - COUNT(DISTINCT record_id)
    FROM valid_sensors
    """
).show(truncate=False)

spark.sql(
    """
    CREATE OR REPLACE TEMP VIEW weather_to_merge AS
    SELECT weather_key, event_time, latitude, longitude,
           precipitation_mm, source, collected_at
    FROM (
      SELECT *, ROW_NUMBER() OVER (
        PARTITION BY weather_key
        ORDER BY collected_at DESC, source_row_hash DESC
      ) AS rn
      FROM valid_weather
    ) WHERE rn = 1
    """
)
spark.sql(
    """
    CREATE OR REPLACE TEMP VIEW sensors_to_merge AS
    SELECT record_id, point_id, event_time, water_level_cm,
           sensor_status, is_synthetic, collected_at,
           responsible_name_synthetic
    FROM (
      SELECT *, ROW_NUMBER() OVER (
        PARTITION BY record_id
        ORDER BY collected_at DESC, source_row_hash DESC
      ) AS rn
      FROM valid_sensors
    ) WHERE rn = 1
    """
)


def count(table: str) -> int:
    return spark.sql(f"SELECT COUNT(*) AS n FROM {namespace}.{table}").first().n


before_weather = count("silver_weather")
before_sensors = count("silver_sensors")

spark.sql(
    f"""
    MERGE INTO {namespace}.silver_weather AS target
    USING weather_to_merge AS source
      ON target.weather_key = source.weather_key
    WHEN MATCHED THEN UPDATE SET
      target.event_time = source.event_time,
      target.latitude = source.latitude,
      target.longitude = source.longitude,
      target.precipitation_mm = source.precipitation_mm,
      target.source = source.source,
      target.collected_at = source.collected_at
    WHEN NOT MATCHED THEN INSERT (
      weather_key, event_time, latitude, longitude,
      precipitation_mm, source, collected_at
    ) VALUES (
      source.weather_key, source.event_time, source.latitude,
      source.longitude, source.precipitation_mm,
      source.source, source.collected_at
    )
    """
)
spark.sql(
    f"""
    MERGE INTO {namespace}.silver_sensors AS target
    USING sensors_to_merge AS source
      ON target.record_id = source.record_id
    WHEN MATCHED THEN UPDATE SET
      target.point_id = source.point_id,
      target.event_time = source.event_time,
      target.water_level_cm = source.water_level_cm,
      target.sensor_status = source.sensor_status,
      target.is_synthetic = source.is_synthetic,
      target.collected_at = source.collected_at,
      target.responsible_name_synthetic = source.responsible_name_synthetic
    WHEN NOT MATCHED THEN INSERT (
      record_id, point_id, event_time, water_level_cm,
      sensor_status, is_synthetic, collected_at,
      responsible_name_synthetic
    ) VALUES (
      source.record_id, source.point_id, source.event_time,
      source.water_level_cm, source.sensor_status,
      source.is_synthetic, source.collected_at,
      source.responsible_name_synthetic
    )
    """
)

print(f"silver_weather: antes={before_weather}; depois={count('silver_weather')}")
print(f"silver_sensors: antes={before_sensors}; depois={count('silver_sensors')}")
print(f"quality_rejections: {count('quality_rejections')} linhas distintas por regra")
spark.sql(
    f"""
    SELECT source_name, rule_code, COUNT(*) AS rejeicoes_distintas
    FROM {namespace}.quality_rejections
    GROUP BY source_name, rule_code
    ORDER BY source_name, rule_code
    """
).show(100, truncate=False)

print("Duplicatas das chaves de negócio na Prata (esperado: nenhuma):")
spark.sql(
    f"""
    SELECT 'weather' AS source_name, weather_key AS business_key, COUNT(*) AS n
    FROM {namespace}.silver_weather GROUP BY weather_key HAVING COUNT(*) > 1
    UNION ALL
    SELECT 'sensors', record_id, COUNT(*)
    FROM {namespace}.silver_sensors GROUP BY record_id HAVING COUNT(*) > 1
    """
).show(truncate=False)
