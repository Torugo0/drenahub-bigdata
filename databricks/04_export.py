# Databricks notebook source
"""Exportação reproduzível da Ouro para o MVP local.

Execute após 03_gold.py. Baixe drenahub_gold_export.zip do Volume e extraia
seus cinco arquivos em mvp/dados/. As tabelas são pequenas neste piloto; o
escritor usa toLocalIterator() para não criar part-*.csv aleatórios.
"""

import csv
import json
import re
import shutil
import tempfile
import zipfile
from datetime import date, datetime, timezone
from pathlib import Path


def identifier(value: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z_0-9]*", value):
        raise ValueError(f"Identificador SQL inválido: {value!r}")
    return value


dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "default")
dbutils.widgets.text("volume", "drenahub_raw")
dbutils.widgets.text("export_subdir", "exports")
catalog = identifier(dbutils.widgets.get("catalog"))
schema = identifier(dbutils.widgets.get("schema"))
volume = identifier(dbutils.widgets.get("volume"))
export_subdir = identifier(dbutils.widgets.get("export_subdir"))
namespace = f"`{catalog}`.`{schema}`"
export_dir = Path(f"/Volumes/{catalog}/{schema}/{volume}/{export_subdir}")
spark.sql("SET TIME ZONE 'UTC'")
export_dir.mkdir(parents=True, exist_ok=True)


def csv_value(value):
    if value is None:
        return ""
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, bool):
        return str(value).lower()
    return value


exports = {
    "dim_local.csv": (
        ["local_id", "ponto_id", "nome_local", "is_synthetic"],
        f"""SELECT local_id, ponto_id, nome_local, is_synthetic
            FROM {namespace}.dim_local ORDER BY local_id""",
    ),
    "dim_tempo.csv": (
        ["tempo_id", "data_hora", "data", "hora", "hora_inicio"],
        f"""SELECT tempo_id, data_hora, data, hora, hora_inicio
            FROM {namespace}.dim_tempo ORDER BY data_hora""",
    ),
    "fato_monitoramento.csv": (
        ["leitura_id", "local_id", "tempo_id", "nivel_agua_cm",
         "em_atencao", "sensor_status", "is_synthetic",
         "responsavel_hash", "collected_at"],
        f"""SELECT leitura_id, local_id, tempo_id, nivel_agua_cm,
                   em_atencao, sensor_status, is_synthetic,
                   responsavel_hash, collected_at
            FROM {namespace}.fato_monitoramento ORDER BY leitura_id""",
    ),
    "precipitacao_horaria.csv": (
        ["data_hora", "latitude", "longitude", "precipitacao_mm",
         "source", "collected_at"],
        f"""SELECT data_hora, latitude, longitude, precipitacao_mm,
                   source, collected_at
            FROM {namespace}.precipitacao_horaria ORDER BY data_hora""",
    ),
}

counts = {}
for filename, (fields, query) in exports.items():
    output = export_dir / filename
    count = 0
    with output.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for row in spark.sql(query).toLocalIterator():
            writer.writerow({key: csv_value(row[key]) for key in fields})
            count += 1
    counts[filename] = count
    print(f"{output}: {count} linhas")

metadata = {
    "exported_at_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    "catalog": catalog,
    "schema": schema,
    "source": "Databricks Delta Gold",
    "files": counts,
    "timezone": "UTC",
    "note": "Pontos e responsáveis são sintéticos; chuva é observação real Open-Meteo.",
}
metadata_path = export_dir / "export_metadata.json"
metadata_path.write_text(
    json.dumps(metadata, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
)

zip_path = export_dir / "drenahub_gold_export.zip"
# Volumes não suportam escrita aleatória; ZIP precisa ser montado no disco
# local e depois copiado sequencialmente para o Volume.
local_tmp_dir = Path("/local_disk0/tmp")
with tempfile.TemporaryDirectory(dir=local_tmp_dir if local_tmp_dir.is_dir() else None) as tmp:
    local_zip = Path(tmp) / zip_path.name
    with zipfile.ZipFile(local_zip, "w", zipfile.ZIP_DEFLATED) as archive:
        for filename in [*exports.keys(), metadata_path.name]:
            archive.write(export_dir / filename, arcname=filename)
    shutil.copyfile(local_zip, zip_path)

print(f"Pacote para baixar: {zip_path}")
print("Extraia o pacote em mvp/dados/ e execute streamlit run mvp/app.py")
