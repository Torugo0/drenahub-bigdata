"""Valida um par de chegada e gera manifesto SHA-256 e amostras pequenas.

Uso: python -m src.prepare_batch --weather chegada/weather_...jsonl \
    --sensors chegada/sensors_...jsonl --run-id manual__... \
    --manifest entrega_local/evidencias/lote1_manifesto.json --sample-dir amostras
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from src.generate_sensors import POINTS

ROOT = Path(__file__).resolve().parents[1]


def _inspect(path: Path, source: str) -> tuple[dict, list[dict]]:
    if not path.is_file() or path.stat().st_size == 0:
        raise FileNotFoundError(f"Arquivo ausente ou vazio: {path}")
    digest = hashlib.sha256()
    rows: list[dict] = []
    with path.open("rb") as handle:
        for raw_line in handle:
            digest.update(raw_line)
            if not raw_line.strip():
                raise ValueError(f"Linha em branco em {path}")
            rows.append(json.loads(raw_line))
    if not rows:
        raise ValueError(f"Nenhum registro em {path}")
    collected_times = {row.get("collected_at") for row in rows}
    if len(collected_times) != 1:
        raise ValueError(f"Horários de coleta inconsistentes em {path}")
    if source == "weather":
        if not all(row.get("source") == "open-meteo-archive" for row in rows):
            raise ValueError("Arquivo meteorológico não identificado como Open-Meteo archive")
    elif source == "sensors":
        if not all(row.get("is_synthetic") is True and row.get("point_id") in POINTS for row in rows):
            raise ValueError("Arquivo de sensores contém origem ou ponto inesperado")
    else:
        raise ValueError(source)
    event_times = [datetime.fromisoformat(row["event_time"].replace("Z", "+00:00")) for row in rows]
    manifest = {
        "file": path.name,
        "bytes": path.stat().st_size,
        "sha256": digest.hexdigest(),
        "rows": len(rows),
        "collected_at": next(iter(collected_times)),
        "min_event_time": min(event_times).isoformat().replace("+00:00", "Z"),
        "max_event_time": max(event_times).isoformat().replace("+00:00", "Z"),
    }
    if source == "sensors":
        counts = Counter(row["record_id"] for row in rows)
        manifest["unique_record_ids"] = len(counts)
        manifest["duplicate_rows"] = sum(count - 1 for count in counts.values())
        manifest["null_levels"] = sum(row["water_level_cm"] is None for row in rows)
        manifest["negative_levels"] = sum(row["water_level_cm"] is not None and row["water_level_cm"] < 0 for row in rows)
        manifest["above_300_levels"] = sum(row["water_level_cm"] is not None and row["water_level_cm"] > 300 for row in rows)
    return manifest, rows


def build_manifest(weather_path: Path, sensors_path: Path, run_id: str) -> tuple[dict, list[dict], list[dict]]:
    weather_info, weather_rows = _inspect(weather_path, "weather")
    sensors_info, sensors_rows = _inspect(sensors_path, "sensors")
    weather_hours = {row["event_time"] for row in weather_rows}
    sensor_hours = {row["event_time"] for row in sensors_rows}
    if weather_hours != sensor_hours:
        raise ValueError("Horas dos sensores e da meteorologia não coincidem")
    weather_date = datetime.fromisoformat(weather_info["collected_at"].replace("Z", "+00:00")).astimezone(ZoneInfo("America/Sao_Paulo")).date()
    sensors_date = datetime.fromisoformat(sensors_info["collected_at"].replace("Z", "+00:00")).astimezone(ZoneInfo("America/Sao_Paulo")).date()
    if weather_date != sensors_date:
        raise ValueError("Fontes coletadas em dias locais diferentes")
    manifest = {
        "run_id": run_id,
        "collection_date_sao_paulo": weather_date.isoformat(),
        "manifest_generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "weather": weather_info,
        "sensors": sensors_info,
        "note": "Weather é dado real Open-Meteo; sensores são inteiramente sintéticos.",
    }
    return manifest, weather_rows, sensors_rows


def _write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def write_samples(directory: Path, weather_rows: list[dict], sensor_rows: list[dict]) -> None:
    # Uma pequena seleção real que mostre precipitação zero e positiva, quando houver.
    dry = [row for row in weather_rows if row["precipitation_mm"] == 0][:3]
    rain = [row for row in weather_rows if row["precipitation_mm"] is not None and row["precipitation_mm"] > 0][:5]
    weather_sample = sorted({row["event_time"]: row for row in (dry + rain or weather_rows[:8])}.values(), key=lambda row: row["event_time"])
    _write_csv(directory / "open_meteo_amostra.csv", weather_sample)

    sample: list[dict] = []
    seen: set[str] = set()

    def add_first(predicate) -> None:
        for row in sensor_rows:
            if predicate(row) and row["record_id"] not in seen:
                sample.append(row)
                seen.add(row["record_id"])
                return

    add_first(lambda row: row["water_level_cm"] is not None and 0 <= row["water_level_cm"] < 20)
    add_first(lambda row: row["water_level_cm"] is not None and 20 <= row["water_level_cm"] <= 300)
    add_first(lambda row: row["water_level_cm"] is None)
    add_first(lambda row: row["water_level_cm"] is not None and row["water_level_cm"] < 0)
    add_first(lambda row: row["water_level_cm"] is not None and row["water_level_cm"] > 300)
    counts = Counter(row["record_id"] for row in sensor_rows)
    duplicate_id = next((record_id for record_id, count in counts.items() if count > 1), None)
    if duplicate_id:
        sample.extend(row for row in sensor_rows if row["record_id"] == duplicate_id)
    if len(sample) < 6:
        raise ValueError("Amostra não cobriu os casos de qualidade exigidos")
    _write_csv(directory / "sensores_sinteticos_amostra.csv", sample)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--weather", required=True, type=Path)
    parser.add_argument("--sensors", required=True, type=Path)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--manifest", required=True, type=Path)
    parser.add_argument("--sample-dir", type=Path)
    args = parser.parse_args()
    manifest, weather_rows, sensor_rows = build_manifest(args.weather, args.sensors, args.run_id)
    args.manifest.parent.mkdir(parents=True, exist_ok=True)
    args.manifest.write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    if args.sample_dir:
        write_samples(args.sample_dir, weather_rows, sensor_rows)
    print(json.dumps(manifest, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
