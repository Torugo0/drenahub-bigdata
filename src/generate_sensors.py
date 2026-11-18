"""Gera leituras sintéticas reproduzíveis para três pontos fictícios.

Uma leitura por ponto e hora UTC. Valores e anomalias são função de seed,
ponto e horário; assim, a mesma observação é idêntica em lotes sobrepostos.
Regras por índice absoluto (hora Unix x 3 + índice do ponto): múltiplos de
41 -> nulo, de 67 -> -5 cm, de 89 -> 350 cm. A prioridade é nessa ordem.
Múltiplos de 53 são duplicados exatamente para testar deduplicação.
Faixa válida: 0 a 300 cm. Atenção: >= 20 cm.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import random
import uuid
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

try:
    from src.collect_weather import _filename_stamp, _utc_iso, default_date_range
except ModuleNotFoundError:  # execução direta: python src/generate_sensors.py
    from collect_weather import _filename_stamp, _utc_iso, default_date_range

LOGGER = logging.getLogger(__name__)
SEED = 20261117
POINTS = (
    "GARAGEM_A_RALO_01",
    "GARAGEM_A_RALO_02",
    "GARAGEM_B_RALO_01",
)
NAMESPACE = uuid.UUID("7a8e3185-2d36-441d-97af-6f6863ab06de")


def build_sensor_rows(start_date: date, end_date: date, seed: int = SEED) -> list[dict]:
    if end_date < start_date:
        raise ValueError("end_date deve ser posterior ou igual a start_date")
    hours = (end_date - start_date).days * 24 + 24
    first_hour = datetime.combine(start_date, datetime.min.time(), tzinfo=timezone.utc)
    rows: list[dict] = []
    for hour_index in range(hours):
        moment = first_hour + timedelta(hours=hour_index)
        absolute_hour = int(moment.timestamp() // 3600)
        for point_index, point_id in enumerate(POINTS):
            index = absolute_hour * len(POINTS) + point_index
            rng = random.Random(f"{seed}:{point_id}:{moment.isoformat()}")
            base = round(rng.uniform(1, 9), 2)
            # Episódios de elevação sintéticos, sem afirmação de medição real.
            elevated = (absolute_hour + point_index * 3) % 19 in (0, 1, 2)
            level = round(base + (rng.uniform(17, 48) if elevated else 0), 2)
            status = "ok"
            if index % 41 == 0:
                level = None
                status = "offline"
            elif index % 67 == 0:
                level = -5.0
                status = "fault"
            elif index % 89 == 0:
                level = 350.0
                status = "fault"
            row = {
                "record_id": str(uuid.uuid5(NAMESPACE, f"{point_id}|{_utc_iso(moment)}")),
                "point_id": point_id,
                "event_time": _utc_iso(moment),
                "water_level_cm": level,
                "sensor_status": status,
                "is_synthetic": True,
                "responsible_name_synthetic": f"RESPONSAVEL_FICTICIO_0{point_index + 1}",
            }
            rows.append(row)
            if index % 53 == 0:
                rows.append(row.copy())
    return rows


def generate_sensors(
    output_dir: str | Path,
    start_date: date | None = None,
    end_date: date | None = None,
    seed: int = SEED,
) -> dict:
    default_start, default_end = default_date_range()
    start_date = start_date or default_start
    end_date = end_date or default_end
    rows = build_sensor_rows(start_date, end_date, seed)
    collected_at = datetime.now(timezone.utc)
    target_dir = Path(output_dir)
    target_dir.mkdir(parents=True, exist_ok=True)
    path = target_dir / f"sensors_{_filename_stamp(collected_at)}.jsonl"
    temp = target_dir / f".{path.name}.tmp"
    try:
        with temp.open("x", encoding="utf-8", newline="\n") as handle:
            for row in rows:
                handle.write(json.dumps({**row, "collected_at": _utc_iso(collected_at)}, ensure_ascii=False) + "\n")
            handle.flush()
            os.fsync(handle.fileno())
        temp.replace(path)
    except BaseException:
        temp.unlink(missing_ok=True)
        raise
    result = {
        "path": str(path.resolve()),
        "row_count": len(rows),
        "unique_record_ids": len({row["record_id"] for row in rows}),
        "null_levels": sum(row["water_level_cm"] is None for row in rows),
        "negative_levels": sum(row["water_level_cm"] is not None and row["water_level_cm"] < 0 for row in rows),
        "above_300_levels": sum(row["water_level_cm"] is not None and row["water_level_cm"] > 300 for row in rows),
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "collected_at": _utc_iso(collected_at),
        "seed": seed,
    }
    LOGGER.info("Sensores sintéticos: %s registros em %s", len(rows), path)
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", default=os.environ.get("DRENAHUB_DATA_DIR", "chegada"))
    parser.add_argument("--start-date", type=date.fromisoformat)
    parser.add_argument("--end-date", type=date.fromisoformat)
    parser.add_argument("--seed", type=int, default=SEED)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    print(json.dumps(generate_sensors(args.output_dir, args.start_date, args.end_date, args.seed), ensure_ascii=False))


if __name__ == "__main__":
    main()
