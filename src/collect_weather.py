"""Coleta precipitação horária histórica real da Open-Meteo para São Paulo.

O período padrão são sete datas UTC completas anteriores à data local da
coleta em America/Sao_Paulo. Os timestamps das observações são UTC. A API usa
uma célula de grade próxima da coordenada solicitada; latitude e longitude de
cada registro são as coordenadas efetivas devolvidas pela Open-Meteo.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

LOGGER = logging.getLogger(__name__)
API_URL = "https://archive-api.open-meteo.com/v1/archive"
SOURCE = "open-meteo-archive"
PILOT_LATITUDE = -23.5505
PILOT_LONGITUDE = -46.6333
LOCAL_TZ = ZoneInfo("America/Sao_Paulo")
DEFAULT_DAYS = 7


def default_date_range(now: datetime | None = None) -> tuple[date, date]:
    """Retorna sete datas UTC encerradas, escolhidas pelo dia local de São Paulo."""
    now = now or datetime.now(timezone.utc)
    local_today = now.astimezone(LOCAL_TZ).date()
    return local_today - timedelta(days=DEFAULT_DAYS), local_today - timedelta(days=1)


def _session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
    )
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


def _utc_iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _filename_stamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")


def fetch_weather(start_date: date, end_date: date, session: requests.Session | None = None) -> tuple[list[dict], str]:
    """Busca e valida as séries horárias; não preenche ausências com valores inventados."""
    if end_date < start_date:
        raise ValueError("end_date deve ser posterior ou igual a start_date")
    params = {
        "latitude": PILOT_LATITUDE,
        "longitude": PILOT_LONGITUDE,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "hourly": "precipitation",
        "timezone": "UTC",
    }
    client = session or _session()
    response = client.get(API_URL, params=params, timeout=60)
    response.raise_for_status()
    payload = response.json()
    hourly = payload.get("hourly") or {}
    times = hourly.get("time")
    values = hourly.get("precipitation")
    if not isinstance(times, list) or not isinstance(values, list) or len(times) != len(values):
        raise ValueError("Resposta Open-Meteo sem séries horárias alinhadas")
    if not times:
        raise ValueError("Open-Meteo retornou série horária vazia")
    if (payload.get("hourly_units") or {}).get("precipitation") != "mm":
        raise ValueError("Unidade de precipitação inesperada")
    latitude = payload.get("latitude")
    longitude = payload.get("longitude")
    if not isinstance(latitude, (int, float)) or not isinstance(longitude, (int, float)):
        raise ValueError("Coordenadas da grade Open-Meteo ausentes")
    rows: list[dict] = []
    seen: set[str] = set()
    for raw_time, amount in zip(times, values):
        moment = datetime.fromisoformat(raw_time).replace(tzinfo=timezone.utc)
        if not start_date <= moment.date() <= end_date:
            raise ValueError(f"Timestamp fora da janela solicitada: {raw_time}")
        event_time = _utc_iso(moment)
        if event_time in seen:
            raise ValueError(f"Timestamp duplicado na API: {event_time}")
        seen.add(event_time)
        if amount is not None and not isinstance(amount, (int, float)):
            raise ValueError(f"Precipitação não numérica: {amount!r}")
        rows.append(
            {
                "event_time": event_time,
                "latitude": latitude,
                "longitude": longitude,
                "precipitation_mm": amount,
                "source": SOURCE,
            }
        )
    return rows, response.url


def collect_weather(
    output_dir: str | Path,
    start_date: date | None = None,
    end_date: date | None = None,
    session: requests.Session | None = None,
) -> dict:
    """Grava um arquivo JSONL completo por execução, de forma atômica."""
    default_start, default_end = default_date_range()
    start_date = start_date or default_start
    end_date = end_date or default_end
    rows, request_url = fetch_weather(start_date, end_date, session=session)
    collected_at = datetime.now(timezone.utc)
    target_dir = Path(output_dir)
    target_dir.mkdir(parents=True, exist_ok=True)
    path = target_dir / f"weather_{_filename_stamp(collected_at)}.jsonl"
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
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "collected_at": _utc_iso(collected_at),
        "request_url": request_url,
        "latitude": rows[0]["latitude"],
        "longitude": rows[0]["longitude"],
    }
    LOGGER.info("Open-Meteo: %s registros em %s", len(rows), path)
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", default=os.environ.get("DRENAHUB_DATA_DIR", "chegada"))
    parser.add_argument("--start-date", type=date.fromisoformat)
    parser.add_argument("--end-date", type=date.fromisoformat)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    print(json.dumps(collect_weather(args.output_dir, args.start_date, args.end_date), ensure_ascii=False))


if __name__ == "__main__":
    main()
