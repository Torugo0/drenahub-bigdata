"""DAG diária: fontes em paralelo, verificação de chegada e logs.

Airflow 3, agendamento às 06:00 em America/Sao_Paulo. Cada execução grava
arquivos novos com o timestamp efetivo da coleta, sem simular uma data passada.
"""

from __future__ import annotations

import json
import logging
import os
import sys
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.sdk import dag, task

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from src.collect_weather import collect_weather  # noqa: E402
from src.generate_sensors import generate_sensors  # noqa: E402
from src.notify_failure import notify_failure  # noqa: E402

DATA_DIR = Path(os.environ.get("DRENAHUB_DATA_DIR", str(REPO_ROOT / "chegada")))
LOGGER = logging.getLogger(__name__)


@dag(
    dag_id="drenahub_pipeline",
    description="Coleta real Open-Meteo e sensores sintéticos DrenaHub",
    schedule="0 6 * * *",
    start_date=pendulum.datetime(2026, 11, 17, tz="America/Sao_Paulo"),
    catchup=False,
    default_args={
        "retries": int(os.environ.get("DRENAHUB_TASK_RETRIES", "2")),
        "retry_delay": timedelta(minutes=5),
        "on_failure_callback": notify_failure,
    },
    tags=["drenahub", "big-data"],
)
def drenahub_pipeline():
    @task(task_id="coletar_open_meteo")
    def weather_task() -> dict:
        result = collect_weather(DATA_DIR)
        LOGGER.info("Coleta Open-Meteo concluída: %s", result)
        return result

    @task(task_id="gerar_sensores_sinteticos")
    def sensors_task() -> dict:
        result = generate_sensors(DATA_DIR)
        LOGGER.info("Sensores sintéticos gerados: %s", result)
        return result

    @task(task_id="verificar_chegada")
    def verify_arrival(weather: dict, sensors: dict) -> dict:
        if (weather["start_date"], weather["end_date"]) != (sensors["start_date"], sensors["end_date"]):
            raise ValueError("Janelas de coleta das duas fontes não coincidem")
        checked = {}
        for source, result in (("weather", weather), ("sensors", sensors)):
            path = Path(result["path"])
            if not path.is_file() or path.stat().st_size == 0:
                raise FileNotFoundError(f"Arquivo de chegada ausente ou vazio: {path}")
            count = 0
            with path.open(encoding="utf-8") as handle:
                for line in handle:
                    row = json.loads(line)
                    if row.get("collected_at") != result["collected_at"]:
                        raise ValueError(f"Timestamp de coleta inconsistente em {path}")
                    count += 1
            if count != result["row_count"] or count == 0:
                raise ValueError(f"Contagem de chegada divergente em {path}: {count} != {result['row_count']}")
            LOGGER.info("Chegada validada: fonte=%s arquivo=%s registros=%d", source, path, count)
            checked[source] = {"path": str(path), "row_count": count}
        return checked

    # Sem dependência mútua: as duas coletas podem rodar em paralelo.
    verify_arrival(weather_task(), sensors_task())


drenahub_pipeline()
