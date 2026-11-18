"""Notificação local de falhas da DAG, sem depender de credenciais externas."""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

LOGGER = logging.getLogger(__name__)
REPO_ROOT = Path(__file__).resolve().parents[1]


def notify_failure(context: dict) -> None:
    """Registra alerta JSONL persistente e emite mensagem no log do Airflow.

    O diretório padrão é ignorado pelo Git. DRENAHUB_ALERT_DIR permite um
    destino local alternativo. Não transmite dados para terceiros.
    """
    instance = context.get("task_instance") or context.get("ti")
    alert = {
        "alert_type": "airflow_task_failure",
        "alerted_at": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "dag_id": getattr(instance, "dag_id", context.get("dag_id")),
        "task_id": getattr(instance, "task_id", context.get("task_id")),
        "run_id": getattr(instance, "run_id", context.get("run_id")),
        "exception_type": type(context.get("exception")).__name__ if context.get("exception") else None,
    }
    directory = Path(os.environ.get("DRENAHUB_ALERT_DIR", str(REPO_ROOT / "entrega_local" / "evidencias")))
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / "airflow_alertas.jsonl"
    payload = (json.dumps(alert, ensure_ascii=False) + "\n").encode("utf-8")
    descriptor = os.open(path, os.O_CREAT | os.O_APPEND | os.O_WRONLY, 0o600)
    try:
        os.write(descriptor, payload)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    LOGGER.error("ALERTA DE FALHA AIRFLOW: %s", alert)
