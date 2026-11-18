"""Load and validate the reproducible Gold export consumed by the dashboard.

The CSVs are produced by ``databricks/04_export.py``. The dashboard never
creates substitute data when an export is missing or malformed.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd


REQUIRED_FILES = (
    "fato_monitoramento.csv",
    "dim_local.csv",
    "dim_tempo.csv",
    "precipitacao_horaria.csv",
    "export_metadata.json",
)


class GoldExportError(ValueError):
    """The Gold export is absent or violates its published data contract."""


@dataclass(frozen=True)
class GoldData:
    monitoramento: pd.DataFrame
    precipitacao: pd.DataFrame
    atualizado_em: datetime


def _require_columns(frame: pd.DataFrame, filename: str, columns: set[str]) -> None:
    missing = columns.difference(frame.columns)
    if missing:
        raise GoldExportError(f"{filename}: colunas ausentes: {', '.join(sorted(missing))}")


def _unique(frame: pd.DataFrame, filename: str, column: str) -> None:
    if frame[column].isna().any() or frame[column].duplicated().any():
        raise GoldExportError(f"{filename}: {column} deve ser preenchida e única")


def _parse_times(frame: pd.DataFrame, filename: str, column: str) -> None:
    try:
        frame[column] = pd.to_datetime(frame[column], errors="raise", utc=True).dt.tz_convert(
            ZoneInfo("America/Sao_Paulo")
        )
    except (ValueError, TypeError) as exc:
        raise GoldExportError(f"{filename}: {column} contém data/hora inválida") from exc
    if frame[column].isna().any():
        raise GoldExportError(f"{filename}: {column} contém data/hora nula")


def _parse_attention(series: pd.Series) -> pd.Series:
    values = series.astype("string").str.strip().str.lower()
    allowed = {"true": True, "false": False, "1": True, "0": False}
    if values.isna().any() or not values.isin(allowed).all():
        raise GoldExportError("fato_monitoramento.csv: em_atencao deve ser booleano")
    return values.map(allowed).astype(bool)


def load_gold(export_dir: str | Path) -> GoldData:
    """Read a verified UTC export; expose series in São Paulo local time."""
    directory = Path(export_dir)
    missing = [name for name in REQUIRED_FILES if not (directory / name).is_file()]
    if missing:
        raise GoldExportError(
            f"Exportação da Ouro incompleta em {directory}: {', '.join(missing)}"
        )

    try:
        fact = pd.read_csv(directory / "fato_monitoramento.csv", dtype={"leitura_id": "string"})
        places = pd.read_csv(directory / "dim_local.csv")
        times = pd.read_csv(directory / "dim_tempo.csv")
        rain = pd.read_csv(directory / "precipitacao_horaria.csv")
        metadata = json.loads((directory / "export_metadata.json").read_text(encoding="utf-8"))
    except (OSError, UnicodeError, pd.errors.ParserError, json.JSONDecodeError) as exc:
        raise GoldExportError(f"Falha ao ler a exportação da Ouro: {exc}") from exc

    _require_columns(fact, "fato_monitoramento.csv", {"leitura_id", "local_id", "tempo_id", "nivel_agua_cm", "em_atencao"})
    _require_columns(places, "dim_local.csv", {"local_id", "ponto_id"})
    _require_columns(times, "dim_tempo.csv", {"tempo_id", "data_hora"})
    _require_columns(rain, "precipitacao_horaria.csv", {"data_hora", "precipitacao_mm"})

    _unique(fact, "fato_monitoramento.csv", "leitura_id")
    _unique(places, "dim_local.csv", "local_id")
    _unique(times, "dim_tempo.csv", "tempo_id")
    _unique(rain, "precipitacao_horaria.csv", "data_hora")

    _parse_times(times, "dim_tempo.csv", "data_hora")
    _parse_times(rain, "precipitacao_horaria.csv", "data_hora")
    _unique(rain, "precipitacao_horaria.csv", "data_hora")
    try:
        fact["nivel_agua_cm"] = pd.to_numeric(fact["nivel_agua_cm"], errors="raise")
        rain["precipitacao_mm"] = pd.to_numeric(rain["precipitacao_mm"], errors="raise")
    except (ValueError, TypeError) as exc:
        raise GoldExportError("Exportação da Ouro contém métrica não numérica") from exc
    if fact["nivel_agua_cm"].isna().any() or rain["precipitacao_mm"].isna().any():
        raise GoldExportError("Exportação da Ouro contém métrica nula")
    if (rain["precipitacao_mm"] < 0).any():
        raise GoldExportError("precipitacao_horaria.csv: precipitação negativa")
    fact["em_atencao"] = _parse_attention(fact["em_atencao"])

    monitoramento = fact.merge(
        places[["local_id", "ponto_id"]], on="local_id", how="left", validate="many_to_one"
    ).merge(
        times[["tempo_id", "data_hora"]], on="tempo_id", how="left", validate="many_to_one"
    )
    if monitoramento[["ponto_id", "data_hora"]].isna().any().any():
        raise GoldExportError("fato_monitoramento.csv: chave sem correspondência nas dimensões")

    try:
        updated_raw = metadata["exported_at_utc"]
        updated = datetime.fromisoformat(updated_raw.replace("Z", "+00:00"))
        if updated.tzinfo is None:
            raise ValueError("timezone ausente")
    except (KeyError, AttributeError, TypeError, ValueError) as exc:
        raise GoldExportError("export_metadata.json: exported_at_utc inválido") from exc

    return GoldData(
        monitoramento=monitoramento.sort_values("data_hora"),
        precipitacao=rain.sort_values("data_hora"),
        atualizado_em=updated,
    )
