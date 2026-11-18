"""Testes locais sem substituir a coleta real usada como evidência."""

from __future__ import annotations

import json
import hashlib
from datetime import date, datetime, timezone
from types import SimpleNamespace

import pytest
import requests

from src.collect_weather import collect_weather, default_date_range, fetch_weather
from src.generate_sensors import SEED, build_sensor_rows, generate_sensors
from src.notify_failure import notify_failure
from src.prepare_batch import build_manifest


class FakeResponse:
    url = "https://archive-api.open-meteo.com/v1/archive?test=true"

    def raise_for_status(self):
        return None

    def json(self):
        return {
            "latitude": -23.51,
            "longitude": -46.61,
            "hourly_units": {"precipitation": "mm"},
            "hourly": {
                "time": ["2026-11-10T00:00", "2026-11-10T01:00"],
                "precipitation": [0.0, 1.2],
            },
        }


class FakeSession:
    def get(self, url, params, timeout):
        assert params["timezone"] == "UTC"
        assert timeout == 60
        return FakeResponse()


def test_default_range_uses_sao_paulo_civil_date():
    now = datetime(2026, 11, 18, 0, 10, tzinfo=timezone.utc)
    assert default_date_range(now) == (date(2026, 11, 10), date(2026, 11, 16))


def test_weather_fetch_validates_and_preserves_api_values(tmp_path):
    rows, _ = fetch_weather(date(2026, 11, 10), date(2026, 11, 10), FakeSession())
    assert [row["precipitation_mm"] for row in rows] == [0.0, 1.2]
    assert rows[0]["latitude"] == -23.51
    result = collect_weather(tmp_path, date(2026, 11, 10), date(2026, 11, 10), FakeSession())
    written = [json.loads(line) for line in open(result["path"], encoding="utf-8")]
    assert len(written) == result["row_count"] == 2
    assert all(row["source"] == "open-meteo-archive" and row["collected_at"] for row in written)


def test_weather_http_failure_does_not_write_partial_file(tmp_path):
    class ErrorResponse(FakeResponse):
        def raise_for_status(self):
            raise requests.HTTPError("503")

    class ErrorSession(FakeSession):
        def get(self, url, params, timeout):
            return ErrorResponse()

    with pytest.raises(requests.HTTPError):
        collect_weather(tmp_path, date(2026, 11, 10), date(2026, 11, 10), ErrorSession())
    assert list(tmp_path.iterdir()) == []


def test_sensor_seed_business_keys_and_quality_cases(tmp_path):
    start = date(2026, 11, 10)
    end = date(2026, 11, 16)
    rows = build_sensor_rows(start, end, SEED)
    assert rows == build_sensor_rows(start, end, SEED)
    assert len({row["point_id"] for row in rows}) == 3
    keys = [row["record_id"] for row in rows]
    assert len(keys) > len(set(keys))
    assert any(row["water_level_cm"] is None for row in rows)
    assert any(row["water_level_cm"] is not None and row["water_level_cm"] < 0 for row in rows)
    assert any(row["water_level_cm"] is not None and row["water_level_cm"] > 300 for row in rows)
    assert all(row["is_synthetic"] for row in rows)
    assert all(row["responsible_name_synthetic"].startswith("RESPONSAVEL_FICTICIO_") for row in rows)
    overlapping = build_sensor_rows(date(2026, 11, 11), date(2026, 11, 17), SEED)
    by_id = {row["record_id"]: row for row in rows}
    for row in overlapping:
        if row["record_id"] in by_id:
            assert row == by_id[row["record_id"]]
    result = generate_sensors(tmp_path, start, end)
    written = [json.loads(line) for line in open(result["path"], encoding="utf-8")]
    assert result["row_count"] == len(written) == len(rows)
    assert result["unique_record_ids"] == len(set(keys))


def test_failure_callback_writes_local_notification(tmp_path, monkeypatch):
    monkeypatch.setenv("DRENAHUB_ALERT_DIR", str(tmp_path))
    notify_failure(
        {
            "task_instance": SimpleNamespace(dag_id="drenahub_pipeline", task_id="coletar_open_meteo", run_id="controlled_failure_test"),
            "exception": RuntimeError("falha controlada"),
        }
    )
    alert = json.loads((tmp_path / "airflow_alertas.jsonl").read_text(encoding="utf-8"))
    assert alert["alert_type"] == "airflow_task_failure"
    assert alert["task_id"] == "coletar_open_meteo"
    assert alert["exception_type"] == "RuntimeError"
    assert datetime.fromisoformat(alert["alerted_at"].replace("Z", "+00:00")).tzinfo == timezone.utc


def test_batch_manifest_checks_bytes_and_rejects_misaligned_hours(tmp_path):
    weather_path = tmp_path / "weather.jsonl"
    sensors_path = tmp_path / "sensors.jsonl"
    weather = {
        "event_time": "2026-11-10T00:00:00Z",
        "latitude": -23.51,
        "longitude": -46.61,
        "precipitation_mm": 0.3,
        "source": "open-meteo-archive",
        "collected_at": "2026-11-18T00:25:46Z",
    }
    sensor = {
        "record_id": "id-1",
        "point_id": "GARAGEM_A_RALO_01",
        "event_time": "2026-11-10T00:00:00Z",
        "water_level_cm": 25.0,
        "sensor_status": "ok",
        "is_synthetic": True,
        "responsible_name_synthetic": "RESPONSAVEL_FICTICIO_01",
        "collected_at": "2026-11-18T00:25:45Z",
    }
    weather_path.write_text(json.dumps(weather) + "\n", encoding="utf-8")
    sensors_path.write_text(json.dumps(sensor) + "\n", encoding="utf-8")
    manifest, _, _ = build_manifest(weather_path, sensors_path, "test_run")
    assert manifest["weather"]["sha256"] == hashlib.sha256(weather_path.read_bytes()).hexdigest()
    assert manifest["sensors"]["rows"] == 1
    sensor["event_time"] = "2026-11-10T01:00:00Z"
    sensors_path.write_text(json.dumps(sensor) + "\n", encoding="utf-8")
    with pytest.raises(ValueError, match="Horas dos sensores"):
        build_manifest(weather_path, sensors_path, "test_run")
