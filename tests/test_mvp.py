"""Contract tests for the Gold export consumed by the Streamlit MVP."""

from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from mvp.data_loader import GoldExportError, load_gold


class GoldLoaderTests(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.path = Path(self.directory.name)
        self._write(
            "fato_monitoramento.csv",
            "leitura_id,local_id,tempo_id,nivel_agua_cm,em_atencao\n"
            "synthetic-1,1,100,12.0,false\n"
            "synthetic-2,2,100,35.0,true\n",
        )
        self._write("dim_local.csv", "local_id,ponto_id\n1,GARAGEM_A_RALO_01\n2,GARAGEM_A_RALO_02\n")
        self._write("dim_tempo.csv", "tempo_id,data_hora\n100,2026-11-17T12:00:00Z\n")
        self._write("precipitacao_horaria.csv", "data_hora,precipitacao_mm\n2026-11-17T12:00:00Z,4.2\n")
        (self.path / "export_metadata.json").write_text(
            json.dumps({"exported_at_utc": "2026-11-17T13:00:00Z"}), encoding="utf-8"
        )

    def tearDown(self) -> None:
        self.directory.cleanup()

    def _write(self, name: str, contents: str) -> None:
        (self.path / name).write_text(contents, encoding="utf-8")

    def test_many_sensor_readings_do_not_multiply_hourly_rain(self) -> None:
        gold = load_gold(self.path)
        self.assertEqual(len(gold.monitoramento), 2)
        self.assertEqual(int(gold.monitoramento["em_atencao"].sum()), 1)
        self.assertEqual(gold.precipitacao["precipitacao_mm"].sum(), 4.2)
        self.assertEqual(gold.monitoramento.iloc[0]["data_hora"].hour, 9)

    def test_missing_export_is_reported(self) -> None:
        (self.path / "dim_local.csv").unlink()
        with self.assertRaisesRegex(GoldExportError, "dim_local.csv"):
            load_gold(self.path)

    def test_dangling_dimension_key_is_rejected(self) -> None:
        self._write("dim_local.csv", "local_id,ponto_id\n1,GARAGEM_A_RALO_01\n")
        with self.assertRaisesRegex(GoldExportError, "chave sem correspondência"):
            load_gold(self.path)

    def test_duplicate_weather_hour_is_rejected(self) -> None:
        self._write(
            "precipitacao_horaria.csv",
            "data_hora,precipitacao_mm\n"
            "2026-11-17T12:00:00Z,4.2\n"
            "2026-11-17T12:00:00+00:00,4.2\n",
        )
        with self.assertRaisesRegex(GoldExportError, "data_hora deve ser preenchida e única"):
            load_gold(self.path)

    def test_duplicate_reading_key_is_rejected(self) -> None:
        self._write(
            "fato_monitoramento.csv",
            "leitura_id,local_id,tempo_id,nivel_agua_cm,em_atencao\n"
            "synthetic-1,1,100,12.0,false\n"
            "synthetic-1,2,100,35.0,true\n",
        )
        with self.assertRaisesRegex(GoldExportError, "leitura_id deve ser preenchida e única"):
            load_gold(self.path)

    def test_streamlit_consumes_curated_export(self) -> None:
        try:
            from streamlit.testing.v1 import AppTest
        except ImportError:
            self.skipTest("Streamlit não instalado neste interpretador")
        app_path = Path(__file__).resolve().parents[1] / "mvp" / "app.py"
        with patch.dict(os.environ, {"DRENAHUB_GOLD_DIR": str(self.path)}):
            app = AppTest.from_file(str(app_path)).run()
        self.assertEqual(len(app.exception), 0)
        self.assertEqual(len(app.error), 0)
        self.assertEqual([metric.value for metric in app.metric], ["2", "2", "1", "4.2 mm"])


if __name__ == "__main__":
    unittest.main()
