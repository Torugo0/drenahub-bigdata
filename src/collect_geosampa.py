"""Baixa ocorrências públicas do GeoSampa como contexto, sem cruzamento espacial.

A camada WFS consultada contém ocorrências da Defesa Civil de 2026. Os
atributos são preservados em CSV pequeno para consulta exploratória. Esta
fonte é complementar e não participa da DAG nem de inferências de risco.
"""

from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen


WFS_URL = "https://wfs.geosampa.prefeitura.sp.gov.br/geoserver/geoportal/wfs"
LAYER = "geoportal:risco_ocorrencia_alagamento"
DEFAULT_OUTPUT = Path(__file__).resolve().parents[1] / "amostras" / "geosampa_ocorrencias_2026.csv"
FIELDS = (
    "occurrence_id",
    "occurred_at",
    "occurrence_type",
    "district",
    "subprefecture",
    "original_source",
    "source_loaded_at",
    "collected_at",
    "source_url",
)


def collect(output: Path = DEFAULT_OUTPUT, page_size: int = 500) -> dict[str, object]:
    if page_size < 1:
        raise ValueError("page_size deve ser positivo")
    collected_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    rows: list[dict[str, str]] = []
    start_index = 0
    total = None
    while total is None or start_index < total:
        query = urlencode(
            {
                "service": "WFS",
                "version": "2.0.0",
                "request": "GetFeature",
                "typeNames": LAYER,
                "startIndex": start_index,
                "count": page_size,
                "outputFormat": "application/json",
            }
        )
        request = Request(f"{WFS_URL}?{query}", headers={"Accept": "application/json"})
        with urlopen(request, timeout=30) as response:
            payload = json.load(response)
        features = payload.get("features")
        if not isinstance(features, list):
            raise ValueError("GeoSampa não retornou uma lista de ocorrências")
        if total is None:
            total = int(payload["numberMatched"])
        if not features and start_index < total:
            raise ValueError("Paginação GeoSampa terminou antes do total informado")
        for feature in features:
            properties = feature.get("properties") or {}
            rows.append(
                {
                    "occurrence_id": str(properties.get("cd_identificador") or feature.get("id") or ""),
                    "occurred_at": str(properties.get("dt_ocorrencia") or ""),
                    "occurrence_type": str(properties.get("dc_tipo_ocorrencia") or ""),
                    "district": str(properties.get("nm_distrito") or ""),
                    "subprefecture": str(properties.get("nm_subprefeitura") or ""),
                    "original_source": str(properties.get("sg_fonte_original") or ""),
                    "source_loaded_at": str(properties.get("dt_carga") or ""),
                    "collected_at": collected_at,
                    "source_url": WFS_URL,
                }
            )
        start_index += len(features)
    if len(rows) != total:
        raise ValueError(f"Contagem GeoSampa inconsistente: {len(rows)} de {total}")
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_suffix(output.suffix + ".tmp")
    with temporary.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(rows)
    temporary.replace(output)
    return {"rows": len(rows), "collected_at": collected_at, "output": str(output)}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()
    print(json.dumps(collect(args.output), ensure_ascii=False))


if __name__ == "__main__":
    main()
