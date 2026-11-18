"""DrenaHub monitoring MVP, backed only by a validated Gold export."""

from __future__ import annotations

import os
from datetime import date
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import streamlit as st

from data_loader import GoldExportError, load_gold


st.set_page_config(page_title="DrenaHub | Monitoramento", page_icon="💧", layout="wide")
st.title("DrenaHub · Monitoramento piloto")
st.caption("Precipitação real da [Open-Meteo](https://open-meteo.com/en/docs/historical-weather-api) ([CC BY 4.0](https://open-meteo.com/en/licence)) e leituras sintéticas dos sensores DrenaHub")

export_dir = Path(os.environ.get("DRENAHUB_GOLD_DIR", Path(__file__).parent / "dados"))


@st.cache_data(show_spinner="Carregando exportação da Ouro...")
def read_export(path: str, signature: tuple[tuple[str, int], ...]):
    # The signature invalidates Streamlit's cache when any export file changes.
    return load_gold(path)


try:
    signature = tuple(
        sorted((file.name, file.stat().st_mtime_ns) for file in export_dir.glob("*") if file.is_file())
    )
    gold = read_export(str(export_dir), signature)
except (GoldExportError, OSError) as exc:
    st.error(str(exc))
    st.info(
        "Execute a exportação da camada Ouro e coloque os cinco arquivos em "
        f"`{export_dir}`. Também é possível definir `DRENAHUB_GOLD_DIR`."
    )
    st.stop()

updated_local = gold.atualizado_em.astimezone(ZoneInfo("America/Sao_Paulo"))
st.caption(f"Dados atualizados em {updated_local:%d/%m/%Y %H:%M} (São Paulo)")

levels = gold.monitoramento.copy()
rain = gold.precipitacao.copy()
all_dates = pd.concat([levels["data_hora"], rain["data_hora"]]).dropna()
if all_dates.empty:
    st.warning("A exportação da Ouro está vazia.")
    st.stop()

first_date = all_dates.min().date()
last_date = all_dates.max().date()
all_points = sorted(levels["ponto_id"].dropna().unique().tolist())

with st.sidebar:
    st.header("Filtros")
    date_selection = st.date_input(
        "Período", value=(first_date, last_date), min_value=first_date, max_value=last_date
    )
    selected_points = st.multiselect("Pontos", options=all_points, default=all_points)
    st.caption(f"Fonte da exportação: `{export_dir}`")

if isinstance(date_selection, tuple) and len(date_selection) == 2:
    start_date, end_date = date_selection
elif isinstance(date_selection, date):
    start_date = end_date = date_selection
else:
    st.info("Selecione o início e o fim do período.")
    st.stop()

levels = levels.loc[
    levels["data_hora"].dt.date.between(start_date, end_date)
    & levels["ponto_id"].isin(selected_points)
]
rain = rain.loc[rain["data_hora"].dt.date.between(start_date, end_date)]

col1, col2, col3, col4 = st.columns(4)
col1.metric("Pontos monitorados", levels["ponto_id"].nunique())
col2.metric("Leituras válidas", len(levels))
col3.metric("Observações em atenção", int(levels["em_atencao"].sum()))
col4.metric("Chuva acumulada", f"{rain['precipitacao_mm'].sum():.1f} mm")

with st.expander("Pontos monitorados"):
    if selected_points:
        st.dataframe(pd.DataFrame({"Ponto": selected_points}), hide_index=True)
    else:
        st.info("Nenhum ponto selecionado.")

st.subheader("Histórico de precipitação")
if rain.empty:
    st.info("Sem observações de chuva no período selecionado.")
else:
    st.line_chart(rain.set_index("data_hora")["precipitacao_mm"], y_label="mm/h")

st.subheader("Nível de água por ponto")
if levels.empty:
    st.info("Sem leituras válidas para os filtros selecionados.")
else:
    sensor_series = levels.pivot_table(
        index="data_hora", columns="ponto_id", values="nivel_agua_cm", aggfunc="max"
    )
    st.line_chart(sensor_series, y_label="cm")

st.subheader("Ocorrências de atenção")
attention = levels.loc[levels["em_atencao"], ["data_hora", "ponto_id", "nivel_agua_cm"]]
if attention.empty:
    st.info("Nenhuma observação em atenção para os filtros selecionados.")
else:
    st.dataframe(
        attention.rename(
            columns={"data_hora": "Data/hora", "ponto_id": "Ponto", "nivel_agua_cm": "Nível (cm)"}
        ).sort_values("Data/hora", ascending=False),
        hide_index=True,
    )

st.caption(
    "As ocorrências indicam observações acima do limite operacional definido na camada Ouro; "
    "não representam alagamentos confirmados ou evitados."
)
