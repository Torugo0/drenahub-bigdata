# Amostras verificáveis

- `open_meteo_amostra.csv`: oito linhas selecionadas do arquivo real `weather_20261118T002546726950Z.jsonl`, coletado pela DAG em 17/11/2026 às 21:25 (São Paulo). Inclui horas sem precipitação e com precipitação positiva. Latitude e longitude são as coordenadas da célula da grade devolvidas pela Open-Meteo.
- `sensores_sinteticos_amostra.csv`: sete linhas do arquivo `sensors_20261118T002545948372Z.jsonl` da mesma execução. Inclui leitura normal, nível de atenção, nulo, valor negativo, valor acima de 300 cm e duas linhas duplicadas do mesmo `record_id`. **Todas as leituras são sintéticas**, seed `20261117`; não representam sensores físicos.
- `geosampa_ocorrencias_2026.csv`: contexto histórico municipal obtido separadamente, sem cruzamento geográfico com os sensores fictícios.

O manifesto completo de integridade do par da DAG está em `entrega_local/evidencias/lote1_manifesto.json` (fora do Git). Para regenerar as duas amostras do par de arquivos, execute `python -m src.prepare_batch` conforme o README principal.

## Créditos e licenças dos dados

- **Open-Meteo:** [Historical Weather API](https://open-meteo.com/en/docs/historical-weather-api), dados sob [CC BY 4.0](https://open-meteo.com/en/licence). A amostra foi convertida da resposta da API para JSONL e CSV e reduzida a oito linhas; fonte e horário de coleta permanecem nas colunas.
- **Prefeitura de São Paulo / GeoSampa:** [serviço WFS](https://wfs.geosampa.prefeitura.sp.gov.br/geoserver/geoportal/wfs), dados sob [CC BY-SA 4.0](https://prefeitura.sp.gov.br/web/licenciamento/w/licen%C3%A7a-para-uso-de-dados-do-geosampa). A camada `geoportal:risco_ocorrencia_alagamento` foi adaptada de GeoJSON para CSV, limitada a registros de 2026 e publicada sem geometria.
- **Sensores DrenaHub:** dados integralmente sintéticos gerados pelo código deste projeto; não são medições físicas.
