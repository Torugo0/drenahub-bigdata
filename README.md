# DrenaHub - DrenaData

Pipeline de engenharia de dados da Global Solution FIAP 2026. O piloto combina precipitação horária **real** da Open-Meteo para São Paulo com leituras **sintéticas** de três pontos fictícios de drenagem. O objetivo é apoiar inspeções e manutenção preventiva; as métricas não representam alagamentos confirmados ou evitados.

**Integrantes:** Gabriel Machado Carrara Pimentel, Lourenzo Ramos e Vítor Hugo Rodrigues. Os RMs constam somente no relatório local da disciplina.

## Perguntas do MVP

1. Como variou a precipitação no período monitorado?
2. Quais pontos tiveram as maiores leituras de nível de água?
3. Quantas leituras válidas ultrapassaram o limite operacional de atenção?
4. Em quais horas houve chuva na estação piloto e elevação sintética nos pontos?
5. Qual percentual de leituras válidas exigiria atenção operacional?

## Arquitetura

```text
Open-Meteo Archive (real) ─┐
                           ├─> DAG Airflow 3 ─> chegada/*.jsonl ─> Databricks Volume
Sensores DrenaHub (sint.) ──┘                              │
                                                           v
                                 Bronze (COPY INTO) ─> Prata (qualidade + MERGE)
                                                           │
                                                           v
                            MVP Streamlit <─ CSV/ZIP <─ Ouro (fato + dimensões)

GeoSampa WFS ─> amostras/geosampa_ocorrencias_2026.csv (contexto separado)
```

As duas tarefas da DAG não dependem uma da outra e convergem em `verificar_chegada`. O `COPY INTO` mantém controle de arquivos já carregados; a Prata deduplica chaves de negócio e faz `MERGE`. A precipitação horária fica em tabela própria na Ouro, de modo que três leituras de sensores na mesma hora não tripliquem a chuva acumulada.

### Fontes

| Fonte | Uso | Formato e frequência |
| --- | --- | --- |
| [Open-Meteo Historical Weather API](https://open-meteo.com/en/docs/historical-weather-api) | Precipitação horária real, histórico | JSON da API para JSONL; coleta diária dos sete dias civis completos anteriores em São Paulo |
| Sensores DrenaHub | Três pontos fictícios, dados explicitamente sintéticos | JSONL; geração diária com seed `20261117` |
| [GeoSampa / Defesa Civil](https://metadados.geosampa.prefeitura.sp.gov.br/geonetwork/srv/api/records/432c06b1-03a1-4b1f-9210-423e1b58e869) | Contexto público complementar | WFS GeoJSON para CSV; coleta separada, sem cruzamento espacial |

A coordenada piloto solicitada à Open-Meteo é `(-23.5505, -46.6333)`; o JSONL registra as coordenadas da grade efetivamente retornadas pela API. O GeoSampa foi consultado pela camada WFS `geoportal:risco_ocorrencia_alagamento`: 315 ocorrências de 2026 foram obtidas em 17/11/2026, com datas de 04/01 a 27/10. O CSV contém apenas atributos públicos, sem geometria e sem inferência de risco por ponto.

Os créditos, licenças e transformações das amostras públicas estão em [`amostras/README.md`](amostras/README.md): Open-Meteo sob CC BY 4.0 e GeoSampa sob CC BY-SA 4.0. O projeto usa a API gratuita da Open-Meteo apenas para fins acadêmicos.

### Dados sintéticos e qualidade

Os pontos são `GARAGEM_A_RALO_01`, `GARAGEM_A_RALO_02` e `GARAGEM_B_RALO_01`. `src/generate_sensors.py` gera um UUID estável por ponto e hora UTC, valores reproduzíveis para a seed, episódios sintéticos de elevação, nulos, valores negativos, valores acima de 300 cm e duplicações controladas. O intervalo válido adotado para o piloto é 0 a 300 cm; o limite de atenção padrão é 20 cm. Cada linha traz `is_synthetic=true` e nome fictício de responsável. O nome é convertido em SHA-256 na Ouro; o MVP não o exibe.

## Estrutura

| Caminho | Finalidade |
| --- | --- |
| `dags/drenahub_pipeline.py` | DAG diária, coletas paralelas, verificação, logs e retries |
| `src/collect_weather.py` | Coleta e validação da Open-Meteo |
| `src/generate_sensors.py` | Geração reproduzível dos sensores sintéticos |
| `src/notify_failure.py` | Callback que registra alertas de falha em JSONL local e no log |
| `src/prepare_batch.py` | Validação de par de arquivos, SHA-256 e amostras pequenas |
| `src/collect_geosampa.py` | Coleta complementar WFS, fora da DAG |
| `databricks/01_bronze.py` | Ingestão incremental JSONL em Delta via `COPY INTO` |
| `databricks/02_silver.py` | Conversão de tipos, qualidade, rejeições, deduplicação e `MERGE` |
| `databricks/03_gold.py` | `fato_monitoramento`, `dim_local`, `dim_tempo` e chuva horária separada |
| `databricks/04_export.py` | CSVs da Ouro, metadados e ZIP para o MVP |
| `databricks/consultas.sql` | Cinco consultas obrigatórias |
| `databricks/resultados_lote1.md` | Resultados e interpretação das cinco consultas executadas no primeiro lote |
| `amostras/` | Pequenos recortes verificáveis das três fontes |
| `run_airflow_wsl.sh` | Launcher Airflow 3 para WSL2 Ubuntu |
| `mvp/app.py`, `mvp/data_loader.py` | Dashboard e validação da exportação |
| `tests/` | Testes locais de coleta, geração e contrato do MVP |
| `entrega_local/` | PDF, checklist e evidências locais; **inteiramente ignorada pelo Git** |
| `requirements.txt`, `.env.example` | Dependências locais e exemplo de configuração sem segredos |

`chegada/`, `mvp/dados/*.csv`, `.env` e os arquivos de execução do Airflow também são ignorados pelo Git. Não há credenciais no repositório.

## Instalação local (Windows)

```powershell
py -3.10 -m venv .venv
.venv\Scripts\python.exe -m pip install -r requirements.txt
.venv\Scripts\python.exe -m pytest -q
```

Para gerar lotes diretamente, sem Airflow:

```powershell
.venv\Scripts\python.exe -m src.collect_weather --output-dir chegada
.venv\Scripts\python.exe -m src.generate_sensors --output-dir chegada
.venv\Scripts\python.exe -m src.collect_geosampa --output amostras/geosampa_ocorrencias_2026.csv
```

Cada execução cria `weather_YYYYMMDDTHHMMSSffffffZ.jsonl` e `sensors_YYYYMMDDTHHMMSSffffffZ.jsonl`, com timestamp **real** de coleta em UTC. A janela padrão usa os sete dias completos anteriores à data civil de São Paulo. Para uma janela explícita, acrescente `--start-date AAAA-MM-DD --end-date AAAA-MM-DD` aos dois coletores. Não altere datas de coleta para aparentar execuções em dias diferentes.

`amostras/open_meteo_amostra.csv` e `amostras/sensores_sinteticos_amostra.csv` contêm pequenos recortes **do par realmente coletado pela DAG**; `amostras/README.md` descreve a proveniência. Para recriar as amostras e o manifesto de integridade do lote 1:

```powershell
.venv\Scripts\python.exe -m src.prepare_batch --weather chegada/weather_20261118T002546726950Z.jsonl --sensors chegada/sensors_20261118T002545948372Z.jsonl --run-id 'manual__2026-11-18T00:25:43.055322+00:00' --manifest entrega_local/evidencias/lote1_manifesto.json --sample-dir amostras
```

## Airflow 3

O Airflow roda no WSL2 Ubuntu neste computador; Docker não foi encontrado. A instalação verificada usa Python 3.14 e Airflow 3.4.0, com constraints oficiais:

```bash
python3 -m venv ~/.venvs/drenahub-airflow
~/.venvs/drenahub-airflow/bin/python -m pip install apache-airflow==3.4.0 --constraint https://raw.githubusercontent.com/apache/airflow/constraints-3.4.0/constraints-3.14.txt
```

Na raiz do projeto, execute `wsl -d Ubuntu -- bash run_airflow_wsl.sh` no PowerShell, ou `bash run_airflow_wsl.sh` dentro do Ubuntu. O launcher aponta `AIRFLOW__CORE__DAGS_FOLDER` para `dags/`, cria o serviço standalone e expõe `http://localhost:8080`. Credenciais locais geradas pelo Airflow ficam fora do Git. A DAG `drenahub_pipeline` roda diariamente às **06h em America/Sao_Paulo**; também pode ser disparada pela UI. `DRENAHUB_DATA_DIR` pode apontar para outro diretório de chegada, mas deve ser absoluto ao rodar no WSL. As duas coletas são paralelas e `verificar_chegada` falha se arquivo, contagem ou timestamp estiver inconsistente.

Uma execução real pelo scheduler em **17/11/2026 às 21h25** (`manual__2026-11-18T00:25:43.055322+00:00`) terminou com as três tarefas em sucesso. Os intervalos das coletas se sobrepuseram. Graph, Grid, horários e logs estão em `entrega_local/evidencias/`. O callback `on_failure_callback` grava um alerta JSONL local e no log; uma falha isolada com `DRENAHUB_TASK_RETRIES=0` comprovou dois alertas. O padrão de produção é duas retentativas com atraso de cinco minutos. `DRENAHUB_ALERT_DIR` personaliza o diretório local dos alertas. Nenhuma notificação externa é enviada.

Durante esta sessão de desenvolvimento, o Airflow foi iniciado e `http://localhost:8080` respondeu. A próxima execução agendada foi confirmada para **18/11/2026 às 06h (São Paulo)** no momento da configuração; sua realização exige o computador ligado, acordado e o WSL ativo. Em outra sessão ou em um clone, inicie o launcher novamente. O lote 2 só será considerado concluído depois que os novos arquivos e sua data real forem verificados.

## Databricks Free Edition

1. Confirme no Catalog os nomes do catálogo e esquema disponíveis. Os widgets dos notebooks usam `workspace.default` como padrão; ajuste se o workspace diferir.
2. Crie um Volume gerenciado `drenahub_raw` e a pasta `chegada` nele. Com os nomes padrão, o caminho é `/Volumes/workspace/default/drenahub_raw/chegada/`.
3. Envie os pares `weather_*.jsonl` e `sensors_*.jsonl` de **dois dias de coleta reais**. O primeiro lote já foi enviado; o segundo aguarda uma coleta em outro dia civil. Registre a lista de arquivos no Volume.
4. Importe os arquivos `.py` de `databricks/` como notebooks e execute `01_bronze.py`, `02_silver.py`, `03_gold.py` e `04_export.py`, nessa ordem. Ajuste os widgets `catalog`, `schema` e `volume` caso necessário.
5. Para demonstrar a Bronze, execute após o primeiro lote, anote contagens; envie o segundo lote, execute e anote; execute outra vez sem arquivos novos e confirme incremento zero. O notebook imprime contagens antes/depois e por `collected_at`.
6. A Prata imprime rejeições por regra e duplicatas antes da deduplicação, depois faz `MERGE` por `weather_key` e `record_id`. Confirme ausência de chaves duplicadas na Prata.
7. A Ouro cria `fato_monitoramento` (uma leitura válida por `leitura_id`), `dim_local`, `dim_tempo` e `precipitacao_horaria` (uma linha por hora). O notebook valida as chaves e contagens.
8. No SQL Editor, selecione o catálogo/esquema usado e execute **separadamente** os cinco blocos de `databricks/consultas.sql`. Salve resultados e prints em `entrega_local/evidencias/`.
9. `04_export.py` grava `/Volumes/workspace/default/drenahub_raw/exports/drenahub_gold_export.zip`. Baixe o ZIP e extraia seus cinco arquivos em `mvp/dados/`.

### Resultado verificado do primeiro lote

O Volume gerenciado `workspace.default.drenahub_raw` recebeu o primeiro par da DAG. Os notebooks Bronze, Prata, Ouro e exportação foram executados no Databricks Free Edition, com resultados consultados no warehouse `Serverless Starter Warehouse` em 17/11/2026 (São Paulo). As evidências JSON da execução ficam em `entrega_local/evidencias/` e as cinco consultas completas, com resultados e interpretação, em [`databricks/resultados_lote1.md`](databricks/resultados_lote1.md).

| Camada / verificação | Resultado real do lote 1 |
| --- | --- |
| Bronze, antes → depois | `bronze_weather` 0 → 168; `bronze_sensors` 0 → 514 |
| Bronze, reexecução sem arquivo novo | 168 e 514; incremento zero nas duas tabelas |
| Prata | 168 horas meteorológicas; 478 leituras válidas distintas de sensores |
| Qualidade da Prata | 26 linhas de sensores distintas rejeitadas; 52 violações registradas, pois as mesmas 26 infringiram `S_LEVEL` e `S_STATUS` |
| Ouro | `dim_local` 3; `dim_tempo` 168; `fato_monitoramento` 478; `precipitacao_horaria` 168 |
| LGPD e atenção | 478 fatos com hash do responsável fictício, 3 hashes distintos, 73 fatos em atenção |
| Integridade da Ouro | Zero fatos sem dimensão, zero `leitura_id` duplicado e zero hora de chuva duplicada |

As **cinco consultas SQL foram executadas com status `SUCCEEDED`**: chuva acumulada **31,4 mm** em sete dias (maior dia: 12/11, **15,4 mm**); níveis máximos sintéticos de **51,23 a 54,70 cm** nos três pontos; **73** observações em atenção; **21** combinações hora/ponto com chuva na grade piloto e elevação sintética; **15,27%** das 478 leituras válidas em atenção. A quinta consulta mede demanda operacional de inspeção, não alagamentos evitados. O detalhamento por linha e a interpretação estão em [`databricks/resultados_lote1.md`](databricks/resultados_lote1.md).

Esses resultados comprovam somente o **primeiro lote**. A ingestão do segundo lote em outro dia e a contagem incremental subsequente continuam pendentes; nenhuma data de coleta foi simulada.

## Dashboard

Neste checkout de desenvolvimento, a exportação real da Ouro do primeiro lote foi extraída em `mvp/dados/` e validada por `load_gold`. Os CSVs e o JSON são ignorados pelo Git; em um clone novo, execute `04_export.py`, baixe `drenahub_gold_export.zip` do Volume e extraia seus cinco arquivos em `mvp/dados/` antes de iniciar o dashboard:

```powershell
.venv\Scripts\python.exe -m streamlit run mvp/app.py
```

O MVP mostra histórico de precipitação, séries de nível de água, pontos, observações em atenção e data de atualização da exportação. Sem os cinco arquivos curados, exibe uma mensagem de ausência; não usa valores hardcoded. `DRENAHUB_GOLD_DIR` pode apontar para outra pasta de exportação. Os CSVs e metadados exportados não são versionados.

## Testes e resultados observados

Execute novamente:

```powershell
.venv\Scripts\python.exe -m pytest -q
```

Em 17/11/2026 às 21h25 (São Paulo), a DAG executada pelo scheduler gerou o primeiro par de arquivos: **168** horas reais da Open-Meteo (10 a 16/11 UTC, 31,4 mm acumulados na série retornada) e **514** linhas sintéticas de sensores (504 IDs únicos, 10 linhas duplicadas, 13 níveis nulos, 8 negativos e 6 acima de 300 cm). A UI Graph/Grid mostrou as três tarefas em sucesso; os intervalos de execução das duas coletas se sobrepuseram. Esses números foram verificados nos JSONL locais, com manifesto SHA-256. O GeoSampa retornou 315 ocorrências públicas de 2026. Os **12 testes locais** passaram e o servidor Streamlit respondeu HTTP 200. No Databricks, Bronze, Prata, Ouro e as cinco consultas SQL foram verificadas com os resultados acima. A exportação real da Ouro foi carregada localmente pelo `load_gold` sem erro.

**Limitações atuais:** o segundo lote precisa ocorrer em outro dia civil real e ser enviado ao Volume; depois será necessário repetir Bronze, Prata, Ouro, consultas e exportação para registrar as contagens incrementais. O dashboard já foi capturado com os dados curados em `entrega_local/evidencias/`. O repositório deve estar público durante a correção FIAP.

O relatório PDF e os prints são mantidos em `entrega_local/`, fora do GitHub.
