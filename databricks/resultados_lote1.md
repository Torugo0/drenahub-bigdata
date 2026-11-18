# Resultados SQL verificados no Databricks - lote 1

Execução real em **17/11/2026** (São Paulo), após a carga do primeiro par gerado pela DAG. Warehouse: `Serverless Starter Warehouse`; catálogo/esquema: `workspace.default`. Os cinco statements retornaram `SUCCEEDED`. Os JSONs completos da API Statement Execution estão em `entrega_local/evidencias/sql_01.json` a `sql_05.json` (fora do Git). SQL reproduzível: [`consultas.sql`](consultas.sql).

**Escopo:** precipitação real da Open-Meteo de 10 a 16/11/2026 UTC e leituras de três sensores inteiramente sintéticos. O limite operacional usado na Ouro é **20 cm**. Não são medições físicas de alagamento.

## 1. Precipitação acumulada por dia

Statement ID: `01f1e2e8-f7b2-19b9-a5aa-94882972517e`.

| Dia UTC | Precipitação acumulada (mm) | Horas com dados |
| --- | ---: | ---: |
| 10/11 | 0,3 | 24 |
| 11/11 | 0,6 | 24 |
| 12/11 | 15,4 | 24 |
| 13/11 | 3,7 | 24 |
| 14/11 | 1,0 | 24 |
| 15/11 | 2,9 | 24 |
| 16/11 | 7,5 | 24 |

**Interpretação:** a série acumulou **31,4 mm** em 168 horas. O maior total diário ocorreu em 12/11, com 15,4 mm. A soma foi feita somente na tabela de chuva horária, sem multiplicar a métrica pelo número de sensores.

## 2. Maior nível de água por ponto

Statement ID: `01f1e2e8-f8a9-1b97-ae86-4b4307ae9563`.

| Ponto fictício | Maior nível válido (cm) | Leituras válidas |
| --- | ---: | ---: |
| GARAGEM_A_RALO_02 | 54,70 | 159 |
| GARAGEM_B_RALO_01 | 53,02 | 159 |
| GARAGEM_A_RALO_01 | 51,23 | 160 |

**Interpretação:** os máximos sintéticos dos três pontos ficaram acima do limite de atenção de 20 cm. Os valores ilustram a lógica operacional; não indicam ocorrência física.

## 3. Observações acima do limite de atenção

Statement ID: `01f1e2e8-f9a6-1c0e-ac8e-806e90606e38`.

| Ponto fictício | Leituras em atenção | Leituras válidas |
| --- | ---: | ---: |
| GARAGEM_A_RALO_01 | 26 | 160 |
| GARAGEM_B_RALO_01 | 25 | 159 |
| GARAGEM_A_RALO_02 | 22 | 159 |

**Interpretação:** foram **73** observações sintéticas em atenção entre 478 leituras válidas. A diferença entre os pontos é pequena neste lote gerado pela seed `20261117`.

## 4. Horas com chuva e elevação de água

Statement ID: `01f1e2e8-fa9e-12e8-a39c-dd6abbc12496`. Cada linha é uma combinação de hora UTC e ponto com chuva real positiva na estação piloto e leitura sintética acima do limite.

| Hora UTC | Ponto fictício | Chuva (mm/h) | Maior nível (cm) |
| --- | --- | ---: | ---: |
| 10/11 18:00 | GARAGEM_A_RALO_02 | 0,3 | 45,87 |
| 11/11 14:00 | GARAGEM_A_RALO_01 | 0,1 | 31,85 |
| 11/11 15:00 | GARAGEM_A_RALO_01 | 0,1 | 28,59 |
| 11/11 16:00 | GARAGEM_A_RALO_01 | 0,2 | 23,50 |
| 12/11 22:00 | GARAGEM_B_RALO_01 | 0,2 | 40,04 |
| 12/11 23:00 | GARAGEM_B_RALO_01 | 3,1 | 51,10 |
| 13/11 00:00 | GARAGEM_B_RALO_01 | 0,3 | 24,95 |
| 13/11 17:00 | GARAGEM_B_RALO_01 | 0,5 | 33,26 |
| 13/11 18:00 | GARAGEM_B_RALO_01 | 0,2 | 28,40 |
| 13/11 19:00 | GARAGEM_B_RALO_01 | 0,2 | 21,22 |
| 13/11 20:00 | GARAGEM_A_RALO_02 | 0,6 | 27,74 |
| 13/11 21:00 | GARAGEM_A_RALO_02 | 0,3 | 45,05 |
| 14/11 17:00 | GARAGEM_A_RALO_02 | 0,1 | 51,46 |
| 14/11 18:00 | GARAGEM_A_RALO_01 | 0,3 | 30,45 |
| 14/11 19:00 | GARAGEM_A_RALO_01 | 0,4 | 33,31 |
| 14/11 20:00 | GARAGEM_A_RALO_01 | 0,1 | 47,36 |
| 16/11 03:00 | GARAGEM_B_RALO_01 | 0,1 | 40,65 |
| 16/11 04:00 | GARAGEM_B_RALO_01 | 0,6 | 53,02 |
| 16/11 05:00 | GARAGEM_A_RALO_02 | 0,8 | 31,01 |
| 16/11 06:00 | GARAGEM_A_RALO_02 | 2,0 | 54,70 |
| 16/11 07:00 | GARAGEM_A_RALO_02 | 0,6 | 32,54 |

**Interpretação:** há **21** combinações hora/ponto com coocorrência temporal neste lote. A chuva é observação de uma grade da Open-Meteo para São Paulo; o nível é simulado. A consulta não demonstra causalidade nem chuva medida em cada ralo.

## 5. Percentual operacional de atenção

Statement ID: `01f1e2e8-fbab-1526-8c0d-3235bca2831a`.

| Leituras válidas | Em atenção | Percentual em atenção |
| ---: | ---: | ---: |
| 478 | 73 | 15,27% |

**Interpretação:** neste lote sintético, aproximadamente **15 de cada 100 leituras válidas** acionariam atenção operacional para inspeção. Esta métrica **não** representa alagamentos evitados.
