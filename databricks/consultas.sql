-- DrenaHub / Global Solution FIAP 2026
-- Selecione o catálogo e esquema usados nos notebooks (padrão:
-- workspace.default) no SQL Editor antes de executar as consultas.
-- Execute cada bloco separadamente para registrar resultado e print.
-- Todas as datas e horas são interpretadas em UTC.

-- 1. Precipitação acumulada por dia na coordenada piloto Open-Meteo.
-- A soma usa apenas a tabela horária: a chuva não é repetida por sensor.
SELECT
  to_date(data_hora) AS dia_utc,
  ROUND(SUM(precipitacao_mm), 2) AS precipitacao_acumulada_mm,
  COUNT(*) AS horas_com_dados
FROM precipitacao_horaria
GROUP BY to_date(data_hora)
ORDER BY dia_utc;

-- 2. Maior nível válido de água por ponto fictício.
SELECT
  l.ponto_id,
  ROUND(MAX(f.nivel_agua_cm), 2) AS maior_nivel_agua_cm,
  COUNT(*) AS observacoes_validas
FROM fato_monitoramento f
JOIN dim_local l ON f.local_id = l.local_id
GROUP BY l.ponto_id
ORDER BY maior_nivel_agua_cm DESC, l.ponto_id;

-- 3. Quantidade de observações acima do limite de atenção (20 cm por padrão).
-- O flag em_atencao foi calculado com o widget attention_cm na carga Ouro.
SELECT
  l.ponto_id,
  SUM(CASE WHEN f.em_atencao THEN 1 ELSE 0 END) AS observacoes_em_atencao,
  COUNT(*) AS observacoes_validas
FROM fato_monitoramento f
JOIN dim_local l ON f.local_id = l.local_id
GROUP BY l.ponto_id
ORDER BY observacoes_em_atencao DESC, l.ponto_id;

-- 4. Horas com chuva na estação piloto e nível alto no ponto monitorado.
-- Associação temporal para inspeção; não atribui causalidade nem chuva local.
WITH nivel_por_hora AS (
  SELECT
    t.hora_inicio,
    l.ponto_id,
    MAX(f.nivel_agua_cm) AS maior_nivel_agua_cm,
    SUM(CASE WHEN f.em_atencao THEN 1 ELSE 0 END) AS leituras_em_atencao
  FROM fato_monitoramento f
  JOIN dim_tempo t ON f.tempo_id = t.tempo_id
  JOIN dim_local l ON f.local_id = l.local_id
  GROUP BY t.hora_inicio, l.ponto_id
)
SELECT
  n.hora_inicio AS hora_utc,
  n.ponto_id,
  p.precipitacao_mm,
  ROUND(n.maior_nivel_agua_cm, 2) AS maior_nivel_agua_cm,
  n.leituras_em_atencao
FROM nivel_por_hora n
JOIN precipitacao_horaria p ON n.hora_inicio = p.data_hora
WHERE p.precipitacao_mm > 0 AND n.leituras_em_atencao > 0
ORDER BY hora_utc, n.ponto_id;

-- 5. Impacto operacional: percentual de leituras válidas que exigem atenção.
-- Denominador = fato_monitoramento, composta só de leituras aprovadas na Prata.
-- Esta métrica não representa alagamentos evitados.
SELECT
  COUNT(*) AS observacoes_validas,
  SUM(CASE WHEN em_atencao THEN 1 ELSE 0 END) AS observacoes_em_atencao,
  CASE WHEN COUNT(*) = 0 THEN NULL
       ELSE ROUND(
         100.0 * SUM(CASE WHEN em_atencao THEN 1 ELSE 0 END) / COUNT(*), 2
       ) END AS percentual_em_atencao
FROM fato_monitoramento;
