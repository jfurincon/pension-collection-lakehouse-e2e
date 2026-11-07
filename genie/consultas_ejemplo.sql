-- =============================================================================
-- Consultas de ejemplo del Genie Agent "Asistente de Recaudo y Cartera".
-- Cada bloque es un par pregunta -> SQL que Genie usa como patrón.
-- En Databricks: Genie Agents -> Instructions -> SQL Queries.
-- =============================================================================

-- ¿Qué regional tiene más saldo vencido?
SELECT Regional, MEASURE(`Saldo vencido total`) AS saldo_vencido
FROM recaudo_dev.gold.metricas_cartera
WHERE Periodo = '2025-12'
GROUP BY Regional
ORDER BY saldo_vencido DESC;

-- ¿Cómo ha evolucionado el cumplimiento por año?
SELECT Anio,
       MEASURE(`Porcentaje de cumplimiento`)  AS cumplimiento,
       MEASURE(`Porcentaje de pago oportuno`) AS pago_oportuno
FROM recaudo_dev.gold.metricas_recaudo
GROUP BY Anio
ORDER BY Anio;

-- ¿Cuál es la brecha de recaudo?
SELECT MEASURE(`Brecha de recaudo`) AS brecha_recaudo
FROM recaudo_dev.gold.metricas_recaudo;

-- ¿Qué canal de cobranza funciona mejor en cada tramo?
SELECT `Tramo de mora al gestionar`, `Tipo de gestion`,
       MEASURE(`Gestiones`)     AS gestiones,
       MEASURE(`Tasa de exito`) AS tasa_exito
FROM recaudo_dev.gold.metricas_cobranza
GROUP BY ALL
ORDER BY `Tramo de mora al gestionar`, tasa_exito DESC;

-- ¿Qué empresas al día tienen más riesgo de caer en mora?
SELECT d.RazonSocial, d.ActividadEconomica AS Sector, d.Regional,
       s.ProbabilidadIncumplimiento, s.NivelRiesgo,
       array_join(s.Razones, ' · ') AS Razones
FROM recaudo_dev.ml.score_riesgo s
JOIN recaudo_dev.ml.features_aportante_mes f
  ON f.AportanteID = s.AportanteID AND f.Periodo = s.Periodo
JOIN recaudo_dev.gold.dim_aportante d
  ON d.AportanteID = s.AportanteID AND d.EsActual
WHERE s.Periodo = '2025-12' AND f.DiasMora = 0
ORDER BY s.ProbabilidadIncumplimiento DESC
LIMIT 20;

-- ¿Qué porcentaje de empresas en mora de 1 a 30 días se recupera al mes siguiente?
SELECT TramoDestino,
       sum(Aportantes) / sum(sum(Aportantes)) OVER () AS porcentaje
FROM recaudo_dev.gold.agg_transicion_tramos
WHERE TramoOrigen = '1-30' AND PeriodoOrigen BETWEEN '2025-01' AND '2025-11'
GROUP BY TramoDestino;
