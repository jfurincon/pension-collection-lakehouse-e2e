-- =============================================================================
-- Metric views: definiciones de negocio escritas UNA sola vez.
--
-- Una metric view separa las dimensiones (por qué se agrupa) de las medidas
-- (cómo se calcula cada KPI). Tableros, consultas SQL y Genie leen de aquí, así
-- que "% de cumplimiento" significa lo mismo en todas partes.
--
-- Cómo correrlo: ábrelo en el editor SQL de Databricks, con un SQL warehouse,
-- y ejecuta todo. Requiere que el pipeline gold_recaudo haya corrido.
-- Si tu catálogo no se llama recaudo_dev, reemplázalo en todo el archivo.
--
-- Consultar una metric view:
--   SELECT Periodo, Regional, MEASURE(`Porcentaje de cumplimiento`)
--   FROM recaudo_dev.gold.metricas_recaudo
--   GROUP BY ALL ORDER BY Periodo;
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Recaudo: esperado vs. pagado
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW recaudo_dev.gold.metricas_recaudo
WITH METRICS
LANGUAGE YAML
COMMENT 'Recaudo de aportes a pensión: valor esperado, valor recaudado, cumplimiento y oportunidad de pago.'
AS $$
version: 1.1
source: recaudo_dev.gold.fact_aporte
joins:
  - name: aportante
    source: recaudo_dev.gold.dim_aportante
    'on': source.AportanteSK = aportante.AportanteSK
dimensions:
  - name: Periodo
    expr: source.Periodo
    comment: "Periodo de cotización (yyyy-MM). Se paga en el mes siguiente."
  - name: Anio
    expr: LEFT(source.Periodo, 4)
  - name: Regional
    expr: aportante.Regional
  - name: Departamento
    expr: aportante.Departamento
  - name: Ciudad
    expr: aportante.Ciudad
  - name: Sector
    expr: aportante.ActividadEconomica
  - name: Tamano empresa
    expr: aportante.TamanoEmpresa
  - name: Estado de pago
    expr: source.EstadoPago
    comment: "Oportuno, Tardío, Parcial, No pagado o Pendiente (aún no vence)."
measures:
  - name: Valor esperado
    expr: SUM(source.ValorEsperado)
    comment: "Lo que los aportantes debían pagar según sus cotizantes (COP)."
  - name: Valor recaudado
    expr: SUM(source.ValorPagado)
    comment: "Capital efectivamente pagado al corte (COP), sin intereses de mora."
  - name: Porcentaje de cumplimiento
    expr: SUM(source.ValorPagado) / NULLIF(SUM(source.ValorEsperado), 0)
    comment: "Valor recaudado / valor esperado."
  - name: Aportes
    expr: COUNT(1)
    comment: "Obligaciones aportante-periodo."
  - name: Porcentaje de pago oportuno
    expr: COUNT_IF(source.EsOportuno) / NULLIF(COUNT_IF(source.EsVencido), 0)
    comment: "Aportes pagados completos antes de la fecha límite, sobre los aportes ya vencidos."
  - name: Aportantes
    expr: COUNT(DISTINCT source.AportanteID)
  - name: Cotizantes mes
    expr: SUM(source.NumeroCotizantes)
    comment: "Suma de cotizantes por periodo. Para un solo mes equivale a los cotizantes activos."
$$;

-- -----------------------------------------------------------------------------
-- Cartera y mora (foto al cierre de cada mes)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW recaudo_dev.gold.metricas_cartera
WITH METRICS
LANGUAGE YAML
COMMENT 'Cartera vencida al cierre de cada mes. Los saldos son una foto: filtra o agrupa por Periodo para no sumar meses distintos.'
AS $$
version: 1.1
source: recaudo_dev.gold.fact_mora
joins:
  - name: aportante
    source: recaudo_dev.gold.dim_aportante
    'on': source.AportanteSK = aportante.AportanteSK
dimensions:
  - name: Periodo
    expr: source.Periodo
    comment: "Mes de corte de la foto de cartera (yyyy-MM)."
  - name: Regional
    expr: aportante.Regional
  - name: Departamento
    expr: aportante.Departamento
  - name: Sector
    expr: aportante.ActividadEconomica
  - name: Tamano empresa
    expr: aportante.TamanoEmpresa
  - name: Tramo de mora
    expr: source.EstadoMora
    comment: "Sin Mora, 1-30, 31-60, 61-90 o >90 días."
  - name: Orden tramo
    expr: source.OrdenTramo
  - name: Tiene acuerdo
    expr: source.TieneAcuerdo
measures:
  - name: Saldo vencido total
    expr: SUM(source.SaldoTotal)
    comment: "Capital + intereses adeudados al cierre (COP)."
  - name: Saldo capital
    expr: SUM(source.SaldoCapital)
  - name: Saldo intereses
    expr: SUM(source.SaldoInteres)
  - name: Aportantes
    expr: COUNT(DISTINCT source.AportanteID)
  - name: Aportantes en mora
    expr: COUNT(DISTINCT CASE WHEN source.EnMora THEN source.AportanteID END)
  - name: Porcentaje de aportantes en mora
    expr: COUNT_IF(source.EnMora) / NULLIF(COUNT(1), 0)
    comment: "Para un solo Periodo es el % de aportantes en mora; para varios, el promedio de los meses."
  - name: Saldo mayor a 90 dias
    expr: SUM(CASE WHEN source.EstadoMora = '>90' THEN source.SaldoTotal ELSE 0 END)
  - name: Porcentaje de saldo mayor a 90 dias
    expr: SUM(CASE WHEN source.EstadoMora = '>90' THEN source.SaldoTotal ELSE 0 END) / NULLIF(SUM(source.SaldoTotal), 0)
  - name: Dias de mora promedio
    expr: AVG(CASE WHEN source.EnMora THEN source.DiasMora END)
    comment: "Promedio de días de mora entre los aportantes que están en mora."
$$;

-- -----------------------------------------------------------------------------
-- Cobranza
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW recaudo_dev.gold.metricas_cobranza
WITH METRICS
LANGUAGE YAML
COMMENT 'Gestiones de cobranza: volumen, contacto, éxito y acuerdos de pago firmados.'
AS $$
version: 1.1
source: recaudo_dev.gold.fact_cobranza
joins:
  - name: aportante
    source: recaudo_dev.gold.dim_aportante
    'on': source.AportanteSK = aportante.AportanteSK
dimensions:
  - name: Periodo
    expr: source.Periodo
    comment: "Mes de la gestión (yyyy-MM)."
  - name: Tipo de gestion
    expr: source.TipoGestion
  - name: Resultado
    expr: source.ResultadoGestion
  - name: Tramo de mora al gestionar
    expr: source.TramoMoraAlGestionar
  - name: Gestor
    expr: source.Gestor
  - name: Regional
    expr: aportante.Regional
  - name: Sector
    expr: aportante.ActividadEconomica
measures:
  - name: Gestiones
    expr: COUNT(1)
  - name: Gestiones exitosas
    expr: COUNT_IF(source.EsExitosa)
  - name: Tasa de exito
    expr: COUNT_IF(source.EsExitosa) / NULLIF(COUNT(1), 0)
  - name: Tasa de contacto
    expr: COUNT_IF(source.HuboContacto) / NULLIF(COUNT(1), 0)
    comment: "Gestiones en las que se habló con el aportante (resultado distinto de Sin Contacto)."
  - name: Acuerdos firmados
    expr: COUNT(source.AcuerdoID)
  - name: Aportantes gestionados
    expr: COUNT(DISTINCT source.AportanteID)
$$;

-- -----------------------------------------------------------------------------
-- Riesgo
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW recaudo_dev.gold.metricas_riesgo
WITH METRICS
LANGUAGE YAML
COMMENT 'Probabilidad de incumplimiento por aportante y mes. ScoreVersion distingue reglas (reglas-v1) y modelos de ML.'
AS $$
version: 1.1
source: recaudo_dev.gold.fact_riesgo
joins:
  - name: aportante
    source: recaudo_dev.gold.dim_aportante
    'on': source.AportanteSK = aportante.AportanteSK
dimensions:
  - name: Periodo
    expr: source.Periodo
  - name: Nivel de riesgo
    expr: source.NivelRiesgo
  - name: Version del score
    expr: source.ScoreVersion
  - name: Regional
    expr: aportante.Regional
  - name: Sector
    expr: aportante.ActividadEconomica
  - name: Tamano empresa
    expr: aportante.TamanoEmpresa
measures:
  - name: Aportantes evaluados
    expr: COUNT(DISTINCT source.AportanteID)
  - name: Probabilidad promedio
    expr: AVG(source.ProbabilidadIncumplimiento)
  - name: Aportantes riesgo alto o critico
    expr: COUNT(DISTINCT CASE WHEN source.NivelRiesgo IN ('Alto', 'Critico') THEN source.AportanteID END)
$$;

-- Prueba rápida: cumplimiento y oportunidad por año
SELECT Anio,
       MEASURE(`Valor esperado`)              AS esperado,
       MEASURE(`Valor recaudado`)             AS recaudado,
       MEASURE(`Porcentaje de cumplimiento`)  AS cumplimiento,
       MEASURE(`Porcentaje de pago oportuno`) AS oportuno
FROM recaudo_dev.gold.metricas_recaudo
GROUP BY ALL
ORDER BY Anio;
