-- =============================================================================
-- Herramientas del agente de cobranza: funciones de Unity Catalog.
--
-- Un agente de IA no consulta las tablas por su cuenta: llama "herramientas".
-- Cada herramienta es una función SQL con nombre, parámetros y una descripción
-- (COMMENT). El modelo lee esas descripciones para decidir cuál usar y con qué
-- argumentos, así que deben ser claras.
--
-- Cómo correrlo: editor SQL de Databricks con un SQL warehouse, ejecutar todo.
-- Requiere Gold y los notebooks 04 y 06 (features y scores ml-v1).
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. Perfil de un aportante en el último mes calificado
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION recaudo_dev.ml.perfil_aportante(
    aportante_id STRING COMMENT 'Identificador del aportante, por ejemplo APT-000123'
)
RETURNS TABLE (
    AportanteID STRING, RazonSocial STRING, Sector STRING, TamanoEmpresa STRING,
    Regional STRING, Ciudad STRING, Periodo STRING,
    ProbabilidadIncumplimiento DOUBLE, NivelRiesgo STRING, Razones STRING,
    TramoMora STRING, DiasMora INT, PeriodosAdeudados INT, SaldoVencido DOUBLE,
    TieneAcuerdo BOOLEAN, CuotasAcuerdoVencidasImpagas INT,
    MesesEnMoraUltimos12 INT, MaxDiasMoraUltimos12 INT,
    PctPeriodosPagadosATiempo12m DOUBLE, PagosExtemporaneos12m INT,
    CotizantesFrenteAHace3Meses DOUBLE, Gestiones3m INT, GestionesRechazadas3m INT
)
COMMENT 'Perfil de riesgo y cobranza de un aportante (empresa) en el último mes: probabilidad de mora > 30 días en los próximos 3 meses (score ml-v1), nivel de riesgo, las 3 razones del score, situación de mora actual, historial de pago de 12 meses y gestiones recientes. Úsala primero cuando pregunten por una empresa concreta.'
RETURN
    SELECT d.AportanteID, d.RazonSocial, d.ActividadEconomica, d.TamanoEmpresa,
           d.Regional, d.Ciudad, s.Periodo,
           s.ProbabilidadIncumplimiento, s.NivelRiesgo, array_join(s.Razones, ' · '),
           m.EstadoMora, CAST(f.DiasMora AS INT), CAST(f.PeriodosAdeudados AS INT),
           CAST(f.SaldoTotal AS DOUBLE),
           CAST(f.TieneAcuerdo AS BOOLEAN), CAST(f.CuotasVencidasImpagas AS INT),
           CAST(f.MesesEnMora12m AS INT), CAST(f.MaxDiasMora12m AS INT),
           CAST(f.TasaPagoOportuno12m AS DOUBLE), CAST(f.PagosExtemporaneos12m AS INT),
           CAST(f.TendenciaCotizantes AS DOUBLE), CAST(f.Gestiones3m AS INT),
           CAST(f.GestionesRechazadas3m AS INT)
    FROM recaudo_dev.ml.score_riesgo s
    JOIN recaudo_dev.ml.features_aportante_mes f
      ON f.AportanteID = s.AportanteID AND f.Periodo = s.Periodo
    JOIN recaudo_dev.gold.dim_aportante d
      ON d.AportanteID = s.AportanteID AND d.EsActual
    LEFT JOIN recaudo_dev.gold.fact_mora m
      ON m.AportanteID = s.AportanteID AND m.Periodo = s.Periodo
    WHERE s.AportanteID = perfil_aportante.aportante_id
      AND s.Periodo = (SELECT max(Periodo) FROM recaudo_dev.ml.score_riesgo);

-- -----------------------------------------------------------------------------
-- 2. Últimas gestiones de cobranza de un aportante, con sus notas
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION recaudo_dev.ml.historial_gestiones(
    aportante_id STRING COMMENT 'Identificador del aportante, por ejemplo APT-000123'
)
RETURNS TABLE (
    FechaGestion DATE, TipoGestion STRING, ResultadoGestion STRING,
    TramoMoraAlGestionar STRING, NotaGestion STRING
)
COMMENT 'Las 5 gestiones de cobranza más recientes de un aportante: fecha, canal (Llamada, Correo, Visita, AcuerdoPago), resultado y la nota del gestor. Úsala para saber qué se ha intentado, qué respondió la empresa y evitar repetir lo que no funcionó.'
RETURN
    SELECT to_date(CAST(FechaGestionKey AS STRING), 'yyyyMMdd'), TipoGestion,
           ResultadoGestion, TramoMoraAlGestionar, NotaGestion
    FROM recaudo_dev.gold.fact_cobranza
    WHERE AportanteID = historial_gestiones.aportante_id
    QUALIFY row_number() OVER (ORDER BY FechaGestionKey DESC, GestionID DESC) <= 5;

-- -----------------------------------------------------------------------------
-- 3. Qué canal funciona mejor en un tramo de mora
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION recaudo_dev.ml.efectividad_canales(
    tramo STRING COMMENT 'Tramo de mora: 1-30, 31-60, 61-90 o >90'
)
RETURNS TABLE (
    TipoGestion STRING, Gestiones BIGINT, TasaExito DOUBLE, TasaContacto DOUBLE
)
COMMENT 'Tasa de éxito y de contacto de cada canal de cobranza (Llamada, Correo, Visita, AcuerdoPago) para empresas en un tramo de mora, calculada con las gestiones de 2025. Úsala para elegir el canal con más probabilidad de éxito. Para empresas al día usa el tramo 1-30.'
RETURN
    SELECT TipoGestion, CAST(sum(Gestiones) AS BIGINT),
           try_divide(sum(Exitosas), sum(Gestiones)),
           try_divide(sum(ConContacto), sum(Gestiones))
    FROM recaudo_dev.gold.agg_efectividad_cobranza
    WHERE TramoMoraAlGestionar = efectividad_canales.tramo
      AND Periodo >= '2025-01'
    GROUP BY TipoGestion;

-- -----------------------------------------------------------------------------
-- 4. Alertas tempranas: empresas al día con mayor riesgo
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION recaudo_dev.ml.alertas_tempranas()
RETURNS TABLE (
    AportanteID STRING, RazonSocial STRING, Sector STRING, Regional STRING,
    ProbabilidadIncumplimiento DOUBLE, NivelRiesgo STRING, Razones STRING
)
COMMENT 'Las 20 empresas que hoy están al día (0 días de mora) con mayor probabilidad de caer en mora > 30 días en los próximos 3 meses, según el score ml-v1 del último mes. Úsala cuando pidan priorizar cobranza preventiva o a quién llamar primero.'
RETURN
    SELECT d.AportanteID, d.RazonSocial, d.ActividadEconomica, d.Regional,
           s.ProbabilidadIncumplimiento, s.NivelRiesgo, array_join(s.Razones, ' · ')
    FROM recaudo_dev.ml.score_riesgo s
    JOIN recaudo_dev.ml.features_aportante_mes f
      ON f.AportanteID = s.AportanteID AND f.Periodo = s.Periodo
    JOIN recaudo_dev.gold.dim_aportante d
      ON d.AportanteID = s.AportanteID AND d.EsActual
    WHERE s.Periodo = (SELECT max(Periodo) FROM recaudo_dev.ml.score_riesgo)
      AND f.DiasMora = 0
    QUALIFY row_number() OVER (ORDER BY s.ProbabilidadIncumplimiento DESC) <= 20;

-- -----------------------------------------------------------------------------
-- Pruebas rápidas
-- -----------------------------------------------------------------------------
SELECT * FROM recaudo_dev.ml.alertas_tempranas();
SELECT * FROM recaudo_dev.ml.perfil_aportante('APT-007176');
SELECT * FROM recaudo_dev.ml.historial_gestiones('APT-007176');
SELECT * FROM recaudo_dev.ml.efectividad_canales('1-30');
