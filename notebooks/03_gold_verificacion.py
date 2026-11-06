# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Gold: verificación
# MAGIC
# MAGIC Corre este notebook después del pipeline `gold_recaudo` y de crear las metric views. Comprueba:
# MAGIC 1. **Ninguna fila se pierde** al unir cada hecho con la versión del aportante (SCD2).
# MAGIC 2. **Integridad del modelo estrella:** llaves sustitutas únicas y todas las llaves de los hechos existen en sus dimensiones.
# MAGIC 3. **Los KPIs tienen sentido** y las metric views responden igual que las tablas.

# COMMAND ----------

dbutils.widgets.text("catalogo", "recaudo_dev")
C = dbutils.widgets.get("catalogo")

HECHOS = {  # tabla silver -> tabla gold
    "aporte_detalle": "fact_aporte_detalle", "aporte": "fact_aporte", "pago": "fact_pago",
    "mora": "fact_mora", "cobranza": "fact_cobranza", "acuerdo_pago": "fact_acuerdo_cuota",
    "riesgo": "fact_riesgo",
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Silver = Gold en cada hecho

# COMMAND ----------

filas = []
for s, g in HECHOS.items():
    ns = spark.table(f"{C}.silver.{s}").count()
    gold = spark.table(f"{C}.gold.{g}")
    if g == "fact_riesgo":   # también trae los scores ml-v1 del notebook 06: se cuadra solo reglas-v1
        gold = gold.where("ScoreVersion = 'reglas-v1'")
    ng = gold.count()
    filas.append((g, ns, ng, "OK" if ns == ng else "REVISAR"))
display(spark.createDataFrame(filas, "tabla string, filas_silver long, filas_gold long, estado string"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Integridad del modelo estrella

# COMMAND ----------

checks = spark.sql(f"""
SELECT 'dim_aportante: AportanteSK única' AS prueba,
       count(*) - count(DISTINCT AportanteSK) AS problemas FROM {C}.gold.dim_aportante
UNION ALL
SELECT 'dim_aportante: una versión actual por empresa',
       count(DISTINCT AportanteID) - count_if(EsActual) FROM {C}.gold.dim_aportante
UNION ALL
SELECT 'dim_cotizante: CotizanteSK única',
       count(*) - count(DISTINCT CotizanteSK) FROM {C}.gold.dim_cotizante
UNION ALL
SELECT 'fact_mora: AportanteSK sin dimensión',
       count(*) FROM {C}.gold.fact_mora f LEFT ANTI JOIN {C}.gold.dim_aportante d USING (AportanteSK)
UNION ALL
SELECT 'fact_aporte_detalle: CotizanteSK sin dimensión',
       count(*) FROM {C}.gold.fact_aporte_detalle f LEFT ANTI JOIN {C}.gold.dim_cotizante d USING (CotizanteSK)
UNION ALL
SELECT 'fact_pago: FechaPagoKey sin dim_tiempo',
       count(*) FROM {C}.gold.fact_pago f LEFT ANTI JOIN {C}.gold.dim_tiempo t ON f.FechaPagoKey = t.FechaKey
UNION ALL
SELECT 'fact_acuerdo_cuota: vencimiento sin dim_tiempo',
       count(*) FROM {C}.gold.fact_acuerdo_cuota f LEFT ANTI JOIN {C}.gold.dim_tiempo t ON f.FechaVencimientoKey = t.FechaKey
""")
display(checks)
print("INTEGRIDAD OK" if checks.where("problemas <> 0").count() == 0 else "REVISAR las pruebas con problemas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. El SCD2 en acción
# MAGIC
# MAGIC Una empresa que cambió de ciudad debe tener su cartera repartida entre las dos ciudades, según la fecha.

# COMMAND ----------

display(spark.sql(f"""
WITH trasladada AS (
    SELECT AportanteID FROM {C}.gold.dim_aportante
    GROUP BY AportanteID HAVING count(DISTINCT GeografiaID) > 1 LIMIT 1
)
SELECT d.AportanteID, d.Ciudad, d.ValidoDesde, d.ValidoHasta,
       min(f.Periodo) AS primer_mes, max(f.Periodo) AS ultimo_mes, count(f.Periodo) AS meses
FROM {C}.gold.dim_aportante d
JOIN trasladada USING (AportanteID)
LEFT JOIN {C}.gold.fact_mora f ON f.AportanteSK = d.AportanteSK
GROUP BY ALL ORDER BY d.ValidoDesde
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. KPIs ejecutivos
# MAGIC
# MAGIC Valores esperados aproximados: pago oportuno ≈ 80%, aportantes en mora ≈ 20–24%, índice de cartera vencida ≈ 2–3% de la facturación anual. El cumplimiento de los últimos meses es menor porque esos periodos todavía se están cobrando; el de 2025-12 es 0 porque aún no vence.

# COMMAND ----------

display(spark.sql(f"""
SELECT Periodo,
       round(PctCumplimiento, 3)       AS cumplimiento,
       round(PctOportuno, 3)           AS oportuno,
       round(PctAportantesEnMora, 3)   AS aportantes_en_mora,
       round(IndiceCarteraVencida, 3)  AS indice_cartera_vencida,
       round(TasaRecuperacion, 3)      AS tasa_recuperacion,
       round(PctSaldoMas90, 3)         AS saldo_mas_90
FROM {C}.gold.agg_indicadores_mensuales
ORDER BY Periodo
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Matriz de transición (roll rate) de un mes
# MAGIC
# MAGIC Filas = tramo en el mes de origen; columnas = tramo al mes siguiente. Lo normal: la mayoría se queda en su tramo o avanza al siguiente; una parte se recupera a "Sin Mora".

# COMMAND ----------

display(spark.sql(f"""
SELECT * FROM (
  SELECT TramoOrigen, OrdenTramoOrigen, TramoDestino, Aportantes
  FROM {C}.gold.agg_transicion_tramos WHERE PeriodoOrigen = '2025-06'
)
PIVOT (sum(Aportantes) FOR TramoDestino IN ('Sin Mora', '1-30', '31-60', '61-90', '>90'))
ORDER BY OrdenTramoOrigen
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Las metric views responden lo mismo que las tablas
# MAGIC
# MAGIC Requiere haber ejecutado `src/gold/metric_views.sql`.

# COMMAND ----------

comparacion = spark.sql(f"""
WITH desde_metricas AS (
    SELECT Periodo, MEASURE(`Porcentaje de cumplimiento`) AS cumplimiento_mv
    FROM {C}.gold.metricas_recaudo GROUP BY ALL
)
SELECT a.Periodo, round(a.PctCumplimiento, 6) AS desde_tabla, round(m.cumplimiento_mv, 6) AS desde_metric_view
FROM {C}.gold.agg_indicadores_mensuales a JOIN desde_metricas m USING (Periodo)
ORDER BY a.Periodo
""")
display(comparacion)
print("METRIC VIEWS OK" if comparacion.where("desde_tabla <> desde_metric_view").count() == 0 else "REVISAR")
