# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Silver: verificación
# MAGIC
# MAGIC Corre este notebook **después** de ejecutar el pipeline `silver_recaudo`. Comprueba tres cosas:
# MAGIC 1. **Cuadre:** cada fila de Bronze terminó en Silver o en cuarentena. La única diferencia permitida son los pagos duplicados, que se descartan a propósito.
# MAGIC 2. **Cuarentena:** qué reglas fallaron y cuántas veces. Deben ser exactamente los errores que el generador inyectó.
# MAGIC 3. **Historial de aportantes (SCD tipo 2):** que las empresas trasladadas o inactivadas tengan varias versiones.

# COMMAND ----------

dbutils.widgets.text("catalogo", "recaudo_dev")
CATALOGO = dbutils.widgets.get("catalogo")
TABLAS = ["aportante", "cotizante", "tiempo", "geografia", "aporte_detalle", "aporte",
          "pago", "mora", "cobranza", "acuerdo_pago", "riesgo"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Cuadre Bronze = Silver + cuarentena

# COMMAND ----------

filas = []
for t in TABLAS:
    bronze = spark.table(f"{CATALOGO}.bronze.{t}").count()
    # aportante en Silver tiene una fila por versión, igual que Bronze
    silver = spark.table(f"{CATALOGO}.silver.{t}").count()
    cuarentena = spark.table(f"{CATALOGO}.silver.{t}_cuarentena").count()
    filas.append((t, bronze, silver, cuarentena, bronze - silver - cuarentena))

cuadre = spark.createDataFrame(
    filas, "tabla string, bronze long, silver long, cuarentena long, diferencia long")
display(cuadre)

# COMMAND ----------

# MAGIC %md
# MAGIC La columna `diferencia` debe ser **0** en todas las tablas excepto `pago`, donde equivale a los PagoID duplicados que se descartaron. Lo comprobamos:

# COMMAND ----------

duplicados = spark.sql(f"""
    SELECT count(*) - count(DISTINCT PagoID) AS duplicados
    FROM {CATALOGO}.bronze.pago
""").first()["duplicados"]
dif_pago = [f for f in filas if f[0] == "pago"][0][4]
otras = [f for f in filas if f[0] != "pago" and f[4] != 0]
print(f"Duplicados de PagoID en Bronze: {duplicados:,} | diferencia en pago: {dif_pago:,}")
print("CUADRE OK" if duplicados == dif_pago and not otras else f"REVISAR: {otras}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. ¿Qué cayó en cuarentena y por qué?

# COMMAND ----------

consultas = [
    f"""SELECT '{t}' AS tabla, concat_ws(', ', _reglas_falladas) AS reglas, count(*) AS filas
        FROM {CATALOGO}.silver.{t}_cuarentena GROUP BY 2"""
    for t in TABLAS
]
display(spark.sql(" UNION ALL ".join(consultas) + " ORDER BY filas DESC"))

# COMMAND ----------

# MAGIC %md
# MAGIC Lo esperado, según los errores que inyecta el generador:
# MAGIC
# MAGIC | Tabla | Regla | Origen del error |
# MAGIC | --- | --- | --- |
# MAGIC | aporte | `pagado_no_negativo` | 0,2% de montos con signo invertido |
# MAGIC | pago | `fecha_posible` | 0,3% de pagos con fecha anterior al periodo |
# MAGIC | cotizante | `aportante_existe` | 0,2% de cotizantes con un AportanteID que no existe |
# MAGIC | aporte_detalle | `cotizante_existe` | Efecto en cascada: el detalle de esos cotizantes |
# MAGIC
# MAGIC El dígito de verificación inválido (1% de los aportantes) **no** aparece aquí: esa regla solo advierte. La empresa sigue en Silver con `DvValido = false`.

# COMMAND ----------

display(spark.sql(f"""
    SELECT DvValido, count(*) AS versiones
    FROM {CATALOGO}.silver.aportante
    GROUP BY DvValido
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Historial de aportantes (SCD tipo 2)
# MAGIC
# MAGIC `__START_AT` es la fecha desde la que rige cada versión y `__END_AT` hasta cuándo. La versión vigente tiene `__END_AT` vacío.

# COMMAND ----------

display(spark.sql(f"""
    WITH con_historial AS (
        SELECT AportanteID FROM {CATALOGO}.silver.aportante
        GROUP BY AportanteID HAVING count(*) > 1
        LIMIT 5
    )
    SELECT a.AportanteID, a.RazonSocial, a.GeografiaID, a.EstadoAportante,
           a.__START_AT, a.__END_AT
    FROM {CATALOGO}.silver.aportante a
    JOIN con_historial USING (AportanteID)
    ORDER BY a.AportanteID, a.__START_AT
"""))

# COMMAND ----------

display(spark.sql(f"""
    SELECT
        (SELECT count(DISTINCT AportanteID) FROM {CATALOGO}.silver.aportante) AS empresas,
        (SELECT count(*) FROM {CATALOGO}.silver.aportante)                    AS versiones,
        (SELECT count(*) FROM {CATALOGO}.silver.aportante_actual)             AS vigentes
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Métricas de calidad desde el registro de eventos (opcional)
# MAGIC
# MAGIC Cada ejecución del pipeline deja en su *event log* cuántas filas pasaron y fallaron cada regla. Es lo mismo que ves en la pestaña **Data quality** del pipeline, pero consultable con SQL, útil para un tablero de calidad.

# COMMAND ----------

display(spark.sql(f"""
    SELECT
        timestamp,
        origin.flow_name AS flujo,
        exp.name         AS regla,
        exp.passed_records AS pasaron,
        exp.failed_records AS fallaron
    FROM event_log(TABLE({CATALOGO}.silver.aporte))
    LATERAL VIEW explode(
        from_json(details:flow_progress.data_quality.expectations,
                  'array<struct<name:string, dataset:string, passed_records:long, failed_records:long>>')
    ) AS exp
    WHERE event_type = 'flow_progress'
      AND details:flow_progress.data_quality.expectations IS NOT NULL
    ORDER BY timestamp DESC
"""))
