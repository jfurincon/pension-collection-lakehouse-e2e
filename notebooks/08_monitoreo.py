# Databricks notebook source
# MAGIC %md
# MAGIC # 08 · Monitoreo del modelo
# MAGIC
# MAGIC Revisa cada mes si el modelo `ml-v1` sigue sirviendo y guarda el resultado en `recaudo_dev.ml.monitoreo_modelo`.
# MAGIC
# MAGIC | Pregunta | Métricas | Meses |
# MAGIC | --- | --- | --- |
# MAGIC | **Desempeño:** ¿sigue acertando? | AUC-ROC, captura del 20% más riesgoso, brecha de calibración | Solo los que ya tienen resultado (pasaron 3 meses) |
# MAGIC | **Estabilidad:** ¿los datos se parecen a los de entrenamiento? | PSI del score y de 6 variables clave | Todos, incluso los más recientes |
# MAGIC
# MAGIC El **PSI** (*Population Stability Index*) compara dos distribuciones: menos de 0,10 es estable, entre 0,10 y 0,25 hay que vigilar, y más de 0,25 es un cambio fuerte. La referencia son los meses de entrenamiento de 2024, cuando las variables de 12 meses ya tienen historia completa.
# MAGIC
# MAGIC Toda la lógica está en `src/ml/monitoreo.py`.

# COMMAND ----------

dbutils.widgets.text("catalogo", "recaudo_dev")
dbutils.widgets.dropdown("fallar_si_alerta", "no", ["no", "si"])
dbutils.widgets.dropdown("simular_deriva", "no", ["no", "si"])
C = dbutils.widgets.get("catalogo")
FALLAR = dbutils.widgets.get("fallar_si_alerta") == "si"
SIMULAR = dbutils.widgets.get("simular_deriva") == "si"
TABLA = f"{C}.ml.monitoreo_modelo"

import importlib, os, sys
from datetime import datetime
from pathlib import Path

raiz = Path(os.getcwd())
while not (raiz / "config" / "parametros.yaml").exists() and raiz != raiz.parent:
    raiz = raiz.parent
sys.path.append(str(raiz / "src" / "ml"))
import monitoreo as mon
importlib.reload(mon)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Calcular

# COMMAND ----------

scores = spark.sql(f"""
    SELECT AportanteID, Periodo, Muestra, ProbabilidadIncumplimiento, ModeloVersion
    FROM {C}.ml.score_riesgo
""").toPandas()
features = spark.table(f"{C}.ml.features_aportante_mes") \
    .select("AportanteID", "Periodo", mon.ETIQUETA, *mon.VARIABLES_PSI).toPandas()

if SIMULAR:
    # Prueba de las alertas: en el último mes, las empresas "pagan a tiempo" un 40%
    # menos. No se guarda nada; solo muestra cómo reacciona el monitoreo.
    ultimo = features["Periodo"].max()
    features.loc[features["Periodo"] == ultimo, "TasaPagoOportuno12m"] *= 0.6
    print(f"DERIVA SIMULADA en {ultimo}: TasaPagoOportuno12m × 0,6 (no se guarda)")

monitoreo = mon.calcular(scores, features)
res = mon.resumen(monitoreo)
print("Estado:", res["estado"], "| último mes con resultado:", res["ultimo_mes_desempeno"],
      "| último mes:", res["ultimo_mes_estabilidad"])
for a in res["alertas"]:
    print(f"  {a['Estado']}: {a['Metrica']} {a['Variable'] or ''} = {a['Valor']:.3f} ({a['Periodo']})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Desempeño por mes
# MAGIC
# MAGIC Referencia del periodo de prueba: AUC 0,92 y captura del 20% más riesgoso de 0,77. Los meses de entrenamiento salen optimistas y no generan alertas.

# COMMAND ----------

display(monitoreo[monitoreo["Tipo"] == "Desempeño"]
        .pivot_table(index=["Periodo", "Muestra"], columns="Metrica", values="Valor")
        .round(3).reset_index())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Estabilidad (PSI) por mes

# COMMAND ----------

display(monitoreo[monitoreo["Tipo"] == "Estabilidad"]
        .pivot_table(index="Periodo", columns="Variable", values="Valor")
        .round(3).reset_index())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Guardar
# MAGIC
# MAGIC Se reescribe el historial completo en cada corrida: así un cambio de umbral o de referencia se aplica a todos los meses. Con la deriva simulada no se guarda nada.

# COMMAND ----------

if SIMULAR:
    print("Deriva simulada: la tabla no se modifica")
else:
    salida = monitoreo.assign(
        ModeloVersion=",".join(sorted(scores["ModeloVersion"].astype(str).unique())),
        FechaCalculo=datetime.now())
    (spark.createDataFrame(salida)
     .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(TABLA))
    spark.sql(f"""COMMENT ON TABLE {TABLA} IS 'Monitoreo mensual del modelo ml-v1: desempeño
    (AUC-ROC, captura top 20%, brecha de calibración) en meses con resultado, y estabilidad (PSI
    del score y de variables clave) frente a la referencia 2024-01 a 2024-09. Estado: OK, VIGILAR,
    ALERTA o REFERENCIA.'""")
    print(f"{spark.table(TABLA).count():,} filas en {TABLA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Señal para el job
# MAGIC
# MAGIC El estado queda como *task value* para que otras tareas del job lo lean. Con `fallar_si_alerta = si` (así corre en el job), una **ALERTA** hace fallar la tarea: el job queda en rojo y se envía el correo de falla. **VIGILAR** no detiene nada.

# COMMAND ----------

try:
    dbutils.jobs.taskValues.set(key="estado_monitoreo", value=res["estado"])
except Exception:
    pass  # fuera de un job no hay a quién pasarle el valor

if FALLAR and res["estado"] == "ALERTA" and not SIMULAR:
    raise AssertionError(f"Monitoreo en ALERTA: {res['alertas']}")
