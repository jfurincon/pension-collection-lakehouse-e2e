# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 06 · ML: scoring batch
# MAGIC
# MAGIC Califica a todos los aportantes en todos los meses con el modelo `champion` y escribe `recaudo_dev.ml.score_riesgo`, con la versión de score `ml-v1`.
# MAGIC
# MAGIC Cada fila trae:
# MAGIC - la **probabilidad** de mora > 30 días en los próximos 3 meses y su **nivel** (Bajo, Medio, Alto, Crítico, con los mismos umbrales de `reglas-v1`);
# MAGIC - las **3 razones principales** del score, en texto legible, por ejemplo "Días de mora al corte: 47";
# MAGIC - la **muestra** a la que pertenece el mes (entrenamiento, validación, prueba o producción), para no confundir resultados de entrenamiento con resultados reales.
# MAGIC
# MAGIC Después de correrlo, vuelve a ejecutar la pipeline `gold_recaudo`: `fact_riesgo` une estos scores con los de `reglas-v1`.

# COMMAND ----------

# MAGIC %pip install -q lightgbm "mlflow-skinny[databricks]>=3.1"

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("catalogo", "recaudo_dev")
C = dbutils.widgets.get("catalogo")
MODELO = f"{C}.ml.modelo_incumplimiento"
SALIDA = f"{C}.ml.score_riesgo"

import importlib, os, sys
from datetime import datetime
from pathlib import Path
import pandas as pd
import mlflow
from mlflow import MlflowClient
from pyspark.sql import types as T

raiz = Path(os.getcwd())
while not (raiz / "config" / "parametros.yaml").exists() and raiz != raiz.parent:
    raiz = raiz.parent
sys.path.append(str(raiz / "src" / "ml"))
import preparacion as prep
importlib.reload(prep)

mlflow.set_registry_uri("databricks-uc")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cargar el modelo campeón

# COMMAND ----------

version = MlflowClient().get_model_version_by_alias(MODELO, "champion")
modelo = mlflow.sklearn.load_model(f"models:/{MODELO}@champion")
print(f"{MODELO} versión {version.version}: {version.description}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Calificar y explicar

# COMMAND ----------

datos = spark.table(f"{C}.ml.features_aportante_mes").toPandas()
probabilidad = modelo.predict_proba(datos[prep.FEATURES])[:, 1]
contrib = prep.contribuciones(modelo, datos)
razones = prep.razones(contrib, datos, n=3)

scores = pd.DataFrame({
    "AportanteID": datos["AportanteID"],
    "FechaEvaluacion": pd.to_datetime(datos["FechaCorte"]).dt.date,
    "Periodo": datos["Periodo"],
    "ProbabilidadIncumplimiento": probabilidad.round(4),
    "NivelRiesgo": prep.nivel(probabilidad),
    "ScoreVersion": "ml-v1",
    "ModeloVersion": str(version.version),
    "Muestra": prep.muestra(datos["Periodo"]),
    "Razones": razones,
    "FechaScoring": datetime.now(),
})
display(scores.head(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Guardar

# COMMAND ----------

esquema = T.StructType([
    T.StructField("AportanteID", T.StringType(), False),
    T.StructField("FechaEvaluacion", T.DateType(), False),
    T.StructField("Periodo", T.StringType(), False),
    T.StructField("ProbabilidadIncumplimiento", T.DoubleType(), False),
    T.StructField("NivelRiesgo", T.StringType(), False),
    T.StructField("ScoreVersion", T.StringType(), False),
    T.StructField("ModeloVersion", T.StringType(), False),
    T.StructField("Muestra", T.StringType(), False),
    T.StructField("Razones", T.ArrayType(T.StringType()), True),
    T.StructField("FechaScoring", T.TimestampType(), False),
])
(spark.createDataFrame(scores, schema=esquema)
 .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(SALIDA))
spark.sql(f"""COMMENT ON TABLE {SALIDA} IS 'Scores del modelo de ML (ml-v1): probabilidad de mora > 30 días
en los próximos 3 meses, nivel de riesgo y las 3 razones principales. Muestra indica si el mes se usó
para entrenar (resultado optimista) o es fuera de muestra (validación, prueba, producción).'""")
print(f"{spark.table(SALIDA).count():,} scores escritos en {SALIDA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resultado: último mes calificado

# COMMAND ----------

display(spark.sql(f"""
    WITH ultimo AS (SELECT max(Periodo) AS p FROM {SALIDA})
    SELECT s.NivelRiesgo,
           count(*) AS aportantes,
           round(100 * count(*) / sum(count(*)) OVER (), 1) AS pct,
           round(avg(s.ProbabilidadIncumplimiento), 3) AS probabilidad_media
    FROM {SALIDA} s JOIN ultimo u ON s.Periodo = u.p
    GROUP BY s.NivelRiesgo
    ORDER BY CASE s.NivelRiesgo WHEN 'Critico' THEN 1 WHEN 'Alto' THEN 2 WHEN 'Medio' THEN 3 ELSE 4 END
"""))

# COMMAND ----------

# Lista de cobro priorizada: los 20 aportantes más riesgosos del último mes y por qué
display(spark.sql(f"""
    WITH ultimo AS (SELECT max(Periodo) AS p FROM {SALIDA})
    SELECT s.AportanteID, d.RazonSocial, d.ActividadEconomica, d.Regional,
           s.ProbabilidadIncumplimiento, s.NivelRiesgo, s.Razones
    FROM {SALIDA} s
    JOIN ultimo u ON s.Periodo = u.p
    JOIN {C}.gold.dim_aportante d ON d.AportanteID = s.AportanteID AND d.EsActual
    ORDER BY s.ProbabilidadIncumplimiento DESC
    LIMIT 20
"""))