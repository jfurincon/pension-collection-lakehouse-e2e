# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 04 · ML: tabla de features
# MAGIC
# MAGIC Construye `recaudo_dev.ml.features_aportante_mes`: una fila por aportante y mes de corte, con 35 variables que describen su comportamiento y la etiqueta que el modelo debe predecir.
# MAGIC
# MAGIC **Etiqueta (`Mora30Prox3m`):** 1 si el aportante tendrá mora de más de 30 días en algún cierre de los próximos 3 meses.
# MAGIC
# MAGIC **Regla de oro, sin fuga de datos:** las variables del mes *t* solo usan lo que el fondo sabía al cierre de *t*: fotos mensuales de cartera, eventos con fecha anterior al corte y el valor esperado de cada periodo. Nunca se usa `EstadoPago`, `ValorPagado` ni `EstadoAcuerdo`, porque reflejan el estado al final de los datos, no el de cada mes.
# MAGIC
# MAGIC Toda la lógica está en `src/ml/preparacion.py`, compartido con los notebooks 05 y 06.

# COMMAND ----------

dbutils.widgets.text("catalogo", "recaudo_dev")
C = dbutils.widgets.get("catalogo")
TABLA = f"{C}.ml.features_aportante_mes"

# COMMAND ----------

import importlib, os, sys
from pathlib import Path
from pyspark.sql import functions as F

# Sube de carpeta en carpeta hasta la raíz del repo y agrega src/ml al path
raiz = Path(os.getcwd())
while not (raiz / "config" / "parametros.yaml").exists() and raiz != raiz.parent:
    raiz = raiz.parent
sys.path.append(str(raiz / "src" / "ml"))

import preparacion as prep
importlib.reload(prep)   # toma los cambios si editaste el archivo sin reiniciar
print("Raíz del repo:", raiz)
print(f"{len(prep.FEATURES)} features · etiqueta: {prep.ETIQUETA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Construir y guardar

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {C}.ml COMMENT 'Features, scores y modelos de riesgo de incumplimiento'")

# Si la tabla ya existe, se quita la llave primaria antes de sobrescribirla.
if spark.catalog.tableExists(TABLA):
    spark.sql(f"ALTER TABLE {TABLA} DROP CONSTRAINT IF EXISTS pk_features_aportante_mes")

features = prep.construir_features(spark, C)
(features.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(TABLA))
print("Filas escritas:", spark.table(TABLA).count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Llave primaria y descripciones
# MAGIC
# MAGIC La llave `(AportanteID, FechaCorte TIMESERIES)` declara que es una tabla de features con historia: Unity Catalog la reconoce como *feature table* y la herramienta de Feature Engineering puede hacer uniones "al momento" (*point-in-time*) sin fuga de datos. Las descripciones de cada columna quedan en el catálogo para quien la use después, incluido Genie.

# COMMAND ----------

for col in ("AportanteID", "FechaCorte"):
    spark.sql(f"ALTER TABLE {TABLA} ALTER COLUMN {col} SET NOT NULL")
spark.sql(f"""ALTER TABLE {TABLA} ADD CONSTRAINT pk_features_aportante_mes
              PRIMARY KEY (AportanteID, FechaCorte TIMESERIES)""")

spark.sql(f"""COMMENT ON TABLE {TABLA} IS 'Features por aportante y mes de corte para el modelo de
probabilidad de incumplimiento. Solo usa información disponible al cierre de cada mes.
Etiqueta {prep.ETIQUETA}: mora > 30 días en algún cierre de los próximos 3 meses.'""")
comentarios = {c: etiqueta for c, (etiqueta, _) in prep.DESCRIPCION.items()}
comentarios[prep.ETIQUETA] = "Etiqueta: 1 si tiene mora > 30 días en algún cierre de t+1 a t+3 (vacía si no hay 3 meses de futuro)"
comentarios["FechaCorte"] = "Último día del mes de corte"
for col, texto in comentarios.items():
    spark.sql(f"ALTER TABLE {TABLA} ALTER COLUMN {col} COMMENT '{texto}'")
print("Llave primaria y", len(comentarios), "descripciones agregadas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verificaciones

# COMMAND ----------

# 1. Una fila por cada foto de cartera, sin duplicados
filas_mora = spark.table(f"{C}.gold.fact_mora").count()
f = spark.table(TABLA)
duplicados = f.count() - f.select("AportanteID", "FechaCorte").distinct().count()
print(f"Filas features: {f.count():,} | filas fact_mora: {filas_mora:,} | duplicados: {duplicados}")
assert f.count() == filas_mora and duplicados == 0, "La tabla de features no cuadra con fact_mora"

# COMMAND ----------

# 2. ¿Por qué el horizonte es de 3 meses? Tasa de la etiqueta según la situación actual.
#    Con "mora > 30 en t+1", una empresa al día tendría tasa 0: es imposible acumular
#    más de ~29 días en un mes. A 3 meses sí hay un problema real que predecir.
display(spark.sql(f"""
    SELECT CASE WHEN DiasMora = 0 THEN '1. Al día'
                WHEN DiasMora <= 30 THEN '2. Mora 1-30'
                ELSE '3. Mora > 30' END AS situacion_hoy,
           count(*) AS filas,
           round(avg({prep.ETIQUETA}), 3) AS tasa_etiqueta
    FROM {TABLA}
    WHERE {prep.ETIQUETA} IS NOT NULL
    GROUP BY 1 ORDER BY 1
"""))

# COMMAND ----------

# 3. Filas y tasa por partición temporal (los últimos 3 meses no tienen etiqueta)
import pandas as pd
resumen = (f.select("Periodo", prep.ETIQUETA).toPandas()
           .assign(muestra=lambda d: prep.muestra(d["Periodo"]))
           .groupby("muestra").agg(filas=("Periodo", "size"),
                                   desde=("Periodo", "min"), hasta=("Periodo", "max"),
                                   tasa_etiqueta=(prep.ETIQUETA, "mean")))
display(resumen.reset_index())

# COMMAND ----------

# 4. Nulos por feature: solo deben aparecer en las variables que dependen de historia
nulos = f.select([(F.avg(F.col(c).isNull().cast("double"))).alias(c) for c in prep.FEATURES]).toPandas().T
nulos.columns = ["pct_nulos"]
display(nulos[nulos["pct_nulos"] > 0].reset_index().rename(columns={"index": "feature"}))

# COMMAND ----------

# 5. Vistazo a un aportante
display(f.where("AportanteID = 'APT-000001'").orderBy("FechaCorte"))