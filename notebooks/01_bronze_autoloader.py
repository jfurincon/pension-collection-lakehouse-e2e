# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 01 · Bronze: ingesta con Auto Loader
# MAGIC
# MAGIC Lee los CSV que dejó el generador en el Volume `landing` y los guarda tal cual en tablas Delta del esquema `bronze`.
# MAGIC
# MAGIC Principios de esta capa:
# MAGIC - **Sin transformar:** todas las columnas entran como texto. Los tipos, la limpieza y las reglas de calidad son trabajo de Silver.
# MAGIC - **Auditable:** cada fila guarda cuándo se cargó y de qué archivo vino.
# MAGIC - **Idempotente:** Auto Loader recuerda qué archivos ya leyó (checkpoint). Si vuelves a correr el notebook, solo carga archivos nuevos y nunca duplica.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parámetros

# COMMAND ----------

dbutils.widgets.text("catalogo", "recaudo_dev")
dbutils.widgets.text("landing", "/Volumes/recaudo_dev/raw/landing")

CATALOGO = dbutils.widgets.get("catalogo")
LANDING = dbutils.widgets.get("landing").rstrip("/")
CHECKPOINTS = f"/Volumes/{CATALOGO}/bronze/checkpoints"

# Carpeta del landing -> tabla bronze. Bronze no usa los prefijos Dim/Fact:
# esa semántica pertenece al modelo Gold.
TABLAS = {
    "DimAportante": "aportante",
    "DimCotizante": "cotizante",
    "DimTiempo": "tiempo",
    "DimGeografia": "geografia",
    "FactAporteDetalle": "aporte_detalle",
    "FactAportes": "aporte",
    "FactPagos": "pago",
    "FactMora": "mora",
    "FactCobranza": "cobranza",
    "FactAcuerdoPago": "acuerdo_pago",
    "FactRiesgo": "riesgo",
}

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOGO}.bronze")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOGO}.bronze.checkpoints")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preparar el landing
# MAGIC
# MAGIC Auto Loader lee carpetas, no archivos sueltos. Con `particionar=True` el generador deja las tablas de hechos en carpetas, pero las dimensiones quedan como archivos en la raíz. Esta celda mueve cada dimensión a su propia carpeta. Si ya están ordenadas, no hace nada.

# COMMAND ----------

for archivo in dbutils.fs.ls(LANDING):
    nombre = archivo.name
    if nombre.endswith(".csv") and nombre[:-4] in TABLAS:
        destino = f"{LANDING}/{nombre[:-4]}/{nombre}"
        dbutils.fs.mv(archivo.path, destino)
        print(f"Movido {nombre} -> {destino}")

display(dbutils.fs.ls(LANDING))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingesta
# MAGIC
# MAGIC Opciones importantes del lector:
# MAGIC - `cloudFiles.inferColumnTypes = false`: todo entra como texto, así un valor raro no rompe la carga.
# MAGIC - `escape = '"'`: pandas escribe las comillas dentro de un texto duplicándolas (`""`). Sin esta opción, las notas de cobranza con comillas se partirían mal.
# MAGIC - `_rescued_data`: si una fila trae columnas que no encajan con el esquema, el contenido se guarda ahí en vez de perderse.
# MAGIC - `trigger(availableNow=True)`: procesa todo lo pendiente y se detiene. Funciona en cómputo serverless.

# COMMAND ----------

from pyspark.sql import functions as F


def ingerir(carpeta: str, tabla: str):
    destino = f"{CATALOGO}.bronze.{tabla}"
    lector = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", f"{CHECKPOINTS}/{tabla}/schema")
        .option("cloudFiles.inferColumnTypes", "false")
        .option("header", "true")
        .option("escape", '"')
        .load(f"{LANDING}/{carpeta}")
        .withColumn("_ingest_ts", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )
    consulta = (
        lector.writeStream
        .option("checkpointLocation", f"{CHECKPOINTS}/{tabla}/checkpoint")
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(destino)
    )
    consulta.awaitTermination()
    return destino


for carpeta, tabla in TABLAS.items():
    destino = ingerir(carpeta, tabla)
    print(f"{carpeta:<20} -> {destino}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verificación
# MAGIC
# MAGIC Compara las filas de cada tabla bronze con las que reportó el generador en `_resumen.json`. Deben coincidir exactamente: Bronze no filtra nada, ni siquiera los errores inyectados.

# COMMAND ----------

import json

with open(f"{LANDING}/_resumen.json", encoding="utf-8") as f:
    esperadas = json.load(f)["filas"]

filas = []
for carpeta, tabla in TABLAS.items():
    reales = spark.table(f"{CATALOGO}.bronze.{tabla}").count()
    filas.append((tabla, esperadas.get(carpeta), reales, "OK" if reales == esperadas.get(carpeta) else "REVISAR"))

display(spark.createDataFrame(filas, "tabla string, filas_generador long, filas_bronze long, estado string"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Vistazo rápido

# COMMAND ----------

display(spark.sql(f"""
    SELECT _source_file, count(*) AS filas, max(_ingest_ts) AS cargado
    FROM {CATALOGO}.bronze.aporte
    GROUP BY _source_file
    ORDER BY _source_file
"""))