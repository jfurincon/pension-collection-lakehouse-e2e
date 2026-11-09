# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Inicializar un ambiente
# MAGIC
# MAGIC Prepara un catálogo vacío para que el job mensual pueda correr:
# MAGIC 1. Crea el catálogo, los esquemas `raw`, `bronze`, `silver`, `gold` y `ml`, y el Volume `raw.landing`.
# MAGIC 2. Genera los datos sintéticos en el Volume, igual que en la etapa de Datos (semilla 42, un archivo por mes).
# MAGIC
# MAGIC Si el landing ya tiene archivos, no los vuelve a generar, salvo que `regenerar = si`. Lo corre el job `recaudo_inicializacion`, una sola vez por ambiente.

# COMMAND ----------

# MAGIC %pip install -q faker holidays pyyaml

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("catalogo", "recaudo_dev")
dbutils.widgets.text("escala", "1.0")
dbutils.widgets.dropdown("regenerar", "no", ["no", "si"])
C = dbutils.widgets.get("catalogo")
ESCALA = float(dbutils.widgets.get("escala"))
REGENERAR = dbutils.widgets.get("regenerar") == "si"
LANDING = f"/Volumes/{C}/raw/landing"

import os
import sys
from pathlib import Path

raiz = Path(os.getcwd())
while not (raiz / "config" / "parametros.yaml").exists() and raiz != raiz.parent:
    raiz = raiz.parent
sys.path.append(str(raiz / "src" / "generador"))
print("Raíz del repo:", raiz, "| catálogo:", C, "| escala:", ESCALA)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Objetos del catálogo

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {C} COMMENT 'Recaudo y cartera de pensiones (datos sintéticos)'")
for esquema, comentario in [
    ("raw", "Archivos de origen (Volume landing)"),
    ("bronze", "Datos tal como llegaron, todo como texto"),
    ("silver", "Datos limpios y validados, con cuarentena"),
    ("gold", "Modelo estrella, agregados y metric views"),
    ("ml", "Features, scores, modelos y herramientas del agente"),
]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {C}.{esquema} COMMENT '{comentario}'")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {C}.raw.landing COMMENT 'CSV del generador sintético'")
print("Catálogo, esquemas y Volume listos")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Datos sintéticos

# COMMAND ----------

def tiene_archivos(ruta: str) -> bool:
    try:
        return len(dbutils.fs.ls(ruta)) > 0
    except Exception:
        return False

if tiene_archivos(LANDING) and not REGENERAR:
    print(f"{LANDING} ya tiene archivos: no se regeneran (usa regenerar = si para forzarlo)")
else:
    from generar import generar
    info = generar(raiz / "config" / "parametros.yaml", LANDING, escala=ESCALA, particionar=True)
    print({k: info[k] for k in ("pct_aportantes_con_mora", "pct_en_mora_ultimo_corte")})

display(dbutils.fs.ls(LANDING))
