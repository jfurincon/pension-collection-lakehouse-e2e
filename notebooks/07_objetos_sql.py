# Databricks notebook source
# MAGIC %md
# MAGIC # 07 · Objetos SQL: metric views y herramientas del agente
# MAGIC
# MAGIC Ejecuta `src/gold/metric_views.sql` y `src/agente/herramientas.sql` en el catálogo que se le indique.
# MAGIC
# MAGIC Esos archivos tienen escrito `recaudo_dev`, para que se puedan abrir y correr a mano en el editor SQL. Este notebook los lee, cambia el catálogo por el del parámetro y ejecuta cada sentencia. Así el job mensual puede crearlos en cualquier ambiente (`recaudo_dev`, `recaudo_prod`…) sin duplicar el SQL.
# MAGIC
# MAGIC Todas las sentencias son `CREATE OR REPLACE`: correrlo varias veces no cambia nada.

# COMMAND ----------

dbutils.widgets.text("catalogo", "recaudo_dev")
C = dbutils.widgets.get("catalogo")

import os
import sys
from pathlib import Path

raiz = Path(os.getcwd())
while not (raiz / "config" / "parametros.yaml").exists() and raiz != raiz.parent:
    raiz = raiz.parent
print("Raíz del repo:", raiz, "| catálogo:", C)

sys.path.append(str(raiz / "src"))
from operacion.objetos_sql import sentencias, nombre_objeto   # separa el SQL y cambia el catálogo

ARCHIVOS = [raiz / "src" / "gold" / "metric_views.sql",
            raiz / "src" / "agente" / "herramientas.sql"]

# COMMAND ----------

for archivo in ARCHIVOS:
    lista = sentencias(archivo.read_text(encoding="utf-8"), C)
    print(f"{archivo.relative_to(raiz)}: {len(lista)} sentencias")
    for s in lista:
        spark.sql(s)
        print("   ok", nombre_objeto(s))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Comprobación rápida

# COMMAND ----------

display(spark.sql(f"""
    SELECT Anio, MEASURE(`Porcentaje de cumplimiento`) AS cumplimiento,
           MEASURE(`Brecha de recaudo`) AS brecha
    FROM {C}.gold.metricas_recaudo
    GROUP BY ALL ORDER BY Anio
"""))
display(spark.sql(f"SELECT * FROM {C}.ml.efectividad_canales('1-30')"))
