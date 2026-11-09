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
import re
from pathlib import Path

raiz = Path(os.getcwd())
while not (raiz / "config" / "parametros.yaml").exists() and raiz != raiz.parent:
    raiz = raiz.parent
print("Raíz del repo:", raiz, "| catálogo:", C)

ARCHIVOS = [raiz / "src" / "gold" / "metric_views.sql",
            raiz / "src" / "agente" / "herramientas.sql"]

# COMMAND ----------

def sentencias(texto: str, catalogo: str) -> list[str]:
    """Separa un archivo SQL en sentencias y cambia el catálogo.

    - Quita las líneas que son solo comentario (pueden contener ';').
    - Corta donde una línea termina en ';'. Los ';' dentro de un texto o del
      YAML de una metric view no están al final de una línea, así que no cortan.
    - Omite los SELECT de prueba del final de cada archivo.
    """
    lineas = [l for l in texto.splitlines() if not l.lstrip().startswith("--")]
    partes = re.split(r";[ \t]*\n", "\n".join(lineas) + "\n")
    salida = []
    for p in partes:
        p = p.strip()
        if not p or p.upper().startswith("SELECT"):
            continue
        salida.append(p.replace("recaudo_dev.", f"{catalogo}."))
    return salida

# COMMAND ----------

for archivo in ARCHIVOS:
    lista = sentencias(archivo.read_text(encoding="utf-8"), C)
    print(f"{archivo.relative_to(raiz)}: {len(lista)} sentencias")
    for s in lista:
        nombre = re.search(r"(?:VIEW|FUNCTION)\s+(\S+?)[\s(]", s)
        spark.sql(s)
        print("   ok", nombre.group(1) if nombre else s[:60])

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
