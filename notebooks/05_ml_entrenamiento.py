# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 05 · ML: entrenamiento y comparación
# MAGIC
# MAGIC Entrena dos modelos de **probabilidad de incumplimiento a 3 meses**, los compara con el score por reglas (`reglas-v1`) y registra el mejor en Unity Catalog como `recaudo_dev.ml.modelo_incumplimiento` con el alias `champion`.
# MAGIC
# MAGIC | Paso | Qué se hace |
# MAGIC | --- | --- |
# MAGIC | Partición temporal | Entrenamiento hasta 2024-09 · validación 2025-01 a 2025-03 · prueba 2025-06 a 2025-09, con meses de separación entre bloques |
# MAGIC | Modelos | Regresión logística (línea base interpretable) y LightGBM (boosting de árboles) |
# MAGIC | Referencia | El score por reglas `reglas-v1`, evaluado contra la misma etiqueta |
# MAGIC | Selección | El de mejor AUC-PR en **validación**. La prueba solo se usa para reportar |
# MAGIC | Registro | Cada modelo queda como un *run* de MLflow; el ganador se registra en Unity Catalog |
# MAGIC
# MAGIC Requiere haber corrido `04_ml_features`.

# COMMAND ----------

# MAGIC %pip install -q lightgbm "mlflow-skinny[databricks]>=3.1"

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("catalogo", "recaudo_dev")
C = dbutils.widgets.get("catalogo")
FEATURES_TABLA = f"{C}.ml.features_aportante_mes"
MODELO = f"{C}.ml.modelo_incumplimiento"

import importlib, os, sys, warnings
from pathlib import Path
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import mlflow
from mlflow import MlflowClient

warnings.filterwarnings("ignore", category=UserWarning)
raiz = Path(os.getcwd())
while not (raiz / "config" / "parametros.yaml").exists() and raiz != raiz.parent:
    raiz = raiz.parent
RUTA_PREP = raiz / "src" / "ml" / "preparacion.py"
sys.path.append(str(RUTA_PREP.parent))
import preparacion as prep
importlib.reload(prep)

mlflow.set_registry_uri("databricks-uc")
usuario = spark.sql("SELECT current_user()").first()[0]
mlflow.set_experiment(f"/Users/{usuario}/recaudo_probabilidad_incumplimiento")
print("Experimento:", f"/Users/{usuario}/recaudo_probabilidad_incumplimiento")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Datos y partición temporal

# COMMAND ----------

datos = spark.table(FEATURES_TABLA).toPandas()
reglas = spark.sql(f"""
    SELECT AportanteID, Periodo, ProbabilidadIncumplimiento AS ScoreReglas
    FROM {C}.gold.fact_riesgo WHERE ScoreVersion = 'reglas-v1'
""").toPandas()
datos = datos.merge(reglas, on=["AportanteID", "Periodo"], how="left")

entrenamiento, validacion, prueba = prep.dividir(datos)
X_tr, X_va = prep.matriz(entrenamiento), prep.matriz(validacion)
y_tr = entrenamiento[prep.ETIQUETA].astype(int)
y_va = validacion[prep.ETIQUETA].astype(int)

display(pd.DataFrame({
    "bloque": ["entrenamiento", "validación", "prueba"],
    "desde": [d["Periodo"].min() for d in (entrenamiento, validacion, prueba)],
    "hasta": [d["Periodo"].max() for d in (entrenamiento, validacion, prueba)],
    "filas": [len(d) for d in (entrenamiento, validacion, prueba)],
    "tasa_etiqueta": [round(d[prep.ETIQUETA].mean(), 4) for d in (entrenamiento, validacion, prueba)],
}))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Entrenar y registrar cada candidato en MLflow
# MAGIC
# MAGIC Cada *run* guarda parámetros, métricas en validación y prueba (en tres segmentos: todos, al día hoy y en mora hoy) y el modelo empaquetado con su preparación de datos.

# COMMAND ----------

def evaluar(columna_score):
    """Métricas en validación y prueba, con prefijo para MLflow."""
    salida = {}
    for nombre, d in (("val", validacion), ("test", prueba)):
        salida.update({f"{nombre}_{k}": v for k, v in prep.metricas_por_segmento(d, columna_score).items()})
    return salida


def registrar_run(nombre, modelo, parametros):
    """Califica validación y prueba, registra el run y devuelve sus métricas."""
    for d in (validacion, prueba):
        d[f"p_{nombre}"] = modelo.predict_proba(d[prep.FEATURES])[:, 1]
    m = evaluar(f"p_{nombre}")
    ejemplo = entrenamiento[prep.FEATURES].head(5).copy()
    ejemplo[prep.NUMERICAS] = ejemplo[prep.NUMERICAS].astype(float)
    with mlflow.start_run(run_name=nombre) as run:
        mlflow.log_params({**parametros, "etiqueta": prep.ETIQUETA, "fin_entrenamiento": prep.FIN_ENTRENAMIENTO,
                           "n_features": len(prep.FEATURES)})
        mlflow.log_metrics(m)
        # cloudpickle: el modelo incluye la función de preparación (preparacion.matriz),
        # que el formato skops no admite. code_paths empaqueta ese archivo con el modelo.
        info = mlflow.sklearn.log_model(modelo, name="modelo", input_example=ejemplo,
                                        code_paths=[str(RUTA_PREP)],
                                        serialization_format="cloudpickle")
    return {"run_id": run.info.run_id, "model_uri": info.model_uri, **m}


resultados = {}

# Referencia: el score por reglas, evaluado contra la misma etiqueta
with mlflow.start_run(run_name="reglas-v1 (referencia)") as run:
    m = evaluar("ScoreReglas")
    mlflow.log_params({"tipo": "reglas", "etiqueta": prep.ETIQUETA})
    mlflow.log_metrics(m)
resultados["reglas-v1"] = {"run_id": run.info.run_id, "model_uri": None, **m}

# Regresión logística
logistica = prep.modelo_logistico().fit(X_tr, y_tr)
resultados["logistica"] = registrar_run("logistica", prep.empaquetar(logistica, entrenamiento),
                                        {"tipo": "regresion_logistica", "C": 1.0})

# LightGBM con parada temprana en validación
lgbm = prep.entrenar_lightgbm(X_tr, y_tr, X_va, y_va)
resultados["lightgbm"] = registrar_run("lightgbm", prep.empaquetar(lgbm, entrenamiento),
                                       {"tipo": "lightgbm", **prep.PARAMS_LGBM,
                                        "n_estimators_final": lgbm.best_iteration_})
print("LightGBM se detuvo en", lgbm.best_iteration_, "árboles")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Comparación
# MAGIC
# MAGIC - **AUC-ROC:** qué tan bien ordena del más al menos riesgoso (0,5 = azar).
# MAGIC - **AUC-PR:** precisión promedio; es más exigente cuando los incumplidos son minoría.
# MAGIC - **KS:** máxima separación entre buenos y malos, el estándar en riesgo de crédito.
# MAGIC - **Lift top 10%:** cuántas veces más incumplidos hay en el 10% más riesgoso que en el promedio.
# MAGIC - **Brier:** error de la probabilidad (menor es mejor), que mide si "20%" significa de verdad 20%.

# COMMAND ----------

filas = []
for nombre, r in resultados.items():
    for bloque, prefijo in (("validación", "val"), ("prueba", "test")):
        filas.append({
            "modelo": nombre, "bloque": bloque,
            "AUC-ROC": r[f"{prefijo}_todos_auc_roc"], "AUC-PR": r[f"{prefijo}_todos_auc_pr"],
            "KS": r[f"{prefijo}_todos_ks"], "Lift top 10%": r[f"{prefijo}_todos_lift_top10"],
            "Brier": r[f"{prefijo}_todos_brier"],
            "AUC al día hoy": r[f"{prefijo}_al_dia_hoy_auc_roc"],
            "AUC en mora hoy": r[f"{prefijo}_en_mora_hoy_auc_roc"],
        })
comparacion = pd.DataFrame(filas).round(3)
display(comparacion)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Elegir y registrar el campeón
# MAGIC
# MAGIC El ganador se elige solo con **validación** (AUC-PR). Si se eligiera con prueba, la prueba dejaría de ser una medida honesta del desempeño futuro.

# COMMAND ----------

candidatos = {k: v for k, v in resultados.items() if v["model_uri"]}
campeon = max(candidatos, key=lambda k: candidatos[k]["val_todos_auc_pr"])
r = candidatos[campeon]
print(f"Campeón: {campeon} · AUC-PR validación {r['val_todos_auc_pr']:.3f} · "
      f"AUC-ROC prueba {r['test_todos_auc_roc']:.3f} (reglas: {resultados['reglas-v1']['test_todos_auc_roc']:.3f})")

version = mlflow.register_model(r["model_uri"], MODELO)
cliente = MlflowClient()
cliente.set_registered_model_alias(MODELO, "champion", version.version)
cliente.update_model_version(
    MODELO, version.version,
    description=(f"{campeon}. Probabilidad de mora > 30 días en los próximos 3 meses. "
                 f"AUC-ROC prueba {r['test_todos_auc_roc']:.3f} vs reglas-v1 "
                 f"{resultados['reglas-v1']['test_todos_auc_roc']:.3f}. Entrenado hasta {prep.FIN_ENTRENAMIENTO}."))
print(f"Registrado {MODELO} versión {version.version} con alias 'champion'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ¿Por qué decide así? Importancia de las variables
# MAGIC
# MAGIC Promedio del valor absoluto de las contribuciones (valores SHAP en LightGBM, coeficiente × valor en la logística) sobre el periodo de prueba. Indica cuánto mueve cada variable el score, en promedio.

# COMMAND ----------

modelo_campeon = prep.empaquetar(lgbm if campeon == "lightgbm" else logistica, entrenamiento)
contrib = prep.contribuciones(modelo_campeon, prueba)
importancia = (contrib.abs().mean().sort_values(ascending=True).tail(15)
               .rename(lambda c: prep.DESCRIPCION[c][0]))

fig, ax = plt.subplots(figsize=(8, 6))
importancia.plot.barh(ax=ax, color="#2f6db5")
ax.set_xlabel("Contribución media absoluta (log-odds)")
ax.set_title(f"Variables que más mueven el score · {campeon}")
plt.tight_layout()
with mlflow.start_run(run_id=r["run_id"]):
    mlflow.log_figure(fig, "importancia_variables.png")
display(fig)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deciles en prueba: campeón vs. reglas
# MAGIC
# MAGIC Se ordena a los aportantes por score y se parten en 10 grupos iguales (decil 1 = el más riesgoso). Un buen score concentra a los incumplidos en los primeros deciles. La columna `captura_acumulada` responde: "si el equipo de cobranza solo gestiona el 20% más riesgoso, ¿qué porcentaje de los futuros incumplidos alcanza?".

# COMMAND ----------

deciles_campeon = prep.tabla_deciles(prueba[prep.ETIQUETA], prueba[f"p_{campeon}"])
deciles_reglas = prep.tabla_deciles(prueba[prep.ETIQUETA], prueba["ScoreReglas"])
tabla = deciles_campeon[["decil", "tasa_real", "captura_acumulada"]].merge(
    deciles_reglas[["decil", "tasa_real", "captura_acumulada"]], on="decil",
    suffixes=(f"_{campeon}", "_reglas")).round(3)
display(tabla)

fig, ax = plt.subplots(figsize=(8, 4))
ancho = 0.4
ax.bar(tabla["decil"] - ancho / 2, tabla[f"tasa_real_{campeon}"], ancho, label=campeon, color="#2f6db5")
ax.bar(tabla["decil"] + ancho / 2, tabla["tasa_real_reglas"], ancho, label="reglas-v1", color="#b0b0b0")
ax.axhline(prueba[prep.ETIQUETA].mean(), color="#555", linestyle="--", linewidth=1, label="tasa promedio")
ax.set_xticks(tabla["decil"])
ax.set_xlabel("Decil de score (1 = más riesgoso)")
ax.set_ylabel("Tasa real de incumplimiento")
ax.set_title("Tasa de incumplimiento por decil · periodo de prueba")
ax.legend()
plt.tight_layout()
with mlflow.start_run(run_id=r["run_id"]):
    mlflow.log_figure(fig, "deciles_prueba.png")
    mlflow.log_table(tabla, "deciles_prueba.json")
display(fig)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Calibración: ¿la probabilidad dice la verdad?
# MAGIC
# MAGIC Si el modelo dice 30% a un grupo de aportantes, cerca del 30% de ellos debería incumplir. La línea punteada es la calibración perfecta.

# COMMAND ----------

cal = deciles_campeon.sort_values("prob_media")
fig, ax = plt.subplots(figsize=(5, 5))
ax.plot([0, 1], [0, 1], linestyle="--", color="#999")
ax.plot(cal["prob_media"], cal["tasa_real"], marker="o", color="#2f6db5")
ax.set_xlabel("Probabilidad media predicha")
ax.set_ylabel("Tasa real observada")
ax.set_title(f"Calibración · {campeon} · prueba")
plt.tight_layout()
with mlflow.start_run(run_id=r["run_id"]):
    mlflow.log_figure(fig, "calibracion_prueba.png")
display(fig)