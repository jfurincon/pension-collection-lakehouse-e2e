"""Monitoreo del modelo de probabilidad de incumplimiento (score ml-v1).

Responde dos preguntas cada mes:

1. Desempeño: ¿el modelo sigue acertando? Solo se puede medir en meses cuyo
   resultado ya se conoce (pasaron 3 meses). Métricas: AUC-ROC, captura del
   20% más riesgoso y brecha de calibración.
2. Estabilidad: ¿los datos que recibe se parecen a los que vio al entrenar?
   Se mide en todos los meses, incluso los recientes sin resultado, con el PSI
   (Population Stability Index) del score y de las variables principales.

El notebook 08 usa estas funciones y guarda el resultado en ml.monitoreo_modelo.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
from sklearn.metrics import roc_auc_score

ETIQUETA = "Mora30Prox3m"

# Referencia del PSI: meses de entrenamiento con historia completa. En 2023 las
# empresas tienen en promedio 6,5 meses de historia (los datos empiezan en
# 2023-01), y las variables de 12 meses toman valores extremos. Usarlos como
# referencia daba un PSI de 0,17 en TasaPagoOportuno12m sin que nada cambiara.
REFERENCIA = ("2024-01", "2024-09")

# Variables cuya distribución se vigila: las de mayor importancia en el modelo
# y una de comportamiento reciente.
VARIABLES_PSI = ["DiasMora", "PeriodosAdeudados", "TasaPagoOportuno12m",
                 "PagosExtemporaneos12m", "TendenciaCotizantes", "Gestiones3m"]

# Umbrales: (vigilar, alerta). Para el desempeño, "peor" es más bajo; para la
# brecha de calibración y el PSI, "peor" es más alto.
UMBRALES = {
    "auc_roc": (0.88, 0.85),              # en prueba fue 0,92
    "captura_top20": (0.70, 0.65),        # en prueba fue 0,77
    "brecha_calibracion": (0.03, 0.05),   # |probabilidad media - tasa real|
    "psi": (0.10, 0.25),                  # convención estándar en riesgo de crédito
}
MAYOR_ES_MEJOR = {"auc_roc": True, "captura_top20": True, "brecha_calibracion": False, "psi": False}


def psi(referencia: pd.Series, actual: pd.Series, bins: int = 10) -> float:
    """Population Stability Index entre dos distribuciones.

    Corta la referencia en `bins` grupos de igual tamaño (cuantiles) y compara
    qué porcentaje de cada distribución cae en cada grupo:
        PSI = Σ (actual% - referencia%) · ln(actual% / referencia%)
    Menos de 0,10: estable. Entre 0,10 y 0,25: vigilar. Más de 0,25: cambio fuerte.
    """
    ref = pd.to_numeric(referencia, errors="coerce").dropna().to_numpy(float)
    act = pd.to_numeric(actual, errors="coerce").dropna().to_numpy(float)
    if len(ref) == 0 or len(act) == 0:
        return float("nan")
    cortes = np.unique(np.quantile(ref, np.linspace(0, 1, bins + 1)))
    if len(cortes) < 2:            # variable constante en la referencia
        return 0.0
    cortes[0], cortes[-1] = -np.inf, np.inf
    p_ref = np.histogram(ref, cortes)[0] / len(ref)
    p_act = np.histogram(act, cortes)[0] / len(act)
    eps = 1e-4
    p_ref, p_act = np.clip(p_ref, eps, None), np.clip(p_act, eps, None)
    return float(np.sum((p_act - p_ref) * np.log(p_act / p_ref)))


def estado(metrica: str, valor: float) -> str:
    """OK, VIGILAR o ALERTA según los umbrales de la métrica."""
    if valor is None or np.isnan(valor):
        return "SIN DATO"
    clave = "psi" if metrica.startswith("psi") else metrica
    vigilar, alerta = UMBRALES[clave]
    if MAYOR_ES_MEJOR[clave]:
        return "ALERTA" if valor < alerta else "VIGILAR" if valor < vigilar else "OK"
    return "ALERTA" if valor > alerta else "VIGILAR" if valor > vigilar else "OK"


def desempeno_mes(y: pd.Series, p: pd.Series) -> dict:
    """AUC-ROC, captura del 20% más riesgoso y brecha de calibración de un mes."""
    y, p = np.asarray(y, int), np.asarray(p, float)
    if len(np.unique(y)) < 2:
        return {}
    orden = np.argsort(-p)
    top = orden[: int(np.ceil(0.2 * len(p)))]
    return {
        "auc_roc": float(roc_auc_score(y, p)),
        "captura_top20": float(y[top].sum() / y.sum()),
        "brecha_calibracion": float(abs(p.mean() - y.mean())),
    }


def calcular(scores: pd.DataFrame, features: pd.DataFrame) -> pd.DataFrame:
    """Tabla larga de monitoreo: una fila por periodo, métrica y variable.

    scores:   AportanteID, Periodo, Muestra, ProbabilidadIncumplimiento (ml-v1)
    features: AportanteID, Periodo, la etiqueta y las VARIABLES_PSI
    La referencia para el PSI son los meses de REFERENCIA (entrenamiento con
    historia completa).
    """
    datos = scores.merge(features[["AportanteID", "Periodo", ETIQUETA] + VARIABLES_PSI],
                         on=["AportanteID", "Periodo"], how="left")
    ref = datos[datos["Periodo"].between(*REFERENCIA)]
    filas = []
    for (periodo, muestra), d in datos.groupby(["Periodo", "Muestra"], sort=True):
        con_resultado = d[d[ETIQUETA].notna()]
        if len(con_resultado):
            for metrica, valor in desempeno_mes(con_resultado[ETIQUETA],
                                                con_resultado["ProbabilidadIncumplimiento"]).items():
                filas.append((periodo, muestra, "Desempeño", metrica, None, valor))
        filas.append((periodo, muestra, "Estabilidad", "psi_score", "ProbabilidadIncumplimiento",
                      psi(ref["ProbabilidadIncumplimiento"], d["ProbabilidadIncumplimiento"])))
        for v in VARIABLES_PSI:
            filas.append((periodo, muestra, "Estabilidad", "psi_variable", v, psi(ref[v], d[v])))
    out = pd.DataFrame(filas, columns=["Periodo", "Muestra", "Tipo", "Metrica", "Variable", "Valor"])
    out["Estado"] = [estado(m, v) for m, v in zip(out["Metrica"], out["Valor"])]
    clave = out["Metrica"].where(~out["Metrica"].str.startswith("psi"), "psi")
    out["UmbralVigilar"] = clave.map(lambda k: UMBRALES[k][0])
    out["UmbralAlerta"] = clave.map(lambda k: UMBRALES[k][1])
    # Los meses de entrenamiento son optimistas para el desempeño y son la
    # referencia del PSI: se guardan, pero no generan alertas.
    out.loc[out["Muestra"] == "entrenamiento", "Estado"] = "REFERENCIA"
    return out


def resumen(monitoreo: pd.DataFrame) -> dict:
    """Estado del último mes con resultado (desempeño) y del último mes (estabilidad)."""
    m = monitoreo[monitoreo["Muestra"] != "entrenamiento"]
    des = m[m["Tipo"] == "Desempeño"]
    est = m[m["Tipo"] == "Estabilidad"]
    ult_des, ult_est = des["Periodo"].max(), est["Periodo"].max()
    alertas = pd.concat([des[des["Periodo"] == ult_des], est[est["Periodo"] == ult_est]])
    alertas = alertas[alertas["Estado"].isin(["VIGILAR", "ALERTA"])]
    peor = "ALERTA" if (alertas["Estado"] == "ALERTA").any() else \
           "VIGILAR" if len(alertas) else "OK"
    return {"estado": peor, "ultimo_mes_desempeno": ult_des, "ultimo_mes_estabilidad": ult_est,
            "alertas": alertas[["Periodo", "Metrica", "Variable", "Valor", "Estado"]].to_dict("records")}
