"""Métricas del monitoreo del modelo (src/ml/monitoreo.py)."""
import numpy as np
import pandas as pd

import monitoreo as mon

rng = np.random.default_rng(0)


def test_psi_cero_si_no_cambia():
    x = pd.Series(rng.normal(size=5000))
    assert mon.psi(x, x) < 1e-9


def test_psi_detecta_un_cambio_fuerte():
    ref = pd.Series(rng.normal(size=5000))
    assert mon.psi(ref, ref + 1.5) > 0.25


def test_psi_variable_constante():
    assert mon.psi(pd.Series([0] * 100), pd.Series([0] * 50 + [1] * 50)) == 0.0


def test_estado_respeta_el_sentido_de_cada_metrica():
    assert mon.estado("auc_roc", 0.92) == "OK"
    assert mon.estado("auc_roc", 0.87) == "VIGILAR"
    assert mon.estado("auc_roc", 0.80) == "ALERTA"
    assert mon.estado("psi_score", 0.05) == "OK"
    assert mon.estado("psi_variable", 0.30) == "ALERTA"
    assert mon.estado("brecha_calibracion", 0.04) == "VIGILAR"


def test_desempeno_de_un_modelo_perfecto():
    y = np.array([0] * 80 + [1] * 20)
    p = np.where(y == 1, 0.9, 0.1)
    d = mon.desempeno_mes(y, p)
    assert d["auc_roc"] == 1.0 and d["captura_top20"] == 1.0


def _datos_de_juguete(n=2000):
    """Scores calibrados: la etiqueta se sortea con la misma probabilidad del score."""
    filas = [{"AportanteID": f"A{i}", "Periodo": per, "x": rng.normal()}
             for per in ["2024-01", "2024-05", "2025-07"] for i in range(n)]
    f = pd.DataFrame(filas)
    prob = 1 / (1 + np.exp(-(4 * f["x"] - 3)))
    f[mon.ETIQUETA] = (rng.random(len(f)) < prob).astype(float)
    for v in mon.VARIABLES_PSI:
        f[v] = rng.normal(size=len(f))
    s = f[["AportanteID", "Periodo"]].assign(
        ProbabilidadIncumplimiento=prob,
        Muestra=np.where(f["Periodo"] < "2024-10", "entrenamiento", "prueba"))
    return s, f


def test_calcular_marca_la_referencia_y_no_alerta_si_nada_cambia():
    s, f = _datos_de_juguete()
    m = mon.calcular(s, f)
    assert set(m.loc[m["Muestra"] == "entrenamiento", "Estado"]) == {"REFERENCIA"}
    assert mon.resumen(m)["estado"] == "OK", mon.resumen(m)["alertas"]


def test_calcular_alerta_si_una_variable_cambia():
    s, f = _datos_de_juguete()
    f.loc[f["Periodo"] == "2025-07", "DiasMora"] += 2
    r = mon.resumen(mon.calcular(s, f))
    assert r["estado"] == "ALERTA"
    assert {"Variable": "DiasMora", "Estado": "ALERTA"}.items() <= next(
        a for a in r["alertas"] if a["Variable"] == "DiasMora").items()
