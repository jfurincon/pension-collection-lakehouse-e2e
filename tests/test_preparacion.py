"""Reglas de negocio del modelo que no deben cambiar sin querer (src/ml/preparacion.py)."""
import numpy as np
import pandas as pd

import preparacion as prep


def test_particion_temporal_con_meses_de_separacion():
    m = prep.muestra(pd.Series(["2024-09", "2024-10", "2025-01", "2025-04", "2025-06", "2025-10"]))
    assert list(m) == ["entrenamiento", "separacion", "validacion", "separacion", "prueba", "produccion"]


def test_niveles_con_los_umbrales_de_reglas_v1():
    assert list(prep.nivel(np.array([0.10, 0.20, 0.50, 0.90]))) == ["Bajo", "Medio", "Alto", "Critico"]


def test_features_no_usan_columnas_del_futuro():
    """EstadoPago, ValorPagado y EstadoAcuerdo reflejan el corte final: serían fuga de datos."""
    prohibidas = {"EstadoPago", "ValorPagado", "EstadoAcuerdo"}
    assert not prohibidas & set(prep.FEATURES)


def test_metricas_basicas():
    y = np.array([0, 0, 1, 1])
    m = prep.metricas(y, np.array([0.1, 0.2, 0.8, 0.9]))
    assert m["auc_roc"] == 1.0
