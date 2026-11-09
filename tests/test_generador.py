"""El generador produce un dataset que cumple las 12 metas de correlación.

Se corre al 10% del tamaño (unos segundos). Con menos datos el azar pesa más
y algunas metas pueden salir de su tolerancia sin que el código esté mal.
"""
import pandas as pd
import pytest

from conftest import RAIZ
from generar import generar
from validar import validar

CONFIG = RAIZ / "config" / "parametros.yaml"


@pytest.fixture(scope="module")
def datos(tmp_path_factory):
    salida = tmp_path_factory.mktemp("raw")
    info = generar(CONFIG, salida, escala=0.1, particionar=True)
    return salida, info


def test_crea_las_11_tablas(datos):
    salida, info = datos
    esperadas = {"DimAportante", "DimCotizante", "DimTiempo", "DimGeografia", "FactAporteDetalle",
                 "FactAportes", "FactPagos", "FactMora", "FactCobranza", "FactAcuerdoPago", "FactRiesgo"}
    assert esperadas <= set(info["filas"])
    assert all(n > 0 for n in info["filas"].values())


def test_hechos_particionados_por_mes(datos):
    salida, _ = datos
    meses = list((salida / "FactMora").glob("*.csv"))
    assert len(meses) == 36


def test_cumple_las_metas(datos):
    salida, _ = datos
    assert validar(salida, CONFIG), "Alguna meta de correlación quedó fuera de tolerancia"


def test_es_reproducible(tmp_path):
    """Misma semilla, mismos datos: base de que los CSV no se versionen."""
    a = generar(CONFIG, tmp_path / "a", escala=0.02)
    b = generar(CONFIG, tmp_path / "b", escala=0.02)
    assert a["filas"] == b["filas"]
    pd.testing.assert_frame_equal(pd.read_csv(tmp_path / "a" / "FactMora.csv"),
                                  pd.read_csv(tmp_path / "b" / "FactMora.csv"))
