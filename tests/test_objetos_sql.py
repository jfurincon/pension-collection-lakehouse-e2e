"""El notebook 07 separa bien los archivos SQL y cambia el catálogo."""
from conftest import RAIZ
from operacion.objetos_sql import nombre_objeto, sentencias


def _leer(ruta):
    return (RAIZ / ruta).read_text(encoding="utf-8")


def test_metric_views_en_otro_catalogo():
    s = sentencias(_leer("src/gold/metric_views.sql"), "recaudo_prod")
    assert [nombre_objeto(x) for x in s] == [f"recaudo_prod.gold.metricas_{n}"
                                            for n in ("recaudo", "cartera", "cobranza", "riesgo")]
    assert not any("recaudo_dev" in x for x in s)


def test_herramientas_del_agente():
    s = sentencias(_leer("src/agente/herramientas.sql"), "recaudo_prod")
    assert len(s) == 4
    assert all(x.startswith("CREATE OR REPLACE FUNCTION recaudo_prod.ml.") for x in s)


def test_ignora_comentarios_con_punto_y_coma():
    texto = "-- SELECT 1;\nCREATE VIEW recaudo_dev.gold.v AS SELECT 1;\nSELECT * FROM x;\n"
    assert sentencias(texto, "c") == ["CREATE VIEW c.gold.v AS SELECT 1"]
