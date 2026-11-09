"""El bundle es válido sin conectarse a Databricks.

Usa los modelos de databricks-bundles, que rechazan campos inexistentes, y
comprueba que cada notebook y archivo referenciado exista en el repo.
"""
import yaml
from databricks.bundles.jobs import Job
from databricks.bundles.pipelines import Pipeline

from conftest import RAIZ

RECURSOS = RAIZ / "resources"


def _cargar(nombre):
    return yaml.safe_load((RECURSOS / nombre).read_text(encoding="utf-8"))["resources"]


def test_targets_dev_y_prod():
    cfg = yaml.safe_load((RAIZ / "databricks.yml").read_text(encoding="utf-8"))
    assert cfg["targets"]["dev"]["variables"]["catalogo"] == "recaudo_dev"
    assert cfg["targets"]["prod"]["variables"]["catalogo"] == "recaudo_prod"


def test_pipelines_validas_y_codigo_existente():
    for nombre, p in _cargar("pipelines.yml")["pipelines"].items():
        Pipeline.from_dict(p)
        for lib in p["libraries"]:
            assert (RECURSOS / lib["file"]["path"]).resolve().exists(), nombre


def test_jobs_validos_y_notebooks_existentes():
    jobs = _cargar("jobs.yml")["jobs"]
    for nombre, j in jobs.items():
        Job.from_dict(j)
        for t in j["tasks"]:
            if "notebook_task" in t:
                ruta = (RECURSOS / t["notebook_task"]["notebook_path"]).resolve()
                assert ruta.exists(), f"{nombre}.{t['task_key']}: falta {ruta.name}"


def test_dependencias_apuntan_a_tareas_del_mismo_job():
    for nombre, j in _cargar("jobs.yml")["jobs"].items():
        claves = {t["task_key"] for t in j["tasks"]}
        for t in j["tasks"]:
            for d in t.get("depends_on", []):
                assert d["task_key"] in claves, f"{nombre}.{t['task_key']} depende de {d['task_key']}"
