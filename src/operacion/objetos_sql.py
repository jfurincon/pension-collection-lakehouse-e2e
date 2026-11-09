"""Utilidades para ejecutar los archivos SQL del repo en cualquier catálogo.

metric_views.sql y herramientas.sql tienen escrito recaudo_dev para poder
correrlos a mano en el editor SQL. El notebook 07 usa sentencias() para
separarlos y cambiar el catálogo según el ambiente (dev, prod).
"""
from __future__ import annotations

import re


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


def nombre_objeto(sentencia: str) -> str:
    """Nombre de la vista o función que crea una sentencia (para los mensajes)."""
    m = re.search(r"(?:VIEW|FUNCTION)\s+(\S+?)[\s(]", sentencia)
    return m.group(1) if m else sentencia[:60]
