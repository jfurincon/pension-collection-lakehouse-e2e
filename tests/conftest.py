"""Configuración común de las pruebas: agrega las carpetas de código al path."""
import sys
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
for carpeta in ["src", "src/generador", "src/ml"]:
    sys.path.insert(0, str(RAIZ / carpeta))
