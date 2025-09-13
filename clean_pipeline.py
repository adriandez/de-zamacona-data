#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
clean_pipeline.py
Elimina artefactos generados por los scripts y deja el proyecto en estado inicial.

Uso:
  python3 clean_pipeline.py
  python3 clean_pipeline.py --hard   # además borra cachés/temporales fuera de out/
"""

from __future__ import annotations
import shutil
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parent
OUT = ROOT / "out"

# Rutas adicionales si algún script genera contenido fuera de out/ (opcional)
EXTRA_TARGETS: list[str] = [
    # "alguna/carpeta/temporal",
]

# Patrones a borrar en modo --hard (solo basura; NO borra data/)
HARD_PATTERNS = [
    "**/__pycache__",      # carpetas
    "**/*.pyc",            # caché Python
    "**/.DS_Store",        # macOS
    "**/~$*.xlsx",         # temp excel
]

def rm(path: Path):
    if path.is_dir():
        shutil.rmtree(path, ignore_errors=True)
        print(f"🗑️  borrado dir: {path}")
    elif path.exists():
        path.unlink(missing_ok=True)
        print(f"🗑️  borrado: {path}")

def main():
    hard = "--hard" in sys.argv

    # 1) Borra carpeta out/ completa
    if OUT.exists():
        rm(OUT)

    # 2) Recréala vacía para el siguiente run
    OUT.mkdir(exist_ok=True)
    print(f"📁 creada: {OUT}")

    # 3) Limpieza extra de targets concretos (si los configuras)
    for tgt in EXTRA_TARGETS:
        p = ROOT / tgt
        if p.exists():
            rm(p)

    # 4) Modo --hard: limpia cachés/temporales en todo el repo (excepto data/)
    if hard:
        for pat in HARD_PATTERNS:
            for p in ROOT.glob(pat):
                # protege la carpeta data/
                try:
                    p.relative_to(ROOT / "data")
                    continue
                except ValueError:
                    pass
                rm(p if p.is_dir() else p)

    print("✅ Limpieza completada.")
    return 0

if __name__ == "__main__":
    sys.exit(main())