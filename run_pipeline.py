#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
run_pipeline.py
Ejecuta la cadena de scripts en orden. Detiene en el primer error y muestra logs claros.

Uso:
  python3 run_pipeline.py
  python3 run_pipeline.py --continue
  python3 run_pipeline.py --with-patches        # inserta patch_* tras normalize_names.py
  python3 run_pipeline.py --continue --with-patches
"""

from __future__ import annotations
import subprocess
import sys
from pathlib import Path
from typing import List, Optional

# —— Config —— #
ROOT = Path(__file__).resolve().parent
PY = sys.executable  # usa el mismo intérprete
OUT_DIR = ROOT / "out"

# Bloques base (orden correcto)
BASE_STAGE: List[str] = [
    "consolidate_raw.py",    # crea out/Zamacona_all_raw.xlsx
    "prepare_columns.py",    # si lo usas
    "consolidate.py",        # armoniza y saca out/Zamacona_all.xlsx
    "normalize_names.py",    # normalización/flags/colores
]

PATCH_STAGE: List[str] = [
    "patch_surnames_control.py",   # mantiene listas de control (si las usas)
    "patch_whitelist_and_syns.py", # mantiene data/whitelist_surnames.txt y surname_synonyms.csv
]

ANALYSIS_STAGE = [
    "audit_surnames.py",
    "mark_rejected_surnames.py",
    "find_zamacona_in_non_green.py",
    "only_green_surnames.py",
    "finalize_output.py",
    "check_dedup.py",
    "count_raw.py",
    "canonicalize_strict_dupes.py",
]

# —— Auto-detección de RAW para consolidate_raw.py —— #
CANDIDATE_DIRS = ["", "raw", "data", "data/raw", "inputs", "input"]
CANDIDATE_GLOBS = ["zamacona_*.xlsx", "*zamacona*.xlsx", "*.xlsx"]

def detect_raw_args() -> Optional[List[str]]:
    """
    Busca ficheros RAW en ubicaciones conocidas y devuelve los args
    para consolidate_raw.py: ['--dir', <dir>, '--glob', <glob>, '--include-tsv']
    """
    for d in CANDIDATE_DIRS:
        base = (ROOT / d).resolve() if d else ROOT
        if not base.exists():
            continue
        for g in CANDIDATE_GLOBS:
            if any(base.glob(g)):
                return ["--dir", str(base), "--glob", g, "--include-tsv"]
    return None

def run(cmd: list[str]) -> int:
    print(f"\n──▶ Ejecutando: {' '.join(cmd)}")
    try:
        proc = subprocess.run(cmd, cwd=str(ROOT), check=False)
        print(f"──■ Código de salida: {proc.returncode}")
        return proc.returncode
    except Exception as e:
        print(f"✖ Error ejecutando {cmd}: {e}")
        return 1

def build_script_list(with_patches: bool) -> List[str]:
    scripts: List[str] = []
    for s in BASE_STAGE:
        scripts.append(s)
        if s == "normalize_names.py" and with_patches:
            scripts.extend(PATCH_STAGE)
    scripts.extend(ANALYSIS_STAGE)
    return scripts

def main() -> int:
    allow_continue = "--continue" in sys.argv
    with_patches = "--with-patches" in sys.argv

    OUT_DIR.mkdir(exist_ok=True)

    scripts = build_script_list(with_patches)
    print("Fase:", "NORMAL+PATCHES" if with_patches else "NORMAL")
    print("Orden:", " -> ".join(scripts))

    # Detecta RAW antes de llamar a consolidate_raw.py
    raw_args: Optional[List[str]] = None
    if "consolidate_raw.py" in scripts:
        raw_args = detect_raw_args()
        if raw_args:
            print(f"[INFO] RAW detectado: {' '.join(raw_args)}")
        else:
            print("✖ No se encontraron ficheros RAW en ubicaciones conocidas (., raw/, data/, data/raw/, inputs/).")
            print("   Coloca tus Excel en alguna de esas carpetas o ajusta CANDIDATE_DIRS/CANDIDATE_GLOBS.")
            if not allow_continue:
                return 1  # aborta aquí para evitar error más abajo

    overall_rc = 0
    for script in scripts:
        script_path = ROOT / script
        if not script_path.exists():
            print(f"… (saltado) {script} no existe en el proyecto.")
            continue

        # Inyecta args auto-detectados solo para consolidate_raw.py
        if script == "consolidate_raw.py" and raw_args:
            cmd = [PY, str(script_path)] + raw_args
        else:
            cmd = [PY, str(script_path)]

        rc = run(cmd)
        if rc != 0:
            overall_rc = rc
            print(f"✖ Falló: {script}")
            if not allow_continue:
                print("Deteniendo pipeline (usa --continue para intentar seguir).")
                return overall_rc

    print("\n✅ Pipeline finalizado.")
    return overall_rc

if __name__ == "__main__":
    sys.exit(main())