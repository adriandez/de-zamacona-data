#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
run_pipeline.py (modo simple: logs / apply)

Modos:
  --logs   : (por defecto) Normaliza y genera informes. Inferencia en dry-run (NO escribe enhanced).
  --apply  : Igual que --logs, pero añade --apply a infer_surnames_from_parents.py y
             promueve out/Zamacona_normalized_enhanced.* → out/Zamacona_normalized_patched.*

Otros flags:
  --with-patches : inserta patch_* tras normalize_names.py
  --continue     : no detiene la cadena al primer error
"""

from __future__ import annotations
import subprocess
import sys
from pathlib import Path
from typing import List, Optional
from shutil import copy2

# ---- Config ----
ROOT = Path(__file__).resolve().parent
PY = sys.executable
OUT = ROOT / "out"

ENH_XLSX = OUT / "Zamacona_normalized_enhanced.xlsx"
ENH_CSV  = OUT / "Zamacona_normalized_enhanced.csv"
PAT_XLSX = OUT / "Zamacona_normalized_patched.xlsx"
PAT_CSV  = OUT / "Zamacona_normalized_patched.csv"

BASE: List[str] = [
    "consolidate_raw.py",   # crea out/Zamacona_all_raw.xlsx
    "prepare_columns.py",
    "consolidate.py",       # out/Zamacona_all.xlsx
    "normalize_names.py",   # splits + flags + status
]

PATCH: List[str] = [
    "patch_surnames_control.py",
    "patch_whitelist_and_syns.py",
]

# Orden análisis (insertamos infer entre find_zamacona... y only_green_surnames)
ANALYSIS_BASE: List[str] = [
    "audit_surnames.py",
    "mark_rejected_surnames.py",
    "find_zamacona_in_non_green.py",
    # infer_surnames_from_parents.py  (se inserta dinámicamente)
    "only_green_surnames.py",
    "finalize_output.py",
    "check_dedup.py",
    "count_raw.py",
    "canonicalize_strict_dupes.py",
    # "analyze_duplicates.py",  # si lo quieres, descomenta
]

# Auto-detector para consolidate_raw.py
CANDIDATE_DIRS = ["", "raw", "data", "data/raw", "inputs", "input"]
CANDIDATE_GLOBS = ["zamacona_*.xlsx", "*zamacona*.xlsx", "*.xlsx"]

def detect_raw_args() -> Optional[List[str]]:
    for d in CANDIDATE_DIRS:
        base = (ROOT / d).resolve() if d else ROOT
        if not base.exists():
            continue
        for g in CANDIDATE_GLOBS:
            if any(base.glob(g)):
                return ["--dir", str(base), "--glob", g, "--include-tsv"]
    return None

def has_flag(flag: str) -> bool:
    return flag in sys.argv

def run(cmd: list[str]) -> int:
    print(f"\n──▶ Ejecutando: {' '.join(cmd)}")
    try:
        p = subprocess.run(cmd, cwd=str(ROOT), check=False)
        print(f"──■ Código de salida: {p.returncode}")
        return p.returncode
    except Exception as e:
        print(f"✖ Error ejecutando {cmd}: {e}")
        return 1

def promote_enhanced_to_patched():
    promoted = False
    if ENH_XLSX.exists():
        copy2(ENH_XLSX, PAT_XLSX); promoted = True
        print(f"[OK] Promovido {ENH_XLSX.name} → {PAT_XLSX.name}")
    if ENH_CSV.exists():
        copy2(ENH_CSV, PAT_CSV); promoted = True
        print(f"[OK] Promovido {ENH_CSV.name} → {PAT_CSV.name}")
    if not promoted:
        print("[INFO] No se encontró salida enhanced para promover.")

def build_order(with_patches: bool, mode_apply: bool) -> List[str]:
    order: List[str] = []
    # base
    order += BASE
    if with_patches:
        order += PATCH
    # análisis con infer en su sitio
    for s in ANALYSIS_BASE:
        order.append(s)
        if s == "find_zamacona_in_non_green.py":
            order.append("infer_surnames_from_parents.py")  # se ejecuta dry o apply según modo
    return order

def main() -> int:
    mode_apply = has_flag("--apply")
    with_patches = has_flag("--with-patches")
    allow_continue = has_flag("--continue")

    OUT.mkdir(exist_ok=True)

    order = build_order(with_patches, mode_apply)
    print("Modo:", "APPLY" if mode_apply else "LOGS (dry-run)")
    print("Fase:", "NORMAL+PATCHES" if with_patches else "NORMAL")
    print("Orden:", " -> ".join(order))

    # RAW args para consolidate_raw.py
    raw_args = None
    if "consolidate_raw.py" in order:
        raw_args = detect_raw_args()
        if raw_args:
            print(f"[INFO] RAW detectado: {' '.join(raw_args)}")
        else:
            print("✖ No se encontraron RAW en (., raw/, data/, data/raw/, inputs/).")
            if not allow_continue:
                return 1

    overall_rc = 0
    for script in order:
        path = ROOT / script
        if not path.exists():
            print(f"… (saltado) {script} no existe.")
            continue

        # Comandos por script
        if script == "consolidate_raw.py" and raw_args:
            cmd = [PY, str(path)] + raw_args
        elif script == "infer_surnames_from_parents.py":
            cmd = [PY, str(path)] + (["--apply"] if mode_apply else [])
        else:
            cmd = [PY, str(path)]

        rc = run(cmd)
        if rc != 0:
            overall_rc = rc
            print(f"✖ Falló: {script}")
            if not allow_continue:
                print("Deteniendo pipeline (usa --continue para intentar seguir).")
                return overall_rc
        else:
            if script == "infer_surnames_from_parents.py" and mode_apply:
                # Promueve enhanced → patched para que el resto consuma la versión enriquecida
                promote_enhanced_to_patched()

    print("\n✅ Pipeline finalizado.")
    return overall_rc

if __name__ == "__main__":
    sys.exit(main())