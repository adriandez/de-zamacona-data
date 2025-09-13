#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
analyze_duplicates.py

Distingue:
  1) Duplicado EXACTO (todas las columnas iguales, con opción de ignorar 'status').
  2) Duplicado por arkId (mismo arkId).
  3) Duplicado por (arkId + origen) donde 'origen' se autodetecta entre columnas:
     ['source','file','origin','doc','document','image','batch','input','sourceFile','src'] (case-insensitive).

Genera en out/:
  - full_row_dupes.tsv                → filas que son 100% duplicadas (más allá de la primera)
  - ark_dupe_summary.tsv             → resumen por arkId (cuántas filas, cuántos orígenes distintos)
  - ark_source_dupe_summary.tsv      → resumen por (arkId, origen)
  - ark_diff_cols.tsv                → para arkId con >1 fila, qué columnas difieren
  - dupes_report.txt                 → resumen legible con métricas clave

Uso:
  python3 analyze_duplicates.py
  python3 analyze_duplicates.py --keep-status   # considera 'status' en el cálculo de duplicado exacto
"""

from __future__ import annotations
from pathlib import Path
import sys
import re
import pandas as pd
import numpy as np

ROOT = Path(__file__).resolve().parent
OUT = ROOT / "out"
SRC = OUT / "Zamacona_normalized_patched.xlsx"

ORIGIN_CANDIDATES = [
    "source","file","origin","doc","document","image","batch","input","sourcefile","src",
    "book","folio","page","imageid","microfilm","roll"
]

def find_origin_col(cols):
    lc = {c.lower(): c for c in cols}
    # prioriza columnas más obvias
    for key in ORIGIN_CANDIDATES:
        if key in lc:
            return lc[key]
    # heurística: algo que contenga 'file' o 'source' en el nombre
    for c in cols:
        if re.search(r"(file|source|origin|doc|image)", c, flags=re.I):
            return c
    return None

def main():
    if not SRC.exists():
        print(f"[ERROR] No existe {SRC}. Ejecuta antes find_zamacona_in_non_green.py.", file=sys.stderr)
        sys.exit(1)

    keep_status = "--keep-status" in sys.argv

    df = pd.read_excel(SRC)
    if "arkId" not in df.columns:
        print("[ERROR] No existe columna 'arkId' en el dataset.", file=sys.stderr)
        sys.exit(1)

    # Normaliza nombres de columnas (solo para búsqueda; no cambia df)
    cols = list(df.columns)
    origin_col = find_origin_col(cols)

    # — 1) Duplicados EXACTOS —
    # Por defecto ignoramos 'status' (visual) si existe
    cols_for_exact = list(df.columns)
    if "status" in cols_for_exact and not keep_status:
        cols_for_exact.remove("status")

    # Usa stringificación estable para comparar NaN vs vacío:
    def _norm_val(v):
        if pd.isna(v):
            return ""
        return str(v).strip()

    exact_key = df[cols_for_exact].applymap(_norm_val).astype(str).agg("||".join, axis=1)
    df["_exact_key"] = exact_key
    dup_mask_exact = df.duplicated(subset=["_exact_key"], keep="first")
    full_dupes = df[dup_mask_exact].copy()
    full_dupes.to_csv(OUT / "full_row_dupes.tsv", sep="\t", index=False, encoding="utf-8")

    # — 2) Duplicados por arkId —
    df["_ark"] = df["arkId"].astype(str)
    g_ark = df.groupby("_ark", sort=False)
    ark_summary = g_ark.size().reset_index(name="row_count")
    # número de orígenes distintos por ark
    if origin_col and origin_col in df.columns:
        ark_dist_origs = g_ark[origin_col].nunique(dropna=False).reset_index(name="distinct_origins")
        ark_summary = ark_summary.merge(ark_dist_origs, on="_ark", how="left")
    else:
        ark_summary["distinct_origins"] = np.nan
    ark_summary.rename(columns={"_ark": "arkId"}, inplace=True)
    ark_summary.to_csv(OUT / "ark_dupe_summary.tsv", sep="\t", index=False, encoding="utf-8")

    # — 3) Duplicados por (arkId + origen) —
    if origin_col and origin_col in df.columns:
        df["_origin"] = df[origin_col].astype(str)
        g_pair = df.groupby(["_ark", "_origin"], sort=False).size().reset_index(name="row_count")
        g_pair.rename(columns={"_ark": "arkId", "_origin": origin_col}, inplace=True)
        g_pair.to_csv(OUT / "ark_source_dupe_summary.tsv", sep="\t", index=False, encoding="utf-8")
    else:
        pd.DataFrame(columns=["arkId","origin","row_count"]).to_csv(
            OUT / "ark_source_dupe_summary.tsv", sep="\t", index=False, encoding="utf-8"
        )

    # — 4) Qué columnas difieren por arkId (>1 fila) —
    # Para cada ark con >1 fila, marcamos qué columnas tienen más de 1 valor distinto
    diff_rows = []
    multi_arks = ark_summary.loc[ark_summary["row_count"] > 1, "arkId"].astype(str).tolist()
    candidate_cols = [c for c in df.columns if not c.startswith("_")]  # exclude internal

    for ark in multi_arks:
        block = df[df["_ark"] == ark]
        diffs = []
        for c in candidate_cols:
            nuniq = block[c].astype(str).nunique(dropna=False)
            if nuniq > 1:
                diffs.append(c)
        diff_rows.append({"arkId": ark, "diff_cols": ", ".join(diffs), "diff_count": len(diffs)})

    pd.DataFrame(diff_rows).sort_values(["diff_count"], ascending=False).to_csv(
        OUT / "ark_diff_cols.tsv", sep="\t", index=False, encoding="utf-8"
    )

    # — 5) Resumen legible —
    total = len(df)
    ark_unique = df["_ark"].nunique()
    exact_dupes = full_dupes.shape[0]
    ark_dupes = total - ark_unique
    origin_info = f" (usando columna origen='{origin_col}')" if origin_col else " (no se detectó columna de origen)"

    with open(OUT / "dupes_report.txt", "w", encoding="utf-8") as f:
        f.write("=== DUPLICATES REPORT ===\n")
        f.write(f"Total filas: {total}\n")
        f.write(f"arkId únicos: {ark_unique}\n")
        f.write(f"Duplicados por arkId: {ark_dupes}\n")
        f.write(f"Duplicados EXACTOS (todas columnas iguales{' + status' if keep_status else ' (ignorando status)'}): {exact_dupes}\n")
        f.write(f"Origen autodetectado: {origin_col}{origin_info}\n")
        f.write("\nArchivos generados:\n")
        f.write("- full_row_dupes.tsv\n- ark_dupe_summary.tsv\n- ark_source_dupe_summary.tsv\n- ark_diff_cols.tsv\n- dupes_report.txt\n")

    print("[OK] Informe generado en out/:")
    print("  - full_row_dupes.tsv (duplicados exactos)")
    print("  - ark_dupe_summary.tsv")
    print("  - ark_source_dupe_summary.tsv")
    print("  - ark_diff_cols.tsv (columnas que difieren por arkId)")
    print("  - dupes_report.txt (resumen)")

if __name__ == "__main__":
    pd.options.mode.chained_assignment = None
    main()