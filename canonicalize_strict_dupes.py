#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations
from pathlib import Path
import sys
import pandas as pd

ROOT = Path(__file__).resolve().parent
OUT  = ROOT / "out"

# Orden de preferencia del input
CAND = [
    OUT / "Zamacona_normalized_patched.xlsx",
    OUT / "Zamacona_normalized.xlsx",
    OUT / "Zamacona_all_raw.xlsx",
]

IGNORE = {"__source_file", "status"}  # columnas que NO cuentan para comparar contenido

def pick_input() -> Path | None:
    for p in CAND:
        if p.exists():
            return p
    return None

def main():
    src = pick_input()
    if not src:
        print("[ERROR] No hay archivo base en out/. Ejecuta primero el pipeline.")
        sys.exit(1)

    OUT.mkdir(exist_ok=True)
    df = pd.read_excel(src)

    has_src = "__source_file" in df.columns
    # columnas para COMPARAR contenido (ignorando IGNORE)
    compare_cols = [c for c in df.columns if c not in IGNORE]

    # clave de contenido (normalización ligera para evitar falsos diffs por espacios)
    key = df[compare_cols].astype(str).apply(lambda s: s.str.strip()).agg("||".join, axis=1)
    df["_k"] = key

    # ¿hay grupos repetidos?
    sizes = df["_k"].value_counts()
    multi_keys = sizes[sizes > 1].index.tolist()

    if not multi_keys:
        # no-op: copiamos como canonical
        canon = df.copy()
        if has_src:
            # cada fila proviene de 1 origen -> soporte 1
            canon["__sources_agg"] = canon["__source_file"].astype(str)
            canon["support_n"] = 1
        canon.drop(columns=["_k"], inplace=True, errors="ignore")
        canon.to_excel(OUT / "Zamacona_canonical.xlsx", index=False)
        canon.to_csv  (OUT / "Zamacona_canonical.csv",  index=False, encoding="utf-8")
        print(f"[OK] Sin duplicados estrictos. Canonical = {len(canon)} filas (copia 1:1).")
        return

    # Hay repetidos estrictos: colapsamos
    # 1) Filas ganadoras = primera por cada clave
    winners = df.drop_duplicates(subset=["_k"], keep="first").copy()

    # 2) Agregamos fuentes y soporte
    if has_src:
        agg = (
            df.groupby("_k")["__source_file"]
              .agg(lambda s: "|".join(sorted(set(map(str, s)))))
              .rename("__sources_agg")
              .reset_index()
        )
        winners = winners.merge(agg, on="_k", how="left")
        winners["support_n"] = winners["__sources_agg"].apply(lambda x: len(x.split("|")) if isinstance(x, str) and x else 1)
    else:
        winners["__sources_agg"] = ""
        winners["support_n"] = 1

    # 3) Limpieza y escritura
    winners.drop(columns=["_k"], inplace=True, errors="ignore")
    winners.to_excel(OUT / "Zamacona_canonical.xlsx", index=False)
    winners.to_csv  (OUT / "Zamacona_canonical.csv",  index=False, encoding="utf-8")

    collapsed = len(df) - len(winners)
    print(f"[OK] Canonical creado: {len(winners)} filas. Colapsadas {collapsed} copias estrictas.")

if __name__ == "__main__":
    pd.options.mode.chained_assignment = None
    main()
