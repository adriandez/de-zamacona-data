#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
only_green_surnames.py

Mantiene el comportamiento original:
  - Carga el normalized final
  - Vacía __surn1/__surn2 en filas no verdes
  - Guarda out/Zamacona_normalized_clean.xlsx

Y añade valor:
  - Soporta input patched o normal
  - Si faltan blacklistFlag/reviewFlag, usa status=="green"
  - Escribe únicos (solo verdes):
      - out/Zamacona_unique_given.txt
      - out/Zamacona_unique_surnames.txt
"""

from __future__ import annotations
from pathlib import Path
import re
from collections import Counter
import sys
import pandas as pd

OUT = Path("out")
IN_PATCHED = OUT / "Zamacona_normalized_patched.xlsx"
IN_NORMAL  = OUT / "Zamacona_normalized.xlsx"

OUT_CLEAN  = OUT / "Zamacona_normalized_clean.xlsx"
OUT_GIVEN  = OUT / "Zamacona_unique_given.txt"
OUT_SURN   = OUT / "Zamacona_unique_surnames.txt"

def pick_input() -> Path:
    if IN_PATCHED.exists(): return IN_PATCHED
    if IN_NORMAL.exists():  return IN_NORMAL
    print("[ERROR] No encuentro normalized en out/. Ejecuta normalize_names.py", file=sys.stderr)
    sys.exit(1)

def green_mask(df: pd.DataFrame) -> pd.Series:
    # 1) status si existe
    if "status" in df.columns:
        s = df["status"].astype(str).str.lower().fillna("")
        return s.str.startswith("green")
    # 2) flags si no hay status
    if {"blacklistFlag","reviewFlag"}.issubset(df.columns):
        b = df["blacklistFlag"].astype(str).fillna("0")
        r = df["reviewFlag"].astype(str).fillna("0")
        return (b == "0") & (r == "0")
    # 3) si faltan todas, no podemos decidir -> todo no-green
    return pd.Series([False]*len(df), index=df.index)

def main():
    src = pick_input()
    df = pd.read_excel(src, dtype=str).fillna("")
    mask_green = green_mask(df)
    mask_non_green = ~mask_green

    # Identificar columnas de apellidos (split)
    surname_cols = [c for c in df.columns if c.endswith("__surn1") or c.endswith("__surn2")]

    # Vaciar esas columnas solo en filas no verdes (comportamiento original)
    if surname_cols:
        df.loc[mask_non_green, surname_cols] = ""

    # Guardar limpio (misma salida que tu script actual)
    df.to_excel(OUT_CLEAN, index=False)
    print(f"[OK] Guardado {OUT_CLEAN.name} con __surn1/__surn2 vacíos en no verdes. (origen: {src.name})")

    # --- Únicos (solo verdes) ---
    greens = df[mask_green].copy()

    # Given únicos: prioriza columnas split '__given'; si no hay, cae a 'given'
    given_cols = [c for c in greens.columns if c.endswith("__given")]
    if not given_cols and "given" in greens.columns:
        given_cols = ["given"]

    given_tokens = []
    for c in given_cols:
        for v in greens[c].astype(str):
            v = " ".join(v.split()).strip()
            if v:
                given_tokens.append(" ".join(w.capitalize() for w in v.split()))
    given_ctr = Counter(given_tokens)
    with open(OUT_GIVEN, "w", encoding="utf-8") as f:
        for tok, cnt in sorted(given_ctr.items(), key=lambda x: (-x[1], x[0].lower())):
            f.write(f"{tok}\t{cnt}\n")

    # Surnames únicos: prioriza '__surn1/__surn2'; si no hay, cae a 'surname/surname2'
    surn_cols = [c for c in greens.columns if c.endswith("__surn1") or c.endswith("__surn2")]
    if not surn_cols:
        for base in ("surname", "surname2"):
            if base in greens.columns:
                surn_cols.append(base)

    surn_tokens = []
    raw = ";".join(greens[c].astype(str).tolist()) if surn_cols else ""
    for t in re.split(r"\s*;\s*", raw):
        t = " ".join(t.split()).strip()
        if t:
            surn_tokens.append(t)
    surn_ctr = Counter(surn_tokens)
    with open(OUT_SURN, "w", encoding="utf-8") as f:
        for tok, cnt in sorted(surn_ctr.items(), key=lambda x: (-x[1], x[0].lower())):
            f.write(f"{tok}\t{cnt}\n")

    print(f"[OK] {OUT_GIVEN.name} ({len(given_ctr)} nombres únicos, SOLO verdes)")
    print(f"[OK] {OUT_SURN.name} ({len(surn_ctr)} apellidos únicos, SOLO verdes)")

if __name__ == "__main__":
    main()