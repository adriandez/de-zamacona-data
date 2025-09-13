#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
infer_surnames_from_parents.py

Objetivo:
  Verificar y (opcionalmente) completar/ordenar los apellidos del hijo usando:
    - fullName__given, fullName__surn1, fullName__surn2
    - fatherFullName__given, fatherFullName__surn1, fatherFullName__surn2
    - motherFullName__given, motherFullName__surn1, motherFullName__surn2

Reglas:
  - Español: hijo.surn1 = padre.surn1 ; hijo.surn2 = madre.surn1
  - Detecta orden invertido (swap): hijo.surn1 == madre.surn1 y hijo.surn2 == padre.surn1
  - Completa apellidos del hijo si faltan y están disponibles en los padres
  - No toca filas grises/amarillas; solo verdes
  - Solo considera entradas con "Zamacona" en fullName__work

Salida:
  - out/Zamacona_infer_log.tsv   (revisión de casos y propuestas)
  - out/Zamacona_normalized_enhanced.xlsx/.csv (solo si --apply)

Uso:
  python3 infer_surnames_from_parents.py
  python3 infer_surnames_from_parents.py --apply   # aplica fill/swap seguros en copia de salida
"""

from __future__ import annotations
from pathlib import Path
import sys
import argparse
import re
import pandas as pd

ROOT = Path(__file__).resolve().parent
OUT  = ROOT / "out"
SRC1 = OUT / "Zamacona_normalized_patched.xlsx"
SRC2 = OUT / "Zamacona_normalized.xlsx"

INFER_LOG = OUT / "Zamacona_infer_log.tsv"
OUT_XLSX  = OUT / "Zamacona_normalized_enhanced.xlsx"
OUT_CSV   = OUT / "Zamacona_normalized_enhanced.csv"

def pick_input() -> Path:
    if SRC1.exists():
        return SRC1
    if SRC2.exists():
        return SRC2
    print("[ERROR] No encuentro normalized en out/ (ni *_patched ni normal).", file=sys.stderr)
    sys.exit(1)

def is_green(row) -> bool:
    # Si existe 'status', úsalo; si no, usa flags
    status = str(row.get("status", "")).lower()
    if status:
        return status.startswith("green")
    b = str(row.get("blacklistFlag", "0")).strip()
    r = str(row.get("reviewFlag", "0")).strip()
    return (b == "0") and (r == "0")

def contains_zamacona(s: str) -> bool:
    return "zamacona" in (s or "").lower()

def norm(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true",
                    help="Aplica fill/swap seguros en copia: out/Zamacona_normalized_enhanced.*")
    args = ap.parse_args()

    src = pick_input()
    OUT.mkdir(exist_ok=True)
    df = pd.read_excel(src, dtype=str).fillna("")

    # Campos que esperamos
    needed = [
        "arkId", "fullName__work",
        "fullName__given", "fullName__surn1", "fullName__surn2",
        "fatherFullName__given", "fatherFullName__surn1", "fatherFullName__surn2",
        "motherFullName__given", "motherFullName__surn1", "motherFullName__surn2",
        "blacklistFlag", "reviewFlag"
    ]
    missing = [c for c in needed if c not in df.columns]
    if missing:
        print(f"[WARN] Faltan columnas esperadas: {missing}. Continuo con lo disponible.")

    # Filtro: verdes + Zamacona + given en hijo/padre/madre
    mask_green = df.apply(is_green, axis=1)
    mask_zama  = df["fullName__work"].astype(str).map(contains_zamacona)
    mask_given = (df["fullName__given"].astype(bool) &
                  df["fatherFullName__given"].astype(bool) &
                  df["motherFullName__given"].astype(bool))
    mask = mask_green & mask_zama & mask_given

    work = df.loc[mask].copy()

    # Normaliza strings
    for c in [
        "fullName__surn1","fullName__surn2",
        "fatherFullName__surn1","fatherFullName__surn2",
        "motherFullName__surn1","motherFullName__surn2"
    ]:
        if c in work.columns:
            work[c] = work[c].map(norm)

    actions = []
    prop_s1 = []
    prop_s2 = []

    for i, row in work.iterrows():
        child_s1  = row.get("fullName__surn1", "")
        child_s2  = row.get("fullName__surn2", "")
        father_s1 = row.get("fatherFullName__surn1", "")
        mother_s1 = row.get("motherFullName__surn1", "")

        # Estado inicial
        action = "no_action"
        ps1 = child_s1
        ps2 = child_s2
        reason = ""

        # Si ambos presentes y coinciden con la regla → ok
        if child_s1 and child_s2 and father_s1 and mother_s1:
            if child_s1 == father_s1 and child_s2 == mother_s1:
                action = "ok_rule"
                reason = "coherent"
            elif (child_s1 == mother_s1) and (child_s2 == father_s1):
                # orden invertido → swap
                ps1, ps2 = father_s1, mother_s1
                action = "swap"
                reason = "swapped_order"
            else:
                # Desajuste: no coincide ni con regla ni con swap
                action = "mismatch"
                reason = "child!=father/mother rule"
        else:
            # Algún apellido del hijo falta → intenta completar
            if not child_s1 and father_s1:
                ps1 = father_s1
                action = "fill"
                reason += "fill_surn1_from_father;"
            if not child_s2 and mother_s1:
                ps2 = mother_s1
                action = "fill" if action in ("no_action","fill") else action
                reason += "fill_surn2_from_mother;"

            # Si ambos padres tienen s1 y el hijo tiene ambos pero invertidos, ya está cubierto arriba.
            # Si el hijo tiene uno mal y otro bien, se marcará mismatch si no aplica fill.
            if action == "no_action":
                # no se pudo completar nada (faltan datos de padres)
                action = "insufficient_parent_data"
                reason = "missing_parent_surn1"

        actions.append((action, reason))
        prop_s1.append(ps1)
        prop_s2.append(ps2)

    work["fullName__surn1_proposed"] = prop_s1
    work["fullName__surn2_proposed"] = prop_s2
    work["proposed_action"] = [a for a, _ in actions]
    work["proposed_reason"] = [r for _, r in actions]

    # Log de revisión (ordenado por acción para priorizar)
    cols_for_log = [
        "arkId",
        "fullName__work",
        "fullName__given","fullName__surn1","fullName__surn2",
        "fatherFullName__given","fatherFullName__surn1","fatherFullName__surn2",
        "motherFullName__given","motherFullName__surn1","motherFullName__surn2",
        "proposed_action","proposed_reason",
        "fullName__surn1_proposed","fullName__surn2_proposed"
    ]
    cols_for_log = [c for c in cols_for_log if c in work.columns]
    log_df = work[cols_for_log].copy()
    # prioriza swap/fill delante, luego mismatches y el resto
    order_key = log_df["proposed_action"].map({
        "swap": 0, "fill": 1, "mismatch": 2, "ok_rule": 3, "insufficient_parent_data": 4, "no_action": 5
    }).fillna(9)
    log_df = log_df.assign(_k=order_key).sort_values(["_k","arkId"]).drop(columns=["_k"])
    log_df.to_csv(INFER_LOG, sep="\t", index=False)
    print(f"[OK] {INFER_LOG.name} generado con {len(log_df)} filas (candidatos revisables).")

    if args.apply:
        # Aplica solo 'fill' y 'swap' (acciones seguras) sobre una copia del dataset completo
        df_out = df.copy()
        apply_idx = work.index[work["proposed_action"].isin(["fill","swap"])]
        for idx in apply_idx:
            df_out.at[idx, "fullName__surn1"] = work.at[idx, "fullName__surn1_proposed"]
            df_out.at[idx, "fullName__surn2"] = work.at[idx, "fullName__surn2_proposed"]
        # Añade columna de control
        df_out["surnameInferenceApplied"] = ""
        df_out.loc[apply_idx, "surnameInferenceApplied"] = work.loc[apply_idx, "proposed_action"].values
        df_out.to_excel(OUT_XLSX, index=False)
        df_out.to_csv(OUT_CSV, index=False)
        print(f"[OK] Salida aplicada: {OUT_XLSX.name} / {OUT_CSV.name}")
    else:
        print("[INFO] Modo dry-run. No se han modificado apellidos del hijo. Usa --apply para escribir copia.")

    # Resumen
    summary = work["proposed_action"].value_counts(dropna=False).to_dict()
    print("[RESUMEN acciones]", summary)

if __name__ == "__main__":
    sys.exit(main())