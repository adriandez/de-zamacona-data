#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations
from pathlib import Path
import argparse, re
import pandas as pd

ROOT = Path(__file__).resolve().parent
OUT  = ROOT / "out"

def read_any(p: Path) -> pd.DataFrame:
    if p.suffix.lower() == ".xlsx":
        return pd.read_excel(p, dtype=str).fillna("")
    return pd.read_csv(p, dtype=str).fillna("")

def write_xlsx_csv(df: pd.DataFrame, base: Path):
    base.parent.mkdir(parents=True, exist_ok=True)
    # XLSX
    with pd.ExcelWriter(base.with_suffix(".xlsx"), engine="xlsxwriter") as w:
        df.to_excel(w, index=False, sheet_name="data")
        wb = w.book; ws = w.sheets["data"]
        hdr = wb.add_format({"bold": True})
        for i, col in enumerate(df.columns):
            ws.write(0, i, col, hdr)
            ws.set_column(i, i, min(max(12, len(col)+2), 45))
        # arkId clicable si existe
        if "arkId" in df.columns:
            link = wb.add_format({"font_color": "blue", "underline": 1})
            ark_idx = df.columns.get_loc("arkId")
            url_idx = df.columns.get_loc("arkUrl") if "arkUrl" in df.columns else None
            for r in range(1, len(df)+1):
                ark = str(df.iloc[r-1, ark_idx] or "")
                if not ark: 
                    continue
                url = (str(df.iloc[r-1, url_idx]) if url_idx is not None else "") or \
                      f"https://www.familysearch.org/{ark if ark.startswith('ark:/') else 'ark:/' + ark}?lang=es"
                ws.write_url(r, ark_idx, url, link, string=ark)
    # CSV
    df.to_csv(base.with_suffix(".csv"), index=False, encoding="utf-8")

def load_list(path: Path) -> list[str]:
    if not path or not path.exists():
        return []
    out = []
    for ln in path.read_text(encoding="utf-8").splitlines():
        s = ln.strip()
        if not s or s.startswith("#"): 
            continue
        out.append(s)
    return out

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", default="out/Zamacona_final.xlsx", help="Dataset de entrada (final)")
    ap.add_argument("--reject-ark-list", default="data/reject_arkids.txt", help="Lista de ARKs a ELIMINAR (uno por línea)")
    ap.add_argument("--reject-surnames", default="data/reject_surnames.txt", help="Lista de apellidos/tokens a ELIMINAR (uno por línea)")
    ap.add_argument("--out-prefix", default="Zamacona_final_clean", help="Prefijo de salida en out/")
    args = ap.parse_args()

    inp = Path(args.inp)
    if not inp.exists():
        print(f"✖ No encuentro el input: {inp}")
        return 1

    df = read_any(inp)

    # columnas relevantes
    for col in ["arkId","fullName__work","fullName__surn1","fullName__surn2"]:
        if col not in df.columns:
            df[col] = ""

    # 1) Rechazo por ARK
    ark_rejects = set(load_list(Path(args.reject_ark_list)))
    def norm_ark(s: str) -> str:
        s = (s or "").strip()
        return s if (not s) or s.startswith("ark:/") else f"ark:/{s}"

    mask_ark = df["arkId"].astype(str).map(norm_ark).isin({norm_ark(a) for a in ark_rejects}) if ark_rejects else pd.Series([False]*len(df))

    # 2) Rechazo por tokens (Zamacola, Zamolla, Zamalloa, etc.)
    tokens = [t.lower() for t in load_list(Path(args.reject_surnames))]
    tokens_pattern = None
    if tokens:
        # palabra parcial, pero en límites de letras (case-insensitive)
        # ej: 'zamacola' también cazará 'de zamacola' o 'zamacolas'
        tokens_pattern = re.compile("|".join(map(re.escape, tokens)), flags=re.IGNORECASE)

    def contains_token(s: str) -> bool:
        if not tokens_pattern:
            return False
        return bool(tokens_pattern.search(s or ""))

    mask_tokens = df.apply(
        lambda r: (
            contains_token(str(r.get("fullName__work",""))) or
            contains_token(str(r.get("fullName__surn1",""))) or
            contains_token(str(r.get("fullName__surn2","")))
        ),
        axis=1
    ) if tokens_pattern else pd.Series([False]*len(df))

    # combinación
    mask_drop = mask_ark | mask_tokens

    kept = df[~mask_drop].copy()
    dropped = df[mask_drop].copy()

    out_base_kept   = OUT / args.out_prefix
    out_base_dropped= OUT / f"{args.out_prefix}_dropped"

    write_xlsx_csv(kept, out_base_kept)
    write_xlsx_csv(dropped, out_base_dropped)

    print(f"[OK] Filtrado → quedan {len(kept)} filas | eliminadas {len(dropped)}")
    print(f"[OK] Guardado: {out_base_kept.name}.xlsx/.csv y {out_base_dropped.name}.xlsx/.csv")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
