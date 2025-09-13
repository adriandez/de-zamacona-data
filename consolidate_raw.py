#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
consolidate_raw.py  (modo estricto compatible con tu consolidación original)

- Escanea ficheros RAW (por defecto 'zamacona_*.xlsx' en la carpeta indicada).
- Para Excel:
    * Lee la hoja (primera por defecto o la indicada con --sheet).
    * Detecta la FILA DE CABECERA buscando 'arkId' (case-insensitive) en
      las primeras 30 filas; si no encuentra, usa fila 6 (index 5).
    * Construye cabeceras desde esa fila, quita columnas Unnamed, normaliza
      espacios y alinea a columnas canónicas del primer archivo.
    * Filtra filas con 'arkId' válido (empieza por 'ark:').
- Añade '__source_file' y guarda:
    * out/Zamacona_all_raw.xlsx
    * out/Zamacona_all_raw.csv
    * out/consolidate_log.txt
    * out/consolidate_index.tsv

Notas:
- No deduplica (eso lo hará canonicalize_strict_dupes.py al final).
- Ignora temporales de Excel (~$*.xlsx).
"""

from __future__ import annotations
import argparse
from pathlib import Path
import sys
import re
import pandas as pd

ROOT = Path(__file__).resolve().parent
OUT_DIR = ROOT / "out"
OUT_DIR.mkdir(exist_ok=True)

DEFAULT_GLOB = "zamacona_*.xlsx"
SUPPORTED_EXT = {".xlsx", ".xls", ".csv", ".tsv"}

LOG_FILE = OUT_DIR / "consolidate_log.txt"
INDEX_TSV = OUT_DIR / "consolidate_index.tsv"
OUT_XLSX = OUT_DIR / "Zamacona_all_raw.xlsx"
OUT_CSV  = OUT_DIR / "Zamacona_all_raw.csv"

def parse_args():
    p = argparse.ArgumentParser(description="Consolida ficheros RAW en un único dataset (modo estricto).")
    p.add_argument("--dir", default=str(ROOT),
                   help="Carpeta a escanear (por defecto, la raíz del proyecto).")
    p.add_argument("--glob", default=DEFAULT_GLOB,
                   help=f"Patrón glob (por defecto: {DEFAULT_GLOB}).")
    p.add_argument("--include-tsv", action="store_true",
                   help="Permitir también .csv/.tsv además de Excel.")
    p.add_argument("--sheet", default=None,
                   help="Nombre/índice de hoja a leer en Excel. Si se omite, se usa la PRIMERA hoja.")
    p.add_argument("--limit", type=int, default=None,
                   help="Leer como máximo N ficheros (útil para pruebas).")
    return p.parse_args()

def normalize_colnames(cols):
    norm = []
    for c in cols:
        c = "" if c is None else str(c)
        c = re.sub(r"\s+", " ", c).strip()
        norm.append(c)
    return norm

def find_ark_col(columns):
    # Busca 'arkId' ignorando mayúsc/minúsc
    low_map = {str(c).strip().casefold(): c for c in columns}
    if "arkid" in low_map:
        return low_map["arkid"]
    # Acepta 'ark' a secas si viniera así
    if "ark" in low_map:
        return low_map["ark"]
    return None

def detect_header_row(sheet_df: pd.DataFrame) -> int:
    # Explora primeras 30 filas en busca de 'arkId'
    max_probe = min(30, len(sheet_df))
    for i in range(max_probe):
        row_vals = normalize_colnames(list(sheet_df.iloc[i]))
        row_lc = [v.casefold() for v in row_vals]
        if "arkid" in row_lc or "ark" in row_lc:
            return i
    # fallback a tu fila habitual (6 -> index 5)
    return 5

def read_excel_strict(path: Path, sheet_name, canonical_cols: list[str] | None):
    # Abre libro y selecciona hoja
    xls = pd.ExcelFile(path, engine="openpyxl")
    target_sheet = sheet_name if sheet_name is not None else xls.sheet_names[0]
    tmp = xls.parse(target_sheet, header=None, dtype=str)

    # Detecta fila de cabecera
    hdr_row = detect_header_row(tmp)
    header = normalize_colnames(list(tmp.iloc[hdr_row]))

    # Datos a partir de la fila siguiente
    data = tmp.iloc[hdr_row + 1 :].copy()
    data.columns = header

    # Quita columnas Unnamed o vacías
    mask_keep = [not str(c).startswith("Unnamed") and str(c).strip() != "" for c in data.columns]
    data = data.loc[:, mask_keep]

    # Normaliza columnas
    data.columns = normalize_colnames(list(data.columns))

    # Filtra por arkId válido
    ark_col = find_ark_col(data.columns)
    if ark_col:
        data = data[data[ark_col].astype(str).str.startswith("ark:", na=False)]
        # renombra a 'arkId' si hace falta
        if ark_col != "arkId":
            data = data.rename(columns={ark_col: "arkId"})
    else:
        # si no hay arkId, descarta todo el archivo
        data = data.iloc[0:0]

    # Quita filas totalmente vacías
    data = data.dropna(how="all")

    # Alinea a columnas canónicas si ya están definidas
    if canonical_cols is not None:
        missing = [c for c in canonical_cols if c not in data.columns]
        for c in missing:
            data[c] = pd.NA
        data = data[canonical_cols]

    # Marca origen
    data["__source_file"] = path.name
    return data, header

def read_csv_or_tsv(path: Path) -> pd.DataFrame:
    if path.suffix.lower() == ".csv":
        df = pd.read_csv(path, dtype=str, keep_default_na=False)
    else:
        df = pd.read_csv(path, dtype=str, keep_default_na=False, sep="\t")
    df.columns = normalize_colnames(df.columns)
    df["__source_file"] = path.name
    return df

def read_one(path: Path, sheet_name, canonical_cols):
    ext = path.suffix.lower()
    if ext in {".xlsx", ".xls"}:
        try:
            return read_excel_strict(path, sheet_name, canonical_cols)
        except Exception as e:
            # Intenta listar hojas para ayudar
            try:
                xls = pd.ExcelFile(path, engine="openpyxl")
                sheets = ", ".join(xls.sheet_names)
                raise RuntimeError(f"Error leyendo '{path.name}': {e} (hojas: {sheets})") from e
            except Exception:
                raise RuntimeError(f"Error leyendo '{path.name}': {e}") from e
    elif ext in {".csv", ".tsv"}:
        df = read_csv_or_tsv(path)
        return df, list(df.columns)
    else:
        raise RuntimeError(f"Extensión no soportada: {ext}")

def main() -> int:
    args = parse_args()
    in_dir = Path(args.dir).resolve()
    if not in_dir.exists():
        print(f"[ERROR] Carpeta no existe: {in_dir}", file=sys.stderr)
        return 1

    # Reúne candidatos + CSV/TSV si se pide, y descarta temporales de Excel
    files = sorted(p for p in in_dir.glob(args.glob) if not p.name.startswith("~$"))
    if args.include_tsv:
        files += sorted(in_dir.glob("*.csv"))
        files += sorted(in_dir.glob("*.tsv"))

    files = [p for p in files if p.suffix.lower() in SUPPORTED_EXT]
    if args.limit is not None:
        files = files[: args.limit]

    if not files:
        print(f"[ERROR] No se encontraron ficheros con patrón '{args.glob}' en {in_dir}", file=sys.stderr)
        return 1

    print(f"[INFO] Encontrados {len(files)} ficheros para consolidar.")

    # limpia log previo
    LOG_FILE.unlink(missing_ok=True)

    frames: list[pd.DataFrame] = []
    index_rows = []
    canonical_cols: list[str] | None = None

    total_rows_raw = 0

    for i, f in enumerate(files, 1):
        try:
            df, header = read_one(f, args.sheet, canonical_cols)
        except Exception as e:
            print(f"[WARN] {e}", file=sys.stderr)
            continue

        # Para el primer archivo, fijamos columnas canónicas y aseguramos __source_file al final
        if canonical_cols is None:
            canonical_cols = [c for c in df.columns if c != "__source_file"] + ["__source_file"]
            df = df[canonical_cols]

        n = len(df)
        total_rows_raw += n
        frames.append(df)

        line = f"{f.name:<35} {n:>6} registros"
        print(line)
        index_rows.append({"file": f.name, "rows": n})
        with open(LOG_FILE, "a", encoding="utf-8") as log:
            log.write(line + "\n")

    if not frames:
        print("[ERROR] Ningún fichero legible.", file=sys.stderr)
        return 1

    all_df = pd.concat(frames, ignore_index=True)

    # Resumen y controles tipo tu script original
    total = len(all_df)
    uniq = all_df["arkId"].nunique() if "arkId" in all_df.columns else 0
    dupes = total - uniq
    print("-------------------------------------------------------")
    print(f"[INFO] filas con arkId válido (suma por archivo): {total_rows_raw}")
    print(f"[INFO] filas tras concatenar: {total}")
    print(f"[INFO] arkId únicos: {uniq}")
    print(f"[INFO] posibles duplicados (mismo arkId): {dupes}")

    # Guarda índice por fichero
    pd.DataFrame(index_rows).to_csv(INDEX_TSV, sep="\t", index=False, encoding="utf-8")

    # Salidas RAW
    all_df.to_excel(OUT_XLSX, index=False)
    all_df.to_csv(OUT_CSV, index=False, encoding="utf-8")

    print(f"[OK] Guardado {OUT_XLSX.name} y {OUT_CSV.name}")
    print(f"[OK] Índice: {INDEX_TSV.name}")
    print(f"[OK] Log:    {LOG_FILE.name}")
    return 0

if __name__ == "__main__":
    pd.options.mode.chained_assignment = None
    sys.exit(main())