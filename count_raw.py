#!/usr/bin/env python3
import os, glob
import pandas as pd

RAW_DIR = "raw"

def detect_header_row(df: pd.DataFrame) -> int:
    for i in range(min(12, len(df))):
        row = [str(x).strip().lower() for x in df.iloc[i].tolist()]
        if "score" in row and "arkid" in row:
            return i
    return 5

def count_rows(path: str) -> int:
    try:
        xl = pd.ExcelFile(path)
        sheet = xl.sheet_names[0]
        tmp = xl.parse(sheet, header=None, dtype=str)
        hdr_row = detect_header_row(tmp)
        data = tmp.iloc[hdr_row+1:].copy()
        data = data.loc[:, ~data.columns.astype(str).str.startswith("Unnamed")]
        data = data.dropna(how="all")
        return len(data)
    except Exception as e:
        print(f"[WARN] error leyendo {os.path.basename(path)}: {e}")
        return 0

def main():
    files = sorted(glob.glob(os.path.join(RAW_DIR, "*.xlsx")))
    if not files:
        print("No hay .xlsx en raw/")
        return

    total = 0
    for f in files:
        n = count_rows(f)
        total += n
        print(f"{os.path.basename(f):40s} {n:5d} registros")
    print("-" * 55)
    print(f"TOTAL en {len(files)} ficheros: {total}")

if __name__ == "__main__":
    main()