#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
consolidate.py

Lee out/Zamacona_all_raw.xlsx (de consolidate_raw.py), armoniza columnas a un esquema
mínimo y deja un dataset coherente para normalize_names.py.

Genera:
  - out/Zamacona_all.xlsx
  - out/Zamacona_all.csv
  - out/consolidate_struct_log.txt (qué columnas se detectaron/mapeos)
"""

from __future__ import annotations
from pathlib import Path
import sys
import re
import pandas as pd
import numpy as np

ROOT = Path(__file__).resolve().parent
OUT = ROOT / "out"
SRC = OUT / "Zamacona_all_raw.xlsx"

OUT_XLSX = OUT / "Zamacona_all.xlsx"
OUT_CSV  = OUT / "Zamacona_all.csv"
LOG_FILE = OUT / "consolidate_struct_log.txt"

# Candidatas por campo canónico (orden importa)
CANDIDATES = {
    "arkId":      ["arkId", "arkID", "ark", "id", "ark_id"],
    "fullName":   ["fullName", "name", "full_name", "nombre_completo"],
    "fullName__work": ["fullName__work", "fullName_work", "nombre_completo_work", "nombre_work"],
    "given":      ["given", "firstName", "nombre", "name_given", "given__work"],
    "surname":    ["surname", "surname1", "lastName1", "apellido1", "apellido_1", "primer_apellido"],
    "surname2":   ["surname2", "lastName2", "apellido2", "apellido_2", "segundo_apellido"],
    "place":      ["place", "lugar", "town", "localidad", "parroquia", "municipio"],
    "date":       ["date", "fecha", "eventDate", "fecha_evento"],
    "event":      ["event", "tipo", "tipo_evento", "recordType"],
    "book":       ["book", "libro", "tomo", "volumen"],
    "folio":      ["folio", "page", "pag", "página", "num_folio"],
    "image":      ["image", "imageId", "imagen", "img", "scan"],
    "notes":      ["notes", "nota", "observaciones", "obs", "comentarios"],
    "status":     ["status", "Status", "STATUS"],
    "__source_file": ["__source_file", "source", "origen_fichero", "input_file"],
}

# Columnas canónicas que intentaremos entregar
CANON_ORDER = [
    "arkId", "status",
    "fullName__work", "fullName", "given", "surname", "surname2",
    "event", "date", "place", "book", "folio", "image",
    "notes",
    "__source_file",
]

def pick_first_col(df: pd.DataFrame, names: list[str]) -> str | None:
    """Devuelve la primera columna existente respetando mayúsc/minúsc y luego casefold."""
    # exact
    for n in names:
        if n in df.columns:
            return n
    # case-insensitive
    low_map = {c.casefold(): c for c in df.columns}
    for n in names:
        if n.casefold() in low_map:
            return low_map[n.casefold()]
    return None

def ensure_cols(df: pd.DataFrame) -> pd.DataFrame:
    log_lines = []
    out = df.copy()

    # 1) Mapear/renombrar a canónicos cuando corresponda
    rename_map = {}
    for canon, candidates in CANDIDATES.items():
        col = pick_first_col(out, candidates)
        if col and col != canon:
            rename_map[col] = canon
            log_lines.append(f"[MAP] {col} → {canon}")
        elif col:
            log_lines.append(f"[OK ] {canon} = {col}")
        else:
            log_lines.append(f"[MISS] {canon} (no detectada; crear si aplica)")

    if rename_map:
        out = out.rename(columns=rename_map)

    # 2) Normalizaciones suaves
    # arkId → str
    if "arkId" in out.columns:
        out["arkId"] = out["arkId"].astype(str).str.strip()
    else:
        # si no hay arkId, crea vacío para no romper flujos posteriores
        out["arkId"] = ""

    # status → por defecto gray
    if "status" not in out.columns:
        out["status"] = "gray"
    else:
        out["status"] = out["status"].fillna("").astype(str)
        out.loc[out["status"].eq(""), "status"] = "gray"

    # 3) fullName__work: si no existe, intenta construirla
    if "fullName__work" not in out.columns or out["fullName__work"].isna().all():
        parts = []
        if "fullName" in out.columns:
            parts.append(out["fullName"].fillna("").astype(str).str.strip())
        else:
            # construir desde given + apellidos
            g = out["given"].fillna("").astype(str).str.strip() if "given" in out.columns else ""
            s1 = out["surname"].fillna("").astype(str).str.strip() if "surname" in out.columns else ""
            s2 = out["surname2"].fillna("").astype(str).str.strip() if "surname2" in out.columns else ""
            if isinstance(g, str):  # cuando no existan, g es ""
                built = g
            else:
                built = g
                if not isinstance(built, str):
                    built = g
            # ensamblar
            if isinstance(g, pd.Series):
                full = (g + " " + s1 + (" " + s2).where(s2.ne(""), "")).str.replace(r"\s+", " ", regex=True).str.strip()
            else:
                full = ""
            parts.append(full)
        out["fullName__work"] = parts[0] if parts else ""

    # 4) Asegura columnas canónicas aunque queden vacías
    for col in CANON_ORDER:
        if col not in out.columns:
            out[col] = ""

    # 5) Orden de columnas amigable
    others = [c for c in out.columns if c not in CANON_ORDER]
    out = out[CANON_ORDER + others]

    # 6) Guardar log
    LOG_FILE.write_text("\n".join(log_lines), encoding="utf-8")

    return out

def drop_empty_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Elimina filas totalmente vacías (todas columnas vacías o NaN). Conserva filas con algún dato."""
    tmp = df.replace({np.nan: "", None: ""})
    mask_nonempty = tmp.apply(lambda r: any(str(v).strip() != "" for v in r), axis=1)
    return df[mask_nonempty].copy()

def main() -> int:
    if not SRC.exists():
        print(f"[ERROR] No existe {SRC}. Ejecuta primero consolidate_raw.py", file=sys.stderr)
        return 1

    df = pd.read_excel(SRC, dtype=str)
    df = df.replace({np.nan: ""})

    # Limpieza de filas vacías absolutas
    before = len(df)
    df = drop_empty_rows(df)
    removed = before - len(df)
    if removed:
        print(f"[INFO] Filas totalmente vacías eliminadas: {removed}")

    # Armonización
    df2 = ensure_cols(df)

    # Salidas
    df2.to_excel(OUT_XLSX, index=False)
    df2.to_csv(OUT_CSV, index=False, encoding="utf-8")

    print(f"[OK] {OUT_XLSX.name} y {OUT_CSV.name} generados ({len(df2)} filas).")
    print(f"[OK] Log de estructura: {LOG_FILE.name}")
    return 0

if __name__ == "__main__":
    pd.options.mode.chained_assignment = None
