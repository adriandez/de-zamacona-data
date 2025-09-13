#!/usr/bin/env python3
import os
import pandas as pd

IN_FILE = "out/Zamacona_all_raw.xlsx"
OUT_DIR = "out"
OUT_EXACT_ALL = os.path.join(OUT_DIR, "dupes_exact_all.tsv")
OUT_EXACT_NOSRC = os.path.join(OUT_DIR, "dupes_exact_no_source.tsv")
OUT_ARK_SUM = os.path.join(OUT_DIR, "ark_dupe_summary.tsv")
OUT_ARK_DIFF = os.path.join(OUT_DIR, "ark_diff_cols.tsv")

def norm_cell(x):
    if x is None: return ""
    s = str(x)
    # normalización muy ligera para evitar “falsos” distintos por espacios
    s = " ".join(s.split())
    return s

def rows_equal(df_subset):
    """Devuelve True si todas las filas del subset son idénticas en todas sus columnas."""
    if len(df_subset) <= 1:
        return True
    # comparamos contra la primera fila normalizada
    first = df_subset.iloc[0].apply(norm_cell)
    return df_subset.applymap(norm_cell).eq(first, axis=1).all(axis=1).all()

def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    df = pd.read_excel(IN_FILE, dtype=str).fillna("")
    # Asegura nombres de cols sin espacios “raros”
    df.columns = [ " ".join(str(c).split()) for c in df.columns ]

    print("Filas totales:", len(df))
    if "arkId" in df.columns:
        unique = df["arkId"].nunique()
        dupes_by_ark = len(df) - unique
        print("arkId únicos:", unique)
        print("Duplicados por arkId:", dupes_by_ark)
    else:
        print("⚠️ No hay columna arkId; no se puede auditar por arkId.")

    # --- 1) Duplicados estricto: TODAS las columnas iguales ---
    # Clave: hash de la fila completa
    df_norm = df.applymap(norm_cell)
    full_hash = pd.util.hash_pandas_object(df_norm, index=False)
    df1 = df.copy()
    df1["__rowhash_all"] = full_hash
    mask_dup_all = df1.duplicated(subset=["__rowhash_all"], keep=False)
    dupes_all = df1.loc[mask_dup_all].sort_values("__rowhash_all")
    # Guardar (con todas las columnas)
    if not dupes_all.empty:
        dupes_all.to_csv(OUT_EXACT_ALL, sep="\t", index=False)
    print(f"Duplicados ESTRICTOS (todas las columnas): {dupes_all.shape[0]} filas en {dupes_all['__rowhash_all'].nunique()} grupos")

    # --- 2) Duplicados estrictos ignorando __source_file ---
    cols_no_src = [c for c in df.columns if c != "__source_file"]
    df_norm2 = df_norm[cols_no_src]
    rowhash_no_src = pd.util.hash_pandas_object(df_norm2, index=False)
    df2 = df.copy()
    df2["__rowhash_no_src"] = rowhash_no_src
    mask_dup_no_src = df2.duplicated(subset=["__rowhash_no_src"], keep=False)
    dupes_no_src = df2.loc[mask_dup_no_src].sort_values("__rowhash_no_src")
    if not dupes_no_src.empty:
        dupes_no_src.to_csv(OUT_EXACT_NOSRC, sep="\t", index=False)
    print(f"Duplicados ESTRICTOS (ignorando __source_file): {dupes_no_src.shape[0]} filas en {dupes_no_src['__rowhash_no_src'].nunique()} grupos")

    # --- 3) Mismo arkId pero con diferencias ---
    if "arkId" in df.columns:
        # resumen por arkId
        grp = df.groupby("arkId", dropna=False)
        rows_per_ark = grp.size().rename("rows").reset_index()
        # ¿todas las filas del ark son idénticas si ignoro __source_file?
        def equal_block(block):
            if len(block) <= 1:
                return True
            return rows_equal(block[cols_no_src])

        same_all = grp.apply(equal_block).rename("all_equal_no_source").reset_index()
        summary = rows_per_ark.merge(same_all, on="arkId", how="left")
        summary.to_csv(OUT_ARK_SUM, sep="\t", index=False)

        # columnas que difieren dentro de cada ark (si hay diferencias)
        diff_records = []
        for ark, block in grp:
            if len(block) <= 1:
                continue
            nb = block[cols_no_src].applymap(norm_cell)
            # columnas con más de un valor distinto
            diff_cols = [c for c in nb.columns if nb[c].nunique(dropna=False) > 1]
            if diff_cols:
                diff_records.append({
                    "arkId": ark,
                    "rows": len(block),
                    "diff_cols": ", ".join(diff_cols[:100])
                })
        diff_df = pd.DataFrame(diff_records).sort_values(["rows","arkId"], ascending=[False, True])
        diff_df.to_csv(OUT_ARK_DIFF, sep="\t", index=False)
        print(f"ARK con diferencias internas (ign. __source_file): {len(diff_df)}")
        print(f"[OK] {OUT_ARK_SUM}")
        print(f"[OK] {OUT_ARK_DIFF}")
    else:
        print("⚠️ Sin arkId: se omite auditoría por arkId.")

if __name__ == "__main__":
    main()