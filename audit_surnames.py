#!/usr/bin/env python3
# Audita y normaliza apellidos (__surn1/__surn2) contra un canon vasco.
# - Lee out/Zamacona_normalized.xlsx
# - Usa data/whitelist_surnames.txt (canónicos) y data/surname_synonyms.csv (variant,canonical)
# - Clasifica variantes observadas: OK / NEAR (<=2) / REJECT
# - Señala apellidos que parecen "nombres de pila"
# Salidas en out/:
#   surnames_ok.tsv, surnames_near.tsv, surnames_reject.tsv, surnames_looks_like_given.tsv
#   surnames_suggestions.csv  (variant, count, best_match, dist, class)

import re
import unicodedata
from pathlib import Path
import pandas as pd

IN_XLSX = Path("out/Zamacona_normalized.xlsx")
OUT_DIR = Path("out")
WL_FILE = Path("data/whitelist_surnames.txt")
SYN_FILE = Path("data/surname_synonyms.csv")  # puede tener comentarios y filas con >2 columnas

OUT_OK   = OUT_DIR / "surnames_ok.tsv"
OUT_NEAR = OUT_DIR / "surnames_near.tsv"
OUT_REJ  = OUT_DIR / "surnames_reject.tsv"
OUT_LIKE_GIVEN = OUT_DIR / "surnames_looks_like_given.tsv"
OUT_SUGG = OUT_DIR / "surnames_suggestions.csv"

NEAR_DIST = 2  # umbral de distancia de edición

# Nombres de pila frecuentes para detectar "colados" en apellidos
GIVEN_COMMON = {
    "maria","jose","juan","francisco","manuel","antonio","josefa","francisca","miguel",
    "magdalena","ramon","dominga","manuela","sebastian","catalina","marina","ana",
    "isabel","theresa","teresa","ignacio","martin","pedro","andres","esteban","gabriel",
    "clemente","pablo","felix","diego","thomas","bartolome","simona","jacinta","bernarda",
    "ramona","mariana","rosa","agustin","vicente","ventura","rafael","xavier","gregoria",
    "ursula","dominga","sebastiana","valentina","placido","anastasia","luisa","julia",
}

def strip_accents(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", s or "") if unicodedata.category(c) != "Mn")

def norm_token(s: str) -> str:
    s = strip_accents((s or "").strip())
    s = re.sub(r"[^\w\s\-']", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def edit_distance(a: str, b: str) -> int:
    a = a.lower(); b = b.lower()
    dp = list(range(len(b)+1))
    for i, ca in enumerate(a, start=1):
        prev = dp[0]; dp[0] = i
        for j, cb in enumerate(b, start=1):
            tmp = dp[j]
            dp[j] = min(dp[j]+1, dp[j-1]+1, prev + (0 if ca==cb else 1))
            prev = tmp
    return dp[-1]

def load_whitelist():
    """
    Devuelve:
      - wl_lower: set de canónicos en minúsculas/normalizados (para comparar)
      - wl_map:   dict lower -> forma original (con mayúsculas/ñ) para reportar
    """
    wl_lower = set()
    wl_map = {}
    if WL_FILE.exists():
        for line in WL_FILE.read_text(encoding="utf-8").splitlines():
            orig = line.strip()
            if not orig:
                continue
            low = norm_token(orig).lower()
            if low:
                wl_lower.add(low)
                wl_map.setdefault(low, orig)
    return wl_lower, wl_map

def load_synonyms() -> dict:
    """
    Carga robusta de sinónimos desde CSV (con tolerancia a:
    - comentarios (#) y comentarios en línea,
    - filas con más de dos columnas: A,B,C -> (A->B) y (B->C),
    - líneas vacías o sucias).
    Retorna: dict variant(lower normalizado) -> canonical (con forma tal cual en el fichero).
    """
    syn = {}
    if not SYN_FILE.exists():
        return syn

    with open(SYN_FILE, "r", encoding="utf-8") as f:
        for lineno, raw in enumerate(f, 1):
            line = raw.strip()
            if not line:
                continue
            # quitar comentario en línea
            if "#" in line:
                line = line.split("#", 1)[0].strip()
                if not line:
                    continue
            # separar por comas
            parts = [p.strip() for p in line.split(",")]
            # saltar cabecera si aparece
            if parts and parts[0].lower() == "variant":
                continue
            # eliminar vacíos
            parts = [p for p in parts if p]
            if len(parts) < 2:
                continue
            # cadena A,B,C,... -> A->B, B->C, ...
            prev = parts[0]
            for nxt in parts[1:]:
                v = norm_token(prev).lower()
                c = nxt.strip()
                if v and c:
                    syn[v] = c
                prev = nxt
    return syn

def main():
    if not IN_XLSX.exists():
        raise SystemExit(f"No encuentro {IN_XLSX}")

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    # carga datos
    df = pd.read_excel(IN_XLSX, dtype=str)
    cols = [c for c in df.columns if c.endswith("__surn1") or c.endswith("__surn2")]
    if not cols:
        raise SystemExit("No encuentro columnas __surn1/__surn2. Ejecuta primero el normalizador.")

    # recoge todas las variantes con conteo
    ser = pd.Series(dtype=str)
    for c in cols:
        ser = pd.concat([ser, df[c].fillna("").astype(str)])
    tokens = []
    for cell in ser:
        for part in str(cell).split(";"):
            t = norm_token(part)
            if t:
                tokens.append(t)

    if not tokens:
        pd.DataFrame(columns=["variant","count","best_match","reason"]).to_csv(OUT_OK, sep="\t", index=False)
        pd.DataFrame(columns=["variant","count","best_match","dist"]).to_csv(OUT_NEAR, sep="\t", index=False)
        pd.DataFrame(columns=["variant","count","looks_like_given"]).to_csv(OUT_REJ, sep="\t", index=False)
        pd.DataFrame(columns=["variant","count","class"]).to_csv(OUT_LIKE_GIVEN, sep="\t", index=False)
        pd.DataFrame(columns=["variant","count","class","best_match","dist","reason","looks_like_given"]).to_csv(OUT_SUGG, index=False)
        print("[OK] No hay apellidos que auditar (todas las celdas vacías).")
        return

    vc = pd.Series(tokens).value_counts().reset_index()
    vc.columns = ["variant","count"]

    # carga canon y sinónimos
    WL_LOWER, WL_MAP = load_whitelist()   # set lower + mapa lower->original
    SYN = load_synonyms()                 # variant(lower) -> canonical(str)

    # clasificar
    rows = []
    for _, row in vc.iterrows():
        var = row["variant"]
        cnt = int(row["count"])
        vlow = var.lower()

        # 1) sinónimo exacto
        if vlow in SYN:
            canon = SYN[vlow]
            cls = "OK"; dist = 0; reason = "synonym"
        # 2) exacto en whitelist
        elif vlow in WL_LOWER:
            canon = WL_MAP.get(vlow, var)
            cls = "OK"; dist = 0; reason = "whitelist"
        else:
            # 3) near match contra el whitelist
            best_key = None; best_dist = 999
            for low_canon in WL_LOWER:
                d = edit_distance(vlow, low_canon)
                if d < best_dist:
                    best_dist = d; best_key = low_canon
                    if d == 0:
                        break
            if best_key is not None and best_dist <= NEAR_DIST:
                canon = WL_MAP.get(best_key, best_key)
                cls = "NEAR"; dist = best_dist; reason = "near"
            else:
                canon = ""; cls = "REJECT"; dist = best_dist if best_key is not None else ""; reason = "no_match"

        looks_like_given = 1 if vlow in GIVEN_COMMON else 0

        rows.append({
            "variant": var,
            "count":   cnt,
            "class":   cls,
            "best_match": canon,
            "dist":    dist,
            "reason":  reason,
            "looks_like_given": looks_like_given
        })

    out = pd.DataFrame(rows)

    # orden: OK -> NEAR -> REJECT; dentro por distancia asc y frecuencia desc
    class_order = {"OK": 0, "NEAR": 1, "REJECT": 2}
    out["__cls_ord"] = out["class"].map(class_order).fillna(99)
    out = out.sort_values(["__cls_ord","dist","count"], ascending=[True, True, False]).drop(columns="__cls_ord")

    # exportar particiones
    out_ok   = out[out["class"]=="OK"][["variant","count","best_match","reason"]]
    out_near = out[out["class"]=="NEAR"][["variant","count","best_match","dist"]]
    out_rej  = out[out["class"]=="REJECT"][["variant","count","looks_like_given"]]

    out_ok.to_csv(OUT_OK, sep="\t", index=False)
    out_near.to_csv(OUT_NEAR, sep="\t", index=False)
    out_rej.to_csv(OUT_REJ, sep="\t", index=False)

    looks = out[(out["looks_like_given"]==1) & (out["class"]!="OK")][["variant","count","class"]]
    looks.to_csv(OUT_LIKE_GIVEN, sep="\t", index=False)

    out.to_csv(OUT_SUGG, index=False)

    print(f"[OK] {OUT_OK}")
    print(f"[OK] {OUT_NEAR}")
    print(f"[OK] {OUT_REJ}")
    print(f"[OK] {OUT_LIKE_GIVEN}")
    print(f"[OK] {OUT_SUGG}")
    print("→ Rellena data/whitelist_surnames.txt y data/surname_synonyms.csv para ir afinando.")

if __name__ == "__main__":
    main()