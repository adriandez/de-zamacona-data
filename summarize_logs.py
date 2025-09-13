#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
summarize_logs.py
Barre out/ y compone un informe ejecutivo de inferencias y registros auxiliares.

Entrada esperada en out/:
- Zamacona_infer_log.tsv
- Zamacona_review_log.txt
- Zamacona_unique_given.txt
- Zamacona_unique_surnames.txt
- Zamacona_normalized_enhanced.{xlsx,csv} (opcional)
- Zamacona_normalized_patched.{xlsx,csv}  (opcional)

Salida:
- out/report_logs.md
- out/report_logs.json
"""

from __future__ import annotations
from pathlib import Path
import argparse, json, os, sys, datetime
from typing import Dict, Any, List, Tuple
import pandas as pd

ROOT = Path(__file__).resolve().parent
OUT_DIR = ROOT / "out"

DEF_MD   = OUT_DIR / "report_logs.md"
DEF_JSON = OUT_DIR / "report_logs.json"

INFER_TSV   = OUT_DIR / "Zamacona_infer_log.tsv"
REVIEW_TXT  = OUT_DIR / "Zamacona_review_log.txt"
UNIQUE_GIV  = OUT_DIR / "Zamacona_unique_given.txt"
UNIQUE_SUR  = OUT_DIR / "Zamacona_unique_surnames.txt"

ENH_XLSX = OUT_DIR / "Zamacona_normalized_enhanced.xlsx"
ENH_CSV  = OUT_DIR / "Zamacona_normalized_enhanced.csv"
PAT_XLSX = OUT_DIR / "Zamacona_normalized_patched.xlsx"
PAT_CSV  = OUT_DIR / "Zamacona_normalized_patched.csv"

def mtime_str(p: Path) -> str:
    try:
        ts = datetime.datetime.fromtimestamp(p.stat().st_mtime)
        return ts.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return ""

def file_meta(p: Path) -> Dict[str, Any]:
    return {
        "exists": p.exists(),
        "path": str(p),
        "size": (p.stat().st_size if p.exists() else 0),
        "mtime": mtime_str(p),
        "name": p.name,
    }

def read_infer(examples_per_action: int) -> Dict[str, Any]:
    if not INFER_TSV.exists():
        return {"exists": False, "meta": file_meta(INFER_TSV)}
    df = pd.read_csv(INFER_TSV, sep="\t", dtype=str).fillna("")
    meta = file_meta(INFER_TSV)
    # Conteos por acción
    action_col = "proposed_action" if "proposed_action" in df.columns else None
    reason_col = "proposed_reason" if "proposed_reason" in df.columns else None

    by_action = {}
    samples: Dict[str, List[Dict[str,str]]] = {}
    top_reasons: Dict[str, int] = {}

    if action_col:
        counts = df[action_col].value_counts(dropna=False)
        for k, v in counts.items():
            by_action[str(k)] = int(v)
        # ejemplos por acción
        for action in counts.index.tolist():
            subset = df[df[action_col] == action]
            head = subset.head(examples_per_action)
            cols = [c for c in [
                "arkId","fullName__work",
                "fullName__given","fullName__surn1","fullName__surn2",
                "fatherFullName__given","fatherFullName__surn1","fatherFullName__surn2",
                "motherFullName__given","motherFullName__surn1","motherFullName__surn2",
                "fullName__surn1_proposed","fullName__surn2_proposed",
                "proposed_reason"
            ] if c in df.columns]
            samples[action] = head[cols].to_dict(orient="records")

    if reason_col:
        reason_counts = df[reason_col].value_counts(dropna=False)
        for k, v in reason_counts.items():
            top_reasons[str(k)] = int(v)

    return {
        "exists": True,
        "meta": meta,
        "rows": int(len(df)),
        "by_action": by_action,
        "top_reasons": top_reasons,
        "samples": samples,
    }

def read_txt_list(p: Path) -> List[str]:
    if not p.exists(): return []
    vals = []
    with open(p, "r", encoding="utf-8") as f:
        for line in f:
            t = line.rstrip("\r\n")
            if t: vals.append(t)
    return vals

def read_unique_kv(p: Path) -> List[Tuple[str,int]]:
    # Formato esperado: "<token>\t<count>"
    out: List[Tuple[str,int]] = []
    if not p.exists(): return out
    with open(p, "r", encoding="utf-8") as f:
        for line in f:
            line = line.rstrip("\r\n")
            if not line: continue
            parts = line.split("\t")
            if len(parts) >= 2:
                tok = parts[0].strip()
                try:
                    cnt = int(parts[1])
                except Exception:
                    cnt = 0
                out.append((tok, cnt))
    return out

def read_applied(df_path: Path) -> Dict[str, Any]:
    """Lee normalized_enhanced/patched y resume surnameInferenceApplied si existe."""
    if not df_path.exists(): return {"exists": False, "meta": file_meta(df_path)}
    try:
        if df_path.suffix.lower() == ".xlsx":
            df = pd.read_excel(df_path, dtype=str).fillna("")
        else:
            df = pd.read_csv(df_path, dtype=str).fillna("")
    except Exception as e:
        return {"exists": True, "meta": file_meta(df_path), "error": str(e)}

    col = "surnameInferenceApplied"
    if col not in df.columns:
        return {"exists": True, "meta": file_meta(df_path), "applied_total": 0, "by_type": {}}

    non_empty = df[df[col].astype(str).str.strip() != ""]
    total = int(len(non_empty))
    by_type = {k: int(v) for k, v in non_empty[col].value_counts().to_dict().items()}
    return {"exists": True, "meta": file_meta(df_path), "applied_total": total, "by_type": by_type}

def build_report_md(data: Dict[str, Any], examples_per_action: int) -> str:
    md = []
    md.append(f"# Informe de logs (generado {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')})\n")

    # Inferencia
    inf = data["infer"]
    md.append("## 1) Inferencia de apellidos (Zamacona_infer_log.tsv)")
    if not inf["exists"]:
        md.append(f"- No existe `{INFER_TSV.name}`.\n")
    else:
        md.append(f"- Fichero: `{inf['meta']['name']}` | filas: **{inf['rows']}** | mtime: {inf['meta']['mtime']}")
        if inf.get("by_action"):
            md.append("\n**Distribución por acción:**")
            for k, v in inf["by_action"].items():
                md.append(f"- {k}: **{v}**")
        if inf.get("top_reasons"):
            md.append("\n**Top razones:**")
            for k, v in list(inf["top_reasons"].items())[:10]:
                md.append(f"- {k}: **{v}**")
        if inf.get("samples"):
            md.append(f"\n**Ejemplos por acción** (máx {examples_per_action} c/u):")
            for action, recs in inf["samples"].items():
                md.append(f"\n- **{action}**:")
                for r in recs:
                    ark = r.get("arkId","")
                    name = r.get("fullName__work","")
                    s1p = r.get("fullName__surn1_proposed","")
                    s2p = r.get("fullName__surn2_proposed","")
                    reason = r.get("proposed_reason","")
                    md.append(f"  - {ark} · {name} → propuesta: [{s1p} | {s2p}] · {reason}")

    # Review pendientes
    rev_list = data["review_list"]
    md.append("\n## 2) Pendientes de revisión (Zamacona_review_log.txt)")
    if not rev_list["exists"]:
        md.append(f"- No existe `{REVIEW_TXT.name}`.")
    else:
        md.append(f"- Entradas: **{rev_list['count']}** | mtime: {rev_list['meta']['mtime']}")
        md.append(f"- (muestra 10) " + "; ".join(rev_list["sample"]))

    # Uniques
    md.append("\n## 3) Top nombres y apellidos (solo verdes)")
    giv = data["unique_given"]
    sur = data["unique_surn"]
    if giv["exists"]:
        md.append(f"- **Nombres únicos**: {giv['count']} (mtime {giv['meta']['mtime']})")
        md.append("  - Top 10: " + ", ".join([f"{t} ({c})" for t,c in giv["top10"]]))
    else:
        md.append(f"- No existe `{UNIQUE_GIV.name}`.")
    if sur["exists"]:
        md.append(f"- **Apellidos únicos**: {sur['count']} (mtime {sur['meta']['mtime']})")
        md.append("  - Top 10: " + ", ".join([f"{t} ({c})" for t,c in sur["top10"]]))
    else:
        md.append(f"- No existe `{UNIQUE_SUR.name}`.")

    # Aplicaciones (enhanced/patched)
    md.append("\n## 4) Aplicaciones realizadas (si existen enhanced/patched)")
    enh = data["enhanced"]
    pat = data["patched"]
    for tag, node in (("Enhanced", enh), ("Patched", pat)):
        if not node["exists"]:
            md.append(f"- {tag}: no encontrado.")
        else:
            meta = node["meta"]
            md.append(f"- {tag}: `{meta['name']}` mtime {meta['mtime']}")
            if "applied_total" in node:
                md.append(f"  - `surnameInferenceApplied`: **{node['applied_total']}**")
                if node.get("by_type"):
                    for k, v in node["by_type"].items():
                        md.append(f"    - {k}: **{v}**")

    # Metadatos
    md.append("\n## 5) Metadatos de archivos")
    for p in [INFER_TSV, REVIEW_TXT, UNIQUE_GIV, UNIQUE_SUR, ENH_XLSX, ENH_CSV, PAT_XLSX, PAT_CSV]:
        meta = file_meta(p)
        md.append(f"- {meta['name']}: exists={meta['exists']} size={meta['size']} mtime={meta['mtime']}")

    # Siguientes pasos
    md.append("\n## 6) Siguientes pasos sugeridos")
    md.append("- Si `swap` y `fill` lucen correctos → ejecutar `run_pipeline.py --apply`.")
    md.append("- Si hay muchos `mismatch`, prioriza revisar los que compartan la misma `proposed_reason` (atacar causas raíz).")
    md.append("- Cuando confirmes la calidad, habilita la promoción automática enhanced→patched en tu pipeline.")

    return "\n".join(md) + "\n"

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=str(DEF_MD), help="Ruta del informe Markdown")
    ap.add_argument("--json", default=str(DEF_JSON), help="Ruta del informe JSON")
    ap.add_argument("--examples", type=int, default=5, help="Ejemplos por acción")
    args = ap.parse_args()

    OUT_DIR.mkdir(exist_ok=True)

    data: Dict[str, Any] = {}

    # 1) Infer
    data["infer"] = read_infer(args.examples)

    # 2) Review list
    rev = {"exists": False, "meta": file_meta(REVIEW_TXT), "count": 0, "sample": []}
    if REVIEW_TXT.exists():
        items = read_txt_list(REVIEW_TXT)
        rev.update({
            "exists": True,
            "count": len(items),
            "sample": items[:10]
        })
    data["review_list"] = rev

    # 3) Uniques
    giv_list = read_unique_kv(UNIQUE_GIV)
    sur_list = read_unique_kv(UNIQUE_SUR)
    data["unique_given"] = {
        "exists": bool(giv_list),
        "meta": file_meta(UNIQUE_GIV),
        "count": len(giv_list),
        "top10": giv_list[:10]
    }
    data["unique_surn"] = {
        "exists": bool(sur_list),
        "meta": file_meta(UNIQUE_SUR),
        "count": len(sur_list),
        "top10": sur_list[:10]
    }

    # 4) Enhanced/Patched (si existen) — preferimos XLSX si está
    enh_path = ENH_XLSX if ENH_XLSX.exists() else ENH_CSV
    pat_path = PAT_XLSX if PAT_XLSX.exists() else PAT_CSV
    data["enhanced"] = read_applied(enh_path)
    data["patched"]  = read_applied(pat_path)

    # 5) Escribir salidas
    md = build_report_md(data, args.examples)
    Path(args.out).write_text(md, encoding="utf-8")
    Path(args.json).write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"[OK] Informe Markdown → {args.out}")
    print(f"[OK] Informe JSON     → {args.json}")

if __name__ == "__main__":
    sys.exit(main())