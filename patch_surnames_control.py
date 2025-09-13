#!/usr/bin/env python3
# Aplica correcciones al control de apellidos:
# - Limpia whitelist (quita nombres de pila y “basura” listada)
# - Añade synonyms claros (variant,canonical), unificados a canónicos que usas
# - Deja ambiguos en comentario (para revisión manual)

import re
from pathlib import Path

DATA_DIR = Path("data")
WL_PATH  = DATA_DIR / "whitelist_surnames.txt"
SYN_PATH = DATA_DIR / "surname_synonyms.csv"

# ---- 1) quitar del whitelist (nombres / ruido que no queremos como apellidos) ----
REMOVE_FROM_WL = {
    # nombres de pila inequívocos (no apellidos)
    "Angela","Antonia","Augustin","Agustina","Bentura","Venttura","Bonifacio",
    "Christoval","Damian","Del","Domingo","Dominica","Elvira","Estevan",
    "Ignacia","Ines","Inigo","Jacobe","Jesus","Joana","Juana",
    "Lorenza","Lorenzo","Lucas","Lucia","Madalena","Marcos","Micaela",
    "Nicolas","Paula","Tomas","Tereza","Xaviera","Vicenta",
    # ruido / no reconocido como apellido
    "Balois","Burdaria","Camura","Camarca","Cuebas","Domeca","Mascarua","Meaurio",
    "Rique","Sanctos","Vilbatua","Bilbatua"
}

# ---- 2) sinónimos seguros -> canónicos (unificados) ----
SYNONYMS = [
    # (nuevos que pediste en el último mensaje)
    ("Echevarria","Etxebarria"),
    ("Echavari","Etxebarria"),
    ("Beascoechea","Beaskoetxea"),
    ("Beacoechea","Beaskoetxea"),
    ("Aldacazaval","Aldazabal"),
    ("Artave","Artabe"),
    ("Balle","Valle"),
    ("Belaosteguiy","Belaostegui"),
    ("Beyntia","Beitia"),
    ("Heyzaga","Eizaga"),
    ("Eyzaga","Eizaga"),
    ("Hordenana","Ordenana"),
    ("Ibanes","Ibanez"),
    ("Ozerin","Ocerin"),
    ("Pagalday","Pagaldai"),
    ("Vengoechea","Bengoechea"),
    ("Vgalde","Ugalde"),
    ("Zagarduy","Sagarduy"),
    ("Thelleria","Telleria"),
    ("Bauptista","Bautista"),

    # (los que ya teníamos de antes)
    ("Birilquicha","Birisquieta"),
    ("Birizqueta","Birisquieta"),
    ("Ubirichaga","Ubiritxaga"),
    ("Herrementeria","Errementeria"),
    ("Balois","Valois"),
    ("Baloisoriundo","Valois"),
    ("Chatave","Txatabe"),
    ("Sanches","Sanchez"),
    ("Aierro","Aiero"),
    ("Basave","Basabe"),
    ("Artuis","Artiz"),
    ("Jortis","Ortiz"),
    ("Artamonis","Artemuniz"),
    ("Artemoniz","Artemuniz"),
    ("Gonzales","Gonzalez"),
    ("Orve","Orbe"),
    ("Laragoti","Larragoiti"),
    ("Echevarria,","Etxebarria"),
]

# ---- 3) ambiguos -> NO aplicar automáticamente; revisar contra contexto ----
AMBIGUOUS_COMMENT = [
    "Artamonis -> Artamendi? Artamona?",
    "Austoa -> Arestua? Astoaga?",
    "Isusorbe -> Isusaga? Isusquiza?",
    "Oynquina -> Oinquina?",
    "Ubirichaga -> Urbinaga/Urbinaga/Urbinaga? (variante con ñ si decides)",
    "Urgoytia -> Urgoitia?",
    "Uricar -> Urkiza? Urricar?",
    "Uriguen -> Urien? Uriguena?",
    "Zerendieta -> Zearendieta? Zerandieta?",
    "Zevericha -> ? (no claro)"
]

def read_lines(p: Path) -> list:
    if not p.exists(): return []
    return [x.rstrip("\n\r") for x in p.read_text(encoding="utf-8").splitlines()]

def write_lines(p: Path, lines: list):
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text("\n".join(lines) + "\n", encoding="utf-8")

def norm(s: str) -> str:
    return re.sub(r"\s+", " ", s.strip())

def main():
    if not WL_PATH.exists():
        raise SystemExit(f"No encuentro {WL_PATH}.")
    # 1) limpiar whitelist
    wl = [norm(x) for x in read_lines(WL_PATH) if norm(x)]
    wl_set = set(wl)
    removed = sorted(w for w in wl_set if w in REMOVE_FROM_WL)
    wl_new = sorted(w for w in wl_set if w not in REMOVE_FROM_WL)
    write_lines(WL_PATH, wl_new)

    # 2) añadir synonyms (evitando duplicados)
    if SYN_PATH.exists():
        syn_lines = read_lines(SYN_PATH)
        if not syn_lines or not syn_lines[0].lower().startswith("variant,canonical"):
            syn_lines.insert(0, "variant,canonical")
    else:
        syn_lines = ["variant,canonical"]

    existing = set()
    for line in syn_lines[1:]:
        parts = [p.strip() for p in line.split(",")]
        if len(parts) == 2:
            existing.add((parts[0], parts[1]))

    added = []
    for v, c in SYNONYMS:
        pair = (v, c)
        if pair not in existing:
            syn_lines.append(f"{v},{c}")
            existing.add(pair)
            added.append(pair)

    # 3) Ambiguos como comentarios al final
    if AMBIGUOUS_COMMENT:
        syn_lines.append("")
        syn_lines.append("# --- AMBIGUOS A REVISAR MANUALMENTE ---")
        for msg in AMBIGUOUS_COMMENT:
            syn_lines.append(f"# {msg}")

    write_lines(SYN_PATH, syn_lines)

    # 4) resumen
    print(f"[OK] Limpiado whitelist: -{len(removed)} entradas")
    if removed:
        print("     Quitados:", ", ".join(removed[:25]) + ("..." if len(removed)>25 else ""))
    print(f"[OK] Añadidos {len(added)} sinónimos a surname_synonyms.csv")
    print(f"[OK] Comentados {len(AMBIGUOUS_COMMENT)} casos ambiguos para revisión")

if __name__ == "__main__":
    main()