#!/usr/bin/env python3
# patch_whitelist_and_syns.py
# - Añade canónicos faltantes al whitelist_surnames.txt
# - Asegura sinónimos clave en surname_synonyms.csv (admite cabecera o comentarios)

from pathlib import Path

DATA_DIR = Path("data")
WL_PATH  = DATA_DIR / "whitelist_surnames.txt"
SYN_PATH = DATA_DIR / "surname_synonyms.csv"

# 1) Canónicos que faltan y que te estaban saliendo en rojo
CANONICALS_TO_ADD = [
    # Canon euskera/uso moderno
    "Etxebarria",     # (equivale a Echevarria/Echebarria/Echavari…)
    "Beaskoetxea",    # (equivale a Beascoechea/Beacoechea…)
    "Eizaga",
    "Bengoechea",
    "Pagaldai",
    "Ugalde",
    "Telleria",
    "Sagarduy",
    "Larragoiti",
    "Errementeria",
    "Birisquieta",
    "Ubiritxaga",
    "Txatabe",
    "Valois",
    "Ordeñana",
    "Ocerin",
    "Artiz",
    "Ortiz",
    "Aiero",
    "Basabe",
    "Orbe",
    # Cast./otros que quieres aceptar tal cual (comparación ya normaliza acentos)
    "Sanchez",
]

# 2) Sinónimos seguros (variant -> canonical)
SYNONYMS = [
    ("Echevarria","Etxebarria"),
    ("Echavari","Etxebarria"),
    ("Echebarria","Etxebarria"),

    ("Beascoechea","Beaskoetxea"),
    ("Beacoechea","Beaskoetxea"),

    ("Heyzaga","Eizaga"),
    ("Eyzaga","Eizaga"),

    ("Vengoechea","Bengoechea"),

    ("Pagalday","Pagaldai"),
    ("Vgalde","Ugalde"),

    ("Zagarduy","Sagarduy"),
    ("Thelleria","Telleria"),

    ("Hordenana","Ordeñana"),
    ("Ibanes","Ibanez"),
    ("Ozerin","Ocerin"),

    ("Balois","Valois"),
    ("Baloisoriundo","Valois"),

    ("Herrementeria","Errementeria"),
    ("Birilquicha","Birisquieta"),
    ("Birizqueta","Birisquieta"),
    ("Ubirichaga","Ubiritxaga"),
    ("Chatave","Txatabe"),
    ("Sanches","Sanchez"),
    ("Aierro","Aiero"),
    ("Basave","Basabe"),
    ("Orve","Orbe"),
    ("Laragoti","Larragoiti"),
    ("Artamonis","Artemuniz"),
    ("Artemoniz","Artemuniz"),
    ("Artave","Artabe"),
]

def read_lines(p: Path) -> list[str]:
    if not p.exists(): return []
    return [x.rstrip("\n\r") for x in p.read_text(encoding="utf-8").splitlines()]

def write_lines(p: Path, lines: list[str]):
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text("\n".join(lines) + "\n", encoding="utf-8")

def main():
    # --- whitelist ---
    if not WL_PATH.exists():
        raise SystemExit(f"No encuentro {WL_PATH}.")
    wl = [x.strip() for x in read_lines(WL_PATH) if x.strip()]
    wl_set_lower = {x.lower() for x in wl}

    to_add = [c for c in CANONICALS_TO_ADD if c.lower() not in wl_set_lower]
    wl_final = wl + to_add
    wl_final = sorted(set(wl_final), key=lambda s: (s.lower(), s))
    write_lines(WL_PATH, wl_final)

    # --- synonyms ---
    syn_lines = []
    if SYN_PATH.exists():
        syn_lines = read_lines(SYN_PATH)
        # normaliza cabecera
        if syn_lines and not syn_lines[0].lower().startswith("variant,canonical"):
            syn_lines.insert(0, "variant,canonical")
    else:
        syn_lines = ["variant,canonical"]

    existing = set()
    for line in syn_lines[1:]:
        if not line or line.lstrip().startswith("#"):
            continue
        core = line.split("#",1)[0].strip()
        if not core:
            continue
        parts = [p.strip() for p in core.split(",")]
        if len(parts) >= 2:
            # A,B,C -> encadenar A->B y B->C
            prev = parts[0]
            for nxt in parts[1:]:
                existing.add((prev.lower(), nxt))
                prev = nxt

    added_pairs = []
    for v, c in SYNONYMS:
        key = (v.lower(), c)
        if key not in existing:
            syn_lines.append(f"{v},{c}")
            existing.add(key)
            added_pairs.append((v,c))

    write_lines(SYN_PATH, syn_lines)

    print(f"[OK] whitelist_surnames.txt → +{len(to_add)} añadidos")
    if to_add:
        print("     ", ", ".join(to_add))
    print(f"[OK] surname_synonyms.csv  → +{len(added_pairs)} sinónimos")
    if added_pairs:
        print("     ", ", ".join([f"{a}→{b}" for a,b in added_pairs]))

if __name__ == "__main__":
    main()