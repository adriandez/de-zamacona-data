import pandas as pd

# ruta relativa desde la raíz del proyecto
INPUT_FILE = "out/Zamacona_all_raw.xlsx"

df = pd.read_excel(INPUT_FILE)

print("Filas totales:", len(df))
if "arkId" in df.columns:
    unique = df["arkId"].nunique()
    dupes = len(df) - unique
    print("arkId únicos:", unique)
    print("Duplicados:", dupes)