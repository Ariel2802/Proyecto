import numpy as np
from sklearn.model_selection import train_test_split
import pandas as pd

archivo = "Proyecto\\Datos\\DatosConYConFeatures - Balanceado.xlsx"

tabla = pd.read_excel(archivo, parse_dates=["timestamp_ventana"])
tabla = tabla.dropna()
tabla = tabla.sort_values(["timestamp_ventana"]).reset_index(drop=True)

columnas_excluir = ["timestamp_ventana", "host", "Y"]

X = tabla.drop(columns=columnas_excluir)

y = tabla["Y"]

indices = np.arange(len(y))

# 70/30 estratificado
train_idx, test_idx = train_test_split(
    indices, test_size=0.30, stratify=y, random_state=42
)

ind_train_rel, ind_val_rel = train_test_split(
    np.arange(len(train_idx)), test_size=0.33, stratify=y[train_idx], random_state=42
)

val_idx   = train_idx[ind_val_rel]
train_idx = train_idx[ind_train_rel]

# Guardar para reusar en TODOS los notebooks
np.save("Proyecto\\Utilidades\\indices\\train_idx.npy", train_idx)
np.save("Proyecto\\Utilidades\\indices\\val_idx.npy",   val_idx)
np.save("Proyecto\\Utilidades\\indices\\test_idx.npy",  test_idx)

print(len(train_idx), len(val_idx), len(test_idx))
