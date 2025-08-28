import pandas as pd
from zoneinfo import ZoneInfo
from datetime import datetime

#Configuración
archivoEntrada = "Proyecto\\Datos\\FinalmiConjuntohistory.csv"
archivoSalida = "Proyecto\\Datos\\FinaldatasetExtraido.csv" # + datetime.now().strftime("%d-%m-%Y %H_%M_%S") + ".csv"
ventanaTiempo = "60min"   # "5min", "10min", "15min"
zonaEcuador = ZoneInfo("America/Guayaquil")

#Cargar dataset crudo
datosCrudos = pd.read_csv(archivoEntrada, parse_dates=["timestamp"])
# Asegurar zona horaria (manejo robusto: si viene naive -> localiza; si ya tiene tz -> convierte)
if datosCrudos["timestamp"].dt.tz is None:
    datosCrudos["timestamp"] = datosCrudos["timestamp"].dt.tz_localize(zonaEcuador)
else:
    datosCrudos["timestamp"] = datosCrudos["timestamp"].dt.tz_convert(zonaEcuador)

# Pivotear: cada item como columna (una fila = host en un instante)
datosPivot = datosCrudos.pivot_table(
    index=["timestamp", "host"],
    columns="item",
    values="value"
).reset_index()

datosPivot = datosPivot.sort_values(["host", "timestamp"])

# Conversión de tráfico (octetos acumulativos → Mbps)
for columna in ["Trafico LAN Recibido", "Trafico LAN Transmitido"]:
    if columna in datosPivot.columns:
        datosPivot[columna] = datosPivot.groupby("host")[columna].diff().clip(lower=0) * 8
        datosPivot[columna] = datosPivot[columna] / 1e6  # conversión a Mbps

# === Convertir ICMP response time a milisegundos (ms) antes de features ===
if "ICMP response time" in datosPivot.columns:
    # Convertir de segundos -> milisegundos
    datosPivot["ICMP response time"] = datosPivot["ICMP response time"] * 1000.0  # ahora está en ms

#Resampleo por ventana temporal
datosPivot = datosPivot.set_index("timestamp")
datosCaracteristicas = []

for nombreHost, grupoHost in datosPivot.groupby("host"):
    # Calcular jitter
    if "ICMP response time" in grupoHost.columns:
        grupoHost = grupoHost.copy()
        grupoHost["jitter_diff"] = grupoHost["ICMP response time"].diff().abs()
    
    # Agregaciones por ventana
    tablaVentana = grupoHost.resample(ventanaTiempo).agg({
        "ICMP response time": ["mean", "std", "min", "max"],
        "jitter_diff": ["mean", "max"],
        "Paquetes unicast enviados ETH": ["mean", "std", "min", "max"],
        "Paquetes unicast recibidos ETH": ["mean", "std", "min", "max"],
        "Trafico LAN Recibido": ["mean", "std", "min", "max"],
        "Trafico LAN Transmitido": ["mean", "std", "min", "max"],
        "Uptime (network)": ["max"]
    })

    tablaVentana["host"] = nombreHost
    datosCaracteristicas.append(tablaVentana)

# Unir todos los hosts
tablaCaracteristicas = pd.concat(datosCaracteristicas).reset_index()

#reemplazar los espacios por _ 
tablaCaracteristicas.columns = [
    "_".join([c for c in col if c]).strip().replace(" ", "_").lower()
    for col in tablaCaracteristicas.columns.values
]

#Guardar dataset final
tablaCaracteristicas.to_csv(archivoSalida, index=False)
print(f"✅ Features extraídos: {tablaCaracteristicas.shape}")
print(f"Archivo generado: {archivoSalida}")
print("Columnas generadas:", list(tablaCaracteristicas.columns))
