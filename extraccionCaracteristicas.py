import pandas as pd
from zoneinfo import ZoneInfo

#Configuración
archivoEntrada = "Final.csv"
archivoSalida = "datasetExtraido.csv"
ventanaTiempo = "5min"   # "5min", "10min", "15min"
zonaEcuador = ZoneInfo("America/Guayaquil")

#Cargar dataset crudo
datosCrudos = pd.read_csv(archivoEntrada, parse_dates=["timestamp"])
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

#Resampleo por ventana temporal
datosPivot = datosPivot.set_index("timestamp")
datosCaracteristicas = []

for nombreHost, grupoHost in datosPivot.groupby("host"):
    # Calcular jitter
    if "ICMP response time" in grupoHost.columns:
        grupoHost["jitter_diff"] = grupoHost["ICMP response time"].diff().abs()
    
    # Agregaciones por ventana
    tablaVentana = grupoHost.resample(ventanaTiempo).agg({
        "ICMP ping": ["mean", "std", "min", "max"],
        "ICMP response time": ["mean", "std", "min", "max"],
        "jitter_diff": ["mean", "max"],
        "Paquetes entrada con error ETH": ["sum"],
        "Paquetes salida con error ETH": ["sum"],
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
