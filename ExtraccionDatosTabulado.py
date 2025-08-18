import requests, pandas as pd
from datetime import datetime, timezone
from dateutil import parser as dtp
import time

print("=== Exportador Zabbix → CSV (UPSE) ===")

fechaInicio = "2025-08-10 00:00"
archivoSalida = "miConjuntoProcesado.csv"

#Entrada por consola

usuario = input("Ingrese el usuario: ")
clave = input("Ingrese la clave: ")

url = input("Ingrese la url de Zabbix: ")

url = "http://" + url + "/zabbix/api_jsonrpc.php"

hostNamesExcluidos = {"zabbix server"}

itemsRecolectar = [
    "ICMP loss",
    "ICMP ping",
    "ICMP response time",
    "Trafico LAN Recibido",
    "Trafico LAN Transmitido",
    "Trafico WLAN Recibido - 2.4GHz",
    "Trafico WLAN Transmitido - 2.4GHz",
    "Trafico WLAN Recibido - 5GHz",
    "Trafico WLAN Transmitido - 5GHz",
    "Uptime (network)"
]

#Para convertir fechaInicio
def toUnix(tsStr: str) -> int:
    return int(dtp.parse(tsStr).replace(tzinfo=None).timestamp())

def zbxCall(session: requests.Session, url: str, zbxAuth: str, method: str, params: dict, retries=3):
    """Llamada JSON-RPC a Zabbix con reintentos."""
    payload = {"jsonrpc": "2.0", "method": method, "params": params, "id": 1, "auth": zbxAuth}
    for intento in range(retries):
        try:
            r = session.post(url, json=payload, timeout=60)
            r.raise_for_status()
            j = r.json()
            if "error" in j:
                raise RuntimeError(f"Zabbix API error: {j['error']}")
            return j["result"]
        except Exception as e:
            print(f"⚠️ Error en llamada {method}: {e} (intento {intento+1}/{retries})")
            time.sleep(30)
    return None

def itemsForHost(session, url, zbxAuth, hostid: str):
    items = []
    for pat in itemsRecolectar:
        res = zbxCall(session, url, zbxAuth, "item.get", {
            "output": ["itemid", "name", "key_", "value_type", "hostid"],
            "hostids": hostid,
            "search": {"name": pat},
            "searchWildcardsEnabled": True
        })
        if res: items.extend(res)
    seen, out = set(), []
    for it in items:
        if it["itemid"] not in seen:
            seen.add(it["itemid"]); out.append(it)
    return out

def fetchHistoryOrTrends(session, url, zbxAuth, itemid: str, valueType: int, tFrom: int, tTill: int):
    histType = 0 if int(valueType) == 0 else 3
    resHist = zbxCall(session, url, zbxAuth, "history.get", {
        "output": "extend",
        "history": histType,
        "itemids": itemid,
        "time_from": tFrom,
        "time_till": tTill,
        "sortfield": "clock",
        "sortorder": "ASC",
        "limit": 100000
    })
    if resHist:
        return resHist, "history"

    resTr = zbxCall(session, url, zbxAuth, "trends.get", {
        "output": "extend",
        "itemids": itemid,
        "time_from": tFrom,
        "time_till": tTill,
        "sortfield": "clock",
        "sortorder": "ASC"
    })
    return resTr, "trends"

def computeRate(series):
    """
    Convierte serie de octetos acumulados a tasas bps, Kbps y Mbps.
    Detecta reinicio de contador (cuando el valor baja).
    """
    out = []
    prev = None
    for s in series:
        ts = int(s["clock"])
        val = float(s["value"])
        if prev is None:
            prev = (ts, val); continue
        dt = ts - prev[0]
        dv = val - prev[1]

        # si el contador se reinició (valor actual < anterior), ignoramos ese salto
        if dv < 0:
            prev = (ts, val)
            continue

        if dt > 0:
            bps = (dv * 8.0) / dt
            out.append({
                "clock": ts,
                "rate_bps": bps,
                "rate_Kbps": bps/1e3,
                "rate_Mbps": bps/1e6
            })
        prev = (ts, val)
    return out

def guardar_parcial(filas, archivoSalida):
    if filas:
        salida = pd.concat(filas, ignore_index=True)
        salida.to_csv(archivoSalida, index=False)
        print(f"💾 Guardado parcial: {archivoSalida} ({len(salida)} filas)")

fechaInicioUnix = toUnix(fechaInicio)
fechaFinUnix = int(datetime.now().timestamp())

#Autenticación
session = requests.Session()
loginPayload = {
    "jsonrpc": "2.0",
    "method": "user.login",
    "params": {"username": usuario, "password": clave},
    "id": 1
}
resp = session.post(url, json=loginPayload, timeout=60)
resp.raise_for_status()
zbxAuth = resp.json()["result"]

#Obtener Hosts
hosts = zbxCall(session, url, zbxAuth, "host.get", {"output": ["hostid", "host", "name"]})
hostsFiltrados = []
for h in hosts:
    nombre = (h.get("name") or h.get("host") or "").strip()
    if nombre.lower() in hostNamesExcluidos:
        continue
    hostsFiltrados.append(h)

print(f"Hosts totales: {len(hosts)} | incluidos: {len(hostsFiltrados)} | excluidos: {len(hosts) - len(hostsFiltrados)}")

#Recolección
filas = []

for h in hostsFiltrados:
    hostid = h["hostid"]
    hostName = (h.get("name") or h.get("host")).strip()
    print("hostName: " + hostName)
    items = itemsForHost(session, url, zbxAuth, hostid)

    for it in items:
        itemid = it["itemid"]
        valueType = int(it["value_type"])
        datos, fuente = fetchHistoryOrTrends(session, url, zbxAuth, itemid, valueType, fechaInicioUnix, fechaFinUnix)
        if not datos: continue
          
        # ¿Es contador de octetos? (para calcular tasa)
        nameLower = (it["name"] or "").lower()
        esOctetos = "trafico" in nameLower

        df = pd.DataFrame(datos)
        df["clock"] = df["clock"].astype(int)
        df = df.sort_values("clock")

        if esOctetos:
            serie = []
            for _, r in df.iterrows():
                v = float(r.get("value", r.get("value_avg", 0.0)))
                serie.append({"clock": int(r["clock"]), "value": v})
            tasas = computeRate(serie)
            if not tasas: continue
            dr = pd.DataFrame(tasas)
            dr["timestamp"] = dr["clock"].apply(lambda x: datetime.fromtimestamp(x, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S"))
            dr["host"] = hostName
            dr["item"] = it["name"]
            filas.append(dr[["timestamp", "host", "item", "rate_bps", "rate_Kbps", "rate_Mbps"]])
        else:
            valorCol = "value" if "value" in df.columns else "value_avg"
            df["timestamp"] = df["clock"].apply(lambda x: datetime.fromtimestamp(int(x), tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S"))
            df["host"] = hostName
            df["item"] = it["name"]
            df["valor"] = df[valorCol].astype(float)
            filas.append(df[["timestamp", "host", "item", "valor"]])

    # guardado parcial por host
    guardar_parcial(filas, archivoSalida)

# Pivot final --------
if filas:
    salida = pd.concat(filas, ignore_index=True)
    salida = salida.rename(columns={"valor":"value"})
    tabla = salida.pivot_table(
        index=["timestamp","host"],
        columns="item",
        values=["value","rate_bps","rate_Kbps","rate_Mbps"],
        aggfunc="first"
    )
    tabla.columns = [f"{v} ({k})" if k!="value" else v for k,v in tabla.columns]
    tabla = tabla.reset_index()

    tabla.to_csv("Final"+archivoSalida, index=False)
    print(f"\n✅ Exportado: {archivoSalida}  ({len(tabla)} filas)")
    print(f"Columnas exportadas: {list(tabla.columns)}")
else:
    print("\n⚠️ No se obtuvieron datos.")