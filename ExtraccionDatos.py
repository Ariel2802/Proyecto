import requests, pandas as pd, time, math
from datetime import datetime, timezone
from dateutil import parser as dtp

print("=== Exportador Zabbix → CSV (UPSE) ===")

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

fechaInicio = "2025-08-10 00:00"

archivoSalida = "miConjunto.csv"


def toUnix(tsStr: str) -> int:
    """Convierte string ISO/‘YYYY-MM-DD HH:MM’ a epoch (UTC)."""
    return int(dtp.parse(tsStr).replace(tzinfo=None).timestamp())

def zbxCall(session: requests.Session, url: str, zbxAuth: str, method: str, params: dict):
    """Llamada JSON-RPC a Zabbix."""
    payload = {"jsonrpc": "2.0", "method": method, "params": params, "id": 1, "auth": zbxAuth}
    try:
        r = session.post(url, json=payload, timeout=60)
        r.raise_for_status()
        j = r.json()
        if "error" in j:
            raise RuntimeError(f"Zabbix API error: {j['error']}")
        return j["result"]
    except Exception as e:
        print(f"⚠️ Error en llamada {method}: {e}")
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
        items.extend(res)
    # eliminar duplicados por itemid
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
    out = []
    prev = None
    for s in series:
        ts = int(s["clock"])
        val = float(s["value"])
        if prev is None:
            prev = (ts, val); continue
        dt = ts - prev[0]
        dv = val - prev[1]
        if dt > 0 and dv >= 0:
            bps = (dv * 8.0) / dt
            out.append({"clock": ts, "rate_bps": bps, "rate_kbps": bps/1e3, "rate_Mbps": bps/1e6})
        prev = (ts, val)
    return out

def guardar_parcial(filas, archivoSalida):
    if filas:
        salida = pd.concat(filas, ignore_index=True)
        salida.to_csv(archivoSalida, index=False)
        print(f"💾 Guardado parcial: {archivoSalida} ({len(salida)} filas)")

fechaInicioUnix = toUnix(fechaInicio)
fechaFinUnix = int(datetime.now().timestamp())


session = requests.Session()
loginPayload = {
    "jsonrpc": "2.0",
    "method": "user.login",
    "params": {
        "username": usuario,
        "password": clave
        },
    "id": 1
}

resp = session.post(url, json=loginPayload, timeout=60)
resp.raise_for_status()
zbxAuth = resp.json()["result"]

#Obtener Hosts
hosts = zbxCall(session, url, zbxAuth, "host.get", {
    "output": ["hostid", "host", "name"]
})

# Filtro de exclusión
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
    try:
        hostid = h["hostid"]
        hostName = (h.get("name") or h.get("host")).strip()
        items = itemsForHost(session, url, zbxAuth, hostid)
        print("hostName: " + hostName)
        
        for it in items:
            try:
                itemid = it["itemid"]
                valueType = int(it["value_type"])
                datos, fuente = fetchHistoryOrTrends(session, url, zbxAuth, itemid, valueType, fechaInicioUnix, fechaFinUnix)
                        
                if not datos:
                    continue
                
                # ¿Es contador de octetos? (para calcular tasa)
                nameLower = (it["name"] or "").lower()
                keyLower  = (it["key_"] or "").lower()
                esOctetos = any(k in nameLower for k in ["trafico"])

                df = pd.DataFrame(datos)
                df["clock"] = df["clock"].astype(int)
                df = df.sort_values("clock")

                if esOctetos:
                    # history: value, trends: value_avg
                    serie = []
                    for _, r in df.iterrows():
                        v = float(r.get("value", r.get("value_avg", 0.0)))
                        serie.append({"clock": int(r["clock"]), "value": v})
                    tasas = computeRate(serie)
                    if not tasas:
                        continue
                    dr = pd.DataFrame(tasas)
                    dr["timestamp"] = dr["clock"].apply(lambda x: datetime.fromtimestamp(x, tz=timezone.utc).isoformat())
                    dr["host"] = hostName
                    dr["item"] = it["name"]
                    filas.append(dr[["timestamp", "host", "item", "rate_bps", "rate_kbps", "rate_Mbps"]])
                else:
                    valorCol = "value" if "value" in df.columns else "value_avg"
                    df["timestamp"] = df["clock"].apply(lambda x: datetime.fromtimestamp(int(x), tz=timezone.utc).isoformat())
                    df["host"] = hostName
                    df["item"] = it["name"]
                    filas.append(df[["timestamp", "host", "item", valorCol]].rename(columns={valorCol: "value"}))

            except Exception as e_item:
                print(f"Error en item {it['name']} ({hostName}): {e_item}")
                continue
            
            # Guardado parcial cada host
            guardar_parcial(filas, archivoSalida)
    except Exception as e_host:
        print(f"Error en host {h['name']}: {e_host}")
        continue
    
    
guardar_parcial(filas, archivoSalida)
print("✅ Proceso completado con salvado incremental")

#Exportación --------
if filas:
    salida = pd.concat(filas, ignore_index=True)
    salida.to_csv("Final.csv", index = False)
    print(f"\n Exportado: {"Final.csv"}  ({len(salida)} filas)")
    print("Columnas posibles: timestamp, host, item, value  /  rate_bps, rate_Mbps")
else:
    print("\n No se obtuvieron datos. Revisa URL, credenciales, ventana de tiempo o nombres de ítems.")
