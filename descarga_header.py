import os
import json
import requests
import pandas as pd
import unicodedata
from datetime import datetime

# ==================================================
# CONFIGURACIÓN
# ==================================================
SENTILO_URL = "http://connectaapi.bcn.cat/data"
DATA_FOLDER = "datos_sensores"
INDEX_JSON = "indice_sensores.json"

LIMIT_ENERGIA = 96      # 24h * 4
LIMIT_INSTANT = 1

os.makedirs(DATA_FOLDER, exist_ok=True)

# Tokens
TOKEN_STD = os.getenv("SENTILO_TOKEN", "").strip()
TOKEN_FV  = os.getenv("SENTILO_TOKEN_FV", "").strip()

if not TOKEN_STD:
    raise RuntimeError("❌ SENTILO_TOKEN no definido")

# ==================================================
# UTILIDADES
# ==================================================
def normalizar(txt):
    txt = str(txt).lower()
    txt = unicodedata.normalize("NFD", txt)
    return "".join(c for c in txt if unicodedata.category(c) != "Mn")

def es_energia(sensor_id, descripcion):
    sid = sensor_id.upper()
    desc = normalizar(descripcion)
    return sid.startswith("0190_MV_") or "energia" in desc

def parse_timestamp(ts):
    try:
        return datetime.strptime(ts, "%d/%m/%YT%H:%M:%S").isoformat()
    except:
        return ts

def parse_value(sensor_id, descripcion, raw):
    try:
        data = json.loads(raw)
        s = data.get("summary", {})

        if es_energia(sensor_id, descripcion):
            if "firstvalue" in s and "lastvalue" in s:
                return float(s["lastvalue"]) - float(s["firstvalue"])
        else:
            if "avg" in s:
                return float(s["avg"])
    except:
        pass
    return None

# ==================================================
# CARGA EXCEL CONFIG
# ==================================================
df = pd.read_excel("Relación sensores AVINYÓ.xls")
df.columns = [c.strip().lower() for c in df.columns]

# ==================================================
# DESCARGA HEADER
# ==================================================
indice = {}
cache = {}   # para sensores energía

for _, r in df.iterrows():

    sensor_id = str(r["sensor_id"]).strip()
    descripcion = str(r.get("descripcion", sensor_id))
    unidad = str(r.get("unitat de mesura", ""))

    provider = str(r.get("provider_id", "")).strip()
    token_env = str(r.get("token_env", "")).strip()

    print(f"\n📡 {sensor_id} – {descripcion}")

    # ----------------------------------------------
    # SENSOR CALCULADO
    # ----------------------------------------------
    if sensor_id == "0190_MV_ENERGIA_CONS":
        try:
            imp = cache["0190_MV_C1_ASB_ACTIVEE"]
            fv  = cache["0524_MV_FVENERGIA"]

            n = min(len(imp["values"]), len(fv["values"]))

            values = [
                imp["values"][-n+i] + fv["values"][-n+i]
                for i in range(n)
            ]
            labels = imp["labels"][-n:]

            data = {
                "sensor_id": sensor_id,
                "descripcion": descripcion,
                "unidad": unidad,
                "tipo_dato": "consumo_intervalo",
                "labels": labels,
                "values": values
            }

            with open(f"{DATA_FOLDER}/{sensor_id}.json", "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)

            indice[sensor_id] = {
                "descripcion": descripcion,
                "unidad": unidad,
                "archivo": f"{sensor_id}.json"
            }

            print(f"   ✅ CALCULADO ({len(values)} puntos)")

        except Exception as e:
            print(f"   ❌ Error cálculo: {e}")
        continue

    # ----------------------------------------------
    # SELECCIÓN TOKEN / PROVIDER
    # ----------------------------------------------
    if provider.lower() == "nan" or not provider:
        print("   ⚠️ Sin provider → saltado")
        continue

    token = TOKEN_FV if token_env == "FV" else TOKEN_STD
    headers = {"IDENTITY_KEY": token, "Accept": "application/json"}

    limit = LIMIT_ENERGIA if es_energia(sensor_id, descripcion) else LIMIT_INSTANT

    url = f"{SENTILO_URL}/{provider}/{sensor_id}"
    params = {"limit": limit, "order": "desc"}

    try:
        r = requests.get(url, headers=headers, params=params, timeout=20)
        r.raise_for_status()
        obs = r.json().get("observations", [])
    except Exception as e:
        print(f"   ❌ Error conexión: {e}")
        continue

    if not obs:
        print("   ⚠️ Sin observaciones")
        continue

    labels, values = [], []

    for o in obs:
        v = parse_value(sensor_id, descripcion, o.get("value"))
        if v is None:
            continue
        labels.append(parse_timestamp(o["timestamp"]))
        values.append(v)

    labels.reverse()
    values.reverse()

    data = {
        "sensor_id": sensor_id,
        "descripcion": descripcion,
        "unidad": unidad,
        "tipo_dato": "instantaneo" if limit == 1 else "consumo_intervalo",
        "labels": labels,
        "values": values
    }

    with open(f"{DATA_FOLDER}/{sensor_id}.json", "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    cache[sensor_id] = data

    indice[sensor_id] = {
        "descripcion": descripcion,
        "unidad": unidad,
        "archivo": f"{sensor_id}.json"
    }

    print(f"   ✅ OK ({len(values)} puntos)")

# ==================================================
# ÍNDICE
# ==================================================
with open(INDEX_JSON, "w", encoding="utf-8") as f:
    json.dump({
        "generado": datetime.now().isoformat(),
        "sensores": indice
    }, f, indent=2, ensure_ascii=False)

print("\n🚀 HEADER actualizado correctamente")
