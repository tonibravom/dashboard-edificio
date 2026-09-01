import os
import json
import requests
import pandas as pd
import unicodedata
from datetime import datetime

# ==================================================
# CONFIG
# ==================================================
SENTILO_URL = "http://connectaapi.bcn.cat/data"
DATA_FOLDER = "datos_sensores"
INDEX_JSON = "indice_sensores.json"

LIMIT_ENERGIA = 96
LIMIT_INSTANT = 1

os.makedirs(DATA_FOLDER, exist_ok=True)

TOKEN_STD = os.getenv("SENTILO_TOKEN", "").strip()


# ==================================================
# 🔥 SENSORES HEADER (LISTA BLANCA)
# ==================================================
HEADER_SENSORS = {

    # energía base
    "0190_MV_C1_ASB_ACTIVEE",
    "0190_MV_CIA_EXPORT",

    # potencia total i factor de potencia
    "0190_HV_C1_ASB_ACTIVEP",
    "0190_HV_C1_ASB_PFACTOR",

    # variables electricas
    "0190_MV_C1_ASB_REACTIVEE",	
    "0190_HV_C1_ASB_REACTIVEP",
    "0190_HV_C1_ASB_CURRL1",
    "0190_HV_C1_ASB_CURRL2",
    "0190_HV_C1_ASB_CURRL3",
    "0190_HV_C1_ASB_VOLTL1",
    "0190_HV_C1_ASB_VOLTL2",
    "0190_HV_C1_ASB_VOLTL3",

    # climatización
    "0190_MV_C2_ASB_ACTIVEE",
    "0190_MV_C41_CGEM21_EACTIVA",

    # plantas
    "0190_MV_C10_CGEM21_EACTIVA",
    "0190_MV_C20_CGEM21_EACTIVA",
    "0190_MV_C30_CGEM21_EACTIVA",
    "0190_MV_C40_CGEM21_EACTIVA",
    "0190_MV_C50_CGEM21_EACTIVA",

    # temperatura / humedad plantas
    "0190_HV_S1_STPRO_TEMP",
    "0190_HV_S1_STPRO_HUM",
    "0190_HV_S2_STPRO_TEMP",
    "0190_HV_S2_STPRO_HUM",
    "0190_HV_S3_STPRO_TEMP",
    "0190_HV_S3_STPRO_HUM",
    "0190_HV_S4_STPRO_TEMP",
    "0190_HV_S4_STPRO_HUM",
    "0190_HV_S5_STPRO_TEMP",
    "0190_HV_S5_STPRO_HUM",

    
}

# ==================================================
# UTILS
# ==================================================
def normalizar(txt):
    txt = str(txt).lower().strip()
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
# CARGA EXCEL + FILTRO HEADER
# ==================================================
df = pd.read_excel("Relación sensores AVINYÓ.xls")
df.columns = [c.strip().lower() for c in df.columns]

df = df[df["sensor_id"].isin(HEADER_SENSORS)]

# ==================================================
# DESCARGA
# ==================================================
indice = {}
cache = {}

for _, r in df.iterrows():

    sensor_id = str(r["sensor_id"]).strip()
    descripcion = str(r.get("descripcion", sensor_id))
    unidad = str(r.get("unitat de mesura", ""))

    provider = str(r.get("provider_id", "")).strip()

    print(f"\n📡 {sensor_id} – {descripcion}")

   


    # ==================================================
    # SENSOR REAL
    # ==================================================
    token = TOKEN_STD

    headers = {
        "IDENTITY_KEY": token,
        "Accept": "application/json"
    }

    limit = LIMIT_ENERGIA if es_energia(sensor_id, descripcion) else LIMIT_INSTANT

    url = f"{SENTILO_URL}/{provider}/{sensor_id}"

    try:
        r = requests.get(url, headers=headers,
                         params={"limit": limit, "order": "desc"},
                         timeout=20)
        r.raise_for_status()
        obs = r.json().get("observations", [])
    except Exception as e:
        print(f"   ❌ Error conexión: {e}")
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

