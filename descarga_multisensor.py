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
TOKEN = os.getenv("SENTILO_TOKEN")
PROVIDER_ID = "SIGE_PR_0190"

EXCEL_FILE = "Relación sensores AVINYÓ.xls"
DATA_FOLDER = "datos_sensores"

os.makedirs(DATA_FOLDER, exist_ok=True)

HEADERS = {
    "IDENTITY_KEY": TOKEN,
    "Accept": "application/json"
}

print("=" * 70)
print(" DESCARGA SENSORES SENTILO → DASHBOARD HTML ")
print("=" * 70)

# ==================================================
# FUNCIONES AUXILIARES
# ==================================================

def normalizar(texto):
    texto = texto.lower()
    texto = unicodedata.normalize("NFD", texto)
    return "".join(c for c in texto if unicodedata.category(c) != "Mn")


def es_energia(descripcion):
    """
    Detecta sensores de energía independientemente de idioma o tildes
    """
    t = normalizar(descripcion)
    return "energia" in t or "energy" in t


def tipo_dato(descripcion):
    return "consumo_intervalo" if es_energia(descripcion) else "instantaneo"


def parse_timestamp(ts):
    try:
        return datetime.strptime(ts, "%d/%m/%YT%H:%M:%S").isoformat()
    except:
        return ts


def parse_value(descripcion, value_raw):
    """
    REGLA DEFINITIVA:
    - Sensores de energía → lastvalue - firstvalue
    - Resto → avg
    """
    try:
        data = json.loads(value_raw)
        summary = data.get("summary", {})

        if es_energia(descripcion):
            if "firstvalue" in summary and "lastvalue" in summary:
                return float(summary["lastvalue"]) - float(summary["firstvalue"])
        else:
            if "avg" in summary:
                return float(summary["avg"])

    except Exception:
        pass

    return None


# ==================================================
# CARGA EXCEL
# ==================================================

df = pd.read_excel(EXCEL_FILE)

COL_SENSOR = "SENSOR / ACTUADOR"
COL_DESC   = "DESCRIPCIÓ"
COL_TYPE   = "TIPUS DE DADA"
COL_UNIT   = "UNITAT DE MESURA"

for col in [COL_SENSOR, COL_DESC, COL_TYPE, COL_UNIT]:
    if col not in df.columns:
        raise ValueError(f"❌ Falta columna '{col}' en el Excel")

# ==================================================
# DESCARGA DE SENSORES
# ==================================================

indice_sensores = {}

for _, row in df.iterrows():

    if str(row[COL_TYPE]).strip().upper() != "JSON":
        continue

    sensor_id   = str(row[COL_SENSOR]).strip()
    descripcion = str(row[COL_DESC]).strip()
    unidad      = str(row[COL_UNIT]).strip()

    print(f"\n📡 {sensor_id} – {descripcion}")

    url = f"{SENTILO_URL}/{PROVIDER_ID}/{sensor_id}"
    params = {"limit": 5000, "order": "desc"}

    try:
        r = requests.get(url, headers=HEADERS, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        print(f"   ❌ Error conexión: {e}")
        continue

    observations = data.get("observations", [])
    if not observations:
        print("   ⚠️ Sin observaciones")
        continue

    labels = []
    values = []

    for obs in observations:
        ts = obs.get("timestamp")
        raw = obs.get("value")

        if not ts or not raw:
            continue

        value = parse_value(descripcion, raw)
        if value is None:
            continue

        labels.append(parse_timestamp(ts))
        values.append(float(value))

    if not values:
        print("   ⚠️ Sin valores válidos")
        continue

    labels.reverse()
    values.reverse()

    sensor_json = {
        "sensor_id": sensor_id,
        "descripcion": descripcion,
        "unidad": unidad,
        "tipo_dato": tipo_dato(descripcion),
        "labels": labels,
        "values": values
    }

    filename = f"{sensor_id}.json"

    with open(os.path.join(DATA_FOLDER, filename), "w", encoding="utf-8") as f:
        json.dump(sensor_json, f, indent=2, ensure_ascii=False)

    indice_sensores[sensor_id] = {
        "descripcion": descripcion,
        "unidad": unidad,
        "tipo_dato": tipo_dato(descripcion),
        "archivo": filename
    }

    print(f"   ✅ OK ({len(values)} puntos)")

# ==================================================
# ÍNDICE PARA DASHBOARD
# ==================================================

indice = {
    "generado": datetime.now().isoformat(),
    "provider": PROVIDER_ID,
    "sensores": indice_sensores
}

with open("indice_sensores.json", "w", encoding="utf-8") as f:
    json.dump(indice, f, indent=2, ensure_ascii=False)

print("\n✅ DESCARGA COMPLETADA")
print(f"📁 Sensores válidos: {len(indice_sensores)}")

