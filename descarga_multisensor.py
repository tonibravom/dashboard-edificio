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
DEFAULT_PROVIDER_ID = "SIGE_PR_0190"

EXCEL_FILE = "Relación sensores AVINYÓ.xls"
DATA_FOLDER = "datos_sensores"
INDEX_JSON = "indice_sensores.json"

LIMIT = 120  # solo últimas horas (más rápido)

os.makedirs(DATA_FOLDER, exist_ok=True)

# ==================================================
# 🔥 SENSORES HEADER (SOLO ESTOS SE DESCARGAN)
# ==================================================
HEADER_SENSORS = {
    # Energía base
    "0190_MV_C1_ASB_ACTIVEE",
    "0524_MV_FVENERGIA",
    "0190_MV_CIA_EXPORT",

    # Climatización
    "0190_MV_C2_ASB_ACTIVEE",
    "0190_MV_C41_CGEM21_EACTIVA",

    # Plantas
    "0190_MV_C10_CGEM21_EACTIVA",
    "0190_MV_C20_CGEM21_EACTIVA",
    "0190_MV_C30_CGEM21_EACTIVA",
    "0190_MV_C40_CGEM21_EACTIVA",
    "0190_MV_C50_CGEM21_EACTIVA",

    # Ambiente plantas
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

    # Exterior
    "0524_HV_TEMP_EXT",
    "0524_HV_IRRAD",
}

CALC_SENSOR_ID = "0190_MV_ENERGIA_CONS"

print("=" * 70)
print(" DESCARGA SENSORES HEADER → DASHBOARD ")
print("=" * 70)

# ==================================================
# FUNCIONES
# ==================================================
def normalizar(texto):
    texto = str(texto).lower()
    texto = unicodedata.normalize("NFD", texto)
    return "".join(c for c in texto if unicodedata.category(c) != "Mn")


def es_energia(sensor_id, descripcion):
    sid = str(sensor_id).upper()
    desc = normalizar(descripcion)

    return (
        sid.startswith("0190_MV_")
        or "energia" in desc
        or "energy" in desc
        or "FVENERGIA" in sid
    )


def parse_timestamp(ts):
    try:
        return datetime.strptime(ts, "%d/%m/%YT%H:%M:%S").isoformat()
    except:
        return ts


def parse_value(sensor_id, descripcion, raw):
    try:
        data = json.loads(raw)
        summary = data.get("summary", {})

        if es_energia(sensor_id, descripcion):
            return float(summary["lastvalue"]) - float(summary["firstvalue"])

        return float(summary.get("avg"))

    except:
        return None


def get_token(env_name):
    return os.getenv(str(env_name).strip(), "").strip()


# ==================================================
# CARGA EXCEL
# ==================================================
df = pd.read_excel(EXCEL_FILE)
df.columns = [c.strip().lower() for c in df.columns]

col_desc = "descripcion"
col_unit = "unitat de mesura"
col_provider = "provider_id"
col_token = "token_env"

indice = {}
series = {}

# ==================================================
# DESCARGA SOLO HEADER
# ==================================================
for _, row in df.iterrows():

    sensor_id = str(row["sensor_id"]).strip()

    if sensor_id not in HEADER_SENSORS:
        continue

    descripcion = str(row[col_desc])
    unidad = str(row[col_unit])
    provider = row.get(col_provider, DEFAULT_PROVIDER_ID)
    token = get_token(row.get(col_token, "SENTILO_TOKEN"))

    print(f"\n📡 {sensor_id}")

    headers = {"IDENTITY_KEY": token, "Accept": "application/json"}
    url = f"{SENTILO_URL}/{provider}/{sensor_id}"

    try:
        r = requests.get(url, headers=headers,
                         params={"limit": LIMIT, "order": "desc"})
        r.raise_for_status()
        obs = r.json().get("observations", [])

    except Exception as e:
        print("❌", e)
        continue

    labels, values = [], []

    for o in obs:
        ts = parse_timestamp(o["timestamp"])
        v = parse_value(sensor_id, descripcion, o["value"])

        if v is not None:
            labels.append(ts)
            values.append(v)

    labels.reverse()
    values.reverse()

    filename = f"{sensor_id}.json"

    with open(os.path.join(DATA_FOLDER, filename), "w") as f:
        json.dump({
            "sensor_id": sensor_id,
            "descripcion": descripcion,
            "unidad": unidad,
            "tipo_dato": "consumo_intervalo" if es_energia(sensor_id, descripcion) else "instantaneo",
            "labels": labels,
            "values": values
        }, f)

    indice[sensor_id] = {
        "descripcion": descripcion,
        "unidad": unidad,
        "archivo": filename
    }

    series[sensor_id] = (labels, values)

    print(f"   ✅ {len(values)} puntos")

# ==================================================
# SENSOR CALCULADO
# ==================================================
if "0190_MV_C1_ASB_ACTIVEE" in series and "0524_MV_FVENERGIA" in series:

    imp_l, imp_v = series["0190_MV_C1_ASB_ACTIVEE"]
    fv_l, fv_v = series["0524_MV_FVENERGIA"]

    fv_map = {l: v for l, v in zip(fv_l, fv_v)}

    calc_v = [imp + fv_map.get(l, 0) for l, imp in zip(imp_l, imp_v)]

    filename = f"{CALC_SENSOR_ID}.json"

    with open(os.path.join(DATA_FOLDER, filename), "w") as f:
        json.dump({
            "sensor_id": CALC_SENSOR_ID,
            "descripcion": "Energia Total Consumida",
            "unidad": "kWh",
            "tipo_dato": "consumo_intervalo",
            "labels": imp_l,
            "values": calc_v
        }, f)

    indice[CALC_SENSOR_ID] = {
        "descripcion": "Energia Total Consumida",
        "unidad": "kWh",
        "archivo": filename
    }

# ==================================================
# ÍNDICE
# ==================================================
with open(INDEX_JSON, "w") as f:
    json.dump({
        "generado": datetime.now().isoformat(),
        "sensores": indice
    }, f, indent=2)

print("\n✅ COMPLETADO")
