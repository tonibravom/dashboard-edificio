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

# Provider por defecto (sensores normales edificio)
PROVIDER_ID_DEFAULT = "SIGE_PR_0190"

# Provider FV (instalación fotovoltaica)
PROVIDER_ID_FV = "ARKENOVA_0524"

EXCEL_FILE = "Relación sensores AVINYÓ.xls"
DATA_FOLDER = "datos_sensores"
INDEX_JSON = "indice_sensores.json"

LIMIT = 250  # suficiente (192 lecturas aprox)

os.makedirs(DATA_FOLDER, exist_ok=True)

# Tokens desde GitHub Secrets
TOKEN_DEFAULT = os.getenv("SENTILO_TOKEN", "").strip()
TOKEN_FV = os.getenv("SENTILO_TOKEN_FV", "").strip()

if not TOKEN_DEFAULT:
    raise RuntimeError("❌ SENTILO_TOKEN no está definido en GitHub Secrets.")

if not TOKEN_FV:
    print("⚠️ AVISO: SENTILO_TOKEN_FV no está definido. Los sensores FV fallarán si existen en el Excel.")

HEADERS_DEFAULT = {
    "IDENTITY_KEY": TOKEN_DEFAULT,
    "Accept": "application/json",
    "User-Agent": "dashboard-edificio/1.0"
}

HEADERS_FV = {
    "IDENTITY_KEY": TOKEN_FV,
    "Accept": "application/json",
    "User-Agent": "dashboard-edificio/1.0"
}

# Sensores FV concretos
FV_SENSORS = {
    "0524_HV_IRRAD",
    "0524_HV_TEMP_EXT",
    "0524_MV_FVENERGIA"
}

print("=" * 70)
print(" DESCARGA SENSORES SENTILO → DASHBOARD HTML (DEFAULT + FV) ")
print("=" * 70)

# ==================================================
# FUNCIONES AUXILIARES
# ==================================================

def normalizar(texto):
    texto = str(texto).lower()
    texto = unicodedata.normalize("NFD", texto)
    return "".join(c for c in texto if unicodedata.category(c) != "Mn")


def es_sensor_fv(sensor_id: str) -> bool:
    sid = str(sensor_id).strip().upper()
    return sid in FV_SENSORS


def es_energia(sensor_id: str, descripcion: str) -> bool:
    """
    Reglas:
    - sensor_id empieza por 0190_MV_
    - o descripcion contiene "energia"
    - o sensor_id == 0525_MV_FVENERGIA (FV energía producida)
    """
    sid = str(sensor_id).strip().upper()
    desc = normalizar(descripcion)

    if sid.startswith("0190_MV_"):
        return True

    if sid == "0524_MV_FVENERGIA":
        return True

    if "energia" in desc or "energy" in desc:
        return True

    return False


def tipo_dato_por_sensor(sensor_id: str, descripcion: str) -> str:
    # metadata para el dashboard
    return "consumo_intervalo" if es_energia(sensor_id, descripcion) else "instantaneo"


def parse_timestamp(ts):
    """
    Sentilo devuelve timestamps tipo: 13/08/2025T07:45:01
    """
    try:
        return datetime.strptime(ts, "%d/%m/%YT%H:%M:%S").isoformat()
    except:
        return ts


def parse_value(sensor_id: str, descripcion: str, value_raw: str):
    """
    - Energía -> lastvalue - firstvalue
    - Resto -> avg
    """
    try:
        data = json.loads(value_raw)
        summary = data.get("summary", {})

        if es_energia(sensor_id, descripcion):
            if "firstvalue" in summary and "lastvalue" in summary:
                return float(summary["lastvalue"]) - float(summary["firstvalue"])
            return None

        # no energía
        if "avg" in summary:
            return float(summary["avg"])

    except Exception:
        pass

    return None


def sentilo_request(provider_id: str, sensor_id: str, headers: dict):
    url = f"{SENTILO_URL}/{provider_id}/{sensor_id}"
    params = {
        "limit": LIMIT,
        "order": "desc"
    }

    r = requests.get(url, headers=headers, params=params, timeout=30)
    r.raise_for_status()
    return r.json()


# ==================================================
# CARGA EXCEL (nuevo formato)
# ==================================================
df = pd.read_excel(EXCEL_FILE)
df.columns = [c.strip().lower() for c in df.columns]

# columnas mínimas obligatorias
if "sensor_id" not in df.columns:
    raise ValueError(f"❌ Falta columna 'sensor_id' en el Excel. Columnas: {list(df.columns)}")

# columnas opcionales
col_desc = "descripcion" if "descripcion" in df.columns else None
col_unit = "unitat de mesura" if "unitat de mesura" in df.columns else ("unidad" if "unidad" in df.columns else None)
col_type = "tipus de dada" if "tipus de dada" in df.columns else ("tipo_dato" if "tipo_dato" in df.columns else None)

# ==================================================
# DESCARGA DE SENSORES
# ==================================================
indice_sensores = {}

for _, row in df.iterrows():

    sensor_id = str(row["sensor_id"]).strip()
    if not sensor_id or sensor_id.lower() == "nan":
        continue

    descripcion = str(row[col_desc]).strip() if col_desc else sensor_id
    unidad = str(row[col_unit]).strip() if col_unit else ""
    tipo_excel = str(row[col_type]).strip().upper() if col_type else "JSON"

    # si existe la columna tipo y NO es JSON, saltamos
    if col_type and tipo_excel != "JSON":
        continue

    # Decide provider + token según sensor
    if es_sensor_fv(sensor_id):
        provider_id = PROVIDER_ID_FV
        headers = HEADERS_FV
        origen = "FV"
    else:
        provider_id = PROVIDER_ID_DEFAULT
        headers = HEADERS_DEFAULT
        origen = "DEFAULT"

    print(f"\n📡 {sensor_id} – {descripcion}  [{origen}]")

    try:
        data = sentilo_request(provider_id, sensor_id, headers)
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

        value = parse_value(sensor_id, descripcion, raw)
        if value is None:
            continue

        labels.append(parse_timestamp(ts))
        values.append(float(value))

    if not values:
        print("   ⚠️ Sin valores válidos")
        continue

    # Sentilo viene DESC -> lo invertimos para el dashboard (ASC)
    labels.reverse()
    values.reverse()

    sensor_json = {
        "sensor_id": sensor_id,
        "descripcion": descripcion,
        "unidad": unidad,
        "tipo_dato": tipo_dato_por_sensor(sensor_id, descripcion),
        "labels": labels,
        "values": values
    }

    filename = f"{sensor_id}.json"

    with open(os.path.join(DATA_FOLDER, filename), "w", encoding="utf-8") as f:
        json.dump(sensor_json, f, indent=2, ensure_ascii=False)

    indice_sensores[sensor_id] = {
        "descripcion": descripcion,
        "unidad": unidad,
        "tipo_dato": tipo_dato_por_sensor(sensor_id, descripcion),
        "archivo": filename
    }

    print(f"   ✅ OK ({len(values)} puntos)")

# ==================================================
# ÍNDICE PARA DASHBOARD
# ==================================================
indice = {
    "generado": datetime.now().isoformat(),
    "provider_default": PROVIDER_ID_DEFAULT,
    "provider_fv": PROVIDER_ID_FV,
    "sensores": indice_sensores
}

with open(INDEX_JSON, "w", encoding="utf-8") as f:
    json.dump(indice, f, indent=2, ensure_ascii=False)

print("\n✅ DESCARGA COMPLETADA")
print(f"📁 Sensores válidos: {len(indice_sensores)}")

