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
PROVIDER_DEFAULT = "SIGE_PR_0190"

EXCEL_FILE = "Relación sensores AVINYÓ.xls"
DATA_FOLDER = "datos_sensores"
INDEX_JSON = "indice_sensores.json"

LIMIT_ENERGIA = 120  # histórico del día

os.makedirs(DATA_FOLDER, exist_ok=True)

TOKEN_DEFAULT = os.getenv("SENTILO_TOKEN", "").strip()
TOKEN_FV = os.getenv("SENTILO_TOKEN_FV", "").strip()

if not TOKEN_DEFAULT:
    raise RuntimeError("❌ SENTILO_TOKEN no definido")

print("=" * 70)
print(" DESCARGA SENSORES LIVE (HEADER) ")
print("=" * 70)

# ==================================================
# FUNCIONES AUXILIARES
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
        or sid.startswith("0524_MV_")
        or "energia" in desc
    )


def tipo_dato(sensor_id, descripcion):
    return "consumo_intervalo" if es_energia(sensor_id, descripcion) else "instantaneo"


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
            if "firstvalue" in summary and "lastvalue" in summary:
                return float(summary["lastvalue"]) - float(summary["firstvalue"])
            return None

        return float(summary.get("avg"))

    except:
        return None


def get_headers(token):
    return {
        "IDENTITY_KEY": token,
        "Accept": "application/json"
    }


# ==================================================
# CARGA EXCEL
# ==================================================

df = pd.read_excel(EXCEL_FILE)
df.columns = [c.strip().lower() for c in df.columns]

if "sensor_id" not in df.columns:
    raise ValueError("❌ Falta columna sensor_id")

col_desc = "descripcion" if "descripcion" in df.columns else None
col_provider = "provider_id" if "provider_id" in df.columns else None
col_token = "token_env" if "token_env" in df.columns else None

indice = {}

# ==================================================
# DESCARGA
# ==================================================

for _, row in df.iterrows():

    sensor_id = str(row["sensor_id"]).strip()
    if not sensor_id or sensor_id.lower() == "nan":
        continue

    descripcion = str(row[col_desc]).strip() if col_desc else sensor_id

    provider = str(row[col_provider]).strip() if col_provider else PROVIDER_DEFAULT
    token_env = str(row[col_token]).strip() if col_token else "SENTILO_TOKEN"

    token = TOKEN_DEFAULT if token_env != "SENTILO_TOKEN_FV" else TOKEN_FV

    if not token:
        print(f"⚠️ Token vacío para {sensor_id}")
        continue

    energia = es_energia(sensor_id, descripcion)
    limit = LIMIT_ENERGIA if energia else 1

    print(f"\n📡 {sensor_id} ({'ENERGIA' if energia else 'LIVE'})")

    url = f"{SENTILO_URL}/{provider}/{sensor_id}"

    try:
        r = requests.get(
            url,
            headers=get_headers(token),
            params={"limit": limit, "order": "desc"},
            timeout=30
        )
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        print(f"❌ Error conexión: {e}")
        continue

    observations = data.get("observations", [])
    if not observations:
        print("⚠️ Sin observaciones")
        continue

    labels = []
    values = []

    for obs in observations:
        ts = obs.get("timestamp")
        raw = obs.get("value")

        if not ts or not raw:
            continue

        v = parse_value(sensor_id, descripcion, raw)
        if v is None:
            continue

        labels.append(parse_timestamp(ts))
        values.append(v)

    if not values:
        print("⚠️ Sin valores válidos")
        continue

    # Solo invertimos si hay histórico
    if energia:
        labels.reverse()
        values.reverse()

    out = {
        "sensor_id": sensor_id,
        "descripcion": descripcion,
        "tipo_dato": tipo_dato(sensor_id, descripcion),
        "labels": labels,
        "values": values
    }

    filename = f"{sensor_id}.json"

    with open(os.path.join(DATA_FOLDER, filename), "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2, ensure_ascii=False)

    indice[sensor_id] = {
        "descripcion": descripcion,
        "archivo": filename
    }

    print(f"✅ OK ({len(values)} puntos)")

# ==================================================
# ÍNDICE
# ==================================================

with open(INDEX_JSON, "w", encoding="utf-8") as f:
    json.dump({
        "generado": datetime.now().isoformat(),
        "sensores": indice
    }, f, indent=2, ensure_ascii=False)

print("\n🚀 DESCARGA LIVE COMPLETADA")
