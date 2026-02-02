import os
import json
import time
import requests
import pandas as pd
import unicodedata
from datetime import datetime

# ==================================================
# CONFIGURACIÓN
# ==================================================
SENTILO_URL = "http://connectaapi.bcn.cat/data"

EXCEL_FILE = "Relación sensores AVINYÓ.xls"
DATA_FOLDER = "datos_sensores"
INDEX_JSON = "indice_sensores.json"

LIMIT = 250  # suficiente para ~2 días

os.makedirs(DATA_FOLDER, exist_ok=True)

# Tokens (GitHub Secrets)
TOKEN_DEFAULT = os.getenv("SENTILO_TOKEN", "").strip()
TOKEN_FV = os.getenv("SENTILO_TOKEN_FV", "").strip()

if not TOKEN_DEFAULT:
    raise RuntimeError("❌ SENTILO_TOKEN no está definido en GitHub Secrets.")

print("=" * 70)
print(" DESCARGA SENSORES SENTILO → DASHBOARD HTML ")
print("=" * 70)

# ==================================================
# FUNCIONES AUXILIARES
# ==================================================
def normalizar(texto):
    texto = str(texto).lower()
    texto = unicodedata.normalize("NFD", texto)
    return "".join(c for c in texto if unicodedata.category(c) != "Mn")


def es_energia(sensor_id: str, descripcion: str) -> bool:
    """
    Reglas:
    - sensor_id empieza por 0190_MV_  (contadores)
    - o descripcion contiene energia/energy
    - o sensor FV energia producida (0524_MV_FVENERGIA) también es contador
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


def get_headers_for_token(token: str):
    return {
        "IDENTITY_KEY": token,
        "Accept": "application/json"
    }


def fetch_sensor_observations(provider_id: str, sensor_id: str, token: str):
    url = f"{SENTILO_URL}/{provider_id}/{sensor_id}"
    params = {
        "limit": LIMIT,
        "order": "desc"
    }

    r = requests.get(url, headers=get_headers_for_token(token), params=params, timeout=30)
    if r.status_code != 200:
        raise RuntimeError(f"HTTP {r.status_code}: {r.text[:250]}")

    data = r.json()
    return data.get("observations", [])


def build_sensor_json(sensor_id: str, descripcion: str, unidad: str, observations: list):
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

    # Sentilo viene DESC -> invertimos a ASC
    labels.reverse()
    values.reverse()

    return {
        "sensor_id": sensor_id,
        "descripcion": descripcion,
        "unidad": unidad,
        "tipo_dato": tipo_dato_por_sensor(sensor_id, descripcion),
        "labels": labels,
        "values": values
    }


def to_map(labels, values):
    """
    Convierte arrays en dict timestamp -> value
    """
    m = {}
    for t, v in zip(labels, values):
        m[t] = v
    return m


def calcular_energia_total_consumida(importada_json, exportada_json, fv_json):
    """
    CONS = IMPORTADA + FV - EXPORTADA
    Se calcula SOLO donde existan timestamps comunes.
    """
    imp = to_map(importada_json["labels"], importada_json["values"])
    exp = to_map(exportada_json["labels"], exportada_json["values"])
    fv  = to_map(fv_json["labels"], fv_json["values"])

    comunes = sorted(set(imp.keys()) & set(exp.keys()) & set(fv.keys()))
    labels = []
    values = []

    for t in comunes:
        cons = imp[t] + fv[t] - exp[t]
        labels.append(t)
        values.append(float(cons))

    return labels, values


# ==================================================
# CARGA EXCEL
# ==================================================
df = pd.read_excel(EXCEL_FILE)
df.columns = [c.strip().lower() for c in df.columns]

if "sensor_id" not in df.columns:
    raise ValueError(f"❌ Falta columna 'sensor_id' en el Excel. Columnas: {list(df.columns)}")

# Columnas opcionales
col_desc = "descripcion" if "descripcion" in df.columns else None
col_unit = "unitat de mesura" if "unitat de mesura" in df.columns else ("unidad" if "unidad" in df.columns else None)
col_type = "tipus de dada" if "tipus de dada" in df.columns else ("tipo_dato" if "tipo_dato" in df.columns else None)

# Nuevas columnas para multi-provider/token
col_provider = "provider_id" if "provider_id" in df.columns else None
col_tokenenv = "token_env" if "token_env" in df.columns else None

# Provider por defecto (si no viene en Excel)
DEFAULT_PROVIDER_ID = "SIGE_PR_0190"

# ==================================================
# DESCARGA / CÁLCULO DE SENSORES
# ==================================================
indice_sensores = {}
cache_json = {}  # para reutilizar sensores descargados en cálculos

def guardar_sensor(sensor_json, descripcion, unidad):
    sensor_id = sensor_json["sensor_id"]
    filename = f"{sensor_id}.json"
    out_path = os.path.join(DATA_FOLDER, filename)

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(sensor_json, f, indent=2, ensure_ascii=False)

    indice_sensores[sensor_id] = {
        "descripcion": descripcion,
        "unidad": unidad,
        "tipo_dato": sensor_json["tipo_dato"],
        "archivo": filename
    }

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

    provider_id = str(row[col_provider]).strip() if col_provider else ""
    token_env = str(row[col_tokenenv]).strip() if col_tokenenv else ""

    # ----------------------------
    # SENSOR CALCULADO (ficticio)
    # ----------------------------
    if provider_id == "" and token_env == "":
        # Caso: sensor ficticio
        # Por ahora solo soportamos el 0190_MV_ENERGIA_CONS
        if sensor_id.upper() != "0190_MV_ENERGIA_CONS":
            print(f"\n🧮 {sensor_id} – {descripcion}")
            print("   ⚠️ Sensor calculado desconocido (no implementado). Saltando.")
            continue

        print(f"\n🧮 {sensor_id} – {descripcion} (CALCULADO)")

        # IDs base (fijos)
        SENSOR_IMPORTADA = "0190_MV_C1_ASB_ACTIVEE"
        SENSOR_EXPORTADA = "0190_MV_CIA_EXPORT"
        SENSOR_FV = "0524_MV_FVENERGIA"

        # Aseguramos que estén en cache (si no, los descargamos)
        def ensure_in_cache(sid):
            if sid in cache_json:
                return cache_json[sid]

            # Detectamos provider/token según sensor
            if sid == SENSOR_FV:
                provider = "ARKENOVA_0524"
                token = TOKEN_FV
                if not token:
                    raise RuntimeError("❌ Falta SENTILO_TOKEN_FV para leer sensores FV.")
            else:
                provider = DEFAULT_PROVIDER_ID
                token = TOKEN_DEFAULT

            print(f"   ↳ descargando base: {sid} ({provider})")
            obs = fetch_sensor_observations(provider, sid, token)
            sj = build_sensor_json(sid, sid, "kWh", obs)
            cache_json[sid] = sj
            return sj

        try:
            imp_json = ensure_in_cache(SENSOR_IMPORTADA)
            exp_json = ensure_in_cache(SENSOR_EXPORTADA)
            fv_json  = ensure_in_cache(SENSOR_FV)

            labels, values = calcular_energia_total_consumida(imp_json, exp_json, fv_json)

            if not values:
                print("   ⚠️ No se han podido calcular puntos (no hay timestamps comunes).")
                continue

            sensor_json = {
                "sensor_id": sensor_id,
                "descripcion": descripcion,
                "unidad": unidad if unidad else "kWh",
                "tipo_dato": "consumo_intervalo",
                "labels": labels,
                "values": values
            }

            guardar_sensor(sensor_json, descripcion, sensor_json["unidad"])
            cache_json[sensor_id] = sensor_json

            print(f"   ✅ OK ({len(values)} puntos calculados)")

        except Exception as e:
            print(f"   ❌ Error calculando {sensor_id}: {e}")

        continue

    # ----------------------------
    # SENSOR NORMAL (Sentilo)
    # ----------------------------
    if not provider_id:
        provider_id = DEFAULT_PROVIDER_ID

    token_to_use = TOKEN_DEFAULT
    if token_env:
        # token_env indica el nombre de la variable de entorno a usar
        token_to_use = os.getenv(token_env, "").strip()

    if not token_to_use:
        print(f"\n📡 {sensor_id} – {descripcion}")
        print(f"   ❌ Token vacío. Revisa token_env='{token_env}' o GitHub Secrets.")
        continue

    print(f"\n📡 {sensor_id} – {descripcion}")

    try:
        observations = fetch_sensor_observations(provider_id, sensor_id, token_to_use)

        if not observations:
            print("   ⚠️ Sin observaciones")
            continue

        sensor_json = build_sensor_json(sensor_id, descripcion, unidad, observations)

        if not sensor_json["values"]:
            print("   ⚠️ Sin valores válidos")
            continue

        guardar_sensor(sensor_json, descripcion, unidad)
        cache_json[sensor_id] = sensor_json

        print(f"   ✅ OK ({len(sensor_json['values'])} puntos)")

        time.sleep(0.15)

    except Exception as e:
        print(f"   ❌ Error conexión: {e}")
        continue


# ==================================================
# ÍNDICE PARA DASHBOARD
# ==================================================
indice = {
    "generado": datetime.now().isoformat(),
    "sensores": indice_sensores
}

with open(INDEX_JSON, "w", encoding="utf-8") as f:
    json.dump(indice, f, indent=2, ensure_ascii=False)

print("\n✅ DESCARGA COMPLETADA")
print(f"📁 Sensores válidos: {len(indice_sensores)}")
