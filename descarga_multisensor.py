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


def minute_key(iso_ts: str) -> str:
    """
    Convierte un ISO timestamp a clave por minuto: YYYY-MM-DDTHH:MM
    """
    try:
        dt = datetime.fromisoformat(iso_ts)
        return dt.strftime("%Y-%m-%dT%H:%M")
    except:
        # fallback bruto si no parsea
        return str(iso_ts)[:16]


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


def to_minute_map(labels, values):
    """
    Convierte arrays en dict minute_key -> value
    Si hay varios puntos en el mismo minuto, nos quedamos con el último.
    """
    m = {}
    for t, v in zip(labels, values):
        k = minute_key(t)
        m[k] = v
    return m


def calcular_energia_total_consumida(importada_json, exportada_json, fv_json):
    """
    CONS = IMPORTADA + FV - EXPORTADA
    Cruce por MINUTO (no por segundo)
    """
    imp = to_minute_map(importada_json["labels"], importada_json["values"])
    exp = to_minute_map(exportada_json["labels"], exportada_json["values"])
    fv  = to_minute_map(fv_json["labels"], fv_json["values"])

    comunes = sorted(set(imp.keys()) & set(exp.keys()) & set(fv.keys()))

    labels = []
    values = []

    for k in comunes:
        cons = imp[k] + fv[k] - exp[k]
        # reconstruimos label ISO "bonito"
        labels.append(k + ":00")
        values.append(float(cons))

    return labels, values


def clean_cell(value) -> str:
    """
    Convierte NaN / None / 'nan' / 'None' a string vacío.
    """
    if value is None:
        return ""
    if isinstance(value, float) and pd.isna(value):
        return ""
    s = str(value).strip()
    if s.lower() in ["nan", "none", "null"]:
        return ""
    return s


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

    sensor_id = clean_cell(row.get("sensor_id", ""))
    if not sensor_id:
        continue

    descripcion = clean_cell(row.get(col_desc, sensor_id)) if col_desc else sensor_id
    unidad = clean_cell(row.get(col_unit, "")) if col_unit else ""
    tipo_excel = clean_cell(row.get(col_type, "JSON")).upper() if col_type else "JSON"

    # si existe la columna tipo y NO es JSON, saltamos
    if col_type and tipo_excel != "JSON":
        continue

    provider_id = clean_cell(row.get(col_provider, "")) if col_provider else ""
    token_env = clean_cell(row.get(col_tokenenv, "")) if col_tokenenv else ""

    # ----------------------------
    # SENSOR CALCULADO (ficticio)
    # ----------------------------
    if provider_id == "" and token_env == "":
        if sensor_id.upper() != "0190_MV_ENERGIA_CONS":
            print(f"\n🧮 {sensor_id} – {descripcion}")
            print("   ⚠️ Sensor calculado desconocido (no implementado). Saltando.")
            continue

        print(f"\n🧮 {sensor_id} – {descripcion} (CALCULADO)")

        SENSOR_IMPORTADA = "0190_MV_C1_ASB_ACTIVEE"
        SENSOR_EXPORTADA = "0190_MV_CIA_EXPORT"
        SENSOR_FV = "0524_MV_FVENERGIA"

        def ensure_in_cache(sid):
            if sid in cache_json:
                return cache_json[sid]

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
                print("   ⚠️ No se han podido calcular puntos (no hay minutos comunes).")
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
