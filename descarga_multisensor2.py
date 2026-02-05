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

LIMIT = 250  # suficiente (2 días aprox)

os.makedirs(DATA_FOLDER, exist_ok=True)

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
    Reglas energía:
    - sensor_id empieza por 0190_MV_
    - o descripcion contiene "energia"/"energy"
    - o sensor_id contiene _MV_ y FVENERGIA
    """
    sid = str(sensor_id).strip().upper()
    desc = normalizar(descripcion)

    if sid.startswith("0190_MV_"):
        return True
    if "energia" in desc or "energy" in desc:
        return True
    if "_MV_" in sid and "FVENERGIA" in sid:
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


def ts_to_minute_iso(ts_iso: str) -> str:
    """
    Normaliza timestamps a minuto (segundos=00) para poder cruzar sensores aunque
    vengan con segundos distintos.
    """
    try:
        dt = datetime.fromisoformat(ts_iso)
        dt = dt.replace(second=0, microsecond=0)
        return dt.isoformat()
    except:
        return ts_iso


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


def get_token_from_env(token_env: str) -> str:
    """
    token_env puede venir como:
    - SENTILO_TOKEN
    - SENTILO_TOKEN_FV
    - vacío/nan
    """
    if token_env is None:
        return ""

    token_env = str(token_env).strip()
    if not token_env or token_env.lower() == "nan":
        return ""

    return os.getenv(token_env, "").strip()


def download_sensor_observations(provider_id: str, sensor_id: str, token: str):
    """
    Descarga observaciones Sentilo y devuelve (labels_iso, values)
    """
    if not token:
        raise RuntimeError("Token vacío")

    headers = {"IDENTITY_KEY": token, "Accept": "application/json"}

    url = f"{SENTILO_URL}/{provider_id}/{sensor_id}"
    params = {"limit": LIMIT, "order": "desc"}

    r = requests.get(url, headers=headers, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()

    observations = data.get("observations", [])
    return observations


def observations_to_series(sensor_id: str, descripcion: str, observations):
    """
    Convierte lista de observations -> labels + values (ASC)
    """
    labels = []
    values = []

    for obs in observations:
        ts = obs.get("timestamp")
        raw = obs.get("value")

        if not ts or not raw:
            continue

        ts_iso = parse_timestamp(ts)
        value = parse_value(sensor_id, descripcion, raw)
        if value is None:
            continue

        labels.append(ts_iso)
        values.append(float(value))

    # vienen DESC -> pasamos a ASC
    labels.reverse()
    values.reverse()

    return labels, values


def save_sensor_json(sensor_id, descripcion, unidad, tipo_dato, labels, values):
    sensor_json = {
        "sensor_id": sensor_id,
        "descripcion": descripcion,
        "unidad": unidad,
        "tipo_dato": tipo_dato,
        "labels": labels,
        "values": values
    }

    filename = f"{sensor_id}.json"
    with open(os.path.join(DATA_FOLDER, filename), "w", encoding="utf-8") as f:
        json.dump(sensor_json, f, indent=2, ensure_ascii=False)

    return filename


# ==================================================
# CARGA EXCEL
# ==================================================
df = pd.read_excel(EXCEL_FILE)
df.columns = [c.strip().lower() for c in df.columns]

if "sensor_id" not in df.columns:
    raise ValueError(f"❌ Falta columna 'sensor_id' en el Excel. Columnas: {list(df.columns)}")

# columnas opcionales
col_desc = "descripcion" if "descripcion" in df.columns else None
col_unit = "unitat de mesura" if "unitat de mesura" in df.columns else ("unidad" if "unidad" in df.columns else None)
col_type = "tipus de dada" if "tipus de dada" in df.columns else ("tipo_dato" if "tipo_dato" in df.columns else None)
col_provider = "provider_id" if "provider_id" in df.columns else None
col_tokenenv = "token_env" if "token_env" in df.columns else None

# ==================================================
# DESCARGA DE SENSORES
# ==================================================
indice_sensores = {}

# guardamos series descargadas para poder calcular sensores ficticios
series_cache = {}  # sensor_id -> {"labels":[], "values":[], "descripcion":..., "unidad":..., "tipo_dato":...}

# 1) Descargamos todos los sensores reales
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

    provider_id = DEFAULT_PROVIDER_ID
    if col_provider:
        pv = str(row[col_provider]).strip()
        if pv and pv.lower() != "nan":
            provider_id = pv

    token_env = "SENTILO_TOKEN"
    if col_tokenenv:
        te = str(row[col_tokenenv]).strip()
        if te and te.lower() != "nan":
            token_env = te

    # sensores "calculados" (provider_id vacío + token_env vacío)
    is_calculado = False
    if col_provider and col_tokenenv:
        pv = str(row[col_provider]).strip()
        te = str(row[col_tokenenv]).strip()
        if (not pv or pv.lower() == "nan") and (not te or te.lower() == "nan"):
            is_calculado = True

    if is_calculado:
        print(f"\n📡 {sensor_id} – {descripcion} (CALCULADO)")
        # se calculará después
        continue

    token = get_token_from_env(token_env)
    if not token:
        print(f"\n📡 {sensor_id} – {descripcion}")
        print(f"   ❌ Token vacío. Revisa token_env='{token_env}' o GitHub Secrets.")
        continue

    print(f"\n📡 {sensor_id} – {descripcion}")

    try:
        observations = download_sensor_observations(provider_id, sensor_id, token)
    except Exception as e:
        print(f"   ❌ Error conexión: {e}")
        continue

    if not observations:
        print("   ⚠️ Sin observaciones")
        continue

    labels, values = observations_to_series(sensor_id, descripcion, observations)

    if not values:
        print("   ⚠️ Sin valores válidos")
        continue

    # guardamos JSON
    filename = save_sensor_json(
        sensor_id=sensor_id,
        descripcion=descripcion,
        unidad=unidad,
        tipo_dato=tipo_dato_por_sensor(sensor_id, descripcion),
        labels=labels,
        values=values
    )

    # guardamos índice
    indice_sensores[sensor_id] = {
        "descripcion": descripcion,
        "unidad": unidad,
        "tipo_dato": tipo_dato_por_sensor(sensor_id, descripcion),
        "archivo": filename
    }

    # guardamos cache para cálculos
    series_cache[sensor_id] = {
        "labels": labels,
        "values": values,
        "descripcion": descripcion,
        "unidad": unidad,
        "tipo_dato": tipo_dato_por_sensor(sensor_id, descripcion)
    }

    print(f"   ✅ OK ({len(values)} puntos)")


# ==================================================
# 2) CALCULAR SENSOR FICTICIO: 0190_MV_ENERGIA_CONS
#    consumida = importada + fv (si falta fv -> 0)
# ==================================================
CALC_SENSOR_ID = "0190_MV_ENERGIA_CONS"
IMPORT_ID = "0190_MV_C1_ASB_ACTIVEE"
FV_ID = "0524_MV_FVENERGIA"

if CALC_SENSOR_ID in df["sensor_id"].astype(str).values:
    # solo lo calculamos si está en el Excel

    if IMPORT_ID not in series_cache:
        print(f"\n📡 {CALC_SENSOR_ID} – Energia Total Consumida (CALCULADO)")
        print(f"   ❌ No se puede calcular: falta {IMPORT_ID} en descargas.")
    else:
        print(f"\n📡 {CALC_SENSOR_ID} – Energia Total Consumida (CALCULADO)")

        import_labels = series_cache[IMPORT_ID]["labels"]
        import_values = series_cache[IMPORT_ID]["values"]

        fv_map = {}
        if FV_ID in series_cache:
            fv_labels = series_cache[FV_ID]["labels"]
            fv_values = series_cache[FV_ID]["values"]

            # map FV por minuto
            for ts, v in zip(fv_labels, fv_values):
                fv_map[ts_to_minute_iso(ts)] = float(v)
        else:
            print(f"   ⚠️ Aviso: no existe {FV_ID}. Se asumirá FV=0 en todas las lecturas.")

        calc_labels = []
        calc_values = []

        for ts, imp in zip(import_labels, import_values):
            key = ts_to_minute_iso(ts)
            fv = fv_map.get(key, 0.0)
            calc_labels.append(ts)
            calc_values.append(float(imp) + float(fv))

        filename = save_sensor_json(
            sensor_id=CALC_SENSOR_ID,
            descripcion="Energia Total Consumida",
            unidad="kWh",
            tipo_dato="consumo_intervalo",
            labels=calc_labels,
            values=calc_values
        )

        indice_sensores[CALC_SENSOR_ID] = {
            "descripcion": "Energia Total Consumida",
            "unidad": "kWh",
            "tipo_dato": "consumo_intervalo",
            "archivo": filename
        }

        print(f"   ✅ OK ({len(calc_values)} puntos) (base={IMPORT_ID})")


# ==================================================
# ÍNDICE PARA DASHBOARD
# ==================================================
indice = {
    "generado": datetime.now().isoformat(),
    "provider": DEFAULT_PROVIDER_ID,
    "sensores": indice_sensores
}

with open(INDEX_JSON, "w", encoding="utf-8") as f:
    json.dump(indice, f, indent=2, ensure_ascii=False)

print("\n✅ DESCARGA COMPLETADA")
print(f"📁 Sensores válidos: {len(indice_sensores)}")
