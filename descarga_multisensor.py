import os
import json
import time
import requests
import pandas as pd
from datetime import datetime, timezone

# ==========================================================
# CONFIG
# ==========================================================
PROVIDER = "SIGE_PR_0190"
EXCEL_PATH = "Relación sensores AVINYÓ.xls"
INDEX_JSON_PATH = "indice_sensores.json"
DATA_DIR = "datos_sensores"

# Sentilo (endpoint correcto)
SENTILO_BASE_URL = "http://connectaapi.bcn.cat/data"
SENTILO_TOKEN = os.getenv("SENTILO_TOKEN", "").strip()


# Rango de descarga (ejemplo: últimos 2 días)
DAYS_BACK = 2

# ==========================================================
# UTILIDADES
# ==========================================================
def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)

def now_iso():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()

def read_excel_sensors(excel_path: str):
    """
    Lee el Excel y devuelve lista de sensores con:
    sensor_id, descripcion, unidad, tipo_dato
    """
    df = pd.read_excel(excel_path)

    # Normaliza nombres de columnas
    df.columns = [c.strip().lower() for c in df.columns]

    # ⚠️ AJUSTE AUTOMÁTICO: intentamos detectar columnas típicas
    # Si tu excel usa otros nombres exactos, dímelo y lo adapto.
    possible_id = ["sensor_id", "sensor", "id", "sensorid"]
    possible_desc = ["descripcion", "descripción", "description", "desc"]
    possible_unit = ["unidad", "unit", "units"]
    possible_type = ["tipo_dato", "tipo", "type"]

    def pick_col(possibles):
        for c in possibles:
            if c in df.columns:
                return c
        return None

    col_id = pick_col(possible_id)
    col_desc = pick_col(possible_desc)
    col_unit = pick_col(possible_unit)
    col_type = pick_col(possible_type)

    if not col_id:
        raise ValueError(f"No encuentro columna de ID sensor en el Excel. Columnas: {list(df.columns)}")

    # si faltan, ponemos valores por defecto
    if not col_desc:
        df["descripcion"] = df[col_id].astype(str)
        col_desc = "descripcion"

    if not col_unit:
        df["unidad"] = ""
        col_unit = "unidad"

    if not col_type:
        df["tipo_dato"] = "instantaneo"
        col_type = "tipo_dato"

    sensores = []
    for _, row in df.iterrows():
        sensor_id = str(row[col_id]).strip()
        if not sensor_id or sensor_id.lower() == "nan":
            continue

        sensores.append({
            "sensor_id": sensor_id,
            "descripcion": str(row[col_desc]).strip(),
            "unidad": str(row[col_unit]).strip(),
            "tipo_dato": str(row[col_type]).strip(),
        })

    return sensores

def generar_indice_sensores(excel_path=EXCEL_PATH, output_json=INDEX_JSON_PATH):
    sensores_excel = read_excel_sensors(excel_path)

    sensores = {}
    for s in sensores_excel:
        sid = s["sensor_id"]
        sensores[sid] = {
            "descripcion": s["descripcion"],
            "unidad": s["unidad"],
            "tipo_dato": s["tipo_dato"],
            "archivo": f"{sid}.json"
        }

    indice = {
        "generado": datetime.now().isoformat(),
        "provider": PROVIDER,
        "sensores": sensores
    }

    with open(output_json, "w", encoding="utf-8") as f:
        json.dump(indice, f, ensure_ascii=False, indent=2)

    print(f"✅ indice_sensores.json actualizado con {len(sensores)} sensores")

def sentilo_get_observations(sensor_id: str, from_ts: str = None, limit: int = 2000):
    """
    Descarga observaciones de un sensor Sentilo.

    OJO: según tu API real, quizá el endpoint exacto sea distinto.
    Si ya te funciona tu script actual, esta función la adaptamos a tu endpoint real.
    """
    if not SENTILO_TOKEN:
        raise RuntimeError("❌ SENTILO_TOKEN no está definido en variables de entorno.")

    headers = {
        "IDENTITY_KEY": SENTILO_TOKEN,
        "Accept": "application/json"
    }

    # Endpoint típico de Sentilo:
    # /data/{provider}/{sensor}
    url = f"{SENTILO_BASE_URL}/data/{PROVIDER}/{sensor_id}"

    params = {}
    if limit:
        params["limit"] = str(limit)
    if from_ts:
        params["from"] = from_ts

    r = requests.get(url, headers=headers, params=params, timeout=60)
    if r.status_code != 200:
        raise RuntimeError(f"❌ Error Sentilo {r.status_code} sensor={sensor_id}: {r.text[:200]}")

    return r.json()

def convertir_a_formato_dashboard(sensor_id: str, meta: dict, sentilo_json: dict):
    """
    Convierte respuesta Sentilo a tu formato JSON:
    {
      sensor_id, descripcion, unidad, tipo_dato,
      labels: [...], values: [...]
    }
    """
    labels = []
    values = []

    # Sentilo suele devolver:
    # { "observations": [ { "timestamp": "...", "value": "..." }, ... ] }
    obs = sentilo_json.get("observations", [])

    for o in obs:
        ts = o.get("timestamp")
        v = o.get("value")

        if ts is None or v is None:
            continue

        try:
            v = float(v)
        except:
            continue

        labels.append(ts)
        values.append(v)

    # Orden por timestamp por seguridad
    combined = sorted(zip(labels, values), key=lambda x: x[0])
    labels = [x[0] for x in combined]
    values = [x[1] for x in combined]

    return {
        "sensor_id": sensor_id,
        "descripcion": meta.get("descripcion", sensor_id),
        "unidad": meta.get("unidad", ""),
        "tipo_dato": meta.get("tipo_dato", "instantaneo"),
        "labels": labels,
        "values": values
    }

def main():
    ensure_dir(DATA_DIR)

    # 1) regenerar indice desde Excel
    generar_indice_sensores(EXCEL_PATH, INDEX_JSON_PATH)

    # 2) cargar índice
    with open(INDEX_JSON_PATH, "r", encoding="utf-8") as f:
        indice = json.load(f)

    sensores = indice.get("sensores", {})
    if not sensores:
        raise RuntimeError("❌ No hay sensores en indice_sensores.json")

    # 3) descargar sensores
    # from = hace DAYS_BACK días
    from_dt = datetime.now(timezone.utc) - pd.Timedelta(days=DAYS_BACK)
    from_ts = from_dt.replace(microsecond=0).isoformat()

    print(f"📥 Descargando datos desde: {from_ts}")
    print(f"📌 Total sensores: {len(sensores)}")

    for i, (sensor_id, meta) in enumerate(sensores.items(), start=1):
        try:
            print(f"[{i}/{len(sensores)}] {sensor_id} ...")

            sentilo_json = sentilo_get_observations(sensor_id, from_ts=from_ts, limit=4000)

            out = convertir_a_formato_dashboard(sensor_id, meta, sentilo_json)

            out_path = os.path.join(DATA_DIR, meta["archivo"])
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(out, f, ensure_ascii=False, indent=2)

            time.sleep(0.2)  # pequeña pausa para no saturar

        except Exception as e:
            print(f"⚠️ Error con {sensor_id}: {e}")

    print("✅ Descarga completada.")

if __name__ == "__main__":
    main()

