import os
import json
import time
import requests
import pandas as pd
from datetime import datetime, timezone, timedelta

# ==========================================================
# CONFIG
# ==========================================================
PROVIDER = "SIGE_PR_0190"
EXCEL_PATH = "Relación sensores AVINYÓ.xls"
INDEX_JSON_PATH = "indice_sensores.json"
DATA_DIR = "datos_sensores"

# Sentilo (endpoint correcto: ya incluye /data)
SENTILO_BASE_URL = "http://connectaapi.bcn.cat/data"
SENTILO_TOKEN = os.getenv("SENTILO_TOKEN", "").strip()

# Descargar últimos X días
DAYS_BACK = 2

# Máximo observaciones por sensor
LIMIT = 4000

# Pausa entre sensores
SLEEP_BETWEEN_SENSORS = 0.2


# ==========================================================
# UTILIDADES
# ==========================================================
def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)

def iso_utc_z(dt: datetime) -> str:
    """
    Devuelve timestamp ISO UTC con formato aceptado por Sentilo:
    YYYY-MM-DDTHH:MM:SSZ
    """
    dt = dt.astimezone(timezone.utc).replace(microsecond=0)
    return dt.isoformat().replace("+00:00", "Z")

def read_excel_sensors(excel_path: str):
    """
    Lee el Excel y devuelve lista de sensores con:
    sensor_id, descripcion, unidad, tipo_dato

    Requisito: debe existir columna EXACTA: sensor_id
    """
    df = pd.read_excel(excel_path)

    # Normaliza nombres de columnas
    df.columns = [str(c).strip().lower() for c in df.columns]

    # --- obligatorio ---
    if "sensor_id" not in df.columns:
        raise ValueError(
            f"❌ No encuentro la columna 'sensor_id' en el Excel.\n"
            f"Columnas detectadas: {list(df.columns)}\n\n"
            f"➡️ Solución: en el Excel crea una columna llamada EXACTAMENTE: sensor_id"
        )

    # opcionales
    if "descripcion" not in df.columns:
        df["descripcion"] = df["sensor_id"].astype(str)

    if "unidad" not in df.columns:
        df["unidad"] = ""

    if "tipo_dato" not in df.columns:
        df["tipo_dato"] = "instantaneo"

    sensores = []
    for _, row in df.iterrows():
        sensor_id = str(row["sensor_id"]).strip()

        if not sensor_id or sensor_id.lower() == "nan":
            continue

        sensores.append({
            "sensor_id": sensor_id,
            "descripcion": str(row["descripcion"]).strip(),
            "unidad": str(row["unidad"]).strip(),
            "tipo_dato": str(row["tipo_dato"]).strip(),
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

    Endpoint:
    http://connectaapi.bcn.cat/data/{PROVIDER}/{sensor_id}?limit=...&from=...
    """
    if not SENTILO_TOKEN:
        raise RuntimeError("❌ SENTILO_TOKEN no está definido en variables de entorno (GitHub Secret).")

    headers = {
        "IDENTITY_KEY": SENTILO_TOKEN,
        "Accept": "application/json"
    }

    url = f"{SENTILO_BASE_URL}/{PROVIDER}/{sensor_id}"

    params = {}
    if limit:
        params["limit"] = str(limit)
    if from_ts:
        params["from"] = from_ts  # IMPORTANTE: formato ...Z

    r = requests.get(url, headers=headers, params=params, timeout=60)

    if r.status_code != 200:
        raise RuntimeError(
            f"❌ Error Sentilo {r.status_code} sensor={sensor_id}: {r.text[:300]}"
        )

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

    # Orden por timestamp
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

    # 1) regenerar índice desde Excel
    generar_indice_sensores(EXCEL_PATH, INDEX_JSON_PATH)

    # 2) cargar índice
    with open(INDEX_JSON_PATH, "r", encoding="utf-8") as f:
        indice = json.load(f)

    sensores = indice.get("sensores", {})
    if not sensores:
        raise RuntimeError("❌ No hay sensores en indice_sensores.json")

    # 3) descargar sensores desde DAYS_BACK días atrás
    from_dt = datetime.now(timezone.utc) - timedelta(days=DAYS_BACK)
    from_ts = iso_utc_z(from_dt)

    print(f"📥 Descargando datos desde: {from_ts}")
    print(f"📌 Total sensores: {len(sensores)}")

    ok = 0
    fail = 0

    for i, (sensor_id, meta) in enumerate(sensores.items(), start=1):
        try:
            print(f"[{i}/{len(sensores)}] {sensor_id} ...")

            sentilo_json = sentilo_get_observations(sensor_id, from_ts=from_ts, limit=LIMIT)
            out = convertir_a_formato_dashboard(sensor_id, meta, sentilo_json)

            out_path = os.path.join(DATA_DIR, meta["archivo"])
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(out, f, ensure_ascii=False, indent=2)

            ok += 1
            time.sleep(SLEEP_BETWEEN_SENSORS)

        except Exception as e:
            fail += 1
            print(f"⚠️ Error con {sensor_id}: {e}")

    print(f"✅ Descarga completada. OK={ok} FAIL={fail}")

if __name__ == "__main__":
    main()


