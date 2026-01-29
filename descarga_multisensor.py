import os
import json
import time
import requests
import pandas as pd
from datetime import datetime

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

# límite de registros (con 250 te sobran para 2 días ~192 puntos)
LIMIT = 250

# ==========================================================
# UTILIDADES
# ==========================================================
def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)

def read_excel_sensors(excel_path: str):
    df = pd.read_excel(excel_path)
    df.columns = [c.strip().lower() for c in df.columns]

    if "sensor_id" not in df.columns:
        raise ValueError(f"No encuentro columna 'sensor_id' en el Excel. Columnas: {list(df.columns)}")

    col_id = "sensor_id"

    col_desc = "descripcion" if "descripcion" in df.columns else None
    col_unit = "unitat de mesura" if "unitat de mesura" in df.columns else ("unidad" if "unidad" in df.columns else None)
    col_type = "tipus de dada" if "tipus de dada" in df.columns else ("tipo_dato" if "tipo_dato" in df.columns else None)

    sensores = []
    for _, row in df.iterrows():
        sensor_id = str(row[col_id]).strip()
        if not sensor_id or sensor_id.lower() == "nan":
            continue

        descripcion = str(row[col_desc]).strip() if col_desc else sensor_id
        unidad = str(row[col_unit]).strip() if col_unit else ""
        tipo_dato = str(row[col_type]).strip() if col_type else "instantaneo"

        sensores.append({
            "sensor_id": sensor_id,
            "descripcion": descripcion,
            "unidad": unidad,
            "tipo_dato": tipo_dato,
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

def sentilo_get_observations(sensor_id: str, limit: int = 250):
    if not SENTILO_TOKEN:
        raise RuntimeError("❌ SENTILO_TOKEN no está definido en variables de entorno (GitHub Secret).")

    headers = {
        "IDENTITY_KEY": SENTILO_TOKEN,
        "Accept": "application/json"
    }

    # Endpoint correcto:
    # http://connectaapi.bcn.cat/data/{provider}/{sensor}
    url = f"{SENTILO_BASE_URL}/{PROVIDER}/{sensor_id}"

    params = {}
    if limit:
        params["limit"] = str(limit)

    r = requests.get(url, headers=headers, params=params, timeout=60)

    if r.status_code != 200:
        raise RuntimeError(f"❌ Error Sentilo {r.status_code} sensor={sensor_id}: {r.text[:300]}")

    return r.json()

def convertir_a_formato_dashboard(sensor_id: str, meta: dict, sentilo_json: dict):
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

    print(f"📥 Descargando datos (sin from) con limit={LIMIT}")
    print(f"📌 Total sensores: {len(sensores)}")

    for i, (sensor_id, meta) in enumerate(sensores.items(), start=1):
        try:
            print(f"[{i}/{len(sensores)}] {sensor_id} ...")

            sentilo_json = sentilo_get_observations(sensor_id, limit=LIMIT)
            out = convertir_a_formato_dashboard(sensor_id, meta, sentilo_json)

            out_path = os.path.join(DATA_DIR, meta["archivo"])
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(out, f, ensure_ascii=False, indent=2)

            time.sleep(0.2)

        except Exception as e:
            print(f"⚠️ Error con {sensor_id}: {e}")

    print("✅ Descarga completada.")

if __name__ == "__main__":
    main()




