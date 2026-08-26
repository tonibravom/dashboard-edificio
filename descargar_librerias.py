import os
import requests

# Crear la carpeta 'js' si no existe en tu repositorio
os.makedirs("js", exist_ok=True)

# Direcciones alternativas de los archivos indispensables
librerias = {
    "js/three.min.js": "https://unpkg.com",
    "js/OrbitControls.js": "https://unpkg.com",
    "js/GLTFLoader.js": "https://unpkg.com"
}

print("Iniciando descarga de componentes 3D...")

for ruta_local, url_remota in librerias.items():
    try:
        print(f"Descargando {ruta_local} desde un servidor seguro...")
        respuesta = requests.get(url_remota, timeout=15)
        respuesta.raise_for_status() # Lanza error si falla la conexión
        
        with open(ruta_local, "w", encoding="utf-8") as archivo:
            archivo.write(respuesta.text)
        print(f"✅ ¡{ruta_local} guardado correctamente!")
    except Exception as e:
        print(f"❌ Error al descargar {ruta_local}: {e}")

print("Proceso finalizado.")
