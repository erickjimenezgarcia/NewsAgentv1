import os
import requests
from datetime import datetime, timedelta
import time

# Configuración
API_URL = "http://localhost:8000/procesar_pdf/"
CARPETA_BASE = "base"
BATCH_SIZE = 3
PAUSE_SECONDS = 60

# Rango de fechas: 01/02/2025 al 28/02/2025
start_date = datetime.strptime("01032025", "%d%m%Y")
end_date = datetime.strptime("31032025", "%d%m%Y")

current_date = start_date
while current_date <= end_date:
    date_str = current_date.strftime("%d%m%Y")
    filename_pdf = f"{date_str}.pdf"
    full_path = os.path.join(CARPETA_BASE, filename_pdf)

    if os.path.exists(full_path):
        print(f"📄 Procesando archivo {filename_pdf}...")

        payload = {
            "filename": date_str,  # sin ".pdf"
            "prompt": "",          # puedes colocar algún prompt por defecto si lo usas
            "batchSize": BATCH_SIZE,
            "pauseSeconds": PAUSE_SECONDS
        }

        try:
            response = requests.post(API_URL, json=payload)  # hasta 10 min
            if response.status_code == 200:
                print(f"✅ Procesado correctamente: {filename_pdf}")
            elif response.status_code == 404:
                print(f"⚠️ No encontrado en el backend (pero existe localmente): {filename_pdf}")
            else:
                print(f"❌ Error procesando {filename_pdf}: {response.status_code} - {response.text}")
        except requests.exceptions.RequestException as e:
            print(f"🚨 Error en la petición para {filename_pdf}: {str(e)}")
    else:
        print(f"⛔ Archivo no encontrado localmente: {filename_pdf}")

    current_date += timedelta(days=1)