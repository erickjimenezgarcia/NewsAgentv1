from qdrant_client import QdrantClient
from qdrant_client.http.models import Filter, FieldCondition
from datetime import datetime
import time

# ⚙️ Configura conexión
QDRANT_HOST = "http://142.93.196.168:6333"
COLLECTION_NAME = "sunass_news_openai"
BATCH_SIZE = 100

# 📦 Conecta a Qdrant
qdrant = QdrantClient(url=QDRANT_HOST, timeout=30.0)

def convertir_date_day_num(date_day_str: str) -> int | None:
    try:
        dt = datetime.strptime(date_day_str, "%d%m%Y")
        return int(dt.strftime("%Y%m%d"))  # '14032025' → 20250314
    except Exception as e:
        print(f"❌ Error al convertir {date_day_str}: {e}")
        return None

def actualizar_payloads():
    offset = None
    total_actualizados = 0
    total_omitidos = 0

    while True:
        puntos, offset = qdrant.scroll(
            collection_name=COLLECTION_NAME,
            offset=offset,
            limit=BATCH_SIZE,
            with_payload=True,
            with_vectors=False
        )

        if not puntos:
            break

        for punto in puntos:
            payload = punto.payload
            punto_id = punto.id

            # Ya tiene date_day_num, no hacemos nada
            if "date_day_num" in payload:
                total_omitidos += 1
                continue

            date_day = payload.get("date_day")
            if not date_day:
                total_omitidos += 1
                continue

            date_day_num = convertir_date_day_num(date_day)
            if not date_day_num:
                total_omitidos += 1
                continue

            # 🛠️ Actualiza solo ese campo
            qdrant.set_payload(
                collection_name=COLLECTION_NAME,
                payload={"date_day_num": date_day_num},
                points=[punto_id]
            )
            total_actualizados += 1

        print(f"🔁 Revisión parcial - actualizados: {total_actualizados}, omitidos: {total_omitidos}")
        time.sleep(0.2)  # evitar abusar del servidor

    print(f"\n✅ Proceso completado.")
    print(f"📌 Total actualizados: {total_actualizados}")
    print(f"📌 Total omitidos (ya tenían date_day_num o eran inválidos): {total_omitidos}")

if __name__ == "__main__":
    actualizar_payloads()
