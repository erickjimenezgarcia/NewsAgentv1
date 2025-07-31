from qdrant_client import QdrantClient
from qdrant_client.models import Filter, FilterSelector

COLLECTION_NAME = "sunass_news_openai"

qdrant = QdrantClient(host="localhost",
        port=6333,)

# ✅ Selecciona todos los puntos con un filtro vacío
qdrant.delete(
    collection_name=COLLECTION_NAME,
    points_selector=FilterSelector(
        filter=Filter(must=[])
    )
)

print(f"✅ Colección '{COLLECTION_NAME}' vaciada correctamente.")
