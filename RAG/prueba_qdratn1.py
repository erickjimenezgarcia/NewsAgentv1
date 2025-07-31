from qdrant_client import QdrantClient
from qdrant_client.models import Filter, FieldCondition, MatchValue

client = QdrantClient(host="localhost", port=6333, timeout=30.0)

result = client.search(
    collection_name="sunass_news_openai",
    query_vector=[0.0] * 1536,  # vector de prueba
    limit=1,
    query_filter=Filter(
        must=[
            FieldCondition(
                key="date_day",
                match=MatchValue(value="14072025")
            )
        ]
    )
)

print(result)
