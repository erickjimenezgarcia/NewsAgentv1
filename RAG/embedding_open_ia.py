import argparse
import os
import re
import json
import uuid
from pathlib import Path
from openai import OpenAI
from qdrant_client import QdrantClient
from qdrant_client.http.models import PointStruct, VectorParams, Distance
import tiktoken

from RAG.utils import load_openai_api_key

# Config
MODEL_NAME = "text-embedding-3-small"
DIMENSIONS = 1536
COLLECTION_NAME = "sunass_news_openai"
ENCODER = tiktoken.encoding_for_model(MODEL_NAME)

client = OpenAI(api_key=load_openai_api_key())
qdrant = QdrantClient(path="./embeddings/qdrant_db")

def chunk_text(text, max_tokens=400, overlap=50):
    tokens = ENCODER.encode(text)
    chunks = []
    start = 0
    while start < len(tokens):
        end = min(start + max_tokens, len(tokens))
        chunk = ENCODER.decode(tokens[start:end])
        chunks.append(chunk)
        start += max_tokens - overlap
    return chunks

def embed_texts(texts):
    response = client.embeddings.create(
        model=MODEL_NAME,
        input=texts
    )
    return [r.embedding for r in response.data]

def process_json(file_path: Path):
    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    date_match = re.search(r'(\d{8})', file_path.stem)
    date_str = date_match.group(1) if date_match else "unknown"
    points = []

    for i, item in enumerate(data.get("content", [])):
        raw_text = item.get("text", "")
        if not raw_text.strip():
            continue

        chunks = chunk_text(raw_text)
        embeddings = embed_texts(chunks)

        for j, (chunk, emb) in enumerate(zip(chunks, embeddings)):
            payload = {
                "text": chunk,
                "date": date_str,
                "file": file_path.name,
                "source_type": item.get("source", ""),
                "section": item.get("section", ""),
                "page": item.get("page", 0),
                "url": item.get("url", ""),
                "chunk_index": j
            }
            points.append(PointStruct(
                id=str(uuid.uuid4()),
                vector=emb,
                payload=payload
            ))
    return points

def ensure_collection():
    collections = qdrant.get_collections().collections
    if COLLECTION_NAME not in [c.name for c in collections]:
        print(f"🗂️ Creando colección {COLLECTION_NAME}...")
        qdrant.create_collection(
            collection_name=COLLECTION_NAME,
            vectors_config=VectorParams(size=DIMENSIONS, distance=Distance.COSINE)
        )

def main(input_dir, fechas):
    ensure_collection()
    for fecha in fechas:
        file_name = f"rag_clean_{fecha}.json"
        file_path = Path(input_dir) / file_name

        if not file_path.exists():
            print(f"⚠️ Archivo no encontrado: {file_name}")
            continue

        print(f"📄 Procesando {file_name}...")
        points = process_json(file_path)

        batch_size = 100
        for i in range(0, len(points), batch_size):
            qdrant.upsert(
                collection_name=COLLECTION_NAME,
                points=points[i:i + batch_size]
            )
        print(f"✅ {len(points)} vectores insertados para {file_name}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Embed JSONs limpios con OpenAI y guardar en Qdrant")
    parser.add_argument("--input", "-i", default="./data", help="Directorio donde están los .json")
    parser.add_argument("--fechas", "-f", nargs="+", required=True, help="Fechas a procesar (ej: 06052025 16042025)")

    args = parser.parse_args()
    main(args.input, args.fechas)
