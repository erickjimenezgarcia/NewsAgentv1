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

from utils import load_openai_api_key

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

def detectar_evento(texto: str):
    texto = texto.lower()
    if any(k in texto for k in ["interrupción", "interrupciones", "corte de agua", "suspensión", "sin agua"]):
        return "interrupcion"
    elif any(k in texto for k in ["denuncia", "reclamo"]):
        return "denuncia"
    elif any(k in texto for k in ["fiscalización", "supervisión", "monitoreo"]):
        return "supervision"
    return None


def process_json(file_path: Path):
    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    date_match = re.search(r'(\d{8})', file_path.stem)
    date_str = date_match.group(1) if date_match else "unknown"
    points = []

    content_root = data.get("extracted_content", {})

    # 1. CONTENIDO_INICIAL
    for item in content_root.get("pdf_paragraphs", {}).get("CONTENIDO_INICIAL", []):
        raw_text = item.get("text", "")
        if not raw_text.strip():
            continue

        chunks = chunk_text(raw_text)
        embeddings = embed_texts(chunks)

        for j, (chunk, emb) in enumerate(zip(chunks, embeddings)):
            payload = {
                "text": chunk,
                "date": date_str,
                "date_day": date_str,         # '10072025'
                "date_month": date_str[2:8],
                "file": file_path.name,
                "source_type": "pdf_paragraph",
                "section": item.get("metadata", {}).get("description", ""),
                "page": item.get("page", 0),
                "url": item.get("metadata", {}).get("url", ""),
                "event_type": detectar_evento(chunk),
                "chunk_index": j
            }
            points.append(PointStruct(id=str(uuid.uuid4()), vector=emb, payload=payload))

    # 2. HTML Pages
    for url, item in content_root.get("html_pages", {}).items():
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
                "source_type": "html",
                "section": item.get("metadata", {}).get("title", ""),
                "page": item.get("page", 0),
                "url": url,
                "event_type": detectar_evento(chunk),
                "date_day": date_str,         # '10072025'
                "date_month": date_str[2:8],
                "chunk_index": j
            }
            points.append(PointStruct(id=str(uuid.uuid4()), vector=emb, payload=payload))

    # 3. Image Texts
    for img_key, item in content_root.get("image_texts", {}).items():
        raw_text = item.get("extracted_text", "")
        if not raw_text.strip():
            continue

        chunks = chunk_text(raw_text)
        embeddings = embed_texts(chunks)

        for j, (chunk, emb) in enumerate(zip(chunks, embeddings)):
            payload = {
                "text": chunk,
                "date": date_str,
                "file": file_path.name,
                "source_type": "image",
                "section": img_key,
                "page": None,
                "url": item.get("url", ""),
                "event_type": detectar_evento(chunk),
                "date_day": date_str,         # '10072025'
                "date_month": date_str[2:8],
                "chunk_index": j
            }
            points.append(PointStruct(id=str(uuid.uuid4()), vector=emb, payload=payload))

    # 4. Facebook Texts
    for fb_url, item in content_root.get("facebook_texts", {}).items():
        raw_text = item.get("extracted_text", "")
        if not raw_text.strip():
            continue

        chunks = chunk_text(raw_text)
        embeddings = embed_texts(chunks)

        for j, (chunk, emb) in enumerate(zip(chunks, embeddings)):
            payload = {
                "text": chunk,
                "date": date_str,
                "file": file_path.name,
                "source_type": "facebook",
                "section": "post",
                "page": None,
                "url": fb_url,
                "event_type": detectar_evento(chunk),
                "date_day": date_str,         # '10072025'
                "date_month": date_str[2:8],
                "chunk_index": j
            }
            points.append(PointStruct(id=str(uuid.uuid4()), vector=emb, payload=payload))

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
