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


# Agrega esto en embedding_open_ia.py

def get_openai_client():
    return OpenAI(api_key=load_openai_api_key())

def get_qdrant_client():
     return QdrantClient(
        url="http://142.93.196.168:6333",
    )


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

def embed_texts(texts, client):
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


def process_json(file_path: Path, client):
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
        embeddings = embed_texts(chunks, client)

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
        embeddings = embed_texts(chunks, client)

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
        embeddings = embed_texts(chunks, client)

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
        embeddings = embed_texts(chunks, client)

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
            
        # 5. Resumen estadístico
    stats = data.get("metadata", {}).get("stats_summary", {})
    resumen_payload = {
        "type": "resumen_estadistico",
        "date": date_str,
        "date_day": date_str,
        "date_month": date_str[2:8],
        "file": file_path.name,
        "total_urls": stats.get("total_urls_in_pdf", 0),
        "html_processed": stats.get("html_processing", {}).get("processed", 0),
        "html_successful": stats.get("html_processing", {}).get("successful", 0),
        "image_attempted": stats.get("image_processing", {}).get("attempted_download", 0),
        "image_downloaded": stats.get("image_processing", {}).get("successful_download", 0),
        "facebook_extracted": stats.get("facebook_processing", {}).get("extracted_texts", 0),
        "time_pdf_extraction": stats.get("timings_seconds", {}).get("pdf_extraction", 0),
        "time_text_extraction_pdf": stats.get("timings_seconds", {}).get("pdf_text_extraction", 0),
        "time_image_download": stats.get("timings_seconds", {}).get("image_download", 0),
        "time_image_api": stats.get("timings_seconds", {}).get("image_api", 0),
        "time_html_scraping": stats.get("timings_seconds", {}).get("html_scraping", 0),
        "time_facebook_processing": stats.get("timings_seconds", {}).get("facebook_processing", 0),
        "run_timestamp": stats.get("run_timestamp", ""),
        "semantic_chunks": stats.get("semantic_cleaning", {}).get("representative_texts", 0)
    }

    dummy_vector = [0.0] * DIMENSIONS  # No lo usarás para búsqueda semántica
    points.append(PointStruct(id=str(uuid.uuid4()), vector=dummy_vector, payload=resumen_payload))


    return points


def ensure_collection(qdrant):
    collections = qdrant.get_collections().collections
    if COLLECTION_NAME not in [c.name for c in collections]:
        print(f"🗂️ Creando colección {COLLECTION_NAME}...")
        qdrant.create_collection(
            collection_name=COLLECTION_NAME,
            vectors_config=VectorParams(size=DIMENSIONS, distance=Distance.COSINE)
        )

def main(input_dir, fechas):
    client = get_openai_client()
    qdrant = get_qdrant_client()
    ensure_collection(qdrant)
    for fecha in fechas:
        file_name = f"clean_{fecha}.json"
        file_path = Path(input_dir) / file_name

        if not file_path.exists():
            print(f"⚠️ Archivo no encontrado: {file_name}")
            continue

        print(f"📄 Procesando {file_name}...")
        points = process_json(file_path, client)

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
