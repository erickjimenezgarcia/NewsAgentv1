"""
Script para generar embeddings a partir de los datos limpios.
Este script realiza el chunking de los textos y genera embeddings para RAG.
"""

import os
import json
import argparse
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
import numpy as np
from datetime import datetime
import re

# Intenta importar las bibliotecas necesarias, con manejo de errores para facilitar la instalación
try:
    from sentence_transformers import SentenceTransformer
    import qdrant_client
    from qdrant_client.http import models
except ImportError:
    print("Instalando dependencias necesarias...")
    import subprocess
    subprocess.check_call(["pip", "install", "sentence-transformers", "qdrant-client"])
    from sentence_transformers import SentenceTransformer
    import qdrant_client
    from qdrant_client.http import models


class EmbeddingGenerator:
    """Clase para generar embeddings a partir de textos limpios."""
    
    def __init__(self, input_dir: str, output_dir: str, model_name: str = "paraphrase-multilingual-MiniLM-L12-v2"):
        """
        Inicializa el generador de embeddings.
        
        Args:
            input_dir: Directorio donde se encuentran los archivos JSON limpios
            output_dir: Directorio donde se guardarán los embeddings
            model_name: Nombre del modelo de SentenceTransformers a utilizar
        """
        self.input_dir = Path(input_dir)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        print(f"Cargando modelo de embeddings: {model_name}")
        self.model = SentenceTransformer(model_name)
        self.chunk_size = 512  # Tamaño máximo de chunk en caracteres
        self.chunk_overlap = 100  # Solapamiento entre chunks
        
        # Inicializar cliente Qdrant
        self.qdrant_path = self.output_dir / "qdrant_db"
        self.qdrant_path.mkdir(exist_ok=True)
        self.qdrant = qdrant_client.QdrantClient(path=str(self.qdrant_path))
        
        # Dimensión del modelo
        self.vector_size = self.model.get_sentence_embedding_dimension()
        print(f"Dimensión del vector de embeddings: {self.vector_size}")
    
    def create_chunks(self, text: str, metadata: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Divide un texto en chunks con solapamiento.
        
        Args:
            text: Texto a dividir
            metadata: Metadatos asociados al texto
            
        Returns:
            Lista de diccionarios con texto y metadatos
        """
        # Si el texto es más corto que el tamaño del chunk, devolverlo completo
        if len(text) <= self.chunk_size:
            return [{
                "text": text,
                "metadata": metadata
            }]
        
        # Dividir el texto en oraciones para hacer chunks más naturales
        sentences = re.split(r'(?<=[.!?])\s+', text)
        chunks = []
        current_chunk = ""
        
        for sentence in sentences:
            # Si añadir la oración excede el tamaño del chunk, guardar el chunk actual
            if len(current_chunk) + len(sentence) > self.chunk_size and current_chunk:
                chunks.append({
                    "text": current_chunk.strip(),
                    "metadata": metadata
                })
                # Comenzar un nuevo chunk con solapamiento
                words = current_chunk.split()
                overlap_words = words[-min(len(words), self.chunk_overlap // 5):]
                current_chunk = " ".join(overlap_words) + " " + sentence
            else:
                # Añadir la oración al chunk actual
                current_chunk += " " + sentence if current_chunk else sentence
        
        # Añadir el último chunk si no está vacío
        if current_chunk:
            chunks.append({
                "text": current_chunk.strip(),
                "metadata": metadata
            })
        
        return chunks
    
    def process_file(self, file_path: Path) -> List[Dict[str, Any]]:
        """
        Procesa un archivo JSON limpio y genera chunks.
        
        Args:
            file_path: Ruta al archivo JSON limpio
            
        Returns:
            Lista de chunks con metadatos
        """
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            all_chunks = []
            
            # Obtener fecha del archivo
            date_match = re.search(r'(\d{8})', file_path.stem)
            date_str = date_match.group(1) if date_match else "unknown_date"
            
            # Procesar cada elemento de contenido
            for item in data.get("content", []):
                source_type = item.get("source", "unknown")
                
                # Crear metadatos básicos
                metadata = {
                    "source_type": source_type,
                    "date": date_str,
                    "file": file_path.name,
                }
                
                # Añadir metadatos específicos según el tipo de fuente
                if source_type == "html":
                    metadata["url"] = item.get("url", "")
                    metadata["title"] = item.get("title", "")
                    metadata["relevance"] = item.get("relevance", 0)
                elif source_type == "facebook":
                    metadata["url"] = item.get("url", "")
                    metadata["pdf_path"] = item.get("pdf_path", "")
                elif source_type == "image":
                    metadata["image_id"] = item.get("image_id", "")
                    metadata["relevance"] = item.get("relevance", 0)
                elif source_type == "pdf":
                    metadata["section"] = item.get("section", "")
                    metadata["page"] = item.get("page", 0)
                    metadata["url"] = item.get("url", "")
                
                # Crear chunks del texto
                text = item.get("text", "")
                if text:
                    chunks = self.create_chunks(text, metadata)
                    all_chunks.extend(chunks)
            
            return all_chunks
        
        except Exception as e:
            print(f"Error procesando {file_path}: {e}")
            return []
    
    def create_collection(self, collection_name: str):
        """
        Crea una colección en Qdrant si no existe.
        
        Args:
            collection_name: Nombre de la colección
        """
        collections = self.qdrant.get_collections().collections
        collection_names = [collection.name for collection in collections]
        
        if collection_name not in collection_names:
            print(f"Creando colección: {collection_name}")
            self.qdrant.create_collection(
                collection_name=collection_name,
                vectors_config=models.VectorParams(
                    size=self.vector_size,
                    distance=models.Distance.COSINE
                )
            )
        else:
            print(f"La colección {collection_name} ya existe")
    
    def generate_embeddings(self, chunks: List[Dict[str, Any]], collection_name: str):
        """
        Genera embeddings para los chunks y los almacena en Qdrant.
        
        Args:
            chunks: Lista de chunks con metadatos
            collection_name: Nombre de la colección de Qdrant
        """
        if not chunks:
            print("No hay chunks para procesar")
            return
        
        # Asegurar que la colección existe
        self.create_collection(collection_name)
        
        # Preparar datos para Qdrant
        texts = [chunk["text"] for chunk in chunks]
        
        print(f"Generando embeddings para {len(texts)} chunks...")
        embeddings = self.model.encode(texts, show_progress_bar=True)
        
        # Preparar puntos para insertar en Qdrant
        points = []
        for i, (chunk, embedding) in enumerate(zip(chunks, embeddings)):
            points.append(models.PointStruct(
                id=i,
                vector=embedding.tolist(),
                payload={
                    "text": chunk["text"],
                    **chunk["metadata"]
                }
            ))
        
        # Insertar puntos en lotes para evitar problemas de memoria
        batch_size = 100
        for i in range(0, len(points), batch_size):
            batch = points[i:i+batch_size]
            self.qdrant.upsert(
                collection_name=collection_name,
                points=batch
            )
            print(f"Insertados {len(batch)} puntos en Qdrant ({i+len(batch)}/{len(points)})")
        
        print(f"Embeddings generados y almacenados en la colección {collection_name}")
    
    def process_directory(self, collection_name: str = "sunass_news"):
        """
        Procesa todos los archivos JSON en el directorio de entrada.
        
        Args:
            collection_name: Nombre de la colección de Qdrant
        """
        # Buscar archivos JSON en el directorio de entrada
        json_files = list(self.input_dir.glob("rag_*.json"))
        
        if not json_files:
            print(f"No se encontraron archivos JSON en {self.input_dir}")
            return
        
        all_chunks = []
        
        for json_file in json_files:
            print(f"Procesando {json_file.name}...")
            
            # Procesar el archivo y obtener chunks
            chunks = self.process_file(json_file)
            all_chunks.extend(chunks)
            
            print(f"Se generaron {len(chunks)} chunks de {json_file.name}")
        
        print(f"Total de chunks generados: {len(all_chunks)}")
        
        # Generar embeddings y almacenarlos en Qdrant
        self.generate_embeddings(all_chunks, collection_name)
        
        # Guardar información de los chunks para referencia
        chunks_info = {
            "total_chunks": len(all_chunks),
            "collection_name": collection_name,
            "model_name": self.model.get_sentence_embedding_dimension(),
            "timestamp": datetime.now().isoformat(),
            "files_processed": [f.name for f in json_files]
        }
        
        with open(self.output_dir / "chunks_info.json", 'w', encoding='utf-8') as f:
            json.dump(chunks_info, f, ensure_ascii=False, indent=2)
        
        print(f"Información de chunks guardada en {self.output_dir / 'chunks_info.json'}")


def main():
    """Función principal."""
    parser = argparse.ArgumentParser(description="Generador de embeddings para RAG")
    parser.add_argument("--input", "-i", default="./data", 
                        help="Directorio de entrada con archivos JSON limpios")
    parser.add_argument("--output", "-o", default="./embeddings", 
                        help="Directorio de salida para embeddings")
    parser.add_argument("--model", "-m", default="paraphrase-multilingual-MiniLM-L12-v2", 
                        help="Modelo de SentenceTransformers a utilizar")
    parser.add_argument("--collection", "-c", default="sunass_news", 
                        help="Nombre de la colección de Qdrant")
    
    args = parser.parse_args()
    
    generator = EmbeddingGenerator(args.input, args.output, args.model)
    generator.process_directory(args.collection)


if __name__ == "__main__":
    main()
