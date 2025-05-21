"""
Módulo para dividir documentos en chunks óptimos para RAG.
Implementa estrategias de chunking basadas en evidencia para maximizar recall.
"""

import os
import time
import yaml
import json
import uuid
from datetime import datetime
from typing import List, Dict, Any, Optional, Union
from langchain.text_splitter import RecursiveCharacterTextSplitter, TokenTextSplitter

class SmartChunker:
    """
    Implementa chunking semántico optimizado según parámetros probados.
    Mantiene unidades semánticas y añade metadatos enriquecidos.
    """
    
    def __init__(self, config_path: str = 'config.yaml', default_date: str = None):
        """
        Inicializa el chunker con la configuración optimizada.
        
        Args:
            config_path: Ruta al archivo de configuración YAML
            default_date: Fecha predeterminada para los chunks (formato DDMMYYYY)
        """
        self.config = self._load_config(config_path)
        self.text_splitter = self._create_text_splitter()
        
        # Usar la fecha proporcionada o la fecha actual como valor predeterminado
        self.default_date = default_date if default_date else datetime.now().strftime('%d%m%Y')
        
    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Carga configuración desde archivo YAML."""
        # Determinar ruta absoluta si es relativa
        if not os.path.isabs(config_path):
            base_dir = os.path.dirname(os.path.abspath(__file__))
            config_path = os.path.join(base_dir, config_path)
            
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        
        return config['chunking'] if 'chunking' in config else config
    
    def _create_text_splitter(self) -> RecursiveCharacterTextSplitter:
        """Crea el divisor de texto según configuración optimizada."""
        chunk_size = self.config.get('chunk_size_chars', 2000)  # ~512 tokens en español
        chunk_overlap = self.config.get('chunk_overlap_chars', 500)  # 25% solapamiento
        separators = self.config.get('separators', ["\n\n", "\n", ". ", " "])
        keep_separator = self.config.get('keep_separator', True)
        
        # Usar RecursiveCharacterTextSplitter por defecto (mejor para mantener contexto)
        text_splitter = RecursiveCharacterTextSplitter(
            chunk_size=chunk_size,
            chunk_overlap=chunk_overlap,
            length_function=len,
            separators=separators,
            keep_separator=keep_separator
        )
        
        return text_splitter
    
    def process_content(self, content_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Procesa una lista de documentos y los divide en chunks optimizados.
        
        Args:
            content_list: Lista de documentos a procesar
            
        Returns:
            Lista de chunks con metadatos enriquecidos
        """
        chunks = []
        
        for item in content_list:
            # Extraer texto y metadatos básicos
            text = item.get('text', '')
            if not text.strip():
                continue  # Saltar items sin contenido
                
            # Construir metadatos enriquecidos
            metadata = {
                "source": item.get("source", "unknown"),
                "url": item.get("url", ""),
                "title": item.get("title", ""),
                "date": item.get("fecha", self.default_date),
                "processing_date": datetime.now().isoformat(),
                "source_id": item.get("image_id", "") or item.get("id", "") or item.get("url", "")
            }
            
            # Añadir campos específicos según el tipo de fuente
            if item.get("source") == "html":
                metadata["relevance"] = item.get("relevance", 0.0)
            elif item.get("source") == "image":
                metadata["image_id"] = item.get("image_id", "")
            
            # Dividir en chunks
            chunk_texts = self.text_splitter.split_text(text)
            
            # Crear documentos finales con metadatos
            for i, chunk_text in enumerate(chunk_texts):
                chunk_id = f"{metadata['source']}_{int(time.time())}_{i}_{uuid.uuid4().hex[:8]}"  # Añadimos componente aleatorio para garantizar unicidad
                chunk = {
                    "chunk_id": chunk_id,
                    "text": chunk_text,
                    "chunk_index": i,
                    "total_chunks": len(chunk_texts),
                    "metadata": metadata.copy()  # Copia para evitar referencias
                }
                
                # Añadir más metadatos específicos del chunk
                chunk["metadata"]["chunk_index"] = i
                chunk["metadata"]["total_chunks"] = len(chunk_texts)
                chunk["metadata"]["chunk_id"] = chunk_id
                
                chunks.append(chunk)
        
        return chunks

    def chunk_text(self, text: str, metadata: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """
        Divide un texto individual en chunks.
        
        Args:
            text: Texto a dividir
            metadata: Metadatos asociados al texto
            
        Returns:
            Lista de chunks con metadatos
        """
        if metadata is None:
            metadata = {}
            
        # Añadir metadatos básicos si no están presentes
        if "source" not in metadata:
            metadata["source"] = "text"
        if "processing_date" not in metadata:
            metadata["processing_date"] = datetime.now().isoformat()
            
        # Crear un documento único para procesar
        doc = {"text": text, **metadata}
        
        return self.process_content([doc])


if __name__ == "__main__":
    # Ejemplo de uso
    chunker = SmartChunker()
    
    # Texto de prueba
    test_doc = {
        "source": "html",
        "text": "Este es un texto de prueba para el sistema RAG. Este texto será dividido en chunks optimizados para RAG según la investigación reciente. La fragmentación correcta es esencial para el rendimiento del sistema RAG. Este debería ser suficientemente largo para generar varios chunks.",
        "url": "https://ejemplo.com/noticia",
        "title": "Noticia de prueba",
        "fecha": "15052025"
    }
    
    # Procesar el documento
    result = chunker.process_content([test_doc])
    
    print(f"Se generaron {len(result)} chunks del documento de prueba.")
    for i, chunk in enumerate(result):
        print(f"\nChunk {i+1}:")
        print(f"ID: {chunk['chunk_id']}")
        print(f"Texto: {chunk['text'][:100]}...")
        print(f"Metadatos: {json.dumps(chunk['metadata'], indent=2)}")
