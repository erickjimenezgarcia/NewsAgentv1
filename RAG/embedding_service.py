"""
Servicio de embeddings con caché multinivel para RAG.
Optimiza llamadas a API y mantiene rendimiento con creciente volumen de datos.
"""

import os
import json
import time
import yaml
import hashlib
import pickle
from typing import List, Dict, Any, Optional, Union
from datetime import datetime, timedelta
from pathlib import Path

# Importamos Google Generative AI para embeddings
import google.generativeai as genai

# Utilidad para reintentos con backoff exponencial
from tenacity import retry, wait_random_exponential, stop_after_attempt

# Importamos nuestro módulo de configuración de API
from config_api import get_api_key, is_api_configured


class EmbeddingCache:
    """Implementación de caché de dos niveles (memoria + disco) para embeddings."""
    
    def __init__(self, cache_dir: str, memory_ttl: int = 3600, disk_ttl: int = 604800):
        """
        Inicializa el caché de embeddings.
        
        Args:
            cache_dir: Directorio para almacenar caché persistente
            memory_ttl: Tiempo de vida en segundos para caché en memoria (1h por defecto)
            disk_ttl: Tiempo de vida en segundos para caché en disco (7 días por defecto)
        """
        self.cache_dir = cache_dir
        self.memory_cache = {}  # {hash: (timestamp, embedding)}
        self.memory_ttl = memory_ttl
        self.disk_ttl = disk_ttl
        
        # Crear directorio de caché si no existe
        os.makedirs(self.cache_dir, exist_ok=True)
    
    def _hash_text(self, text: str) -> str:
        """Genera hash consistente para un texto."""
        return hashlib.md5(text.encode('utf-8')).hexdigest()
    
    def _get_cache_path(self, text_hash: str) -> str:
        """Obtiene ruta de archivo de caché para un hash."""
        return os.path.join(self.cache_dir, f"{text_hash}.pkl")
    
    def get(self, text: str) -> Optional[List[float]]:
        """
        Recupera embedding del caché (memoria o disco).
        
        Args:
            text: Texto para el que buscar embedding
            
        Returns:
            Vector de embedding o None si no está en caché o expiró
        """
        text_hash = self._hash_text(text)
        
        # Intentar recuperar de memoria (L1)
        if text_hash in self.memory_cache:
            timestamp, embedding = self.memory_cache[text_hash]
            
            # Verificar si expiró
            if time.time() - timestamp <= self.memory_ttl:
                return embedding
            else:
                # Expiró en memoria, eliminar
                del self.memory_cache[text_hash]
        
        # Intentar recuperar de disco (L2)
        cache_path = self._get_cache_path(text_hash)
        if os.path.exists(cache_path):
            try:
                with open(cache_path, 'rb') as f:
                    timestamp, embedding = pickle.load(f)
                
                # Verificar si expiró en disco
                if time.time() - timestamp <= self.disk_ttl:
                    # Actualizar caché en memoria
                    self.memory_cache[text_hash] = (timestamp, embedding)
                    return embedding
                else:
                    # Expiró en disco, eliminar
                    os.remove(cache_path)
            except (pickle.PickleError, EOFError, Exception) as e:
                # Error al cargar caché, eliminar archivo
                print(f"Error al cargar caché: {e}")
                if os.path.exists(cache_path):
                    os.remove(cache_path)
        
        return None
    
    def set(self, text: str, embedding: List[float]) -> None:
        """
        Almacena embedding en caché (memoria + disco).
        
        Args:
            text: Texto al que corresponde el embedding
            embedding: Vector de embedding a almacenar
        """
        text_hash = self._hash_text(text)
        timestamp = time.time()
        
        # Guardar en memoria (L1)
        self.memory_cache[text_hash] = (timestamp, embedding)
        
        # Guardar en disco (L2)
        cache_path = self._get_cache_path(text_hash)
        try:
            with open(cache_path, 'wb') as f:
                pickle.dump((timestamp, embedding), f)
        except Exception as e:
            print(f"Error al guardar caché en disco: {e}")
    
    def clear_expired(self) -> int:
        """
        Limpia entradas expiradas del caché.
        
        Returns:
            Número de entradas eliminadas
        """
        count = 0
        current_time = time.time()
        
        # Limpiar memoria
        memory_keys = list(self.memory_cache.keys())
        for key in memory_keys:
            timestamp, _ = self.memory_cache[key]
            if current_time - timestamp > self.memory_ttl:
                del self.memory_cache[key]
                count += 1
        
        # Limpiar disco
        for cache_file in os.listdir(self.cache_dir):
            if not cache_file.endswith('.pkl'):
                continue
                
            cache_path = os.path.join(self.cache_dir, cache_file)
            try:
                with open(cache_path, 'rb') as f:
                    timestamp, _ = pickle.load(f)
                    
                if current_time - timestamp > self.disk_ttl:
                    os.remove(cache_path)
                    count += 1
            except Exception as e:
                # Archivo corrupto, eliminar
                os.remove(cache_path)
                count += 1
                
        return count


class EmbeddingService:
    """Servicio para generar embeddings con Google AI, optimizado con caché."""
    
    def __init__(self, config_path: str = 'config.yaml', api_key: Optional[str] = None):
        """
        Inicializa el servicio de embeddings.
        
        Args:
            config_path: Ruta al archivo de configuración
            api_key: Clave API para Google AI (opcional, si no se proporciona se busca en config_api)
        """
        # Configuración base
        self.config = self._load_config(config_path)
        
        # Configurar API key (orden de prioridad: parámetro > config_api > env)
        self.api_key = api_key
        if not self.api_key:
            self.api_key = get_api_key('google')
            
        if not self.api_key:
            raise ValueError(
                "Se requiere API key para Google AI. Ejecute 'python RAG/setup_api.py' para configurarla"
            )
        
        # Inicializar cliente
        self._initialize_client()
        
        # Configurar parámetros de embeddings
        self.model = self.config.get('embedding', {}).get('model', 'models/text-embedding-004')
        self.batch_size = self.config.get('embedding', {}).get('batch_size', 20)
        self.api_calls_count = 0
        
        # Configurar caché si está habilitado
        if self.config.get('cache_enabled', True):
            cache_dir = self.config.get('cache_dir', 'embeddings_cache')
            self.cache = EmbeddingCache(
                cache_dir=cache_dir,
                memory_ttl=self.config.get('memory_cache_ttl', 3600),
                disk_ttl=self.config.get('disk_cache_ttl', 604800)
            )
        else:
            self.cache = None
            
        # Inicializar cliente de Google AI
        self._initialize_client()
        
        # Contador de llamadas a API para monitoreo
        self.api_calls_count = 0
    
    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Carga configuración desde archivo YAML."""
        # Determinar ruta absoluta si es relativa
        if not os.path.isabs(config_path):
            base_dir = os.path.dirname(os.path.abspath(__file__))
            config_path = os.path.join(base_dir, config_path)
            
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        
        return config['embeddings'] if 'embeddings' in config else config
    
    def _initialize_client(self) -> None:
        """Inicializa cliente de Google AI con la clave API."""
        if not self.api_key:
            raise ValueError("Se requiere GOOGLE_API_KEY para el servicio de embeddings")
            
        genai.configure(api_key=self.api_key)
    
    @retry(wait=wait_random_exponential(min=1, max=60), stop=stop_after_attempt(6))
    def _get_embedding(self, text: str) -> List[float]:
        """
        Obtiene embedding de un texto usando Google AI con reintentos.
        
        Args:
            text: Texto a convertir en embedding
            
        Returns:
            Vector de embedding
        """
        try:
            # Incrementar contador para monitoreo
            self.api_calls_count += 1
            
            # Llamar a API de embeddings
            response = genai.embed_content(
                model=self.model,
                content=text
            )
            
            # Verificar respuesta
            if 'embedding' not in response:
                raise ValueError(f"Formato de respuesta inesperado: {response}")
                
            return response['embedding']
        except Exception as e:
            print(f"Error generando embedding: {e}")
            raise
    
    def get_embeddings(self, chunks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Genera embeddings para una lista de chunks con caché y procesamiento por lotes.
        
        Args:
            chunks: Lista de chunks de texto con metadatos
            
        Returns:
            Lista de chunks con embeddings añadidos
        """
        # Crear copia para no modificar original
        result_chunks = []
        
        # Procesar en lotes para optimizar API
        for i in range(0, len(chunks), self.batch_size):
            batch = chunks[i:i + self.batch_size]
            processed_batch = self._process_batch(batch)
            result_chunks.extend(processed_batch)
            
            # Breve pausa entre lotes para no saturar API
            if i + self.batch_size < len(chunks):
                time.sleep(0.5)
        
        return result_chunks
    
    def _process_batch(self, batch: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Procesa un lote de chunks generando embeddings con caché.
        
        Args:
            batch: Lote de chunks de texto
            
        Returns:
            Lote de chunks con embeddings añadidos
        """
        results = []
        
        for chunk in batch:
            # Crear copia profunda para añadir embedding
            result_chunk = chunk.copy()
            text = chunk['text']
            
            # Verificar caché si está habilitado
            embedding = None
            if self.cache:
                embedding = self.cache.get(text)
            
            # Si no está en caché, generar nuevo embedding
            if embedding is None:
                embedding = self._get_embedding(text)
                
                # Guardar en caché si está habilitado
                if self.cache and embedding:
                    self.cache.set(text, embedding)
            
            # Añadir embedding al resultado
            result_chunk['embedding'] = embedding
            results.append(result_chunk)
        
        return results
    
    def cleanup_cache(self) -> int:
        """
        Limpia entradas expiradas del caché.
        
        Returns:
            Número de entradas eliminadas
        """
        if not self.cache:
            return 0
            
        return self.cache.clear_expired()


if __name__ == "__main__":
    # Ejemplo de uso
    import sys
    from pathlib import Path
    
    # Verificar si la API está configurada
    if not is_api_configured('google'):
        print("⚠️ API key de Google no configurada. Configurando...")
        # Añadir directorio raíz al path para importar módulos
        sys.path.append(str(Path(__file__).parent.parent))
        from config_api import configure_api_key
        configure_api_key('google', interactive=True)
    
    # Crear servicio de embeddings
    embedding_service = EmbeddingService()
    
    # Texto de prueba
    test_chunks = [
        {
            "chunk_id": "test_chunk_1",
            "text": "Este es un texto de prueba para el sistema RAG.",
            "metadata": {"source": "test"}
        },
        {
            "chunk_id": "test_chunk_2",
            "text": "Este es otro texto de prueba con contenido diferente.",
            "metadata": {"source": "test"}
        }
    ]
    
    # Procesar chunks
    result = embedding_service.get_embeddings(test_chunks)
    
    print(f"Se generaron embeddings para {len(result)} chunks.")
    for i, chunk in enumerate(result):
        print(f"\nChunk {i+1}: {chunk['chunk_id']}")
        print(f"Texto: {chunk['text']}")
        print(f"Dimensiones del embedding: {len(chunk['embedding'])}")
        print(f"Primeros valores: {chunk['embedding'][:5]}...")
