"""
Gestor de base de datos vectorial para almacenar y recuperar embeddings.
Implementa búsqueda vectorial eficiente con pgvector optimizado para escalar.
"""

import os
import time
import yaml
import json
import datetime
import logging
from typing import List, Dict, Any, Optional, Tuple, Union
from pathlib import Path

import psycopg2
from psycopg2.extras import execute_values, Json, RealDictCursor

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('VectorStore')

class VectorDBManager:
    """Gestor de base de datos vectorial con pgvector."""
    
    def __init__(self, config_path: str = 'config.yaml'):
        """
        Inicializa el gestor de base de datos vectorial.
        
        Args:
            config_path: Ruta al archivo de configuración
        """
        self.config = self._load_config(config_path)
        self.connection_params = self.config.get('connection', {})
        
        # Determinar esquema y tabla a usar
        self.schema = os.environ.get('RAG_SCHEMA', self.config.get('schema', 'rag'))
        self.table_name = self.config.get('table_name', 'chunks')
        
        # Nombre completo de la tabla (schema.table)
        self.full_table_name = f"{self.schema}.{self.table_name}"
        
        # Otras configuraciones
        self.batch_size = self.config.get('batch_size', 100)
        
        # Verificar y crear recursos necesarios
        self._initialize_db()
    
    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Carga configuración desde archivo YAML."""
        if not os.path.isabs(config_path):
            base_dir = os.path.dirname(os.path.abspath(__file__))
            config_path = os.path.join(base_dir, config_path)
            
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        
        return config['vector_store'] if 'vector_store' in config else config
    
    def _get_connection(self):
        """Crea una conexión a la base de datos."""
        return psycopg2.connect(
            host=self.connection_params.get('host', 'localhost'),
            port=self.connection_params.get('port', 5432),
            dbname=self.connection_params.get('database', 'newsagent'),
            user=self.connection_params.get('user', 'postgres'),
            password=self.connection_params.get('password', 'postgres')
        )
    
    def _initialize_db(self):
        """Inicializa la base de datos, creando tablas e índices si no existen."""
        try:
            conn = self._get_connection()
            try:
                with conn.cursor() as cur:
                    # Verificar si existe extensión pgvector
                    cur.execute("SELECT 1 FROM pg_extension WHERE extname = 'vector'")
                    if cur.fetchone() is None:
                        # Crear extensión pgvector
                        logger.info("Creando extensión pgvector...")
                        cur.execute("CREATE EXTENSION IF NOT EXISTS vector")
                    
                    # Crear tabla
                    self._create_table_if_not_exists(cur)
                    
                    # Crear partición para el mes actual si no existe
                    self._ensure_current_partition(cur)
                    
                    # Crear índices
                    self._create_indices_if_not_exist(cur)
                
                conn.commit()
                logger.info("Base de datos inicializada correctamente")
                
            except Exception as e:
                conn.rollback()
                logger.error(f"Error inicializando base de datos: {e}")
                raise
            finally:
                conn.close()
                
        except Exception as e:
            logger.error(f"Error conectando a base de datos: {e}")
            raise
    
    def _create_table_if_not_exists(self, cursor):
        """Crea la tabla principal si no existe."""
        # Crear esquema si no existe
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {self.schema}")
        
        cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {self.full_table_name} (
            id SERIAL PRIMARY KEY,
            chunk_id TEXT NOT NULL UNIQUE,
            document_id TEXT,
            text TEXT NOT NULL,
            content TEXT NOT NULL,
            embedding vector({self.config.get('dimensions', 768)}),
            metadata JSONB,
            source TEXT,
            url TEXT,
            title TEXT,
            date TEXT,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        )
        """)
        logger.info(f"Tabla {self.table_name} creada o ya existente")
    
    def _create_indices_if_not_exist(self, cursor):
        """Crea índices para búsqueda eficiente si no existen."""
        # Índice para búsqueda por chunk_id
        cursor.execute(f"""
        CREATE INDEX IF NOT EXISTS idx_{self.table_name}_chunk_id 
        ON {self.full_table_name}(chunk_id)
        """)
        
        # Índice para búsqueda por source
        cursor.execute(f"""
        CREATE INDEX IF NOT EXISTS idx_{self.table_name}_source 
        ON {self.full_table_name}(source)
        """)
        
        # Índice para búsqueda por date
        cursor.execute(f"""
        CREATE INDEX IF NOT EXISTS idx_{self.table_name}_date 
        ON {self.full_table_name}(date)
        """)
        
        # Índice HNSW para búsqueda vectorial eficiente
        # Verificar si el índice ya existe antes de intentar crearlo
        cursor.execute(f"""
        SELECT 1 FROM pg_indexes 
        WHERE indexname = 'idx_{self.table_name}_embedding'
        """)
        index_exists = cursor.fetchone() is not None
        
        if not index_exists:
            cursor.execute(f"""
            CREATE INDEX idx_{self.table_name}_embedding ON {self.full_table_name} 
            USING ivfflat (embedding vector_cosine_ops) 
            WITH (lists = 100)
            """)
        
        # Índice GIN para búsqueda en JSONB
        cursor.execute(f"""
        CREATE INDEX IF NOT EXISTS idx_{self.table_name}_metadata ON {self.full_table_name} USING GIN (metadata)
        """)
        
        logger.info(f"Índices para {self.table_name} creados o actualizados")
    
    def _ensure_current_partition(self, cursor):
        """Este método es un placeholder para implementar particionamiento en el futuro."""
        # Esta funcionalidad se implementará en futuras versiones
        pass
    
    def get_database_info(self) -> Dict[str, Any]:
        """Obtiene información sobre la base de datos."""
        try:
            conn = self._get_connection()
            with conn.cursor() as cur:
                # Obtener versión de PostgreSQL
                cur.execute("SELECT version()")
                version = cur.fetchone()[0]
                
                # Verificar si pgvector está instalado
                cur.execute("SELECT extversion FROM pg_extension WHERE extname = 'vector'")
                pgvector_version = cur.fetchone()
                pgvector_version = pgvector_version[0] if pgvector_version else "No instalado"
                
                # Contar registros en la tabla
                try:
                    cur.execute(f"SELECT COUNT(*) FROM {self.full_table_name}")
                    count = cur.fetchone()[0]
                except:
                    count = 0
                
                return {
                    "version": version,
                    "pgvector_version": pgvector_version,
                    "records_count": count
                }
        except Exception as e:
            logger.error(f"Error obteniendo información de la base de datos: {e}")
            return {
                "version": "Error",
                "pgvector_version": "Error",
                "records_count": 0
            }
        finally:
            if conn:
                conn.close()
    
    def upsert_documents(self, chunks: List[Dict[str, Any]]) -> int:
        """
        Inserta o actualiza documentos en la base de datos.
        
        Args:
            chunks: Lista de chunks con embeddings
            
        Returns:
            Número de documentos insertados/actualizados
        """
        if not chunks:
            return 0
        
        # Verificar que los chunks tienen embeddings
        for chunk in chunks:
            if 'embedding' not in chunk:
                raise ValueError(f"Chunk {chunk.get('chunk_id', 'unknown')} no tiene embedding")
        
        # Procesar en lotes para eficiencia
        total_processed = 0
        for i in range(0, len(chunks), self.batch_size):
            batch = chunks[i:i + self.batch_size]
            processed = self._process_batch(batch)
            total_processed += processed
            
        return total_processed
    
    def _process_batch(self, batch: List[Dict[str, Any]]) -> int:
        """
        Procesa un lote de chunks para inserción/actualización.
        
        Args:
            batch: Lote de chunks con embeddings
            
        Returns:
            Número de documentos procesados
        """
        # Eliminar duplicados por chunk_id (manteniendo solo la última ocurrencia)
        unique_chunks = {}
        for chunk in batch:
            chunk_id = chunk.get('chunk_id')
            if chunk_id:
                if chunk_id in unique_chunks:
                    logger.warning(f"ID duplicado encontrado: {chunk_id}. Se usará la última ocurrencia.")
                unique_chunks[chunk_id] = chunk
        
        deduplicated_batch = list(unique_chunks.values())
        if len(deduplicated_batch) < len(batch):
            logger.warning(f"Se encontraron {len(batch) - len(deduplicated_batch)} chunks con IDs duplicados. Solo se conserva la última ocurrencia de cada ID")
        
        # Preparar valores para inserción
        values = []
        skipped = 0
        
        for chunk in deduplicated_batch:
            # Verificar que tenga texto
            text = chunk.get('text', '')
            if not text:
                logger.warning(f"Chunk {chunk.get('chunk_id', 'unknown')} no tiene texto. Será omitido.")
                skipped += 1
                continue
                
            # Metadatos a JSON
            metadata = chunk.get('metadata', {})
            if not isinstance(metadata, dict):
                metadata = {}
                
            # Preparar valores - usar 'text' como campo principal
            values.append((
                chunk.get('chunk_id'),
                chunk.get('document_id', ''),
                text,  # Campo text
                text,  # Duplicar en content para compatibilidad
                chunk.get('embedding'),
                Json(metadata),
                chunk.get('source', ''),
                chunk.get('url', ''),
                chunk.get('title', ''),
                chunk.get('date', '')
            ))
            
        if not values:
            return 0
            
        conn = None
        try:
            conn = self._get_connection()
            with conn.cursor() as cur:
                # Usar INSERT ... ON CONFLICT para upsert
                query = f"""
                INSERT INTO {self.full_table_name} 
                (chunk_id, document_id, text, content, embedding, metadata, source, url, title, date)
                VALUES %s
                ON CONFLICT (chunk_id) DO UPDATE SET
                    document_id = EXCLUDED.document_id,
                    text = EXCLUDED.text,
                    content = EXCLUDED.content,
                    embedding = EXCLUDED.embedding,
                    metadata = EXCLUDED.metadata,
                    source = EXCLUDED.source,
                    url = EXCLUDED.url,
                    title = EXCLUDED.title,
                    date = EXCLUDED.date
                """
                
                execute_values(cur, query, values)
                conn.commit()
                return len(values)
                
        except Exception as e:
            logger.error(f"Error procesando lote: {e}")
            if conn:
                conn.rollback()
            raise
            
        finally:
            if conn:
                conn.close()
    
    def semantic_search(self, 
                       query_embedding: List[float], 
                       filters: Optional[Dict[str, Any]] = None,
                       limit: int = 10) -> List[Dict[str, Any]]:
        """
        Realiza una búsqueda semántica con filtros opcionales.
        
        Args:
            query_embedding: Vector de embedding de la consulta
            filters: Filtros para la búsqueda (source, date, etc.)
            limit: Número máximo de resultados
            
        Returns:
            Lista de documentos similares
        """
        conn = None
        try:
            conn = self._get_connection()
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # Construir consulta base
                base_sql = f"""
                SELECT 
                    id, chunk_id, text, content, metadata, source, url, title, date,
                    1 - (embedding <=> %s::vector) as similarity
                FROM {self.full_table_name}
                """
                
                # Añadir filtros si se proporcionan
                where_clauses = []
                params = [query_embedding]
                
                if filters:
                    if 'source' in filters and filters['source']:
                        where_clauses.append("source = %s")
                        params.append(filters['source'])
                        
                    if 'date' in filters and filters['date']:
                        where_clauses.append("date = %s")
                        params.append(filters['date'])
                        
                    if 'url' in filters and filters['url']:
                        where_clauses.append("url = %s")
                        params.append(filters['url'])
                        
                    if 'min_similarity' in filters and filters['min_similarity'] is not None:
                        where_clauses.append("1 - (embedding <=> %s::vector) > %s")
                        params.append(query_embedding)
                        params.append(float(filters['min_similarity']))
                
                # Añadir cláusula WHERE si hay filtros
                if where_clauses:
                    base_sql += " WHERE " + " AND ".join(where_clauses)
                
                # Ordenar por similitud y limitar resultados
                base_sql += " ORDER BY similarity DESC LIMIT %s"
                params.append(limit)
                
                # Ejecutar consulta
                cur.execute(base_sql, params)
                results = cur.fetchall()
                
                # Convertir resultados a lista de diccionarios
                return [dict(row) for row in results]
                
        except Exception as e:
            logger.error(f"Error en búsqueda semántica: {e}")
            raise
            
        finally:
            if conn:
                conn.close()
    
    def keyword_search(self, 
                      keywords: str, 
                      filters: Optional[Dict[str, Any]] = None,
                      limit: int = 10) -> List[Dict[str, Any]]:
        """
        Realiza una búsqueda por palabras clave con filtros opcionales.
        
        Args:
            keywords: Palabras clave para buscar
            filters: Filtros para la búsqueda
            limit: Número máximo de resultados
            
        Returns:
            Lista de documentos que coinciden con las palabras clave
        """
        conn = None
        try:
            conn = self._get_connection()
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # Construir consulta base
                base_sql = f"""
                SELECT 
                    id, chunk_id, text, content, metadata, source, url, title, date,
                    ts_rank(to_tsvector('spanish', text), plainto_tsquery('spanish', %s)) as rank
                FROM {self.full_table_name}
                """
                
                # Añadir filtros si se proporcionan
                where_clauses = ["plainto_tsquery('spanish', %s) @@ to_tsvector('spanish', text)"]
                params = [keywords, keywords]  # Se usa dos veces: para rank y para la condición
                
                if filters:
                    if 'source' in filters and filters['source']:
                        where_clauses.append("source = %s")
                        params.append(filters['source'])
                        
                    if 'date' in filters and filters['date']:
                        where_clauses.append("date = %s")
                        params.append(filters['date'])
                        
                    if 'url' in filters and filters['url']:
                        where_clauses.append("url = %s")
                        params.append(filters['url'])
                
                # Añadir cláusula WHERE
                base_sql += " WHERE " + " AND ".join(where_clauses)
                
                # Ordenar por relevancia y limitar resultados
                base_sql += " ORDER BY rank DESC LIMIT %s"
                params.append(limit)
                
                # Ejecutar consulta
                cur.execute(base_sql, params)
                results = cur.fetchall()
                
                # Convertir resultados a lista de diccionarios
                return [dict(row) for row in results]
                
        except Exception as e:
            logger.error(f"Error en búsqueda por palabras clave: {e}")
            raise
            
        finally:
            if conn:
                conn.close()
    
    def hybrid_search(self, 
                     query: str,
                     query_embedding: List[float],
                     vector_weight: float = 0.7,
                     keyword_weight: float = 0.3,
                     filters: Optional[Dict[str, Any]] = None,
                     limit: int = 10) -> List[Dict[str, Any]]:
        """
        Realiza una búsqueda híbrida combinando vectorial y palabras clave.
        
        Args:
            query: Consulta de texto
            query_embedding: Vector de embedding de la consulta
            vector_weight: Peso para resultados vectoriales (0.0 - 1.0)
            keyword_weight: Peso para resultados de palabras clave (0.0 - 1.0)
            filters: Filtros para la búsqueda
            limit: Número máximo de resultados
            
        Returns:
            Lista combinada de resultados
        """
        # Normalizar pesos
        total_weight = vector_weight + keyword_weight
        if total_weight <= 0:
            raise ValueError("La suma de pesos debe ser mayor que 0")
            
        vector_weight = vector_weight / total_weight
        keyword_weight = keyword_weight / total_weight
        
        # Obtener resultados de ambas búsquedas
        vector_results = self.semantic_search(
            query_embedding=query_embedding,
            filters=filters,
            limit=limit * 2  # Obtener más resultados para mejor mezcla
        )
        
        keyword_results = self.keyword_search(
            keywords=query,
            filters=filters,
            limit=limit * 2  # Obtener más resultados para mejor mezcla
        )
        
        # Crear diccionarios para acceso rápido por chunk_id
        vector_scores = {r['chunk_id']: r.pop('similarity', 0) for r in vector_results}
        keyword_scores = {r['chunk_id']: r.pop('rank', 0) for r in keyword_results}
        
        # Normalizar puntuaciones si es necesario
        if vector_scores:
            max_vector = max(vector_scores.values()) or 1
            vector_scores = {k: v/max_vector for k, v in vector_scores.items()}
            
        if keyword_scores:
            max_keyword = max(keyword_scores.values()) or 1
            keyword_scores = {k: v/max_keyword for k, v in keyword_scores.items()}
        
        # Combinar resultados
        all_ids = set(vector_scores.keys()) | set(keyword_scores.keys())
        combined_results = []
        
        for chunk_id in all_ids:
            vector_score = vector_scores.get(chunk_id, 0)
            keyword_score = keyword_scores.get(chunk_id, 0)
            
            # Calcular puntuación combinada
            combined_score = (vector_score * vector_weight) + (keyword_score * keyword_weight)
            
            # Obtener el resultado de cualquiera de las dos búsquedas
            result = next((r for r in vector_results + keyword_results if r.get('chunk_id') == chunk_id), None)
            if result:
                result['score'] = combined_score
                combined_results.append(result)
        
        # Ordenar por puntuación combinada y limitar resultados
        combined_results.sort(key=lambda x: x.get('score', 0), reverse=True)
        return combined_results[:limit]


if __name__ == "__main__":
    # Ejemplo de uso
    import numpy as np
    
    # Configurar logging
    logging.basicConfig(level=logging.INFO)
    
    # Crear instancia del gestor
    db_manager = VectorDBManager()
    
    # Ejemplo de inserción
    sample_chunk = {
        'chunk_id': 'test_123',
        'text': 'Este es un texto de ejemplo',
        'embedding': np.random.rand(768).tolist(),
        'metadata': {'source': 'test'},
        'source': 'test',
        'url': 'http://example.com',
        'title': 'Ejemplo',
        'date': '2023-01-01'
    }
    
    # Insertar ejemplo
    db_manager.upsert_documents([sample_chunk])
    
    # Realizar búsqueda
    results = db_manager.semantic_search(
        query_embedding=np.random.rand(768).tolist(),
        limit=5
    )
    
    print("Resultados de búsqueda:", results)
