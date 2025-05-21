"""
Script para recrear desde cero la tabla rag.chunks con la estructura correcta.
"""

import os
import sys
import psycopg2
import logging

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('reset_schema')

def get_connection_params():
    """Obtiene parámetros de conexión."""
    return {
        "host": "localhost",
        "port": 5432,
        "database": "newsagent",
        "user": "postgres",
        "password": "postgres"
    }

def reset_database_schema():
    """Elimina y recrea la tabla rag.chunks."""
    conn_params = get_connection_params()
    
    try:
        conn = psycopg2.connect(**conn_params)
        conn.autocommit = True
        try:
            with conn.cursor() as cur:
                # 1. Verificar si la tabla existe
                cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'rag' AND table_name = 'chunks'
                )
                """)
                table_exists = cur.fetchone()[0]
                
                if table_exists:
                    # 2. Eliminar la tabla si existe
                    cur.execute("DROP TABLE IF EXISTS rag.chunks;")
                    logger.info("Tabla 'rag.chunks' eliminada")
                
                # 3. Crear esquema rag si no existe
                cur.execute("CREATE SCHEMA IF NOT EXISTS rag;")
                logger.info("Esquema 'rag' creado o ya existente")
                
                # 4. Verificar si pgvector está instalado
                cur.execute("CREATE EXTENSION IF NOT EXISTS vector;")
                logger.info("Extensión pgvector creada o ya existente")
                
                # 5. Crear la tabla chunks con todas las columnas necesarias
                cur.execute("""
                CREATE TABLE rag.chunks (
                    id SERIAL PRIMARY KEY,
                    chunk_id TEXT UNIQUE NOT NULL,
                    document_id TEXT,
                    text TEXT NOT NULL,
                    content TEXT NOT NULL,
                    embedding vector(768),
                    metadata JSONB,
                    source TEXT,
                    url TEXT,
                    title TEXT,
                    date TEXT,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                );
                """)
                logger.info("Tabla 'rag.chunks' creada correctamente")
                
                # 6. Crear índices necesarios
                cur.execute("""
                CREATE INDEX idx_chunks_chunk_id ON rag.chunks(chunk_id);
                CREATE INDEX idx_chunks_source ON rag.chunks(source);
                CREATE INDEX idx_chunks_date ON rag.chunks(date);
                """)
                logger.info("Índices básicos creados")
                
                # 7. Verificar si la tabla está vacía
                cur.execute("SELECT COUNT(*) FROM rag.chunks")
                count = cur.fetchone()[0]
                logger.info(f"La tabla 'rag.chunks' contiene {count} registros (debería ser 0)")
                
                return True
                
        except Exception as e:
            conn.rollback()
            logger.error(f"Error recreando tabla: {e}")
            return False
        finally:
            conn.close()
    except Exception as e:
        logger.error(f"Error conectando a la base de datos: {e}")
        return False

if __name__ == "__main__":
    print("Iniciando reinicio de tabla rag.chunks...")
    result = reset_database_schema()
    if result:
        print("Tabla rag.chunks recreada correctamente")
    else:
        print("Error recreando tabla rag.chunks")
