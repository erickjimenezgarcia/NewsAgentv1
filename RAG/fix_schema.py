"""
Script para crear el esquema y la tabla necesarios para el sistema RAG.
"""

import os
import sys
import psycopg2
import logging
from psycopg2 import sql

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('fix_schema')

def get_connection_params():
    """Obtiene parámetros de conexión."""
    return {
        "host": "localhost",
        "port": 5432,
        "database": "newsagent",
        "user": "postgres",
        "password": "postgres"
    }

def fix_database_schema():
    """Crea el esquema rag y la tabla chunks si no existen."""
    conn_params = get_connection_params()
    
    try:
        conn = psycopg2.connect(**conn_params)
        conn.autocommit = True
        try:
            with conn.cursor() as cur:
                # 1. Crear esquema rag si no existe
                cur.execute("CREATE SCHEMA IF NOT EXISTS rag;")
                logger.info("Esquema 'rag' creado o ya existente")
                
                # 2. Verificar si pgvector está instalado
                cur.execute("CREATE EXTENSION IF NOT EXISTS vector;")
                logger.info("Extensión pgvector creada o ya existente")
                
                # 3. Crear la tabla chunks con todas las columnas necesarias
                cur.execute("""
                CREATE TABLE IF NOT EXISTS rag.chunks (
                    id SERIAL PRIMARY KEY,
                    chunk_id TEXT UNIQUE NOT NULL,
                    text TEXT NOT NULL,
                    content TEXT,
                    embedding vector(768),
                    metadata JSONB,
                    source TEXT,
                    url TEXT,
                    title TEXT,
                    date TEXT,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                )
                """)
                logger.info("Tabla 'rag.chunks' creada o ya existente")
                
                # 4. Crear índices necesarios
                cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_chunks_chunk_id ON rag.chunks(chunk_id);
                """)
                
                cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_chunks_source ON rag.chunks(source);
                """)
                
                cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_chunks_date ON rag.chunks(date);
                """)
                
                # 5. Crear índice vectorial si no existe
                try:
                    cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_chunks_embedding ON rag.chunks 
                    USING hnsw (embedding vector_l2_ops)
                    WITH (
                        ef_construction=128,
                        m=16
                    );
                    """)
                    logger.info("Índice vectorial creado o ya existente")
                except Exception as e:
                    logger.warning(f"No se pudo crear el índice vectorial: {e}")
                    logger.warning("Esto es normal si aún no hay datos o si el índice ya existe")
                
                # 6. Verificar si hay datos en la tabla
                cur.execute("SELECT COUNT(*) FROM rag.chunks")
                count = cur.fetchone()[0]
                logger.info(f"La tabla 'rag.chunks' contiene {count} registros")
                
                return True
                
        except Exception as e:
            logger.error(f"Error creando esquema y tabla: {e}")
            return False
    except Exception as e:
        logger.error(f"Error conectando a la base de datos: {e}")
        return False

if __name__ == "__main__":
    print("Iniciando corrección de esquema de base de datos...")
    result = fix_database_schema()
    
    if result:
        print("✅ Esquema y tabla creados correctamente")
    else:
        print("❌ Hubo errores al crear el esquema y la tabla")
