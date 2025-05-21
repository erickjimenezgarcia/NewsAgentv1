"""
Script para verificar y arreglar la base de datos pgvector.
Soluciona problemas de tablas vacías y tipos de datos incompatibles.
"""

import os
import sys
import psycopg2
from psycopg2.extras import RealDictCursor
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('PGVectorFix')

def get_connection_params():
    """Obtiene los parámetros de conexión."""
    return {
        'host': 'localhost', 
        'port': 5432,
        'dbname': 'newsagent',
        'user': 'postgres',
        'password': 'postgres'
    }

def check_tables():
    """Verifica las tablas en la base de datos y su contenido."""
    conn_params = get_connection_params()
    
    try:
        conn = psycopg2.connect(**conn_params)
        try:
            # Verificar tablas existentes
            with conn.cursor() as cur:
                cur.execute("""
                SELECT table_schema, table_name 
                FROM information_schema.tables 
                WHERE table_schema IN ('public', 'rag')
                ORDER BY table_schema, table_name
                """)
                
                tables = cur.fetchall()
                logger.info(f"Tablas encontradas: {len(tables)}")
                
                for schema, table in tables:
                    logger.info(f"  - {schema}.{table}")
                    
                    # Verificar contenido de cada tabla
                    try:
                        cur.execute(f"SELECT COUNT(*) FROM {schema}.{table}")
                        count = cur.fetchone()[0]
                        logger.info(f"    * Registros: {count}")
                    except Exception as e:
                        logger.error(f"    * Error contando registros: {e}")
                
                # Verificar pgvector
                cur.execute("SELECT 1 FROM pg_extension WHERE extname = 'vector'")
                if cur.fetchone():
                    logger.info("✅ Extensión pgvector está instalada")
                else:
                    logger.error("❌ Extensión pgvector NO está instalada")
                
                # Verificar tablas específicas RAG
                cur.execute("SELECT table_name FROM information_schema.tables WHERE table_name = 'noticias_chunks'")
                if cur.fetchone():
                    logger.info("✅ Tabla noticias_chunks existe")
                    
                    # Verificar estructura
                    cur.execute("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'noticias_chunks'")
                    columns = cur.fetchall()
                    logger.info(f"Columnas en noticias_chunks:")
                    for col_name, col_type in columns:
                        logger.info(f"  - {col_name}: {col_type}")
                else:
                    logger.error("❌ Tabla noticias_chunks NO existe")
                
                # Verificar contenido en noticias_chunks
                try:
                    cur.execute("SELECT COUNT(*) FROM noticias_chunks")
                    count = cur.fetchone()[0]
                    if count > 0:
                        logger.info(f"✅ Tabla noticias_chunks tiene {count} registros")
                        
                        # Verificar un registro
                        cur.execute("SELECT chunk_id, content FROM noticias_chunks LIMIT 1")
                        sample = cur.fetchone()
                        if sample:
                            chunk_id, content = sample
                            logger.info(f"Muestra - chunk_id: {chunk_id}")
                            logger.info(f"Muestra - content (primeros 100 chars): {content[:100]}")
                    else:
                        logger.warning(f"⚠️ Tabla noticias_chunks está VACÍA")
                except Exception as e:
                    logger.error(f"Error verificando noticias_chunks: {e}")
        finally:
            conn.close()
    except Exception as e:
        logger.error(f"Error conectando a base de datos: {e}")

def fix_database():
    """Corrige problemas en la base de datos."""
    conn_params = get_connection_params()
    
    try:
        conn = psycopg2.connect(**conn_params)
        try:
            with conn.cursor() as cur:
                # 1. Verificar y crear extensión pgvector
                cur.execute("CREATE EXTENSION IF NOT EXISTS vector")
                logger.info("Extensión pgvector creada o ya existente")
                
                # 2. Recrear tabla noticias_chunks para asegurar compatibilidad
                logger.info("Recreando tabla noticias_chunks...")
                
                # Primero hacer backup si hay datos
                cur.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'noticias_chunks'")
                if cur.fetchone()[0] > 0:
                    cur.execute("SELECT COUNT(*) FROM noticias_chunks")
                    if cur.fetchone()[0] > 0:
                        logger.info("Haciendo backup de datos existentes...")
                        cur.execute("CREATE TABLE IF NOT EXISTS noticias_chunks_backup AS SELECT * FROM noticias_chunks")
                
                # Recrear tabla con estructura correcta
                cur.execute("DROP TABLE IF EXISTS noticias_chunks")
                cur.execute("""
                CREATE TABLE noticias_chunks (
                    id SERIAL PRIMARY KEY,
                    chunk_id TEXT NOT NULL UNIQUE,
                    content TEXT NOT NULL,
                    embedding vector(768),
                    metadata JSONB,
                    source TEXT,
                    url TEXT,
                    title TEXT,
                    date TEXT,
                    document_id TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """)
                
                # 3. Crear índices
                logger.info("Creando índices...")
                
                # Índice para búsqueda por chunk_id
                cur.execute("CREATE INDEX IF NOT EXISTS idx_noticias_chunks_chunk_id ON noticias_chunks(chunk_id)")
                
                # Índice para búsqueda por document_id
                cur.execute("CREATE INDEX IF NOT EXISTS idx_noticias_chunks_document_id ON noticias_chunks(document_id)")
                
                # Índice para búsqueda por fecha
                cur.execute("CREATE INDEX IF NOT EXISTS idx_noticias_chunks_date ON noticias_chunks(date)")
                
                # Índice para búsqueda por fuente
                cur.execute("CREATE INDEX IF NOT EXISTS idx_noticias_chunks_source ON noticias_chunks(source)")
                
                # Índice para búsqueda por texto (GIN para búsqueda full-text)
                cur.execute("CREATE INDEX IF NOT EXISTS idx_noticias_chunks_content_gin ON noticias_chunks USING GIN (to_tsvector('spanish', content))")
                
                # Índice para búsqueda vectorial (HNSW)
                cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_noticias_chunks_embedding ON noticias_chunks 
                USING hnsw (embedding vector_cosine_ops) WITH (ef_construction = 128, m = 16)
                """)
                
                conn.commit()
                logger.info("✅ Base de datos corregida exitosamente")
                
                # 4. Restaurar datos si había backup
                cur.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'noticias_chunks_backup'")
                if cur.fetchone()[0] > 0:
                    cur.execute("SELECT COUNT(*) FROM noticias_chunks_backup")
                    count = cur.fetchone()[0]
                    if count > 0:
                        logger.info(f"Restaurando {count} registros desde backup...")
                        cur.execute("""
                        INSERT INTO noticias_chunks (
                            chunk_id, content, embedding, metadata, source, url, title, date,
                            document_id, created_at, updated_at
                        )
                        SELECT
                            chunk_id, content, embedding, metadata, source, url, title, date,
                            document_id, created_at, updated_at
                        FROM noticias_chunks_backup
                        ON CONFLICT (chunk_id) DO NOTHING
                        """)
                        conn.commit()
                        
                        # Verificar registros restaurados
                        cur.execute("SELECT COUNT(*) FROM noticias_chunks")
                        restored = cur.fetchone()[0]
                        logger.info(f"✅ {restored} registros restaurados")
        except Exception as e:
            conn.rollback()
            logger.error(f"Error corrigiendo base de datos: {e}")
        finally:
            conn.close()
    except Exception as e:
        logger.error(f"Error conectando a base de datos: {e}")

def main():
    """Función principal."""
    print("=== Diagnóstico y reparación de pgvector ===")
    print("1. Verificando estado actual de la base de datos...")
    check_tables()
    
    if input("\n¿Desea corregir la base de datos? (s/n): ").lower() == 's':
        print("\n2. Aplicando correcciones...")
        fix_database()
        
        print("\n3. Verificando estado final...")
        check_tables()
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
