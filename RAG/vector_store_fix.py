"""
Script para corregir los problemas de esquema y consultas en PostgreSQL.
"""

import os
import sys
import psycopg2
import logging
from psycopg2.extras import RealDictCursor

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('VectorStoreFix')

def get_connection():
    """Obtiene una conexión a PostgreSQL."""
    return psycopg2.connect(
        host='localhost',
        port=5432,
        dbname='newsagent',
        user='postgres',
        password='postgres'
    )

def inspect_tables():
    """Inspecciona las tablas disponibles y su contenido."""
    try:
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                # Listar todos los esquemas
                cur.execute("SELECT schema_name FROM information_schema.schemata ORDER BY schema_name")
                schemas = [row[0] for row in cur.fetchall()]
                logger.info(f"Esquemas disponibles: {schemas}")
                
                # Listar todas las tablas
                cur.execute("""
                SELECT table_schema, table_name 
                FROM information_schema.tables 
                WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
                ORDER BY table_schema, table_name
                """)
                tables = cur.fetchall()
                
                logger.info(f"Tablas encontradas: {len(tables)}")
                for schema, table in tables:
                    cur.execute(f"SELECT COUNT(*) FROM {schema}.{table}")
                    count = cur.fetchone()[0]
                    logger.info(f"{schema}.{table}: {count} registros")
                    
                    # Mostrar primera fila si hay datos
                    if count > 0:
                        try:
                            cur.execute(f"SELECT * FROM {schema}.{table} LIMIT 1")
                            row = cur.fetchone()
                            columns = [desc[0] for desc in cur.description]
                            logger.info(f"Columnas: {columns}")
                        except Exception as e:
                            logger.error(f"Error mostrando columnas: {e}")
        finally:
            conn.close()
    except Exception as e:
        logger.error(f"Error conectando a la base de datos: {e}")

def fix_schema_issue():
    """Corrige el problema de esquema creando una vista entre las tablas."""
    try:
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                # Verificar la tabla principal
                cur.execute("SELECT COUNT(*) FROM public.noticias_chunks")
                count = cur.fetchone()[0]
                if count == 0:
                    logger.error("La tabla public.noticias_chunks está vacía. No hay nada que arreglar.")
                    return
                
                logger.info(f"Encontrados {count} registros en public.noticias_chunks")
                
                # Verificar la estructura exacta
                cur.execute("""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_schema = 'public' AND table_name = 'noticias_chunks'
                ORDER BY ordinal_position
                """)
                columns = cur.fetchall()
                logger.info("Estructura de public.noticias_chunks:")
                for col_name, col_type in columns:
                    logger.info(f"  - {col_name}: {col_type}")
                
                # Crear esquema rag si no existe
                cur.execute("CREATE SCHEMA IF NOT EXISTS rag")
                
                # Crear tabla rag.chunks si no existe, con estructura compatible
                cur.execute("""
                CREATE TABLE IF NOT EXISTS rag.chunks (
                    id SERIAL PRIMARY KEY,
                    chunk_id TEXT UNIQUE NOT NULL,
                    document_id TEXT,
                    text TEXT,
                    embedding vector(768),
                    metadata JSONB,
                    source TEXT,
                    url TEXT,
                    title TEXT,
                    date TEXT,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                )
                """)
                
                # Verificar si hay datos en rag.chunks
                cur.execute("SELECT COUNT(*) FROM rag.chunks")
                rag_count = cur.fetchone()[0]
                
                if rag_count == 0:
                    # Migrar datos de public.noticias_chunks a rag.chunks
                    logger.info("Migrando datos a rag.chunks...")
                    
                    try:
                        # Intentar hacer la migración con todas las columnas
                        cur.execute("""
                        INSERT INTO rag.chunks (
                            chunk_id, document_id, text, embedding, metadata, source, url, title, date
                        )
                        SELECT 
                            chunk_id, 
                            COALESCE(metadata->>'document_id', chunk_id) as document_id,
                            content as text, 
                            embedding, 
                            metadata, 
                            source, 
                            url, 
                            title, 
                            date
                        FROM public.noticias_chunks
                        ON CONFLICT (chunk_id) DO NOTHING
                        """)
                        
                        conn.commit()
                        logger.info("Migración completada exitosamente")
                        
                        # Verificar cantidad de registros migrados
                        cur.execute("SELECT COUNT(*) FROM rag.chunks")
                        migrated = cur.fetchone()[0]
                        logger.info(f"Registros migrados: {migrated} de {count}")
                        
                    except Exception as e:
                        conn.rollback()
                        logger.error(f"Error en la migración completa: {e}")
                        
                        # Intentar una migración más básica
                        logger.info("Intentando migración básica...")
                        try:
                            cur.execute("""
                            INSERT INTO rag.chunks (chunk_id, text, embedding)
                            SELECT chunk_id, content, embedding FROM public.noticias_chunks
                            ON CONFLICT (chunk_id) DO NOTHING
                            """)
                            
                            conn.commit()
                            logger.info("Migración básica completada")
                            
                            # Verificar
                            cur.execute("SELECT COUNT(*) FROM rag.chunks")
                            migrated = cur.fetchone()[0]
                            logger.info(f"Registros migrados (básico): {migrated} de {count}")
                            
                        except Exception as e2:
                            conn.rollback()
                            logger.error(f"Error en migración básica: {e2}")
                else:
                    logger.info(f"La tabla rag.chunks ya contiene {rag_count} registros")
                
                # Crear vista para compatibilidad
                logger.info("Creando vista para compatibilidad...")
                cur.execute("DROP VIEW IF EXISTS rag.noticias_chunks_view")
                cur.execute("""
                CREATE VIEW rag.noticias_chunks_view AS
                SELECT * FROM public.noticias_chunks
                """)
                
                conn.commit()
                logger.info("Vista rag.noticias_chunks_view creada con éxito")
                
        except Exception as e:
            conn.rollback()
            logger.error(f"Error corrigiendo esquema: {e}")
        finally:
            conn.close()
    except Exception as e:
        logger.error(f"Error conectando a base de datos: {e}")

def print_query_help():
    """Muestra instrucciones para consultar correctamente."""
    print("\n=== INSTRUCCIONES PARA CONSULTAS ===")
    print("Para realizar consultas con éxito, ejecuta:")
    print("""
python RAG/rag_pipeline.py query "tu consulta" --schema public

El parámetro --schema public es CRUCIAL ya que tus datos están en ese esquema.
Alternativamente, puedes establecer la variable de entorno RAG_SCHEMA=public:

# En Windows:
set RAG_SCHEMA=public
python RAG/rag_pipeline.py query "tu consulta"

# En Linux/Mac:
export RAG_SCHEMA=public
python RAG/rag_pipeline.py query "tu consulta"
    """)

def main():
    """Función principal."""
    print("=" * 50)
    print("CORRECCIÓN DE ESQUEMA POSTGRESQL PARA RAG")
    print("=" * 50)
    
    print("\n1. Inspeccionando base de datos...")
    inspect_tables()
    
    print("\n2. Corrigiendo problemas de esquema...")
    fix_schema_issue()
    
    print("\n3. Verificando resultado final...")
    inspect_tables()
    
    print_query_help()
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
