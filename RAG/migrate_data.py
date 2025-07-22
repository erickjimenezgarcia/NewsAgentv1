"""
Script para migrar datos entre esquemas en PostgreSQL.
Permite mover datos de public.noticias_chunks a rag.chunks
"""

import os
import psycopg2
import logging
import argparse
from psycopg2.extras import Json

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('DataMigration')

def get_connection():
    """Obtener conexión a PostgreSQL."""
    return psycopg2.connect(
        host='localhost',
        port=5432,
        dbname='newsagent',
        user='postgres',
        password='postgres'
    )

def setup_schema_and_tables():
    """Crear esquema rag y tablas si no existen."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            # Crear extensión pgvector si no existe
            cur.execute("CREATE EXTENSION IF NOT EXISTS vector")
            
            # Crear esquema rag si no existe
            cur.execute("CREATE SCHEMA IF NOT EXISTS rag")
            
            # Crear tabla chunks en esquema rag
            cur.execute("""
            CREATE TABLE IF NOT EXISTS rag.chunks (
                id SERIAL PRIMARY KEY,
                chunk_id TEXT UNIQUE NOT NULL,
                document_id TEXT,
                content TEXT NOT NULL,
                embedding VECTOR(768),
                metadata JSONB,
                source TEXT,
                url TEXT,
                title TEXT,
                date TEXT
            )
            """)
            
            # Crear índices
            cur.execute("CREATE INDEX IF NOT EXISTS idx_chunks_chunk_id ON rag.chunks(chunk_id)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_chunks_document_id ON rag.chunks(document_id)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_chunks_date ON rag.chunks(date)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_chunks_source ON rag.chunks(source)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_chunks_content_gin ON rag.chunks USING GIN (to_tsvector('spanish', content))")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_chunks_embedding ON rag.chunks USING hnsw (embedding vector_cosine_ops) WITH (ef_construction = 128, m = 16)")
            
            logger.info("Esquema y tablas creados correctamente")

def migrate_data(source_schema, source_table, target_schema, target_table, drop_source=False):
    """Migrar datos entre tablas."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            # Verificar si la tabla origen existe
            cur.execute(f"""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = '{source_schema}' 
                AND table_name = '{source_table}'
            )
            """)
            if not cur.fetchone()[0]:
                logger.error(f"La tabla {source_schema}.{source_table} no existe")
                return False
            
            # Contar registros en tabla origen
            cur.execute(f"SELECT COUNT(*) FROM {source_schema}.{source_table}")
            source_count = cur.fetchone()[0]
            logger.info(f"Registros en tabla origen: {source_count}")
            
            if source_count == 0:
                logger.warning("La tabla origen está vacía, no hay datos para migrar")
                return False
            
            # Migrar datos
            try:
                cur.execute(f"""
                INSERT INTO {target_schema}.{target_table} 
                    (chunk_id, document_id, content, embedding, metadata, source, url, title, date)
                SELECT 
                    chunk_id, document_id, content, embedding, metadata, source, url, title, date
                FROM {source_schema}.{source_table}
                ON CONFLICT (chunk_id) DO NOTHING
                """)
                
                conn.commit()
                
                # Verificar registros migrados
                cur.execute(f"SELECT COUNT(*) FROM {target_schema}.{target_table}")
                target_count = cur.fetchone()[0]
                
                logger.info(f"Registros en tabla destino después de migración: {target_count}")
                logger.info(f"Registros migrados: {target_count - (target_count - source_count)}")
                
                # Eliminar tabla origen si se solicita
                if drop_source:
                    cur.execute(f"DROP TABLE {source_schema}.{source_table}")
                    logger.info(f"Tabla {source_schema}.{source_table} eliminada")
                
                return True
            except Exception as e:
                conn.rollback()
                logger.error(f"Error durante la migración: {e}")
                return False

def update_config(schema, table_name):
    """Actualizar archivo de configuración."""
    config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.yaml")
    
    if not os.path.exists(config_path):
        logger.error(f"No se encontró el archivo de configuración en {config_path}")
        return False
    
    with open(config_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Actualizar schema
    if "schema:" in content:
        content = content.replace(
            'schema: "public"', 
            f'schema: "{schema}"'
        )
    else:
        # Si no existe la línea schema, añadirla después de password
        content = content.replace(
            'password: "postgres"', 
            f'password: "postgres"\n  schema: "{schema}"'
        )
    
    # Actualizar table_name
    content = content.replace(
        'table_name: "noticias_chunks"', 
        f'table_name: "{table_name}"'
    )
    
    with open(config_path, 'w', encoding='utf-8') as f:
        f.write(content)
    
    logger.info(f"Configuración actualizada: schema='{schema}', table_name='{table_name}'")
    return True

def main():
    parser = argparse.ArgumentParser(description='Migrar datos entre esquemas en PostgreSQL')
    parser.add_argument('--source-schema', default='public', help='Esquema origen (default: public)')
    parser.add_argument('--source-table', default='noticias_chunks', help='Tabla origen (default: noticias_chunks)')
    parser.add_argument('--target-schema', default='rag', help='Esquema destino (default: rag)')
    parser.add_argument('--target-table', default='chunks', help='Tabla destino (default: chunks)')
    parser.add_argument('--drop-source', action='store_true', help='Eliminar tabla origen después de migrar')
    parser.add_argument('--update-config', action='store_true', help='Actualizar config.yaml con nuevo esquema y tabla')
    
    args = parser.parse_args()
    
    print("\n" + "="*80)
    print(" MIGRACIÓN DE DATOS ENTRE ESQUEMAS ".center(80, "="))
    print("="*80 + "\n")
    
    print(f"Origen: {args.source_schema}.{args.source_table}")
    print(f"Destino: {args.target_schema}.{args.target_table}")
    print(f"Eliminar origen: {'Sí' if args.drop_source else 'No'}")
    print(f"Actualizar config: {'Sí' if args.update_config else 'No'}")
    
    # Confirmar operación
    confirm = input("\n¿Confirmar migración? (s/n): ")
    if confirm.lower() != 's':
        print("Operación cancelada")
        return
    
    # Crear esquema y tablas destino
    print("\nCreando esquema y tablas destino...")
    setup_schema_and_tables()
    
    # Migrar datos
    print("\nMigrando datos...")
    success = migrate_data(
        args.source_schema, 
        args.source_table, 
        args.target_schema, 
        args.target_table,
        args.drop_source
    )
    
    if success and args.update_config:
        print("\nActualizando configuración...")
        update_config(args.target_schema, args.target_table)
    
    if success:
        print("\n✅ Migración completada con éxito")
        print("\nPuedes probar el sistema con:")
        print(f"python RAG/rag_pipeline.py query \"agua en Piura\" --date 16052025")
        print(f"streamlit run RAG/app_pgvector.py")
    else:
        print("\n❌ Error durante la migración")

if __name__ == "__main__":
    main()
