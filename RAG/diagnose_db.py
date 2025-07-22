"""
Script para diagnosticar y arreglar problemas de consulta en el sistema RAG.
"""

import os
import sys
import json
import psycopg2
import logging
from psycopg2.extras import RealDictCursor
from typing import List, Dict, Any, Optional

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('DiagnoseDB')

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
    """Inspecciona todas las tablas y sus datos."""
    try:
        with get_connection() as conn:
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
                    
                    # Si hay registros, verificar su estructura
                    if count > 0:
                        # Verificar columnas
                        cur.execute(f"""
                        SELECT column_name
                        FROM information_schema.columns
                        WHERE table_schema = '{schema}' AND table_name = '{table}'
                        ORDER BY ordinal_position
                        """)
                        columns = [col[0] for col in cur.fetchall()]
                        logger.info(f"Columnas en {schema}.{table}: {columns}")
                        
                        # Verificar registros de ejemplo
                        try:
                            cur.execute(f"SELECT * FROM {schema}.{table} LIMIT 3")
                            rows = cur.fetchall()
                            logger.info(f"Ejemplos de registros en {schema}.{table}:")
                            for i, row in enumerate(rows):
                                logger.info(f"  Registro {i+1}: {str(row)[:100]}...")
                        except Exception as e:
                            logger.error(f"Error obteniendo registros de ejemplo: {e}")
                            
    except Exception as e:
        logger.error(f"Error durante la inspección: {e}")

def check_date_filter(date: str):
    """Verifica si hay registros para una fecha específica."""
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                # Buscar en public.noticias_chunks
                cur.execute("SELECT COUNT(*) FROM public.noticias_chunks WHERE date = %s", (date,))
                public_count = cur.fetchone()[0]
                logger.info(f"Registros en public.noticias_chunks con fecha {date}: {public_count}")
                
                # Buscar en rag.chunks si existe
                try:
                    cur.execute("SELECT COUNT(*) FROM rag.chunks WHERE date = %s", (date,))
                    rag_count = cur.fetchone()[0]
                    logger.info(f"Registros en rag.chunks con fecha {date}: {rag_count}")
                except:
                    logger.info("La tabla rag.chunks no existe o no es accesible")
                
                # Verificar formatos de fecha
                cur.execute("SELECT DISTINCT date FROM public.noticias_chunks LIMIT 10")
                dates = [row[0] for row in cur.fetchall()]
                logger.info(f"Formatos de fecha encontrados en public.noticias_chunks: {dates}")
                
                # Verificar formatos específicos
                for format in [date, date[:2]+'/'+date[2:4]+'/'+date[4:], date[:4]+'-'+date[4:6]+'-'+date[6:]]:
                    cur.execute("SELECT COUNT(*) FROM public.noticias_chunks WHERE date = %s", (format,))
                    count = cur.fetchone()[0]
                    logger.info(f"Registros con formato de fecha '{format}': {count}")
    except Exception as e:
        logger.error(f"Error verificando filtro de fecha: {e}")

def get_all_records():
    """Obtiene todos los registros de la tabla principal."""
    try:
        with get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # Obtener todos los registros sin filtros
                cur.execute("SELECT * FROM public.noticias_chunks")
                records = cur.fetchall()
                logger.info(f"Total de registros en public.noticias_chunks: {len(records)}")
                
                # Mostrar algunos datos de ejemplo
                if records:
                    for i, record in enumerate(records[:3]):
                        logger.info(f"Registro {i+1}:")
                        logger.info(f"  chunk_id: {record.get('chunk_id')}")
                        logger.info(f"  content: {str(record.get('content', ''))[:100]}...")
                        logger.info(f"  date: {record.get('date')}")
                        logger.info(f"  source: {record.get('source')}")
                        
                    # Buscar mentions de "piura" o "agua"
                    piura_matches = [r for r in records if 'piura' in str(r.get('content', '')).lower()]
                    agua_matches = [r for r in records if 'agua' in str(r.get('content', '')).lower()]
                    
                    logger.info(f"Registros que contienen 'piura': {len(piura_matches)}")
                    logger.info(f"Registros que contienen 'agua': {len(agua_matches)}")
                    
                    if piura_matches:
                        logger.info("Ejemplo de registro con 'piura':")
                        logger.info(f"  content: {str(piura_matches[0].get('content', ''))[:200]}...")
                        
                    if agua_matches:
                        logger.info("Ejemplo de registro con 'agua':")
                        logger.info(f"  content: {str(agua_matches[0].get('content', ''))[:200]}...")
                
                return records
    except Exception as e:
        logger.error(f"Error obteniendo todos los registros: {e}")
        return []

def perform_direct_query(query_text: str):
    """Realiza una consulta directa de texto."""
    try:
        with get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # Hacer consulta text-search directa sin embeddings
                sql = """
                SELECT chunk_id, content, source, date, url, title,
                       ts_rank_cd(to_tsvector('spanish', content), plainto_tsquery('spanish', %s)) AS rank
                FROM public.noticias_chunks
                WHERE to_tsvector('spanish', content) @@ plainto_tsquery('spanish', %s)
                ORDER BY rank DESC
                LIMIT 5
                """
                
                cur.execute(sql, (query_text, query_text))
                results = cur.fetchall()
                
                logger.info(f"Resultados de búsqueda directa para '{query_text}': {len(results)}")
                
                # Mostrar resultados
                if results:
                    for i, result in enumerate(results):
                        logger.info(f"Resultado {i+1}:")
                        logger.info(f"  chunk_id: {result.get('chunk_id')}")
                        logger.info(f"  content: {str(result.get('content', ''))[:200]}...")
                        logger.info(f"  rank: {result.get('rank')}")
                
                return results
    except Exception as e:
        logger.error(f"Error en búsqueda directa: {e}")
        return []

def fix_rag_pipeline():
    """Modifica temporalmente el código de rag_pipeline.py para que no use filtros de fecha."""
    pipeline_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "rag_pipeline.py")
    temp_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "rag_pipeline_temp.py")
    
    try:
        # Hacer backup
        with open(pipeline_path, 'r', encoding='utf-8') as f:
            content = f.read()
            
        with open(temp_path, 'w', encoding='utf-8') as f:
            f.write(content)
            
        logger.info(f"Backup de rag_pipeline.py creado en {temp_path}")
        
        # Modificar el código para eliminar filtros de fecha temporalmente
        modified_content = content.replace(
            "filters = {'date': date} if date else {}", 
            "# Temporalmente desactivados los filtros de fecha\n        filters = {}"
        )
        
        # Guardar versión modificada
        with open(pipeline_path, 'w', encoding='utf-8') as f:
            f.write(modified_content)
            
        logger.info("Filtros de fecha temporalmente desactivados en rag_pipeline.py")
        logger.info("IMPORTANTE: Ejecuta ahora: python RAG/rag_pipeline.py query \"agua en piura\"")
        
        return True
    except Exception as e:
        logger.error(f"Error modificando rag_pipeline.py: {e}")
        return False

def restore_rag_pipeline():
    """Restaura el archivo original de rag_pipeline.py."""
    pipeline_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "rag_pipeline.py")
    temp_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "rag_pipeline_temp.py")
    
    try:
        # Verificar que exista el backup
        if not os.path.exists(temp_path):
            logger.error("No se encuentra el archivo de backup")
            return False
            
        # Restaurar desde backup
        with open(temp_path, 'r', encoding='utf-8') as f:
            content = f.read()
            
        with open(pipeline_path, 'w', encoding='utf-8') as f:
            f.write(content)
            
        # Eliminar backup
        os.remove(temp_path)
        
        logger.info("rag_pipeline.py restaurado correctamente")
        return True
    except Exception as e:
        logger.error(f"Error restaurando rag_pipeline.py: {e}")
        return False

def main():
    """Función principal."""
    print("=" * 50)
    print("DIAGNÓSTICO Y SOLUCIÓN PARA CONSULTAS RAG")
    print("=" * 50)
    
    print("\n1. Inspeccionando base de datos...")
    inspect_tables()
    
    print("\n2. Verificando filtro de fecha...")
    check_date_filter("14032025")
    
    print("\n3. Obteniendo todos los registros...")
    all_records = get_all_records()
    
    if not all_records:
        print("\n❌ No se encontraron registros en la base de datos.")
        print("Es necesario procesar documentos primero con:")
        print("python RAG/process_clean_data.py 14032025")
        return 1
        
    print("\n4. Realizando búsqueda directa...")
    query_results = perform_direct_query("agua piura")
    
    if query_results:
        print("\n✅ Se encontraron resultados directamente en la base de datos.")
        print("El problema está en los filtros o en la conversión de tipos.")
        
        choice = input("\n¿Desea modificar temporalmente rag_pipeline.py para eliminar filtros de fecha? (s/n): ")
        if choice.lower() == 's':
            if fix_rag_pipeline():
                print("\nEjecute ahora: python RAG/rag_pipeline.py query \"agua en piura\"")
                
                restore_choice = input("\nDespués de probar, ¿desea restaurar el archivo original? (s/n): ")
                if restore_choice.lower() == 's':
                    restore_rag_pipeline()
    else:
        print("\n⚠️ No se encontraron resultados en la búsqueda directa.")
        print("Es posible que necesite procesar más documentos o que los datos no contengan")
        print("información sobre 'piura' o 'agua'.")
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
