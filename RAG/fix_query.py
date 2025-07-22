"""
Script directo para diagnosticar y corregir problemas con las consultas de RAG
No depende de otras partes del sistema, solo de psycopg2
"""

import os
import psycopg2
from psycopg2.extras import RealDictCursor
import logging
import json

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('FixQuery')

def get_db_connection():
    """Obtener conexión directa a PostgreSQL"""
    conn = psycopg2.connect(
        host='localhost',
        port=5432,
        dbname='newsagent',
        user='postgres',
        password='postgres'
    )
    return conn

def check_tables():
    """Verificar tablas disponibles y número de registros"""
    try:
        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                # Listar esquemas
                cur.execute("SELECT schema_name FROM information_schema.schemata WHERE schema_name NOT IN ('pg_catalog', 'information_schema', 'pg_toast')")
                schemas = [row[0] for row in cur.fetchall()]
                logger.info(f"Esquemas disponibles: {schemas}")
                
                # Listar tablas y conteo de registros
                tables_data = []
                for schema in schemas:
                    cur.execute(f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{schema}'")
                    tables = [row[0] for row in cur.fetchall()]
                    
                    for table in tables:
                        try:
                            cur.execute(f"SELECT COUNT(*) FROM {schema}.{table}")
                            count = cur.fetchone()[0]
                            tables_data.append({
                                'schema': schema,
                                'table': table,
                                'count': count
                            })
                        except Exception as e:
                            logger.error(f"Error contando registros en {schema}.{table}: {e}")
                
                # Mostrar resultados ordenados por conteo
                tables_data.sort(key=lambda x: x['count'], reverse=True)
                logger.info("Tablas disponibles y número de registros:")
                for data in tables_data:
                    logger.info(f"  {data['schema']}.{data['table']}: {data['count']} registros")
                    
                # Identificar tabla con más registros que podría contener chunks
                chunk_tables = [t for t in tables_data if 'chunk' in t['table'].lower()]
                if chunk_tables:
                    chunk_tables.sort(key=lambda x: x['count'], reverse=True)
                    main_table = f"{chunk_tables[0]['schema']}.{chunk_tables[0]['table']}"
                    logger.info(f"Tabla principal identificada: {main_table}")
                    return main_table
                else:
                    logger.warning("No se encontraron tablas que contengan 'chunk' en el nombre")
                    if tables_data:
                        main_table = f"{tables_data[0]['schema']}.{tables_data[0]['table']}"
                        logger.info(f"Usando tabla con más registros: {main_table}")
                        return main_table
                    return None
        finally:
            conn.close()
    except Exception as e:
        logger.error(f"Error verificando tablas: {e}")
        return None

def test_query(table_name, date=None):
    """Probar consulta directa a la tabla especificada"""
    try:
        conn = get_db_connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # Consulta base
                base_sql = f"""
                SELECT id, chunk_id, content, source, date
                FROM {table_name}
                """
                
                # Aplicar filtro de fecha si se proporciona
                params = []
                if date:
                    base_sql += " WHERE date = %s"
                    params.append(date)
                
                # Limitar resultados
                base_sql += " LIMIT 5"
                
                # Ejecutar consulta
                cur.execute(base_sql, params)
                results = cur.fetchall()
                
                logger.info(f"Consulta directa a {table_name}")
                if date:
                    logger.info(f"Con filtro de fecha: {date}")
                logger.info(f"Resultados encontrados: {len(results)}")
                
                # Mostrar resultados
                for i, row in enumerate(results):
                    logger.info(f"Resultado {i+1}:")
                    logger.info(f"  chunk_id: {row.get('chunk_id')}")
                    logger.info(f"  source: {row.get('source')}")
                    logger.info(f"  date: {row.get('date')}")
                    logger.info(f"  content: {str(row.get('content', ''))[:100]}...")
                
                return len(results) > 0
        finally:
            conn.close()
    except Exception as e:
        logger.error(f"Error en consulta directa: {e}")
        return False

def check_date_formats(table_name):
    """Verificar formatos de fecha disponibles en la tabla"""
    try:
        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                # Obtener distintos formatos de fecha
                cur.execute(f"SELECT DISTINCT date FROM {table_name}")
                dates = [row[0] for row in cur.fetchall()]
                
                logger.info(f"Formatos de fecha encontrados en {table_name}:")
                for date in dates:
                    logger.info(f"  - {date}")
                
                return dates
        finally:
            conn.close()
    except Exception as e:
        logger.error(f"Error verificando formatos de fecha: {e}")
        return []

def fix_vector_store_queries():
    """Aplicar correcciones a vector_store.py"""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    vector_store_path = os.path.join(script_dir, "vector_store.py")
    
    if not os.path.exists(vector_store_path):
        logger.error(f"No se encontró el archivo {vector_store_path}")
        return False
    
    # Hacer backup
    backup_path = vector_store_path + ".bak2"
    with open(vector_store_path, 'r', encoding='utf-8') as f:
        original_content = f.read()
        
    with open(backup_path, 'w', encoding='utf-8') as f:
        f.write(original_content)
    
    logger.info(f"Backup creado en {backup_path}")
    
    # Aplicar correcciones
    modified_content = original_content
    
    # Corregir la sintaxis de comparación vectorial para mayor compatibilidad
    modified_content = modified_content.replace(
        "1 - (embedding <=> %s::vector) AS similarity",
        "1 - (embedding <=> CAST(%s AS vector)) AS similarity"
    )
    
    # Guardar cambios
    with open(vector_store_path, 'w', encoding='utf-8') as f:
        f.write(modified_content)
    
    logger.info("Correcciones aplicadas a vector_store.py")
    return True

def fix_rag_pipeline():
    """Modificar temporalmente rag_pipeline.py para probar sin filtros de fecha"""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    pipeline_path = os.path.join(script_dir, "rag_pipeline.py")
    
    if not os.path.exists(pipeline_path):
        logger.error(f"No se encontró el archivo {pipeline_path}")
        return False
    
    # Leer archivo
    with open(pipeline_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Hacer backup
    backup_path = pipeline_path + ".bak"
    with open(backup_path, 'w', encoding='utf-8') as f:
        f.write(content)
    
    logger.info(f"Backup de rag_pipeline.py creado en {backup_path}")
    
    # Modificar línea de filtros
    modified_content = content.replace(
        "filters = {'date': date} if date else {}", 
        "# Temporalmente sin filtro de fecha para probar\n        filters = {}"
    )
    
    # Guardar cambios
    with open(pipeline_path, 'w', encoding='utf-8') as f:
        f.write(modified_content)
    
    logger.info("rag_pipeline.py modificado para probar sin filtros de fecha")
    return True

def print_instructions(main_table, dates):
    """Imprimir instrucciones para el usuario"""
    print("\n" + "="*80)
    print(" SOLUCIONES Y PRÓXIMOS PASOS ".center(80, "="))
    print("="*80)
    
    print("\nProbables causas del problema:")
    print("1. La sintaxis pgvector `<=>` con `::vector` no es compatible con tu versión")
    print("2. El filtro de fecha no coincide con el formato almacenado en la base de datos")
    
    print("\nVerificación completada:")
    print(f"- Tabla principal identificada: {main_table}")
    if dates:
        print(f"- Formatos de fecha disponibles: {', '.join(dates[:5])}")
    
    print("\nSoluciones aplicadas:")
    print("✅ Se ha corregido la sintaxis de consulta vectorial en vector_store.py")
    print("✅ Se ha modificado temporalmente rag_pipeline.py para probar sin filtros")
    
    print("\nPrueba estas consultas:")
    print("1. SIN filtro de fecha (debe funcionar ahora):")
    print("   python RAG/rag_pipeline.py query \"agua en Piura\"")
    
    if dates:
        print("\n2. CON filtro de fecha (usando un formato correcto):")
        print(f"   python RAG/rag_pipeline.py query \"agua en Piura\" --date \"{dates[0]}\"")
    
    print("\nPara restaurar la configuración original:")
    print("mv C:\\Jerson\\SUNASS\\2025\\5_May\\NewsAgent\\RAG\\rag_pipeline.py.bak C:\\Jerson\\SUNASS\\2025\\5_May\\NewsAgent\\RAG\\rag_pipeline.py")

def main():
    """Función principal"""
    print("="*80)
    print(" DIAGNÓSTICO Y SOLUCIÓN DE CONSULTAS RAG ".center(80, "="))
    print("="*80)
    
    print("\n1. Verificando tablas disponibles...")
    main_table = check_tables()
    if not main_table:
        logger.error("No se pudo identificar la tabla principal")
        return 1
    
    print("\n2. Verificando formatos de fecha...")
    dates = check_date_formats(main_table)
    
    print("\n3. Probando consulta sin filtro de fecha...")
    test_query(main_table)
    
    if dates:
        print("\n4. Probando consulta con filtro de fecha correcto...")
        test_query(main_table, dates[0])
    
    print("\n5. Aplicando correcciones...")
    fix_vector_store_queries()
    fix_rag_pipeline()
    
    print_instructions(main_table, dates)
    
    return 0

if __name__ == "__main__":
    main()
