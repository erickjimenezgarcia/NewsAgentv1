"""
Consulta directa a la base de datos sin usar el pipeline RAG completo.
Solo requiere psycopg2-binary que ya está instalado.
"""

import os
import sys
import json
import psycopg2
from psycopg2.extras import RealDictCursor

# Conexión a la base de datos
def get_db_connection():
    return psycopg2.connect(
        host='localhost',
        port=5432,
        dbname='newsagent',
        user='postgres',
        password='postgres'
    )

def inspect_database():
    """Inspeccionar esquemas y tablas en la base de datos"""
    print("\n1. Esquemas y tablas disponibles:")
    
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
            SELECT table_schema, table_name, 
                   (SELECT COUNT(*) FROM information_schema.columns 
                    WHERE table_schema = t.table_schema AND table_name = t.table_name) AS column_count
            FROM information_schema.tables t
            WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
            ORDER BY table_schema, table_name
            """)
            
            tables = cur.fetchall()
            if not tables:
                print("  No se encontraron tablas")
                return
                
            for schema, table, column_count in tables:
                # Contar registros
                try:
                    cur.execute(f"SELECT COUNT(*) FROM {schema}.{table}")
                    count = cur.fetchone()[0]
                    print(f"  {schema}.{table}: {count} registros, {column_count} columnas")
                except Exception as e:
                    print(f"  {schema}.{table}: Error contando registros: {e}")

def get_direct_query_results(query_text, max_results=5):
    """Ejecutar búsqueda directa de texto"""
    print(f"\n2. Búsqueda directa de: '{query_text}'")
    
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # Primero buscamos en public.noticias_chunks
                try:
                    print("\nBúsqueda en public.noticias_chunks:")
                    sql = """
                    SELECT 
                        chunk_id, content, source, date,
                        ts_rank_cd(to_tsvector('spanish', content), plainto_tsquery('spanish', %s)) AS relevance
                    FROM public.noticias_chunks
                    WHERE to_tsvector('spanish', content) @@ plainto_tsquery('spanish', %s)
                    ORDER BY relevance DESC
                    LIMIT %s
                    """
                    cur.execute(sql, (query_text, query_text, max_results))
                    results = cur.fetchall()
                    
                    if results:
                        print(f"  ✅ {len(results)} resultados encontrados")
                        for i, result in enumerate(results):
                            print(f"\n  Resultado {i+1}:")
                            print(f"  - Fragmento: {result['content'][:150]}...")
                            print(f"  - Fuente: {result['source']}")
                            print(f"  - Fecha: {result['date']}")
                            print(f"  - Relevancia: {result['relevance']}")
                    else:
                        print("  ❌ No se encontraron resultados")
                except Exception as e:
                    print(f"  Error en búsqueda: {e}")
                
                # Luego intentamos con rag.chunks si existe
                try:
                    print("\nBúsqueda en rag.chunks:")
                    cur.execute("SELECT COUNT(*) FROM rag.chunks")
                    count = cur.fetchone()["count"]
                    
                    if count > 0:
                        sql = """
                        SELECT 
                            chunk_id, content, source, date,
                            ts_rank_cd(to_tsvector('spanish', content), plainto_tsquery('spanish', %s)) AS relevance
                        FROM rag.chunks
                        WHERE to_tsvector('spanish', content) @@ plainto_tsquery('spanish', %s)
                        ORDER BY relevance DESC
                        LIMIT %s
                        """
                        cur.execute(sql, (query_text, query_text, max_results))
                        results = cur.fetchall()
                        
                        if results:
                            print(f"  ✅ {len(results)} resultados encontrados")
                            for i, result in enumerate(results):
                                print(f"\n  Resultado {i+1}:")
                                print(f"  - Fragmento: {result['content'][:150]}...")
                                print(f"  - Fuente: {result['source']}")
                                print(f"  - Fecha: {result['date']}")
                                print(f"  - Relevancia: {result['relevance']}")
                        else:
                            print("  ❌ No se encontraron resultados")
                    else:
                        print("  ℹ️ La tabla está vacía (0 registros)")
                        
                except Exception as e:
                    print(f"  La tabla rag.chunks no existe o error: {e}")
    except Exception as e:
        print(f"Error de conexión: {e}")

def show_dates():
    """Mostrar formatos de fecha disponibles"""
    print("\n3. Formatos de fecha disponibles:")
    
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # Verificar fechas en public.noticias_chunks
                try:
                    cur.execute("SELECT DISTINCT date FROM public.noticias_chunks")
                    dates = [row[0] for row in cur.fetchall()]
                    if dates:
                        print("  En public.noticias_chunks:")
                        for date in dates[:10]:  # mostrar solo los primeros 10
                            cur.execute("SELECT COUNT(*) FROM public.noticias_chunks WHERE date = %s", (date,))
                            count = cur.fetchone()[0]
                            print(f"  - '{date}': {count} registros")
                    else:
                        print("  No hay fechas en public.noticias_chunks")
                except Exception as e:
                    print(f"  Error verificando fechas en public.noticias_chunks: {e}")
                
                # Verificar fechas en rag.chunks
                try:
                    cur.execute("SELECT DISTINCT date FROM rag.chunks")
                    dates = [row[0] for row in cur.fetchall()]
                    if dates:
                        print("\n  En rag.chunks:")
                        for date in dates[:10]:  # mostrar solo los primeros 10
                            cur.execute("SELECT COUNT(*) FROM rag.chunks WHERE date = %s", (date,))
                            count = cur.fetchone()[0]
                            print(f"  - '{date}': {count} registros")
                    else:
                        print("  No hay fechas en rag.chunks")
                except Exception as e:
                    pass  # Tabla podría no existir
    except Exception as e:
        print(f"Error verificando fechas: {e}")

def main():
    print("\n" + "="*80)
    print(" DIAGNÓSTICO DIRECTO DE NOTICIAS EN POSTGRESQL ".center(80, "="))
    print("="*80)
    
    # 1. Inspeccionar base de datos
    inspect_database()
    
    # 2. Realizar búsqueda directa
    if len(sys.argv) > 1:
        query_text = sys.argv[1]
    else:
        query_text = "agua potable Piura"
    
    get_direct_query_results(query_text)
    
    # 3. Mostrar fechas disponibles
    show_dates()
    
    # 4. Sugerencias
    print("\n" + "="*80)
    print(" SOLUCIONES RECOMENDADAS ".center(80, "="))
    print("="*80)
    
    print("\n1. Para buscar SIN filtros de fecha (recomendado primero):")
    print("   python RAG/direct_query.py \"tu consulta aquí\"")
    
    print("\n2. Para buscar usando el formato de fecha correcto:")
    print("   Usa uno de los formatos listados arriba y ejecuta:")
    print("   python RAG/rag_pipeline.py query \"tu consulta aquí\" --date \"formato_fecha_correcto\"")
    
    print("\n3. Para resolver el problema de dependencias:")
    print("   pip install -r requirements.txt --upgrade")
    print("   pip install langchain>=0.1.0")
    
    return 0

if __name__ == "__main__":
    main()
