"""
Script simple para verificar directamente la base de datos sin dependencias complejas.
Solo requiere psycopg2-binary que ya debería estar instalado.
"""

import os
import sys
import json
from datetime import datetime

# Intentar importar psycopg2, que debería estar instalado según tu requirements.txt
try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
    print("✅ psycopg2 importado correctamente")
except ImportError as e:
    print(f"❌ Error importando psycopg2: {e}")
    print("Ejecuta: pip install psycopg2-binary")
    sys.exit(1)

# Conexión a la base de datos
def get_db_connection():
    try:
        conn = psycopg2.connect(
            host='localhost',
            port=5432,
            dbname='newsagent',
            user='postgres',
            password='postgres'
        )
        print("✅ Conexión a PostgreSQL establecida")
        return conn
    except Exception as e:
        print(f"❌ Error conectando a PostgreSQL: {e}")
        sys.exit(1)

def check_tables():
    """Verificar tablas disponibles y conteo de registros"""
    print("\n1. VERIFICANDO TABLAS DISPONIBLES:")
    
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # Listar esquemas
                cur.execute("""
                SELECT schema_name 
                FROM information_schema.schemata 
                WHERE schema_name NOT IN ('pg_catalog', 'information_schema')
                """)
                schemas = [row[0] for row in cur.fetchall()]
                print(f"  Esquemas disponibles: {schemas}")
                
                # Listar tablas de todos los esquemas
                for schema in schemas:
                    print(f"\n  Tablas en esquema '{schema}':")
                    cur.execute(f"""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = '{schema}'
                    """)
                    tables = [row[0] for row in cur.fetchall()]
                    
                    if not tables:
                        print(f"    (No hay tablas en {schema})")
                        continue
                    
                    for table in tables:
                        try:
                            # Verificar si la tabla existe
                            cur.execute(f"SELECT COUNT(*) FROM {schema}.{table}")
                            count = cur.fetchone()[0]
                            print(f"    - {table}: {count} registros")
                            
                            # Si la tabla parece contener noticias, mostrar ejemplos
                            if count > 0 and ('noticia' in table.lower() or 'chunk' in table.lower()):
                                print(f"\n    CONTENIDO DE EJEMPLO EN {schema}.{table}:")
                                
                                # Verificar columnas
                                cur.execute(f"""
                                SELECT column_name 
                                FROM information_schema.columns 
                                WHERE table_schema = '{schema}' AND table_name = '{table}'
                                """)
                                columns = [col[0] for col in cur.fetchall()]
                                print(f"    Columnas: {columns}")
                                
                                # Mostrar registros de ejemplo
                                try:
                                    cur.execute(f"SELECT * FROM {schema}.{table} LIMIT 2")
                                    rows = cur.fetchall()
                                    for i, row in enumerate(rows):
                                        print(f"\n    Registro {i+1}:")
                                        for j, col in enumerate(columns):
                                            value = str(row[j])
                                            if len(value) > 100:
                                                value = value[:100] + "..."
                                            print(f"      {col}: {value}")
                                except Exception as e:
                                    print(f"    Error mostrando registros: {e}")
                        except Exception as e:
                            print(f"    Error con tabla {table}: {e}")
                            
        return True
    except Exception as e:
        print(f"Error verificando tablas: {e}")
        return False

def test_direct_query(query_text="agua"):
    """Realizar búsqueda directa en las tablas de noticias"""
    print(f"\n2. BÚSQUEDA DIRECTA DE: '{query_text}'")
    
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # Buscar tablas de noticias o chunks
                cur.execute("""
                SELECT table_schema, table_name 
                FROM information_schema.tables 
                WHERE table_name LIKE '%noticia%' OR table_name LIKE '%chunk%'
                ORDER BY table_schema, table_name
                """)
                
                tables = cur.fetchall()
                if not tables:
                    print("  ❌ No se encontraron tablas de noticias o chunks")
                    return False
                
                found_results = False
                
                # Probar búsqueda en cada tabla
                for row in tables:
                    schema = row["table_schema"]
                    table = row["table_name"]
                    full_table = f"{schema}.{table}"
                    
                    # Verificar si tiene las columnas necesarias
                    try:
                        cur.execute(f"""
                        SELECT column_name 
                        FROM information_schema.columns 
                        WHERE table_schema = '{schema}' AND table_name = '{table}'
                        """)
                        columns = [col["column_name"] for col in cur.fetchall()]
                        
                        # Verificar si tiene columna de texto (content o text)
                        text_column = None
                        for possible_name in ['content', 'text', 'contenido', 'texto']:
                            if possible_name in columns:
                                text_column = possible_name
                                break
                        
                        if not text_column:
                            print(f"  ℹ️ {full_table} no tiene columna de texto reconocible")
                            continue
                        
                        print(f"\n  Buscando en {full_table} (columna: {text_column}):")
                        
                        # Realizar búsqueda simple
                        sql = f"""
                        SELECT * FROM {full_table}
                        WHERE {text_column} ILIKE %s
                        LIMIT 3
                        """
                        cur.execute(sql, (f"%{query_text}%",))
                        results = cur.fetchall()
                        
                        if results:
                            found_results = True
                            print(f"  ✅ {len(results)} resultados encontrados")
                            
                            # Mostrar resultados
                            for i, result in enumerate(results):
                                print(f"\n  Resultado {i+1}:")
                                for col, val in result.items():
                                    if col == text_column:
                                        val_str = str(val)
                                        if len(val_str) > 100:
                                            val_str = val_str[:100] + "..."
                                        print(f"    {col}: {val_str}")
                                    elif col in ['date', 'fecha', 'source', 'fuente', 'url', 'title', 'titulo']:
                                        print(f"    {col}: {val}")
                        else:
                            print(f"  ❌ No se encontraron resultados para '{query_text}'")
                    except Exception as e:
                        print(f"  Error buscando en {full_table}: {e}")
                
                return found_results
    except Exception as e:
        print(f"Error en búsqueda directa: {e}")
        return False

def check_date_formats():
    """Verificar formatos de fecha en las tablas"""
    print("\n3. FORMATOS DE FECHA EN LAS TABLAS:")
    
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # Buscar tablas con columna de fecha
                cur.execute("""
                SELECT 
                    t.table_schema, 
                    t.table_name,
                    c.column_name
                FROM 
                    information_schema.tables t
                JOIN 
                    information_schema.columns c 
                    ON t.table_schema = c.table_schema AND t.table_name = c.table_name
                WHERE 
                    c.column_name IN ('date', 'fecha') AND
                    t.table_schema NOT IN ('pg_catalog', 'information_schema')
                """)
                
                tables = cur.fetchall()
                if not tables:
                    print("  ❌ No se encontraron tablas con columna de fecha")
                    return False
                
                for schema, table, date_column in tables:
                    full_table = f"{schema}.{table}"
                    print(f"\n  Formatos de fecha en {full_table}.{date_column}:")
                    
                    try:
                        # Obtener valores distintos de fecha
                        cur.execute(f"SELECT DISTINCT {date_column} FROM {full_table} LIMIT 10")
                        dates = [row[0] for row in cur.fetchall()]
                        
                        if not dates:
                            print(f"    (No hay valores de fecha en {full_table})")
                            continue
                            
                        # Mostrar ejemplos y conteo
                        for date_value in dates:
                            cur.execute(f"SELECT COUNT(*) FROM {full_table} WHERE {date_column} = %s", (date_value,))
                            count = cur.fetchone()[0]
                            print(f"    '{date_value}': {count} registros")
                            
                            # Verificar si la fecha '14032025' coincide con este valor
                            if date_value == '14032025':
                                print(f"    ✓ COINCIDE con el filtro de fecha '14032025'")
                            else:
                                # Probar otras variantes de la misma fecha
                                date_variants = []
                                try:
                                    # Si parece ser una fecha en algún formato reconocible, generar variantes
                                    if isinstance(date_value, str) and (len(date_value) >= 8 or '/' in date_value or '-' in date_value):
                                        # Eliminar separadores si los hay
                                        clean_date = date_value.replace('/', '').replace('-', '')
                                        
                                        # Si es una cadena de 8 dígitos, intentar varias interpretaciones
                                        if len(clean_date) == 8 and clean_date.isdigit():
                                            if clean_date == '14032025':
                                                print(f"    ✓ Formato diferente pero MISMA FECHA que '14032025'")
                                except:
                                    pass
                    except Exception as e:
                        print(f"    Error verificando fechas en {full_table}: {e}")
        
        return True
    except Exception as e:
        print(f"Error verificando formatos de fecha: {e}")
        return False

def main():
    print("\n" + "="*80)
    print(" DIAGNÓSTICO DIRECTO DE BASE DE DATOS RAG ".center(80, "="))
    print("="*80)
    
    # 1. Verificar tablas disponibles
    check_tables()
    
    # 2. Realizar búsqueda directa
    search_term = "agua" if len(sys.argv) <= 1 else sys.argv[1]
    test_direct_query(search_term)
    
    # 3. Verificar formatos de fecha
    check_date_formats()
    
    print("\n" + "="*80)
    print(" CONCLUSIONES E INSTRUCCIONES ".center(80, "="))
    print("="*80)
    
    print("\n1. Para resolver el problema de dependencias:")
    print("   pip install langchain==0.1.0")
    
    print("\n2. Para resolver el problema de búsqueda:")
    print("   a) Verifica que hay datos en las tablas correctas")
    print("   b) Ajusta el formato de fecha en el filtro para que coincida con los datos")
    print("   c) Si no hay resultados con fecha, prueba sin filtro: python RAG/rag_pipeline.py query \"tu consulta\"")
    
    return 0

if __name__ == "__main__":
    main()
