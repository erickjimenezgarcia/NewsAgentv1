"""
Script simple para verificar los datos en PostgreSQL sin caracteres Unicode.
"""

import sys

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
    print("psycopg2 importado correctamente")
except ImportError:
    print("Error importando psycopg2")
    print("Ejecuta: pip install psycopg2-binary")
    sys.exit(1)

def get_connection():
    """Obtener conexión a PostgreSQL."""
    try:
        conn = psycopg2.connect(
            host='localhost',
            port=5432,
            dbname='newsagent',
            user='postgres',
            password='postgres'
        )
        print("Conexion establecida con PostgreSQL")
        return conn
    except Exception as e:
        print(f"Error conectando a PostgreSQL: {e}")
        sys.exit(1)

def count_records():
    """Contar registros en las tablas principales."""
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                # Buscar tablas importantes
                tables_to_check = [
                    ('public', 'noticias_chunks'),
                    ('rag', 'chunks')
                ]
                
                for schema, table in tables_to_check:
                    try:
                        cur.execute(f"SELECT COUNT(*) FROM {schema}.{table}")
                        count = cur.fetchone()[0]
                        print(f"Tabla {schema}.{table}: {count} registros")
                        
                        if count > 0:
                            # Verificar filtro de fecha
                            cur.execute(f"SELECT COUNT(*) FROM {schema}.{table} WHERE date = '14032025'")
                            date_count = cur.fetchone()[0]
                            print(f"  Con fecha '14032025': {date_count} registros")
                            
                            # Mostrar otras fechas
                            cur.execute(f"SELECT DISTINCT date FROM {schema}.{table} LIMIT 5")
                            dates = [row[0] for row in cur.fetchall()]
                            print(f"  Ejemplos de fechas: {dates}")
                            
                            # Realizar una búsqueda simple
                            cur.execute(f"SELECT COUNT(*) FROM {schema}.{table} WHERE content ILIKE '%agua%'")
                            content_count = cur.fetchone()[0]
                            print(f"  Registros que contienen 'agua': {content_count}")
                            
                            cur.execute(f"SELECT COUNT(*) FROM {schema}.{table} WHERE content ILIKE '%piura%'")
                            piura_count = cur.fetchone()[0]
                            print(f"  Registros que contienen 'piura': {piura_count}")
                            
                            # Buscar específicamente agua en Piura sin filtro de fecha
                            cur.execute(f"""
                            SELECT COUNT(*) FROM {schema}.{table} 
                            WHERE content ILIKE '%agua%' AND content ILIKE '%piura%'
                            """)
                            combined_count = cur.fetchone()[0]
                            print(f"  Registros que contienen 'agua' y 'piura': {combined_count}")
                            
                            if combined_count > 0:
                                cur.execute(f"""
                                SELECT content FROM {schema}.{table}
                                WHERE content ILIKE '%agua%' AND content ILIKE '%piura%'
                                LIMIT 1
                                """)
                                sample = cur.fetchone()[0]
                                print(f"  Ejemplo de contenido: {sample[:100]}...")
                    except Exception as e:
                        print(f"Error verificando {schema}.{table}: {e}")
    except Exception as e:
        print(f"Error general: {e}")

def fix_rag_pipeline():
    """Modificar rag_pipeline.py para ignorar filtro de fecha"""
    try:
        # Ubicación del archivo
        import os
        script_dir = os.path.dirname(os.path.abspath(__file__))
        pipeline_path = os.path.join(script_dir, "rag_pipeline.py")
        
        if not os.path.exists(pipeline_path):
            print(f"No se encontró el archivo {pipeline_path}")
            return False
        
        # Crear backup
        backup_path = pipeline_path + ".backup"
        with open(pipeline_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        with open(backup_path, 'w', encoding='utf-8') as f:
            f.write(content)
        
        print(f"Backup creado en {backup_path}")
        
        # Modificar el archivo
        if "filters['date'] = args.date" in content:
            modified_content = content.replace(
                "filters['date'] = args.date",
                "# Filtro de fecha temporalmente desactivado\n            #filters['date'] = args.date"
            )
            
            with open(pipeline_path, 'w', encoding='utf-8') as f:
                f.write(modified_content)
                
            print("Pipeline modificado para ignorar filtro de fecha")
            print("Ejecuta ahora: python RAG/rag_pipeline.py query \"agua en Piura\"")
            
            return True
        else:
            print("No se encontró la línea para modificar")
            return False
    except Exception as e:
        print(f"Error modificando pipeline: {e}")
        return False

if __name__ == "__main__":
    print("\n=== VERIFICACION DE DATOS EN POSTGRESQL ===\n")
    count_records()
    
    print("\n=== MODIFICACION DE FILTROS ===\n")
    fix_rag_pipeline()
    
    print("\n=== INSTRUCCIONES ===\n")
    print("1. Para resolver problemas de dependencias:")
    print("   pip install psycopg2-binary langchain==0.1.0")
    
    print("\n2. Para probar sin filtro de fecha:")
    print("   python RAG/rag_pipeline.py query \"agua en Piura\"")
    
    print("\n3. Para restaurar el archivo original:")
    print("   copy C:\\Jerson\\SUNASS\\2025\\5_May\\NewsAgent\\RAG\\rag_pipeline.py.backup C:\\Jerson\\SUNASS\\2025\\5_May\\NewsAgent\\RAG\\rag_pipeline.py")
