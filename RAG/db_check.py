"""
Verificación directa de la base de datos PostgreSQL
Script minimalista que solo usa psycopg2 para verificar los datos
"""

import psycopg2
from psycopg2.extras import RealDictCursor
import json

# Conexión a la base de datos
conn = psycopg2.connect(
    host='localhost',
    port=5432,
    dbname='newsagent',
    user='postgres',
    password='postgres'
)

print("=== VERIFICACIÓN DE DATOS EN POSTGRESQL ===\n")

# Verificar si hay datos en public.noticias_chunks
with conn.cursor() as cur:
    cur.execute("SELECT COUNT(*) FROM public.noticias_chunks")
    count = cur.fetchone()[0]
    print(f"Total de registros en public.noticias_chunks: {count}")
    
    # Ver si hay datos con la fecha específica
    cur.execute("SELECT COUNT(*) FROM public.noticias_chunks WHERE date = '14032025'")
    date_count = cur.fetchone()[0]
    print(f"Registros con fecha '14032025': {date_count}")
    
    # Ver qué fechas hay disponibles
    cur.execute("SELECT DISTINCT date FROM public.noticias_chunks LIMIT 5")
    dates = [row[0] for row in cur.fetchall()]
    print(f"Formatos de fecha disponibles: {dates}")
    
    # Probar consulta sin filtro de fecha
    print("\n=== BÚSQUEDA POR PALABRAS CLAVE ===\n")
    
    # Búsqueda simple por palabras clave
    cur.execute("""
    SELECT COUNT(*) FROM public.noticias_chunks 
    WHERE content ILIKE '%agua%' AND content ILIKE '%piura%'
    """)
    keyword_count = cur.fetchone()[0]
    print(f"Registros que contienen 'agua' y 'piura': {keyword_count}")
    
    # Mostrar ejemplo si hay coincidencias
    if keyword_count > 0:
        with conn.cursor(cursor_factory=RealDictCursor) as rcur:
            rcur.execute("""
            SELECT chunk_id, content, source, date
            FROM public.noticias_chunks 
            WHERE content ILIKE '%agua%' AND content ILIKE '%piura%'
            LIMIT 2
            """)
            results = rcur.fetchall()
            
            print("\n=== RESULTADOS DE EJEMPLO ===\n")
            for i, result in enumerate(results):
                print(f"Resultado {i+1}:")
                print(f"ID: {result['chunk_id']}")
                print(f"Fecha: {result['date']}")
                print(f"Fuente: {result['source']}")
                content = result['content']
                print(f"Contenido: {content[:200]}...\n")
    else:
        print("\n⚠️ No se encontraron registros que coincidan con 'agua' y 'piura'")
        
        # Verificar si hay registros con agua o piura por separado
        cur.execute("SELECT COUNT(*) FROM public.noticias_chunks WHERE content ILIKE '%agua%'")
        agua_count = cur.fetchone()[0]
        print(f"Registros que contienen 'agua': {agua_count}")
        
        cur.execute("SELECT COUNT(*) FROM public.noticias_chunks WHERE content ILIKE '%piura%'")
        piura_count = cur.fetchone()[0]
        print(f"Registros que contienen 'piura': {piura_count}")

print("\n=== CONCLUSIONES Y SOLUCIONES ===\n")

# Si no hay datos para la fecha específica pero sí hay en general
if count > 0 and date_count == 0:
    print("✅ Tu base de datos tiene registros, pero ninguno con la fecha '14032025'")
    print("   SOLUCIÓN: Prueba consultar sin especificar una fecha o usa una de estas fechas:")
    for date in dates:
        print(f"   - {date}")
    
    print("\n   Comando a usar:")
    if dates:
        print(f"   python RAG/rag_pipeline.py query \"regulación de agua potable en Piura\" --date \"{dates[0]}\"")
    else:
        print("   python RAG/rag_pipeline.py query \"regulación de agua potable en Piura\"")
        
# Si hay datos para la fecha pero no coincide el contenido
elif date_count > 0 and keyword_count == 0:
    print("✅ Tu base de datos tiene registros para la fecha '14032025', pero ninguno contiene 'agua' y 'piura'")
    print("   SOLUCIÓN: Prueba con otras palabras clave o verifica que realmente hay noticias sobre este tema")
    
# Si no hay datos en absoluto
elif count == 0:
    print("❌ Tu base de datos está vacía. Necesitas procesar noticias primero.")
    print("   SOLUCIÓN: Ejecuta el proceso de importación/procesamiento de noticias:")
    print("   python RAG/process_clean_data.py")

conn.close()
