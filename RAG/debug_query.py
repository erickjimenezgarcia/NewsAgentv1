"""
Script para depurar y solucionar problemas de consulta en RAG.
No instala dependencias adicionales, usa las existentes.
"""

import os
import json
import time
import logging
from dotenv import load_dotenv
from RAG.vector_store import VectorDBManager

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('RagDebug')

# Cargar variables de entorno
load_dotenv()

def inspect_config():
    """Revisar la configuración actual de VectorDBManager"""
    vectordb = VectorDBManager()
    
    # Mostrar información de configuración
    logger.info(f"Esquema configurado: {vectordb.schema}")
    logger.info(f"Tabla configurada: {vectordb.table_name}")
    logger.info(f"Ruta completa tabla: {vectordb.full_table_name}")
    
    return vectordb

def check_data_existence():
    """Verificar si existen datos en la tabla configurada"""
    vectordb = inspect_config()
    
    # Realizar conteo directo a través de la conexión de VectorDBManager
    try:
        conn = vectordb._get_connection()
        try:
            with conn.cursor() as cur:
                # Conteo total
                cur.execute(f"SELECT COUNT(*) FROM {vectordb.full_table_name}")
                total_count = cur.fetchone()[0]
                logger.info(f"Total de registros en {vectordb.full_table_name}: {total_count}")
                
                # Ver formatos de fecha
                cur.execute(f"SELECT DISTINCT date FROM {vectordb.full_table_name} LIMIT 10")
                dates = [row[0] for row in cur.fetchall()]
                logger.info(f"Formatos de fecha disponibles: {dates}")
                
                # Conteo por fecha específica
                cur.execute(f"SELECT COUNT(*) FROM {vectordb.full_table_name} WHERE date = %s", ('14032025',))
                date_count = cur.fetchone()[0]
                logger.info(f"Registros con fecha 14032025: {date_count}")
                
                # Probar otros formatos de fecha
                for date_format in ['14/03/2025', '2025-03-14']:
                    cur.execute(f"SELECT COUNT(*) FROM {vectordb.full_table_name} WHERE date = %s", (date_format,))
                    count = cur.fetchone()[0]
                    logger.info(f"Registros con fecha {date_format}: {count}")
                
                return total_count > 0
                
        finally:
            conn.close()
    except Exception as e:
        logger.error(f"Error verificando datos: {e}")
        return False

def test_direct_search():
    """Probar búsqueda directa sin filtros"""
    vectordb = inspect_config()
    
    # Consulta directa de prueba
    query = "agua"
    logger.info(f"Realizando búsqueda directa para: '{query}'")
    
    # Realizar búsqueda semántica sin filtros
    try:
        # Obtener embedding para consulta
        from RAG.embedding_service import EmbeddingService
        embedding_service = EmbeddingService()
        query_embedding = embedding_service.embed_query(query)
        
        # Buscar sin filtros
        start_time = time.time()
        results = vectordb.search(query_embedding, filters={}, limit=5)
        end_time = time.time()
        
        logger.info(f"Búsqueda completada en {end_time - start_time:.3f} segundos")
        logger.info(f"Resultados encontrados: {len(results)}")
        
        # Mostrar resultados
        for i, result in enumerate(results):
            logger.info(f"Resultado {i+1}:")
            logger.info(f"  Contenido: {result.get('content', '')[:100]}...")
            logger.info(f"  Similitud: {result.get('similarity', 0):.4f}")
        
        return len(results) > 0
    except Exception as e:
        logger.error(f"Error en búsqueda directa: {e}")
        return False

def test_modified_query():
    """Prueba una versión modificada de la consulta SQL para asegurar compatibilidad"""
    vectordb = inspect_config()
    query = "agua en Piura"
    
    try:
        # Obtener embedding para consulta
        from RAG.embedding_service import EmbeddingService
        embedding_service = EmbeddingService()
        query_embedding = embedding_service.embed_query(query)
        
        # Modificar temporalmente el método search para eliminar restricciones de tipo
        original_search = vectordb.search
        
        def modified_search(query_embedding, filters=None, limit=10):
            """Versión modificada del método search que evita problemas de tipo"""
            try:
                conn = vectordb._get_connection()
                try:
                    with conn.cursor() as cur:
                        # Usar CAST explícito y eliminar algunos filtros problemáticos
                        sql = f"""
                        SELECT 
                            id, chunk_id, content, source, date, url, title,
                            1 - (embedding <=> CAST(%s AS vector)) AS similarity
                        FROM {vectordb.full_table_name}
                        ORDER BY similarity DESC
                        LIMIT {limit}
                        """
                        
                        cur.execute(sql, (query_embedding,))
                        
                        # Procesar resultados
                        results = []
                        for row in cur.fetchall():
                            results.append({
                                'id': row[0],
                                'chunk_id': row[1],
                                'content': row[2],
                                'source': row[3],
                                'date': row[4],
                                'url': row[5],
                                'title': row[6],
                                'similarity': row[7]
                            })
                        
                        return results
                finally:
                    conn.close()
            except Exception as e:
                logger.error(f"Error en búsqueda modificada: {e}")
                return []
        
        # Reemplazar método temporalmente
        vectordb.search = modified_search
        
        # Ejecutar búsqueda modificada
        logger.info("Ejecutando búsqueda con SQL modificado...")
        results = vectordb.search(query_embedding)
        
        # Restaurar método original
        vectordb.search = original_search
        
        # Mostrar resultados
        logger.info(f"Resultados con SQL modificado: {len(results)}")
        for i, result in enumerate(results[:3]):
            logger.info(f"Resultado {i+1}:")
            logger.info(f"  Contenido: {result.get('content', '')[:100]}...")
            logger.info(f"  Similitud: {result.get('similarity', 0):.4f}")
        
        return len(results) > 0
    except Exception as e:
        logger.error(f"Error en test modificado: {e}")
        return False

def fix_rag_pipeline_temporarily():
    """Modifica temporalmente el código de rag_pipeline.py para obtener resultados"""
    pipeline_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "rag_pipeline.py")
    
    try:
        # Hacer backup
        with open(pipeline_path, 'r', encoding='utf-8') as f:
            original_content = f.read()
            
        backup_path = pipeline_path + ".bak"
        with open(backup_path, 'w', encoding='utf-8') as f:
            f.write(original_content)
            
        logger.info(f"Backup creado en {backup_path}")
        
        # Modificar para usar SQL directo sin filtros
        modified_content = original_content.replace(
            "filters = {'date': date} if date else {}", 
            "# Desactivamos temporalmente el filtro de fecha\n        filters = {}"
        )
        
        # Modificar consulta SQL para usar CAST explícito
        if "def search(" in original_content:
            modified_content = modified_content.replace(
                "1 - (embedding <=> %s::vector) AS similarity", 
                "1 - (embedding <=> CAST(%s AS vector)) AS similarity"
            )
        
        with open(pipeline_path, 'w', encoding='utf-8') as f:
            f.write(modified_content)
            
        logger.info("Pipeline RAG modificado temporalmente para maximizar resultados")
        logger.info("IMPORTANTE: Ejecuta ahora: python RAG/rag_pipeline.py query \"agua en Piura\"")
        logger.info(f"Para restaurar: mv {backup_path} {pipeline_path}")
        
        return True
    except Exception as e:
        logger.error(f"Error modificando pipeline: {e}")
        return False

def create_fix_solution():
    """Crear archivo de solución definitiva"""
    fix_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fix_vector_store.py")
    
    try:
        with open(fix_path, 'w', encoding='utf-8') as f:
            f.write("""
                '''
                Corrección para vector_store.py que arregla problemas de tipo y esquema.
                Ejecutar este script para aplicar las correcciones.
                '''

                import os
                import re

                def fix_vector_store():
                    Aplicar correcciones a vector_store.py
                    script_dir = os.path.dirname(os.path.abspath(__file__))
                    vector_store_path = os.path.join(script_dir, "vector_store.py")
                    
                    if not os.path.exists(vector_store_path):
                        print(f"Error: No se encontró el archivo {vector_store_path}")
                        return False
                    
                    # Hacer backup
                    backup_path = vector_store_path + ".bak"
                    with open(vector_store_path, 'r', encoding='utf-8') as f:
                        original_content = f.read()
                        
                    with open(backup_path, 'w', encoding='utf-8') as f:
                        f.write(original_content)
                    
                    print(f"Backup creado en {backup_path}")
                    
                    # Aplicar correcciones
                    modified_content = original_content
                    
                    # 1. Corregir la declaración del tipo vector para evitar problemas de compatibilidad
                    modified_content = re.sub(
                        r'1 - \(embedding <=> %s::vector\) AS similarity',
                        r'1 - (embedding <=> CAST(%s AS vector)) AS similarity',
                        modified_content
                    )
                    
                    # 2. Asegurar que todas las consultas usen self.full_table_name
                    modified_content = re.sub(
                        r'FROM {self\.table_name}',
                        r'FROM {self.full_table_name}',
                        modified_content
                    )
                    
                    # Guardar cambios
                    with open(vector_store_path, 'w', encoding='utf-8') as f:
                        f.write(modified_content)
                    
                    print("✅ Correcciones aplicadas correctamente a vector_store.py")
                    print("Ahora puedes ejecutar: python RAG/rag_pipeline.py query \"agua en Piura\"")
                    
                    return True

                if __name__ == "__main__":
                    fix_vector_store()
                """)
        logger.info(f"Solución definitiva creada en {fix_path}")
        logger.info("Ejecutar: python RAG/fix_vector_store.py")
        
        return True
    except Exception as e:
        logger.error(f"Error creando solución: {e}")
        return False

def main():
    """Función principal"""
    print("\n" + "="*70)
    print(" DIAGNÓSTICO Y SOLUCIÓN DE PROBLEMAS DE CONSULTA RAG ".center(70, "="))
    print("="*70 + "\n")
    
    print("1️⃣ Revisando configuración actual...")
    inspect_config()
    
    print("\n2️⃣ Verificando existencia de datos...")
    has_data = check_data_existence()
    if not has_data:
        print("\n❌ No se encontraron datos en la tabla configurada.")
        print("   Ejecuta primero: python RAG/process_clean_data.py 14032025")
        return 1
    
    print("\n3️⃣ Probando búsqueda directa sin filtros...")
    direct_search_ok = test_direct_search()
    
    if not direct_search_ok:
        print("\n4️⃣ Probando búsqueda con SQL modificado...")
        modified_query_ok = test_modified_query()
        
        if modified_query_ok:
            print("\n✅ La búsqueda modificada funcionó correctamente.")
            print("   El problema está en la sintaxis SQL o en los tipos de datos.")
        else:
            print("\n❌ La búsqueda modificada también falló.")
            print("   Es posible que los datos no contengan información relevante.")
    else:
        print("\n✅ La búsqueda directa funcionó correctamente.")
        print("   El problema está en los filtros de fecha.")
    
    print("\n5️⃣ Creando soluciones...")
    
    # Crear solución permanente
    create_fix_solution()
    
    # Modificar temporalmente para probar
    fix_rag_pipeline_temporarily()
    
    print("\n" + "="*70)
    print(" SOLUCIONES DISPONIBLES ".center(70, "="))
    print("="*70)
    print("\n1. Solución inmediata (temporal):")
    print("   Ejecuta: python RAG/rag_pipeline.py query \"agua en Piura\"")
    print("   (Ya hemos desactivado temporalmente los filtros)")
    
    print("\n2. Solución permanente:")
    print("   Ejecuta: python RAG/fix_vector_store.py")
    print("   Luego: python RAG/rag_pipeline.py query \"regulación de agua potable en Piura\" --date 14032025")
    
    return 0

if __name__ == "__main__":
    main()
