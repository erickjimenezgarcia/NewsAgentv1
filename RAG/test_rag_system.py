#!/usr/bin/env python
"""
Script para probar el sistema RAG completo.
Verifica la configuración de API, conexión a PostgreSQL y procesamiento de datos.
"""

import os
import sys
import time
import argparse
from pathlib import Path
from typing import Dict, Any, List, Optional

# Añadir directorio raíz al path para importar módulos
sys.path.append(str(Path(__file__).parent.parent))

# Importar componentes del sistema RAG
from config_api import is_api_configured, configure_api_key, get_api_key
from embedding_service import EmbeddingService
from vector_store import VectorDBManager
from chunker import SmartChunker
from rag_pipeline import RAGPipeline

def check_api_configuration() -> bool:
    """
    Verifica si la API key de Google está configurada.
    
    Returns:
        True si está configurada, False en caso contrario
    """
    print("\n1. Verificando configuración de API...")
    
    if is_api_configured('google'):
        api_key = get_api_key('google')
        masked_key = f"{api_key[:4]}...{api_key[-4:]}" if len(api_key) > 8 else "****"
        print(f"✅ API key de Google configurada: {masked_key}")
        return True
    else:
        print("❌ API key de Google NO configurada.")
        print("   Ejecutando configuración interactiva...")
        return configure_api_key('google', interactive=True)

def test_embedding_service() -> bool:
    """
    Prueba el servicio de embeddings.
    
    Returns:
        True si funciona correctamente, False en caso contrario
    """
    print("\n2. Probando servicio de embeddings...")
    
    try:
        # Crear servicio de embeddings
        embedding_service = EmbeddingService()
        
        # Texto de prueba
        test_chunks = [
            {
                "chunk_id": "test_chunk_1",
                "text": "Este es un texto de prueba para el sistema RAG de SUNASS.",
                "metadata": {"source": "test"}
            }
        ]
        
        # Procesar chunks
        start_time = time.time()
        result = embedding_service.get_embeddings(test_chunks)
        elapsed_time = time.time() - start_time
        
        if len(result) > 0 and 'embedding' in result[0] and len(result[0]['embedding']) > 0:
            print(f"✅ Embeddings generados correctamente en {elapsed_time:.2f} segundos")
            print(f"   Dimensiones: {len(result[0]['embedding'])}")
            return True
        else:
            print("❌ Error generando embeddings: respuesta vacía o incorrecta")
            return False
    except Exception as e:
        print(f"❌ Error en servicio de embeddings: {e}")
        return False

def test_database_connection(connection_string: Optional[str] = None) -> bool:
    """
    Prueba la conexión a la base de datos PostgreSQL.
    
    Args:
        connection_string: Cadena de conexión a PostgreSQL (opcional)
        
    Returns:
        True si la conexión es exitosa, False en caso contrario
    """
    print("\n3. Probando conexión a PostgreSQL...")
    
    try:
        # Crear gestor de base de datos vectorial
        vector_db = VectorDBManager(connection_string=connection_string)
        
        # Probar conexión
        db_info = vector_db.get_database_info()
        
        print(f"✅ Conexión a PostgreSQL exitosa")
        print(f"   Versión PostgreSQL: {db_info.get('version', 'Desconocida')}")
        print(f"   Extensión pgvector: {'Instalada' if db_info.get('pgvector_installed', False) else 'No instalada'}")
        
        if not db_info.get('pgvector_installed', False):
            print("⚠️ La extensión pgvector no está instalada en la base de datos")
            print("   Ejecute: CREATE EXTENSION vector; en su base de datos PostgreSQL")
            return False
            
        return True
    except Exception as e:
        print(f"❌ Error conectando a PostgreSQL: {e}")
        print("\nPosibles soluciones:")
        print("1. Verifique que PostgreSQL esté en ejecución")
        print("2. Si usa Docker, verifique que el contenedor esté activo:")
        print("   docker ps | grep pgvector")
        print("3. Verifique la cadena de conexión en config.yaml")
        print("4. Para PostgreSQL en Docker, use:")
        print("   postgresql://postgres:postgres@localhost:5432/newsagent")
        return False

def test_chunking() -> bool:
    """
    Prueba el proceso de chunking.
    
    Returns:
        True si funciona correctamente, False en caso contrario
    """
    print("\n4. Probando sistema de chunking...")
    
    try:
        # Crear chunker
        chunker = SmartChunker()
        
        # Texto de prueba
        test_content = [
            {
                "id": "test_doc_1",
                "text": "Este es un documento de prueba para el sistema RAG de SUNASS. " * 20,
                "metadata": {
                    "source": "test",
                    "title": "Documento de prueba",
                    "date": "2025-05-15"
                }
            }
        ]
        
        # Procesar contenido
        chunks = chunker.process_content(test_content)
        
        if len(chunks) > 0:
            print(f"✅ Chunking realizado correctamente")
            print(f"   Documento dividido en {len(chunks)} chunks")
            return True
        else:
            print("❌ Error en chunking: no se generaron chunks")
            return False
    except Exception as e:
        print(f"❌ Error en sistema de chunking: {e}")
        return False

def test_rag_pipeline(connection_string: Optional[str] = None) -> bool:
    """
    Prueba el pipeline RAG completo.
    
    Args:
        connection_string: Cadena de conexión a PostgreSQL (opcional)
        
    Returns:
        True si funciona correctamente, False en caso contrario
    """
    print("\n5. Probando pipeline RAG completo...")
    
    try:
        # Crear pipeline RAG
        pipeline = RAGPipeline(connection_string=connection_string)
        
        # Contenido de prueba
        test_content = [
            {
                "id": "test_doc_pipeline",
                "text": "SUNASS es el organismo regulador de agua y saneamiento en Perú. " * 10,
                "metadata": {
                    "source": "test_pipeline",
                    "title": "Documento de prueba pipeline",
                    "date": "2025-05-15"
                }
            }
        ]
        
        # Procesar contenido
        start_time = time.time()
        result = pipeline.process_content(test_content)
        elapsed_time = time.time() - start_time
        
        if result.get('success', False):
            print(f"✅ Pipeline RAG ejecutado correctamente en {elapsed_time:.2f} segundos")
            print(f"   Documentos procesados: {result.get('documents_processed', 0)}")
            print(f"   Chunks generados: {result.get('chunks_generated', 0)}")
            print(f"   Documentos almacenados: {result.get('documents_stored', 0)}")
            
            # Probar consulta
            query = "¿Qué es SUNASS?"
            print(f"\n   Probando consulta: '{query}'")
            
            query_result = pipeline.query(query, top_k=3)
            
            if query_result.get('success', False) and len(query_result.get('results', [])) > 0:
                print(f"✅ Consulta ejecutada correctamente")
                print(f"   Resultados encontrados: {len(query_result.get('results', []))}")
                return True
            else:
                print(f"⚠️ Consulta ejecutada pero no se encontraron resultados relevantes")
                return True
        else:
            print(f"❌ Error en pipeline RAG: {result.get('error', 'Desconocido')}")
            return False
    except Exception as e:
        print(f"❌ Error en pipeline RAG: {e}")
        return False

def main():
    """Función principal para probar el sistema RAG."""
    parser = argparse.ArgumentParser(description='Probar sistema RAG')
    
    parser.add_argument(
        '--connection-string',
        help='Cadena de conexión a PostgreSQL (ej: postgresql://usuario:contraseña@localhost:5432/newsagent)'
    )
    
    parser.add_argument(
        '--skip-api',
        action='store_true',
        help='Omitir prueba de API'
    )
    
    parser.add_argument(
        '--skip-db',
        action='store_true',
        help='Omitir prueba de base de datos'
    )
    
    # Parsear argumentos
    args = parser.parse_args()
    
    print("=" * 60)
    print("PRUEBA DE SISTEMA RAG PARA NEWSAGENT")
    print("=" * 60)
    
    # Lista de pruebas y resultados
    tests = []
    
    # 1. Verificar API
    if not args.skip_api:
        api_ok = check_api_configuration()
        tests.append(("Configuración de API", api_ok))
        
        if api_ok:
            embedding_ok = test_embedding_service()
            tests.append(("Servicio de embeddings", embedding_ok))
        else:
            tests.append(("Servicio de embeddings", False))
    
    # 2. Verificar base de datos
    if not args.skip_db:
        db_ok = test_database_connection(args.connection_string)
        tests.append(("Conexión a PostgreSQL", db_ok))
    
    # 3. Verificar chunking
    chunking_ok = test_chunking()
    tests.append(("Sistema de chunking", chunking_ok))
    
    # 4. Verificar pipeline completo
    if (args.skip_api or tests[1][1]) and (args.skip_db or (not args.skip_db and db_ok)):
        pipeline_ok = test_rag_pipeline(args.connection_string)
        tests.append(("Pipeline RAG completo", pipeline_ok))
    
    # Resumen de resultados
    print("\n" + "=" * 60)
    print("RESUMEN DE PRUEBAS")
    print("=" * 60)
    
    all_ok = True
    for test_name, test_result in tests:
        status = "✅ CORRECTO" if test_result else "❌ ERROR"
        print(f"{test_name}: {status}")
        all_ok = all_ok and test_result
    
    print("\nEstado general del sistema:", "✅ OPERATIVO" if all_ok else "⚠️ REQUIERE ATENCIÓN")
    
    if not all_ok:
        print("\nRecomendaciones:")
        print("1. Revise los mensajes de error anteriores")
        print("2. Verifique la configuración en config.yaml")
        print("3. Asegúrese de que PostgreSQL esté en ejecución con pgvector")
        print("4. Ejecute 'python RAG/setup_api.py' para configurar la API key")
    
    return 0 if all_ok else 1

if __name__ == "__main__":
    # Crear directorios necesarios
    os.makedirs('logs', exist_ok=True)
    os.makedirs('RAG/output', exist_ok=True)
    os.makedirs('embeddings_cache', exist_ok=True)
    
    # Ejecutar función principal
    sys.exit(main())
