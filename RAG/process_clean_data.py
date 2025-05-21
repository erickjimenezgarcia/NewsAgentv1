"""
Script para procesar datos limpios generados por clean_data.py y alimentarlos al sistema RAG.
Facilita el procesamiento de los archivos generados en el pipeline de limpieza.
"""

import os
import sys
import json
import argparse
from datetime import datetime
from typing import Dict, Any, List
from pathlib import Path

# Importar el pipeline RAG
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from rag_pipeline import RAGPipeline, load_rag_json

def process_clean_file(file_path: str, output_json: bool = False) -> Dict[str, Any]:
    """
    Procesa un archivo limpio y lo ingiere en el sistema RAG.
    
    Args:
        file_path: Ruta al archivo JSON limpio
        output_json: Si es True, guarda resultados detallados en JSON
        
    Returns:
        Resultados del procesamiento
    """
    print(f"Procesando archivo: {file_path}")
    
    # Cargar datos
    try:
        content_list = load_rag_json(file_path)
        print(f"Se cargaron {len(content_list)} documentos")
    except Exception as e:
        print(f"Error cargando archivo {file_path}: {e}")
        return {"success": False, "error": str(e)}
    
    # Extraer la fecha del nombre del archivo (formato rag_clean_DDMMYYYY.json)
    import re
    date_match = re.search(r'rag_clean_(\d{8})', file_path)
    file_date = date_match.group(1) if date_match else None
    
    # Inicializar pipeline RAG con la fecha del archivo
    pipeline = RAGPipeline()
    
    # Configurar el chunker con la fecha correcta
    pipeline.chunker.default_date = file_date
    
    # Procesar contenido
    print("Iniciando procesamiento RAG...")
    results = pipeline.process_content(content_list)
    
    # Si se solicitó, guardar resultados detallados
    if output_json and results.get('success', False):
        output_path = f"RAG/output/processed_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2, ensure_ascii=False)
        print(f"Resultados detallados guardados en: {output_path}")
    
    return results

def main():
    """Función principal para procesar datos desde la línea de comandos."""
    parser = argparse.ArgumentParser(description='Procesar datos limpios para RAG')
    
    parser.add_argument(
        'date',
        help='Fecha de los datos a procesar (formato DDMMYYYY)'
    )
    
    parser.add_argument(
        '--input-dir',
        default='RAG/data',
        help='Directorio donde buscar el archivo limpio (default: RAG/data)'
    )
    
    parser.add_argument(
        '--output-json',
        action='store_true',
        help='Guardar resultados detallados en JSON'
    )
    
    # Parsear argumentos
    args = parser.parse_args()
    
    # Construir ruta al archivo
    if os.path.isabs(args.input_dir):
        input_dir = args.input_dir
    else:
        # Ruta relativa al directorio raíz del proyecto
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        input_dir = os.path.join(project_root, args.input_dir)
    
    # Buscar archivo rag_clean_FECHA.json
    file_path = os.path.join(input_dir, f"rag_clean_{args.date}.json")
    
    if not os.path.exists(file_path):
        print(f"Error: No se encuentra el archivo {file_path}")
        return 1
    
    # Procesar archivo
    results = process_clean_file(file_path, args.output_json)
    
    # Mostrar resultados
    if results.get('success', False):
        print("\nProcesamiento completado correctamente:")
        print(f"- Documentos procesados: {results['documents_processed']}")
        print(f"- Chunks generados: {results['chunks_generated']}")
        print(f"- Documentos almacenados: {results['documents_stored']}")
        print(f"- Tiempo total: {results['processing_time_seconds']:.2f} segundos")
        
        print("\nEl sistema RAG está listo para consultas. Ejemplo de uso:")
        print(f"python RAG/rag_pipeline.py query \"regulación de agua potable en Piura\" --date {args.date}")
    else:
        print(f"\nError en procesamiento: {results.get('error', 'Desconocido')}")
        return 1
    
    return 0

if __name__ == "__main__":
    # Crear directorios necesarios
    os.makedirs('logs', exist_ok=True)
    os.makedirs('RAG/output', exist_ok=True)
    os.makedirs('embeddings_cache', exist_ok=True)
    
    # Ejecutar función principal
    sys.exit(main())
