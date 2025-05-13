"""
Script principal para ejecutar la limpieza semántica.

Este script toma un archivo JSON consolidado, realiza una limpieza semántica 
para eliminar textos redundantes, y genera un archivo de salida en formato Markdown.

Uso:
    python run_semantic_cleaner.py [opciones]

Opciones:
    --input-json RUTA    Ruta al archivo JSON de entrada
    --output-json RUTA   Ruta para guardar el JSON limpio
    --output-md RUTA     Ruta para guardar el archivo Markdown de salida
    --threshold FLOAT    Umbral de similitud (entre 0.0 y 1.0, por defecto 0.7)
    --language IDIOMA    Idioma para el análisis ('spanish' o 'english', por defecto 'spanish')
"""

import os
import sys
import json
import argparse
import logging
from datetime import datetime

from .semantic_cleaner import SemanticCleaner
from .markdown_converter import MarkdownConverter

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('run_semantic_cleaner')

def parse_arguments():
    """
    Parsea los argumentos de línea de comandos.
    
    Returns:
        argparse.Namespace: Argumentos parseados
    """
    parser = argparse.ArgumentParser(description="Limpieza semántica de archivos JSON consolidados")
    
    parser.add_argument(
        "--input-json",
        type=str,
        help="Ruta al archivo JSON de entrada"
    )
    
    parser.add_argument(
        "--output-json",
        type=str,
        help="Ruta para guardar el JSON limpio"
    )
    
    parser.add_argument(
        "--output-md",
        type=str,
        help="Ruta para guardar el archivo Markdown de salida"
    )
    
    parser.add_argument(
        "--threshold",
        type=float,
        default=0.7,
        help="Umbral de similitud (entre 0.0 y 1.0, por defecto 0.7)"
    )
    
    parser.add_argument(
        "--language",
        type=str,
        default="spanish",
        choices=["spanish", "english"],
        help="Idioma para el análisis ('spanish' o 'english', por defecto 'spanish')"
    )
    
    return parser.parse_args()

def load_json(json_path):
    """
    Carga un archivo JSON.
    
    Args:
        json_path (str): Ruta al archivo JSON
        
    Returns:
        dict: Datos JSON cargados o None si hay error
    """
    try:
        with open(json_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Error al cargar el archivo JSON: {str(e)}")
        return None

def save_json(data, json_path):
    """
    Guarda datos en un archivo JSON.
    
    Args:
        data (dict): Datos a guardar
        json_path (str): Ruta donde guardar el archivo
        
    Returns:
        bool: True si se guarda correctamente, False en caso contrario
    """
    try:
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        return True
    except Exception as e:
        logger.error(f"Error al guardar el archivo JSON: {str(e)}")
        return False

def generate_output_paths(input_path):
    """
    Genera rutas de salida predeterminadas basadas en la ruta de entrada.
    
    Args:
        input_path (str): Ruta del archivo de entrada
        
    Returns:
        tuple: (ruta_json_salida, ruta_md_salida)
    """
    input_dir = os.path.dirname(input_path)
    input_filename = os.path.basename(input_path)
    
    # Eliminar extensión
    filename_base = os.path.splitext(input_filename)[0]
    
    # Generar rutas de salida
    output_json = os.path.join(input_dir, f"{filename_base}_clean.json")
    output_md = os.path.join(input_dir, f"{filename_base}_clean.md")
    
    return output_json, output_md

def main(args=None):
    """
    Función principal del script.
    
    Args:
        args (argparse.Namespace, optional): Argumentos parseados
    """
    # Parsear argumentos si no se proporcionan
    if args is None:
        args = parse_arguments()
    
    # Verificar si se proporcionó un archivo de entrada
    if not args.input_json:
        logger.error("Debe proporcionar una ruta de archivo de entrada (--input-json)")
        return
    
    # Cargar archivo JSON
    logger.info(f"Cargando archivo JSON: {args.input_json}")
    json_data = load_json(args.input_json)
    
    if json_data is None:
        return
    
    # Generar rutas de salida predeterminadas si no se proporcionan
    output_json = args.output_json
    output_md = args.output_md
    
    if not output_json or not output_md:
        default_output_json, default_output_md = generate_output_paths(args.input_json)
        
        if not output_json:
            output_json = default_output_json
            
        if not output_md:
            output_md = default_output_md
    
    # Inicializar limpiador semántico
    cleaner = SemanticCleaner(
        similarity_threshold=args.threshold,
        language=args.language
    )
    
    # Realizar limpieza semántica
    logger.info("Realizando limpieza semántica...")
    cleaned_json = cleaner.clean_consolidated_json(json_data)
    
    if cleaned_json is None:
        logger.error("Error en la limpieza semántica")
        return
    
    # Guardar JSON limpio
    logger.info(f"Guardando JSON limpio en: {output_json}")
    if not save_json(cleaned_json, output_json):
        logger.error("Error al guardar el archivo JSON limpio")
        return
    
    # Convertir a Markdown
    logger.info("Convirtiendo a formato Markdown...")
    markdown_converter = MarkdownConverter()
    markdown_converter.convert_to_markdown(cleaned_json, output_md)
    
    logger.info("Proceso completado con éxito")

if __name__ == "__main__":
    main()
