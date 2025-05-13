#!/usr/bin/env python
"""
Script para ejecutar la limpieza semántica como parte del proceso diario.

Este script forma parte del proceso de ejecución diario y se encarga de tomar el archivo 
JSON consolidado más reciente, realizar la limpieza semántica para eliminar textos 
redundantes, y generar un archivo limpio en formatos JSON y Markdown.

Desarrollado para: SUNASS
Fecha: Mayo 2025
"""

import os
import sys
import json
import logging
import argparse
from datetime import datetime, timedelta
import glob

# Asegurarse de que el directorio lib esté en el path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from lib.semantic_cleaner import SemanticCleaner, MarkdownConverter

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs', 'semantic_cleaner.log')),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('clean_news')

def parse_arguments():
    """
    Parsea los argumentos de línea de comandos.
    
    Returns:
        argparse.Namespace: Argumentos parseados
    """
    parser = argparse.ArgumentParser(description="Limpieza semántica de archivos JSON consolidados")
    
    parser.add_argument(
        "--date",
        type=str,
        help="Fecha de los datos en formato DDMMYYYY (por defecto, se usa la fecha actual - 1 día)"
    )
    
    parser.add_argument(
        "--input-dir",
        type=str,
        default=os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "output"),
        help="Directorio que contiene los archivos JSON consolidados"
    )
    
    parser.add_argument(
        "--output-dir",
        type=str,
        default=os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "output", "clean"),
        help="Directorio donde se guardarán los archivos de salida"
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

def get_date_string(args):
    """
    Obtiene la cadena de fecha a procesar.
    
    Args:
        args (argparse.Namespace): Argumentos parseados
        
    Returns:
        str: Cadena de fecha en formato DDMMYYYY
    """
    if args.date:
        return args.date
        
    # Usar fecha de ayer por defecto
    yesterday = datetime.now() - timedelta(days=1)
    return yesterday.strftime("%d%m%Y")

def find_consolidated_file(input_dir, date_str):
    """
    Busca el archivo consolidado para la fecha especificada.
    
    Args:
        input_dir (str): Directorio de entrada
        date_str (str): Cadena de fecha en formato DDMMYYYY
        
    Returns:
        str: Ruta al archivo consolidado o None si no se encuentra
    """
    # Patrón de búsqueda
    pattern = os.path.join(input_dir, f"consolidated_{date_str}.json")
    
    # Buscar archivos que coincidan con el patrón
    matches = glob.glob(pattern)
    
    if not matches:
        logger.error(f"No se encontró archivo consolidado para la fecha {date_str}")
        return None
        
    # Devolver el primer archivo que coincida
    return matches[0]

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

def ensure_directory_exists(directory):
    """
    Asegura que un directorio exista, creándolo si es necesario.
    
    Args:
        directory (str): Ruta del directorio
        
    Returns:
        bool: True si el directorio existe o se crea correctamente, False en caso contrario
    """
    try:
        os.makedirs(directory, exist_ok=True)
        return True
    except Exception as e:
        logger.error(f"Error al crear el directorio {directory}: {str(e)}")
        return False

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

def main():
    """
    Función principal del script.
    """
    # Parsear argumentos
    args = parse_arguments()
    
    # Obtener fecha a procesar
    date_str = get_date_string(args)
    logger.info(f"Procesando datos para la fecha: {date_str}")
    
    # Encontrar archivo consolidado
    input_file = find_consolidated_file(args.input_dir, date_str)
    if not input_file:
        return
    
    # Cargar archivo JSON
    logger.info(f"Cargando archivo JSON: {input_file}")
    json_data = load_json(input_file)
    
    if json_data is None:
        return
    
    # Asegurar que el directorio de salida exista
    if not ensure_directory_exists(args.output_dir):
        return
    
    # Generar rutas de salida
    output_base = os.path.join(args.output_dir, f"clean_{date_str}")
    output_json = f"{output_base}.json"
    output_md = f"{output_base}.md"
    
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
    logger.info(f"Convirtiendo a formato Markdown y guardando en: {output_md}")
    markdown_converter = MarkdownConverter()
    markdown_converter.convert_to_markdown(cleaned_json, output_md)
    
    logger.info("Proceso completado con éxito")

def run_semantic_cleaning(date_str=None, input_dir=None, output_dir=None, threshold=0.7, language="spanish"):
    """
    Función para ejecutar la limpieza semántica desde otro script (como main.py).
    
    Args:
        date_str (str, optional): Fecha en formato DDMMYYYY. Si no se proporciona, se usa la de ayer.
        input_dir (str, optional): Directorio de entrada. Si no se proporciona, se usa el predeterminado.
        output_dir (str, optional): Directorio de salida. Si no se proporciona, se usa el predeterminado.
        threshold (float, optional): Umbral de similitud. Por defecto 0.7.
        language (str, optional): Idioma para el análisis. Por defecto "spanish".
        
    Returns:
        tuple: (ruta_json_limpio, ruta_markdown_limpio) o (None, None) si hay error
    """
    # Crear objeto args simulado
    class Args:
        pass
    
    args = Args()
    args.date = date_str
    args.input_dir = input_dir or os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "output")
    args.output_dir = output_dir or os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "output", "clean")
    args.threshold = threshold
    args.language = language
    
    # Crear directorio de logs si no existe
    log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs')
    ensure_directory_exists(log_dir)
    
    try:
        # Obtener fecha a procesar
        date_str = get_date_string(args)
        logger.info(f"Procesando datos para la fecha: {date_str}")
        
        # Encontrar archivo consolidado
        input_file = find_consolidated_file(args.input_dir, date_str)
        if not input_file:
            return None, None
        
        # Cargar archivo JSON
        logger.info(f"Cargando archivo JSON: {input_file}")
        json_data = load_json(input_file)
        
        if json_data is None:
            return None, None
        
        # Asegurar que el directorio de salida exista
        if not ensure_directory_exists(args.output_dir):
            return None, None
        
        # Generar rutas de salida
        output_base = os.path.join(args.output_dir, f"clean_{date_str}")
        output_json = f"{output_base}.json"
        output_md = f"{output_base}.md"
        
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
            return None, None
        
        # Guardar JSON limpio
        logger.info(f"Guardando JSON limpio en: {output_json}")
        if not save_json(cleaned_json, output_json):
            logger.error("Error al guardar el archivo JSON limpio")
            return None, None
        
        # Convertir a Markdown
        logger.info(f"Convirtiendo a formato Markdown y guardando en: {output_md}")
        markdown_converter = MarkdownConverter()
        markdown_converter.convert_to_markdown(cleaned_json, output_md)
        
        logger.info("Proceso completado con éxito")
        return output_json, output_md
        
    except Exception as e:
        logger.exception(f"Error en run_semantic_cleaning: {str(e)}")
        return None, None

if __name__ == "__main__":
    # Crear directorio de logs si no existe
    log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs')
    ensure_directory_exists(log_dir)
    
    try:
        main()
    except Exception as e:
        logger.exception(f"Error no controlado: {str(e)}")
        sys.exit(1)
