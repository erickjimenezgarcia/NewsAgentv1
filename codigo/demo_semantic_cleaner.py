#!/usr/bin/env python
"""
Script de demostración para el módulo de limpieza semántica.

Este script muestra cómo utilizar el módulo de limpieza semántica con el archivo
consolidado del 15/04/2025 como ejemplo.

Desarrollado para: SUNASS
Fecha: Mayo 2025
"""

import os
import sys
import json
import logging
from datetime import datetime

# Asegurarse de que el directorio lib esté en el path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from lib.semantic_cleaner import SemanticCleaner, MarkdownConverter

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('demo_semantic_cleaner')

def main():
    """
    Función principal de demostración.
    """
    # Rutas de archivos
    input_file = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 
                            "output", "consolidated_15042025.json")
    
    # Directorio de salida
    output_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 
                             "output", "demo")
    
    # Crear directorio de salida si no existe
    os.makedirs(output_dir, exist_ok=True)
    
    # Rutas de salida
    output_json = os.path.join(output_dir, "clean_15042025_demo.json")
    output_md = os.path.join(output_dir, "clean_15042025_demo.md")
    
    # Cargar archivo JSON
    logger.info(f"Cargando archivo JSON: {input_file}")
    try:
        with open(input_file, 'r', encoding='utf-8') as f:
            json_data = json.load(f)
    except Exception as e:
        logger.error(f"Error al cargar el archivo JSON: {str(e)}")
        return
    
    # Configurar umbral de similitud
    similarity_threshold = 0.7
    
    # Mostrar información
    logger.info(f"Archivo de entrada: {input_file}")
    logger.info(f"Archivo JSON de salida: {output_json}")
    logger.info(f"Archivo Markdown de salida: {output_md}")
    logger.info(f"Umbral de similitud: {similarity_threshold}")
    
    # Inicializar limpiador semántico
    cleaner = SemanticCleaner(
        similarity_threshold=similarity_threshold,
        language='spanish'
    )
    
    # Realizar limpieza semántica
    logger.info("Realizando limpieza semántica...")
    cleaned_json = cleaner.clean_consolidated_json(json_data)
    
    if cleaned_json is None:
        logger.error("Error en la limpieza semántica")
        return
    
    # Guardar JSON limpio
    logger.info(f"Guardando JSON limpio en: {output_json}")
    try:
        with open(output_json, 'w', encoding='utf-8') as f:
            json.dump(cleaned_json, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.error(f"Error al guardar el archivo JSON limpio: {str(e)}")
        return
    
    # Convertir a Markdown
    logger.info(f"Convirtiendo a formato Markdown y guardando en: {output_md}")
    markdown_converter = MarkdownConverter()
    markdown_converter.convert_to_markdown(cleaned_json, output_md)
    
    # Mostrar estadísticas
    if "metadata" in cleaned_json and "stats_summary" in cleaned_json["metadata"]:
        stats = cleaned_json["metadata"]["stats_summary"]
        if "semantic_cleaning" in stats:
            semantic_stats = stats["semantic_cleaning"]
            logger.info("Estadísticas de limpieza semántica:")
            logger.info(f"Textos originales: {semantic_stats.get('original_texts', 'N/A')}")
            logger.info(f"Grupos de similitud: {semantic_stats.get('similar_groups', 'N/A')}")
            logger.info(f"Textos representativos: {semantic_stats.get('representative_texts', 'N/A')}")
            logger.info(f"Porcentaje de reducción: {semantic_stats.get('reduction_percentage', 'N/A')}%")
    
    logger.info("Demostración completada con éxito")
    logger.info(f"Revise los archivos generados en el directorio: {output_dir}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.exception(f"Error no controlado: {str(e)}")
        sys.exit(1)
