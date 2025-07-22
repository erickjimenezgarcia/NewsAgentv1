"""
Script para detectar y corregir problemas comunes en la clasificación de URLs.
Este script busca URLs mal formadas o que podrían causar errores en el procesamiento.
"""

import os
import sys
import logging
import json
from urllib.parse import urlparse

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("classification_fix")

def fix_url_dict(url_item):
    """
    Corrige un diccionario de URL para asegurarse de que todos los campos sean del tipo esperado.
    
    Args:
        url_item: Elemento de URL que puede ser string o dict
        
    Returns:
        dict: Diccionario de URL corregido
    """
    # Si es string, convertir a dict
    if isinstance(url_item, str):
        return {"URL": url_item}
    
    # Si ya es dict, validar campos
    if isinstance(url_item, dict):
        # Asegurarse de que 'URL' existe
        if "URL" not in url_item:
            return None  # No podemos procesar un item sin URL
        
        # Asegurarse de que URL es string
        if not isinstance(url_item["URL"], str):
            url_item["URL"] = str(url_item["URL"])
        
        # Si Context es dict, convertirlo a string para evitar errores
        if "Context" in url_item and isinstance(url_item["Context"], dict):
            url_item["Context"] = json.dumps(url_item["Context"])
            
        return url_item
    
    # Si no es string ni dict, retornar None
    return None

def fix_classification_input(links_to_process):
    """
    Corrige una lista de URLs para evitar problemas de tipos.
    
    Args:
        links_to_process: Lista de URLs o diccionarios con URLs
        
    Returns:
        list: Lista corregida
    """
    fixed_links = []
    for item in links_to_process:
        fixed_item = fix_url_dict(item)
        if fixed_item:
            fixed_links.append(fixed_item)
    
    logger.info(f"Links arreglados: {len(fixed_links)} de {len(links_to_process)}")
    return fixed_links

def apply_classifier_safely(classifier, links):
    """
    Aplica el clasificador avanzado con manejo de errores.
    
    Args:
        classifier: Instancia del URLClassifier
        links: Lista de URLs para clasificar
        
    Returns:
        dict: Resultado de la clasificación o None si falla
    """
    try:
        # Primero corregir los inputs
        fixed_links = fix_classification_input(links)
        
        # Luego aplicar el clasificador
        return classifier.classify_urls(fixed_links)
    except Exception as e:
        logger.error(f"Error al aplicar clasificador: {e}")
        return None

def check_file_exists(file_path):
    """
    Verifica si un archivo existe y es accesible.
    """
    return os.path.isfile(file_path) and os.access(file_path, os.R_OK)

if __name__ == "__main__":
    # Este código se ejecuta cuando se llama directamente al script
    if len(sys.argv) > 1:
        file_path = sys.argv[1]
        if check_file_exists(file_path):
            with open(file_path, 'r', encoding='utf-8') as f:
                try:
                    data = json.load(f)
                    fixed_data = fix_classification_input(data)
                    with open(f"{file_path}.fixed.json", 'w', encoding='utf-8') as out:
                        json.dump(fixed_data, out, indent=2)
                    logger.info(f"Archivo corregido guardado como {file_path}.fixed.json")
                except Exception as e:
                    logger.error(f"Error procesando archivo: {e}")
        else:
            logger.error(f"Archivo no encontrado o no accesible: {file_path}")
    else:
        logger.info("Uso: python classification_fix.py [ruta_al_archivo_json]")
