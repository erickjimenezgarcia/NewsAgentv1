"""
Fix para el problema de extracción de textos de Facebook en la limpieza semántica.

Este módulo proporciona una solución para el error de extracción de textos
de Facebook en el proceso de limpieza semántica.
"""

import os
import json
import logging
from datetime import datetime

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('facebook_extractor_fix')

def fix_facebook_texts_extraction(date_str):
    """
    Corrige el problema de extracción de textos de Facebook.
    
    Args:
        date_str (str): Fecha de procesamiento (formato DDMMAAAA)
        
    Returns:
        bool: True si se corrigió correctamente, False en caso contrario
    """
    # Rutas de archivos - Usar rutas relativas al proyecto
    # Determinar la raíz del proyecto (2 niveles arriba de semantic_cleaner)
    current_dir = os.path.dirname(os.path.abspath(__file__))
    lib_dir = os.path.dirname(current_dir)
    codigo_dir = os.path.dirname(lib_dir)
    project_root = os.path.dirname(codigo_dir)
    
    # Construir rutas absolutas usando la raíz del proyecto
    facebook_path = os.path.join(project_root, 'output', f'facebook_texts_{date_str}.json')
    consolidated_path = os.path.join(project_root, 'output', f'consolidated_{date_str}.json')
    clean_dir = os.path.join(project_root, 'output', 'clean')
    clean_json_path = os.path.join(clean_dir, f'clean_{date_str}.json')
    clean_md_path = os.path.join(clean_dir, f'clean_{date_str}.md')
    
    # Imprimir rutas para depuración
    logger.info(f"Ruta del proyecto: {project_root}")
    logger.info(f"Buscando archivo de Facebook en: {facebook_path}")
    
    # Si el archivo no existe en la ruta principal, verificar en la ruta alternativa (para compatibilidad)
    if not os.path.exists(facebook_path):
        alt_facebook_path = os.path.join(os.path.dirname(project_root), 'output', f'facebook_texts_{date_str}.json')
        logger.info(f"Archivo no encontrado, probando ruta alternativa: {alt_facebook_path}")
        if os.path.exists(alt_facebook_path):
            facebook_path = alt_facebook_path
            logger.info(f"Usando ruta alternativa para el archivo de Facebook: {facebook_path}")
    
    # Verificar que los archivos existan
    if not os.path.exists(facebook_path):
        logger.error(f"No se encontró el archivo de textos de Facebook: {facebook_path}")
        return False
    
    if not os.path.exists(clean_json_path):
        logger.error(f"No se encontró el archivo JSON limpio: {clean_json_path}")
        return False
    
    if not os.path.exists(clean_md_path):
        logger.error(f"No se encontró el archivo Markdown limpio: {clean_md_path}")
        return False
    
    try:
        # Cargar datos
        with open(facebook_path, 'r', encoding='utf-8') as f:
            facebook_data = json.load(f)
        
        with open(clean_json_path, 'r', encoding='utf-8') as f:
            clean_data = json.load(f)
        
        # Verificar la estructura de los datos
        if not facebook_data:
            logger.warning("No hay datos de Facebook para procesar")
            return False
        
        # Asegurarse de que la estructura necesaria existe en clean_data
        if "extracted_content" not in clean_data:
            clean_data["extracted_content"] = {}
        
        # Añadir los textos de Facebook al JSON limpio
        clean_data["extracted_content"]["facebook_texts"] = facebook_data
        
        # Guardar el JSON limpio actualizado
        with open(clean_json_path, 'w', encoding='utf-8') as f:
            json.dump(clean_data, f, ensure_ascii=False, indent=2)
        
        # Depurar la estructura de los datos de Facebook
        logger.info(f"Estructura de los datos de Facebook: {list(facebook_data.keys())[0] if facebook_data else 'No hay datos'}")
        if facebook_data and list(facebook_data.values())[0]:
            sample_entry = list(facebook_data.values())[0]
            logger.info(f"Claves en la primera entrada: {list(sample_entry.keys())}")
        
        # Generar sección de Facebook para el Markdown
        md_fb_section = "\n## Contenido de Facebook\n\n"
        for url, data in facebook_data.items():
            # Acceder correctamente a 'extracted_text' según la estructura
            text = data.get("extracted_text", "")
            
            # Si no hay texto, imprimir información para depuración
            if not text:
                logger.warning(f"No se encontró 'extracted_text' en los datos de Facebook para la URL: {url}")
                logger.info(f"Claves disponibles: {list(data.keys())}")
                # Intentar diferentes variantes de la clave
                text = data.get("text", data.get("content", ""))
            
            if text:
                # Tomar las primeras líneas para el título
                lines = [line for line in text.split('\n') if line.strip()]
                title = lines[0] if lines else "Publicación de Facebook"
                title = title[:70] + "..." if len(title) > 70 else title
                
                md_fb_section += f"### {title}\n\n"
                md_fb_section += f"**URL:** [{url}]({url})\n\n"
                
                # Añadir el texto completo, con sanitización básica
                sanitized_text = text.replace('#', '\\#').replace('*', '\\*').replace('_', '\\_')
                md_fb_section += f"{sanitized_text[:1000]}...\n\n" if len(sanitized_text) > 1000 else f"{sanitized_text}\n\n"
                md_fb_section += "---\n\n"
        
        # Verificar si ya existe una sección de Facebook en el archivo
        with open(clean_md_path, 'r', encoding='utf-8') as f:
            current_md_content = f.read()
        
        if "## Contenido de Facebook" not in current_md_content:
            # Añadir la sección de Facebook al final del archivo Markdown
            with open(clean_md_path, 'a', encoding='utf-8') as f:
                f.write(md_fb_section)
            logger.info(f"Sección de Facebook añadida al archivo {clean_md_path}")
        else:
            logger.info(f"El archivo {clean_md_path} ya contiene una sección de Facebook")
        
        logger.info(f"Se han añadido {len(facebook_data)} textos de Facebook al archivo limpio")
        return True
    
    except Exception as e:
        logger.error(f"Error al procesar textos de Facebook: {str(e)}")
        return False
