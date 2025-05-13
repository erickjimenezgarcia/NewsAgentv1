"""
Fix para el problema de extracción de textos de Facebook en la limpieza semántica.

Este módulo proporciona una solución para el error de extracción de textos
de Facebook en el proceso de limpieza semántica.
"""

import os
import json
import logging
import sys

# Añadir path del proyecto para importar los módulos necesarios
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from lib.config_manager import load_config, get_paths

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
    # Obtener rutas desde la configuración central
    try:
        # Obtenemos la ruta base del proyecto
        project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
        config = load_config(project_root)
        
        # Construir rutas usando la configuración y el gestor de rutas
        output_dir = os.path.join(project_root, "output")  # Directorio de salida principal
        clean_dir = os.path.join(output_dir, "clean")  # Directorio de archivos limpios
        
        # Asegurar que el directorio clean existe
        if not os.path.exists(clean_dir):
            os.makedirs(clean_dir, exist_ok=True)
            
        # Rutas de los archivos necesarios
        facebook_path = os.path.join(output_dir, f'facebook_texts_{date_str}.json')
        consolidated_path = os.path.join(output_dir, f'consolidated_{date_str}.json')
        clean_json_path = os.path.join(clean_dir, f'clean_{date_str}.json')
        clean_md_path = os.path.join(clean_dir, f'clean_{date_str}.md')
        
        # Registrar rutas para depuración
        logger.info(f"Usando las siguientes rutas:")
        logger.info(f"- Facebook texts: {facebook_path}")
        logger.info(f"- Consolidated: {consolidated_path}")
        logger.info(f"- Clean JSON: {clean_json_path}")
        logger.info(f"- Clean MD: {clean_md_path}")
        
        # Verificar si existen directorios/archivos alternativos para mayor robustez
        if not os.path.exists(facebook_path):
            # Intentar buscar el archivo en rutas alternativas
            alt_paths = [
                os.path.join(project_root, 'NewsAgent', 'output', f'facebook_texts_{date_str}.json'),
                os.path.join(project_root, '..', 'output', f'facebook_texts_{date_str}.json')
            ]
            for alt_path in alt_paths:
                if os.path.exists(alt_path):
                    logger.info(f"Encontrado archivo alternativo: {alt_path}")
                    facebook_path = alt_path
                    break
        
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
        import traceback
        logger.error(traceback.format_exc())
        return False

# Permitir la ejecución directa para pruebas
if __name__ == "__main__":
    if len(sys.argv) > 1:
        date_str = sys.argv[1]
        result = fix_facebook_texts_extraction(date_str)
        print(f"Resultado: {'Éxito' if result else 'Fallido'}")
    else:
        print("Uso: python facebook_extractor_fix.py DDMMAAAA")
