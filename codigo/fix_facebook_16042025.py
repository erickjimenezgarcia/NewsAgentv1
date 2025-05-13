#!/usr/bin/env python3
"""
Script para resolver específicamente el problema del archivo de Facebook para la fecha 16042025.
Este script extrae el texto directamente del JSON consolidado en lugar del facebook_texts_*.json.
"""

import os
import sys
import json
import logging

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("facebook_fix_16042025.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("fix_facebook_16042025")

def fix_facebook_in_consolidated(date_str="16042025"):
    """
    Corrige el problema específico del archivo 16042025.
    Extrae los datos de Facebook del JSON consolidado y los añade al archivo limpio.
    """
    # Rutas de archivos
    base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    consolidated_path = os.path.join(base_path, 'output', f'consolidated_{date_str}.json')
    clean_dir = os.path.join(base_path, 'output', 'clean')
    clean_json_path = os.path.join(clean_dir, f'clean_{date_str}.json')
    clean_md_path = os.path.join(clean_dir, f'clean_{date_str}.md')
    
    # Verificar que los archivos existan
    if not os.path.exists(consolidated_path):
        logger.error(f"No se encontró el archivo consolidado: {consolidated_path}")
        return False
    
    if not os.path.exists(clean_json_path):
        logger.error(f"No se encontró el archivo JSON limpio: {clean_json_path}")
        return False
    
    if not os.path.exists(clean_md_path):
        logger.error(f"No se encontró el archivo Markdown limpio: {clean_md_path}")
        return False
    
    try:
        # Cargar datos
        with open(consolidated_path, 'r', encoding='utf-8') as f:
            consolidated_data = json.load(f)
        
        with open(clean_json_path, 'r', encoding='utf-8') as f:
            clean_data = json.load(f)
        
        # Verificar la estructura de los datos
        if 'extracted_content' not in consolidated_data or 'facebook_texts' not in consolidated_data['extracted_content']:
            logger.warning("No hay datos de Facebook en el archivo consolidado")
            return False
        
        # Obtener datos de Facebook del consolidado
        facebook_data = consolidated_data['extracted_content']['facebook_texts']
        
        # Depurar la estructura
        logger.info(f"Estructura del consolidated_data: {list(consolidated_data.keys())}")
        logger.info(f"Claves en extracted_content: {list(consolidated_data['extracted_content'].keys())}")
        
        if facebook_data:
            first_key = list(facebook_data.keys())[0]
            logger.info(f"Primera URL de Facebook: {first_key}")
            logger.info(f"Claves en el primer item de Facebook: {list(facebook_data[first_key].keys())}")
            
            # Si hay datos, imprimimos un extracto del primer elemento para debug
            sample_value = facebook_data[first_key]
            for key in sample_value:
                value = sample_value[key]
                if isinstance(value, str):
                    logger.info(f"Ejemplo de '{key}': {value[:100]}...")
        
        # Asegurarse de que la estructura necesaria existe en clean_data
        if "extracted_content" not in clean_data:
            clean_data["extracted_content"] = {}
        
        # Añadir los textos de Facebook al JSON limpio
        clean_data["extracted_content"]["facebook_texts"] = facebook_data
        
        # Guardar el JSON limpio actualizado
        with open(clean_json_path, 'w', encoding='utf-8') as f:
            json.dump(clean_data, f, ensure_ascii=False, indent=2)
        
        # Generar sección de Facebook para el Markdown
        md_fb_section = "\n## Contenido de Facebook\n\n"
        for url, data in facebook_data.items():
            # Determinar qué clave contiene el texto
            text = ""
            for key in data:
                if isinstance(data[key], str) and "text" in key.lower():
                    text = data[key]
                    logger.info(f"Usando el campo '{key}' para la URL {url}")
                    break
            
            # Si no encontramos un campo con 'text', usamos el primero que sea string y no sea PDF path
            if not text:
                for key, value in data.items():
                    if isinstance(value, str) and key != "pdf_path" and key != "processed_date":
                        text = value
                        logger.info(f"Usando campo alternativo '{key}' para la URL {url}")
                        break
            
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
            # Reemplazar la sección de Facebook existente
            new_md_content = current_md_content.split("## Contenido de Facebook")[0] + md_fb_section
            with open(clean_md_path, 'w', encoding='utf-8') as f:
                f.write(new_md_content)
            logger.info(f"Sección de Facebook reemplazada en el archivo {clean_md_path}")
        
        logger.info(f"Se han añadido {len(facebook_data)} textos de Facebook al archivo limpio")
        return True
    
    except Exception as e:
        logger.error(f"Error al procesar textos de Facebook: {str(e)}", exc_info=True)
        return False

if __name__ == "__main__":
    date_str = "16042025"  # Fecha específica para este problema
    if len(sys.argv) > 1:
        date_str = sys.argv[1]
    
    success = fix_facebook_in_consolidated(date_str)
    if success:
        print(f"✅ La corrección de textos de Facebook para la fecha {date_str} se completó exitosamente.")
    else:
        print(f"❌ La corrección de textos de Facebook para la fecha {date_str} falló.")
        sys.exit(1)
