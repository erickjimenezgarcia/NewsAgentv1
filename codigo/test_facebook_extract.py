#!/usr/bin/env python3
"""
Script para probar la extracción de textos de Facebook.
Este script intenta extraer los textos de Facebook utilizando el módulo corregido.
"""

import os
import sys
import logging
import json
from datetime import datetime

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger('test_facebook_extract')

def main():
    # Obtener la fecha como argumento
    if len(sys.argv) > 1:
        date_str = sys.argv[1]
    else:
        # Usar fecha actual
        date_str = datetime.now().strftime('%d%m%Y')
        logger.info(f"No se proporcionó fecha, usando la actual: {date_str}")
    
    # Encontrar la ubicación de los archivos
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = script_dir
    
    # Importar el módulo corregido
    sys.path.insert(0, os.path.join(project_root, 'codigo'))
    try:
        from lib.semantic_cleaner.facebook_extractor_fix import fix_facebook_texts_extraction
        logger.info("Módulo importado correctamente.")
    except ImportError as e:
        logger.error(f"Error al importar el módulo: {e}")
        return 1
    
    # Ejecutar la corrección
    try:
        result = fix_facebook_texts_extraction(date_str)
        if result:
            logger.info("✅ La extracción de textos de Facebook se realizó correctamente.")
            
            # Verificar el resultado
            clean_path = os.path.join(project_root, 'output', 'clean', f'clean_{date_str}.md')
            if os.path.exists(clean_path):
                with open(clean_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                
                if '## Contenido de Facebook' in content:
                    logger.info("✅ La sección de Facebook fue agregada correctamente al archivo Markdown.")
                else:
                    logger.warning("⚠️ La sección de Facebook no se encuentra en el archivo Markdown.")
            else:
                logger.warning(f"⚠️ El archivo Markdown limpio no existe: {clean_path}")
            
            return 0
        else:
            logger.error("❌ La extracción de textos de Facebook falló.")
            return 1
    except Exception as e:
        logger.error(f"❌ Error al ejecutar la corrección: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return 1

if __name__ == "__main__":
    sys.exit(main())
