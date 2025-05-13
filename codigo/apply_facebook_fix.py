#!/usr/bin/env python3
"""
Script para aplicar el parche que corrige el problema de extracción de textos de Facebook.
Este script reemplaza el módulo existente con la versión corregida.
"""

import os
import sys
import shutil
import time
import logging

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger('patch_script')

def backup_file(file_path):
    """Crea una copia de seguridad del archivo original."""
    backup_path = f"{file_path}.bak_{int(time.time())}"
    try:
        shutil.copy2(file_path, backup_path)
        logger.info(f"Copia de seguridad creada: {backup_path}")
        return True
    except Exception as e:
        logger.error(f"Error al crear copia de seguridad: {e}")
        return False

def main():
    # Encontrar la ubicación de los archivos
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    
    # Rutas de los archivos
    original_file = os.path.join(project_root, 'codigo', 'lib', 'semantic_cleaner', 'facebook_extractor_fix.py')
    patched_file = os.path.join(project_root, 'codigo', 'lib', 'semantic_cleaner', 'facebook_extractor_fix_patched.py')
    
    # Verificar que existan los archivos
    if not os.path.exists(original_file):
        logger.error(f"El archivo original no existe: {original_file}")
        return 1
    
    if not os.path.exists(patched_file):
        logger.error(f"El archivo con el parche no existe: {patched_file}")
        return 1
    
    # Crear copia de seguridad
    if not backup_file(original_file):
        logger.error("No se pudo crear una copia de seguridad. Abortando.")
        return 1
    
    # Reemplazar el archivo original con el parche
    try:
        shutil.copy2(patched_file, original_file)
        logger.info(f"Archivo reemplazado correctamente: {original_file}")
        
        # Opcional: Modificar el archivo main2.py para que importe el módulo correcto
        main_file = os.path.join(project_root, 'codigo', 'main2.py')
        if os.path.exists(main_file):
            with open(main_file, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # No es necesario modificar la importación en main2.py, ya que el nombre del módulo no cambia
            # Solo actualizamos el contenido del módulo
            
            logger.info("Parche aplicado correctamente.")
            logger.info("Ahora puedes ejecutar el orquestador normalmente:")
            logger.info("python ./codigo/main2.py 16042025")
        
        return 0
    except Exception as e:
        logger.error(f"Error al reemplazar el archivo: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())
