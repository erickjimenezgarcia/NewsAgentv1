#!/usr/bin/env python3
"""
Script para corregir la integración de textos de Facebook en archivos limpios.

Uso:
    python fix_facebook_integration.py <fecha>
    
Ejemplo:
    python fix_facebook_integration.py 16042025
"""

import os
import sys
import logging
import importlib.util

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("fix_facebook_integration")

def import_module_from_path(module_name, file_path):
    """
    Importa un módulo desde una ruta de archivo.
    
    Args:
        module_name (str): Nombre del módulo
        file_path (str): Ruta al archivo del módulo
        
    Returns:
        module: Módulo importado
    """
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

def main(date_str):
    """
    Función principal para corregir la integración de textos de Facebook.
    
    Args:
        date_str (str): Fecha en formato DDMMAAAA
    """
    # Obtener la ruta absoluta del script actual
    current_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Importar el módulo de corrección
    fix_module_path = os.path.join(current_dir, 'lib', 'semantic_cleaner', 'facebook_extractor_fix.py')
    
    if os.path.exists(fix_module_path):
        try:
            fb_fix_module = import_module_from_path('facebook_extractor_fix', fix_module_path)
            
            # Ejecutar la corrección
            result = fb_fix_module.fix_facebook_texts_extraction(date_str)
            
            if result:
                logger.info(f"Corrección de textos de Facebook completada exitosamente para la fecha {date_str}")
                return 0
            else:
                logger.error(f"La corrección de textos de Facebook falló para la fecha {date_str}")
                return 1
        except Exception as e:
            logger.error(f"Error al ejecutar la corrección: {e}")
            return 1
    else:
        logger.error(f"No se encontró el módulo de corrección en: {fix_module_path}")
        return 1

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Uso: python fix_facebook_integration.py <fecha>")
        print("Ejemplo: python fix_facebook_integration.py 16042025")
        sys.exit(1)
    
    date_str = sys.argv[1]
    
    # Validar formato de fecha
    if not date_str.isdigit() or len(date_str) != 8:
        print("Error: La fecha debe estar en formato DDMMAAAA (8 dígitos)")
        sys.exit(1)
    
    sys.exit(main(date_str))
