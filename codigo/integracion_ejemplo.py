#!/usr/bin/env python
"""
Ejemplo de integración del limpiador semántico con main.py.

Este script muestra cómo integrar la limpieza semántica con el flujo de trabajo principal,
añadiendo el código necesario al final de main.py.

Desarrollado para: SUNASS
Fecha: Mayo 2025
"""

import os
import sys
import logging
from datetime import datetime

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('integracion_ejemplo')

def main_original():
    """
    Simulación del flujo original de main.py.
    """
    logger.info("Ejecutando el flujo principal de procesamiento...")
    
    # Simulación de procesamiento de datos
    date_str = "15042025"  # Fecha de ejemplo
    
    logger.info(f"Procesando datos para la fecha: {date_str}")
    
    # Simulación de generación de archivo consolidado
    logger.info("Archivo consolidado generado correctamente")
    
    return date_str

# Método 1: Importar la función y usarla directamente
def integracion_metodo1():
    """
    Integración mediante importación directa de la función.
    """
    logger.info("=== Método 1: Importación directa ===")
    
    # Simulación de main.py
    date_str = main_original()
    
    # Importar la función de limpieza semántica
    try:
        from clean_news import run_semantic_cleaning
        
        logger.info("Ejecutando limpieza semántica...")
        output_json, output_md = run_semantic_cleaning(date_str=date_str)
        
        if output_json and output_md:
            logger.info(f"Archivos generados: {output_json}, {output_md}")
        else:
            logger.error("Error en la limpieza semántica")
    except ImportError:
        logger.error("Error al importar el módulo clean_news")

# Método 2: Llamar al script como proceso separado
def integracion_metodo2():
    """
    Integración mediante llamada a proceso externo.
    """
    logger.info("=== Método 2: Llamada a proceso externo ===")
    
    # Simulación de main.py
    date_str = main_original()
    
    # Ejecutar clean_news.py como proceso separado
    import subprocess
    
    try:
        logger.info(f"Ejecutando limpieza semántica para la fecha {date_str}...")
        result = subprocess.run(
            [sys.executable, "clean_news.py", "--date", date_str],
            capture_output=True,
            text=True,
            check=True
        )
        
        # Mostrar salida del proceso
        logger.info("Proceso completado con éxito")
        logger.info(f"Salida: {result.stdout}")
    except subprocess.CalledProcessError as e:
        logger.error(f"Error al ejecutar clean_news.py: {e}")
        logger.error(f"Salida de error: {e.stderr}")

# Ejemplo de integración en main.py
def main_integrado():
    """
    Ejemplo de cómo se vería main.py con la integración.
    """
    logger.info("=== Flujo completo integrado ===")
    
    # Código original de main.py
    date_str = main_original()
    
    # NUEVO: Agregar limpieza semántica al final del proceso
    try:
        from clean_news import run_semantic_cleaning
        
        logger.info("Ejecutando limpieza semántica...")
        output_json, output_md = run_semantic_cleaning(date_str=date_str)
        
        if output_json and output_md:
            logger.info(f"Archivos generados: {output_json}, {output_md}")
            
            # Opcional: Hacer algo con los archivos generados
            logger.info("Proceso completo terminado con éxito")
        else:
            logger.error("Error en la limpieza semántica")
    except Exception as e:
        logger.error(f"Error al ejecutar la limpieza semántica: {str(e)}")
        # Continuar con el flujo principal incluso si la limpieza falla
        logger.info("Continuando con el proceso principal")

if __name__ == "__main__":
    logger.info("Ejecutando ejemplos de integración...")
    
    # Mostrar ejemplos de integración
    integracion_metodo1()
    print("\n" + "-" * 50 + "\n")
    integracion_metodo2()
    print("\n" + "-" * 50 + "\n")
    main_integrado()
    
    logger.info("Ejemplos completados")
