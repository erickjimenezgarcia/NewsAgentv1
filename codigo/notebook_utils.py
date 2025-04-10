"""
Utilidades para usar el orquestador desde Jupyter Notebook
"""
import os
import sys
import logging
from datetime import datetime

def configure_logging(project_root):
    """
    Configura el sistema de logging para ser usado desde un notebook
    
    Args:
        project_root (str): Ruta al directorio raíz del proyecto
    
    Returns:
        objeto logger configurado
    """
    # Crear directorio de logs si no existe
    log_dir = os.path.join(project_root, 'logs')
    os.makedirs(log_dir, exist_ok=True)
    log_file_path = os.path.join(log_dir, 'scraper.log')
    
    # Configurar logging global
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file_path, encoding='utf-8'),
            logging.StreamHandler()
        ],
        force=True  # Forzar la reconfiguración del logging (importante para notebooks)
    )
    logger = logging.getLogger("notebook_orchestrator")
    
    # Silenciar logs verbosos
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("selenium").setLevel(logging.WARNING)
    logging.getLogger("webdriver_manager").setLevel(logging.WARNING)
    
    return logger

def setup_environment(project_root):
    """
    Configura el entorno para ejecutar el orquestador desde un notebook
    
    Args:
        project_root (str): Ruta al directorio raíz del proyecto
    
    Returns:
        objeto logger configurado
    """
    # Asegurar que el directorio 'lib' esté en el path para imports
    current_dir = os.path.dirname(os.path.abspath(__file__))
    lib_path = os.path.join(current_dir, 'lib')
    
    if lib_path not in sys.path:
        sys.path.insert(0, lib_path)
    
    # Configurar logging
    logger = configure_logging(project_root)
    
    return logger
