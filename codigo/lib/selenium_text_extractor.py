"""
Módulo para extraer texto de URLs usando Selenium.
"""

import logging
from selenium_content_extractor import SeleniumContentExtractor

logger = logging.getLogger("selenium_text_extractor")

def extract_text_with_selenium(url: str, timeout: int = 30) -> str:
    """Extrae texto de una URL usando Selenium.
    
    Args:
        url: URL para extraer texto
        timeout: Tiempo máximo de espera en segundos
        
    Returns:
        str: Texto extraído o cadena vacía si falla
    """
    extractor = SeleniumContentExtractor(headless=True)
    try:
        result = extractor.extract_content(url, use_cache=True)
        if result.get('success', False):
            return result.get('text', '')
    except Exception as e:
        logger.error(f"Error extrayendo texto de {url}: {e}")
    return ''
