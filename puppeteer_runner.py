"""
Script puente para ejecutar la extracción de contenido con Puppeteer MCP.
Este script conecta nuestro extractor con las herramientas MCP disponibles.
"""

import asyncio
import json
import logging
import sys
from datetime import datetime

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("puppeteer_runner")

# Funciones MCP simuladas para desarrollo local
# Estas serán reemplazadas por las reales en el entorno MCP
async def mcp0_puppeteer_navigate(url, launchOptions=None, allowDangerous=False):
    logger.info(f"[MCP] Navegando a: {url}")
    return {"success": True}

async def mcp0_puppeteer_evaluate(script):
    logger.info(f"[MCP] Ejecutando script (longitud: {len(script)})")
    return {"result": "Simulación de resultado"}

async def mcp0_puppeteer_screenshot(name, selector=None, width=800, height=600, encoded=False):
    logger.info(f"[MCP] Tomando captura: {name}")
    return {"success": True}

# Exponer funciones MCP al espacio global
sys.modules['__main__'].mcp0_puppeteer_navigate = mcp0_puppeteer_navigate
sys.modules['__main__'].mcp0_puppeteer_evaluate = mcp0_puppeteer_evaluate
sys.modules['__main__'].mcp0_puppeteer_screenshot = mcp0_puppeteer_screenshot

# Importar el script de prueba
from test_puppeteer_extractor import test_content_extraction

async def main():
    """Función principal para ejecutar la prueba"""
    logger.info("Iniciando prueba de extracción con Puppeteer")
    
    try:
        results = await test_content_extraction()
        
        # Mostrar resumen de resultados
        for category, category_results in results["extraction_results"].items():
            success_count = sum(1 for r in category_results if r.get("success", False))
            logger.info(f"Categoría {category}: {success_count}/{len(category_results)} extracciones exitosas")
        
        # Mostrar resumen de similitud
        for category, similarity_results in results["similarity_analysis"].items():
            duplicate_count = sum(1 for r in similarity_results if r.get("is_duplicate", False))
            if similarity_results:
                logger.info(f"Categoría {category}: {duplicate_count}/{len(similarity_results)} pares duplicados")
        
        logger.info("Prueba completada exitosamente")
        
    except Exception as e:
        logger.error(f"Error durante la prueba: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(main())
