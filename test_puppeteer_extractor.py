"""
Script de prueba para la extracción de contenido con Puppeteer.
Este script verifica la implementación real con URLs de ejemplo.
"""

import asyncio
import json
import os
import logging
from datetime import datetime
from difflib import SequenceMatcher

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("test_puppeteer")

# Importar el extractor de contenido
from puppeteer_extractor import ContentExtractor

# URLs de prueba (usar una muestra pequeña de cada categoría)
TEST_URLS = {
    "facebook": [
        "https://www.facebook.com/permalink.php?story_fbid=pfbid02jjxa5vMavsFyt2VVpMfg7aLimbMDQw21agnBHdsiyWfSL6BomtXekrcmNyiKkviNl&id=100062993063417",
        "https://www.facebook.com/permalink.php?story_fbid=pfbid02ozZBtnBo2bLjaHnXZ4TTZygwN3fjLhwdZEwab7R7vyUj8k8DKoUsC4rey5YfaEbel&id=100072108931277"
    ],
    "youtube": [
        "https://www.youtube.com/watch?v=IETbsSm-ka0",
        "https://www.youtube.com/watch?v=XxWSnIdKpgo"
    ],
    "news": [
        "https://rpp.pe/economia/economia/sunass-se-requiere-s-138000-millones-para-cerrar-la-brecha-de-agua-potable-y-saneamiento-en-30-anos-noticia-1628807",
        "https://larepublica.pe/sociedad/2025/04/14/trujillo-y-distritos-de-la-libertad-tendran-cortes-de-agua-por-limpieza-de-reservorios-de-sedalib-hasta-mayo-zonas-y-horarios-afectados-1105202"
    ]
}

def calculate_similarity(text1, text2):
    """Calcula la similitud entre dos textos"""
    if not text1 or not text2:
        return 0.0
    
    # Normalizar textos
    text1 = ' '.join(text1.lower().split())
    text2 = ' '.join(text2.lower().split())
    
    # Para textos muy largos, usar solo los primeros 5000 caracteres
    if len(text1) > 5000 or len(text2) > 5000:
        text1 = text1[:5000]
        text2 = text2[:5000]
    
    # Calcular similitud
    matcher = SequenceMatcher(None, text1, text2)
    return matcher.ratio()

async def test_content_extraction():
    """Prueba la extracción de contenido con Puppeteer"""
    # Crear directorio para resultados
    results_dir = "test_results"
    os.makedirs(results_dir, exist_ok=True)
    
    # Inicializar extractor
    extractor = ContentExtractor(cache_dir="cache/puppeteer_test")
    
    # Resultados
    results = {
        "timestamp": datetime.now().isoformat(),
        "extraction_results": {},
        "similarity_analysis": {}
    }
    
    # Probar cada categoría
    for category, urls in TEST_URLS.items():
        logger.info(f"Probando categoría: {category}")
        category_results = []
        
        # Extraer contenido de cada URL
        for url in urls:
            logger.info(f"Extrayendo contenido de: {url}")
            try:
                content = await extractor.extract_content(url, url_type=category)
                
                # Guardar resultado
                category_results.append({
                    "url": url,
                    "success": content.get("success", False),
                    "content_length": len(content.get("text", "")),
                    "has_title": bool(content.get("title", "")),
                    "extraction_time": datetime.now().isoformat()
                })
                
                # Guardar contenido completo en archivo separado
                content_file = os.path.join(
                    results_dir, 
                    f"{category}_{len(category_results)}.json"
                )
                
                with open(content_file, "w", encoding="utf-8") as f:
                    json.dump(content, f, ensure_ascii=False, indent=2)
                
                logger.info(f"Contenido guardado en: {content_file}")
                
            except Exception as e:
                logger.error(f"Error procesando {url}: {e}")
                category_results.append({
                    "url": url,
                    "success": False,
                    "error": str(e)
                })
        
        # Guardar resultados de esta categoría
        results["extraction_results"][category] = category_results
    
    # Análisis de similitud (para detectar duplicados)
    logger.info("Analizando similitud entre contenidos...")
    
    # Para cada categoría, comparar contenidos
    for category in TEST_URLS.keys():
        category_dir = results_dir
        category_files = [
            os.path.join(category_dir, f) 
            for f in os.listdir(category_dir) 
            if f.startswith(f"{category}_") and f.endswith(".json")
        ]
        
        # Comparar cada par de archivos
        similarity_results = []
        
        for i, file1 in enumerate(category_files):
            for j, file2 in enumerate(category_files):
                if i >= j:  # Evitar comparaciones duplicadas y consigo mismo
                    continue
                
                try:
                    # Cargar contenidos
                    with open(file1, "r", encoding="utf-8") as f:
                        content1 = json.load(f)
                    
                    with open(file2, "r", encoding="utf-8") as f:
                        content2 = json.load(f)
                    
                    # Calcular similitud
                    similarity = calculate_similarity(
                        content1.get("text", ""),
                        content2.get("text", "")
                    )
                    
                    # Guardar resultado
                    similarity_results.append({
                        "file1": os.path.basename(file1),
                        "file2": os.path.basename(file2),
                        "url1": content1.get("url", ""),
                        "url2": content2.get("url", ""),
                        "similarity": similarity,
                        "is_duplicate": similarity > 0.85  # Umbral ejemplo
                    })
                    
                    logger.info(f"Similitud entre {file1} y {file2}: {similarity:.4f}")
                    
                except Exception as e:
                    logger.error(f"Error comparando {file1} y {file2}: {e}")
        
        # Guardar resultados de similitud
        results["similarity_analysis"][category] = similarity_results
    
    # Guardar resultados completos
    results_file = os.path.join(results_dir, "extraction_results.json")
    with open(results_file, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    
    logger.info(f"Resultados guardados en: {results_file}")
    
    return results

if __name__ == "__main__":
    # Ejecutar prueba
    asyncio.run(test_content_extraction())
