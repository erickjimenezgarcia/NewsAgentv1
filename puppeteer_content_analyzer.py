import json
import os
import time
import random
from datetime import datetime
from difflib import SequenceMatcher
from urllib.parse import urlparse

# Configuración
CONFIG = {
    "similarity_threshold": {
        "facebook": 0.85,  # Umbral más permisivo para Facebook
        "news": 0.90,      # Noticias pueden ser similares pero tener diferencias editoriales
        "youtube": 0.95,   # Videos son únicos generalmente
        "image": 0.98,     # Imágenes casi idénticas
        "media": 0.95,     # Audio/Video casi idéntico
        "other": 0.85,     # Categoría genérica, umbral moderado
        "default": 0.85    # Valor por defecto
    },
    "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36",
    "timeout": 30000       # 30 segundos de timeout para cargar páginas
}

def get_domain(url):
    """Extrae el dominio base de una URL"""
    try:
        parsed = urlparse(url)
        domain = parsed.netloc
        return domain
    except:
        return "unknown"

def calculate_similarity(text1, text2):
    """Calcula la similitud entre dos textos usando SequenceMatcher"""
    if not text1 or not text2:
        return 0.0
    
    # Normalizamos los textos: convertimos a minúsculas y eliminamos espacios en blanco excesivos
    text1 = ' '.join(text1.lower().split())
    text2 = ' '.join(text2.lower().split())
    
    # Para textos muy largos, utilizamos solo los primeros 5000 caracteres
    # para mejorar el rendimiento
    if len(text1) > 5000 or len(text2) > 5000:
        text1 = text1[:5000]
        text2 = text2[:5000]
    
    matcher = SequenceMatcher(None, text1, text2)
    return matcher.ratio()

def extract_content_with_puppeteer(url, category):
    """
    Esta función será implementada para usar Puppeteer a través de la API
    Simularemos su funcionamiento para la prueba de concepto
    """
    # En un escenario real, aquí usaríamos Puppeteer para extraer:
    # - Para Facebook: el texto del post, imágenes y comentarios principales
    # - Para YouTube: título, descripción y metadatos del video
    # - Para noticias: título, subtítulos y cuerpo principal del artículo
    
    domain = get_domain(url)
    
    # Simulación de extracción para la demostración
    result = {
        "url": url,
        "category": category,
        "domain": domain,
        "timestamp": datetime.now().isoformat(),
        "content_extracted": True,
        "content_length": 0,
        "title": "",
        "main_text": "",
        "success": True,
        "error": None
    }
    
    try:
        # Aquí iría el código real de Puppeteer para extraer contenido
        # Por ahora simulamos con un placeholder
        result["main_text"] = f"Contenido simulado para {url} de categoría {category}"
        result["content_length"] = len(result["main_text"])
        return result
    
    except Exception as e:
        result["success"] = False
        result["error"] = str(e)
        return result

def find_duplicates(content_data, similarity_thresholds=CONFIG["similarity_threshold"]):
    """
    Identifica contenido duplicado basado en umbrales de similitud
    Usa un enfoque adaptativo por categoría
    """
    # Agrupar por categoría
    by_category = {}
    for item in content_data:
        category = item["category"]
        if category not in by_category:
            by_category[category] = []
        by_category[category].append(item)
    
    # Buscar duplicados dentro de cada categoría
    duplicates = []
    unique_items = []
    
    for category, items in by_category.items():
        category_duplicates = []
        category_uniques = []
        
        # Obtener el umbral para esta categoría
        threshold = similarity_thresholds.get(category, similarity_thresholds["default"])
        
        # Algoritmo para detectar duplicados
        for i, item1 in enumerate(items):
            is_duplicate = False
            
            # Comparar con los elementos previos únicos
            for unique_item in category_uniques:
                similarity = calculate_similarity(
                    item1.get("main_text", ""), 
                    unique_item.get("main_text", "")
                )
                
                if similarity >= threshold:
                    # Es un duplicado
                    is_duplicate = True
                    duplicate_info = {
                        "original_url": unique_item["url"],
                        "duplicate_url": item1["url"],
                        "similarity": similarity,
                        "category": category,
                        "threshold_used": threshold
                    }
                    category_duplicates.append(duplicate_info)
                    break
            
            # Si no es un duplicado, agregar a únicos
            if not is_duplicate:
                category_uniques.append(item1)
        
        # Agregar a los resultados globales
        duplicates.extend(category_duplicates)
        unique_items.extend(category_uniques)
    
    return {
        "unique_count": len(unique_items),
        "duplicate_count": len(duplicates),
        "unique_items": unique_items,
        "duplicates": duplicates
    }

def analyze_content():
    """Función principal para analizar contenido y detectar duplicados"""
    # Cargar la categorización de URLs
    try:
        with open('url_categorization.json', 'r', encoding='utf-8') as f:
            categorization = json.load(f)
    except Exception as e:
        print(f"Error al cargar la categorización: {e}")
        return
    
    # Extraer contenido de cada URL con Puppeteer (simulado para la demo)
    all_content = []
    
    # Límite para pruebas
    url_limit = 10  # Para la demo completa, cambiar a None para procesar todas las URLs
    
    # Procesar por categoría
    for category, urls in categorization["categories"].items():
        print(f"Procesando categoría: {category}")
        
        # Limitar cantidad de URLs por categoría en la demo
        category_urls = urls[:url_limit] if url_limit else urls
        
        for url in category_urls:
            print(f"  Analizando: {url}")
            # Aquí usaríamos Puppeteer para extraer contenido
            content_data = extract_content_with_puppeteer(url, category)
            all_content.append(content_data)
            # Esperar un tiempo aleatorio para no sobrecargar servidores
            time.sleep(random.uniform(0.5, 1.5))
    
    print(f"Contenido extraído de {len(all_content)} URLs")
    
    # Buscar duplicados
    duplicate_results = find_duplicates(all_content)
    
    # Guardar resultados
    results = {
        "timestamp": datetime.now().isoformat(),
        "total_urls_analyzed": len(all_content),
        "unique_count": duplicate_results["unique_count"],
        "duplicate_count": duplicate_results["duplicate_count"],
        "duplicate_percentage": round(duplicate_results["duplicate_count"] / len(all_content) * 100, 2) if all_content else 0,
        "duplicates": duplicate_results["duplicates"]
    }
    
    with open('content_analysis_results.json', 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    
    print("\nResultados del análisis:")
    print(f"- URLs analizadas: {results['total_urls_analyzed']}")
    print(f"- URLs únicas: {results['unique_count']}")
    print(f"- URLs duplicadas: {results['duplicate_count']} ({results['duplicate_percentage']}%)")
    print("Resultados guardados en content_analysis_results.json")
    
    return results

if __name__ == "__main__":
    analyze_content()
