import time
import json
import os
from datetime import datetime

# Definir un conjunto de categorías para las URLs basado en el dominio
def categorize_url(url):
    """Categoriza una URL basada en su dominio y patrón"""
    if "facebook.com" in url:
        return "facebook"
    elif "youtube.com" in url or "youtu.be" in url:
        return "youtube"
    elif any(x in url for x in ["rpp.pe", "larepublica.pe", "elperuano", "diariocorreo", "andina.pe", "infobae.com"]):
        return "news"
    elif "imacorpplataforma.com" in url and (url.endswith(".jpg") or url.endswith(".jpeg") or url.endswith(".png")):
        return "image"
    elif url.endswith(".pdf") or "dispositivo" in url:
        return "document"
    elif url.endswith(".mp3") or url.endswith(".mp4"):
        return "media"
    else:
        return "other"

# Función principal para ejecutar la verificación de contenido con Puppeteer
def check_content_with_puppeteer():
    """Verifica el contenido de URLs utilizando Puppeteer y detecta duplicados"""
    # Cargar URLs únicas
    with open('unique_urls.txt', 'r', encoding='utf-8') as f:
        urls = [line.strip() for line in f if line.strip()]
    
    # Estructuras para almacenar resultados
    results = {}
    categories = {}
    
    # Categorizar URLs
    for url in urls:
        category = categorize_url(url)
        if category not in categories:
            categories[category] = []
        categories[category].append(url)
    
    # Imprimir estadísticas de categorización
    print(f"URLs por categoría:")
    for category, cat_urls in categories.items():
        print(f"- {category}: {len(cat_urls)} URLs")
    
    # Guardar la categorización
    categorization = {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "total_urls": len(urls),
        "categories": categories
    }
    
    with open('url_categorization.json', 'w', encoding='utf-8') as f:
        json.dump(categorization, f, indent=2, ensure_ascii=False)
    
    print(f"\nCategorización guardada en url_categorization.json")
    
    # Aquí irían las llamadas a Puppeteer para cada URL
    # Por ahora, solo devolvemos la estructura preparada
    
    return {
        "total_urls": len(urls),
        "categories": categories
    }

if __name__ == "__main__":
    results = check_content_with_puppeteer()
    print(f"\nProceso completado. Analizadas {results['total_urls']} URLs en {len(results['categories'])} categorías.")
