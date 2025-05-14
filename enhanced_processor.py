"""
Módulo integrador para mejorar la extracción, clasificación y deduplicación de URLs.
Compatible con el sistema existente en main2.py.
"""

import os
import json
import logging
import time
from datetime import datetime
import argparse
from urllib.parse import urlparse

# Importar los nuevos módulos
from url_classifier import URLClassifier
from content_deduplicator import ContentDeduplicator

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("enhanced_processor")

class EnhancedURLProcessor:
    """
    Procesador mejorado de URLs que integra clasificación avanzada
    y detección de duplicados basada en contenido.
    """
    
    def __init__(self, input_dir='input', output_dir='output', cache_dir='cache'):
        """
        Inicializa el procesador con rutas configurables
        
        Args:
            input_dir: Directorio de entrada para archivos
            output_dir: Directorio de salida para resultados
            cache_dir: Directorio para caché
        """
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.cache_dir = cache_dir
        
        # Asegurar que los directorios existan
        for directory in [input_dir, output_dir, cache_dir]:
            os.makedirs(directory, exist_ok=True)
        
        # Inicializar componentes
        self.classifier = URLClassifier()
        self.deduplicator = ContentDeduplicator(cache_dir=os.path.join(cache_dir, 'dedup'))
        
        logger.info(f"EnhancedURLProcessor inicializado con directorios:")
        logger.info(f" - Input: {os.path.abspath(input_dir)}")
        logger.info(f" - Output: {os.path.abspath(output_dir)}")
        logger.info(f" - Cache: {os.path.abspath(cache_dir)}")
    
    def load_urls_from_csv(self, csv_path):
        """
        Carga URLs desde un archivo CSV generado por el sistema existente
        
        Args:
            csv_path: Ruta al archivo CSV
            
        Returns:
            list: Lista de diccionarios con URLs y metadatos
        """
        import csv
        
        if not os.path.exists(csv_path):
            logger.error(f"Archivo CSV no encontrado: {csv_path}")
            return []
        
        try:
            urls = []
            with open(csv_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    if 'URL' in row and row['URL']:
                        urls.append(dict(row))
            
            logger.info(f"Cargadas {len(urls)} URLs desde {csv_path}")
            return urls
        except Exception as e:
            logger.error(f"Error cargando URLs desde CSV {csv_path}: {e}")
            return []
    
    def process_csv(self, csv_path, detect_duplicates=True, output_prefix=None):
        """
        Procesa un archivo CSV con URLs, clasificándolas y detectando duplicados
        
        Args:
            csv_path: Ruta al archivo CSV
            detect_duplicates: Si detectar duplicados basados en contenido
            output_prefix: Prefijo para archivos de salida
            
        Returns:
            dict: Resultados del procesamiento
        """
        # Generar prefijo de salida
        if not output_prefix:
            timestamp = datetime.now().strftime('%d%m%Y')
            output_prefix = f"processed_{timestamp}"
        
        # Cargar URLs
        urls_data = self.load_urls_from_csv(csv_path)
        if not urls_data:
            return {
                'status': 'error',
                'message': f"No se pudieron cargar URLs desde {csv_path}"
            }
        
        # Extraer lista de URLs
        urls = [item['URL'] for item in urls_data if 'URL' in item]
        
        # Clasificar URLs
        logger.info("Clasificando URLs...")
        start_time = time.time()
        classified = self.classifier.classify_urls(urls_data)
        logger.info(f"Clasificación completada en {time.time() - start_time:.2f} segundos")
        
        # Guardar clasificación detallada
        classification_path = os.path.join(self.output_dir, f"{output_prefix}_classification.json")
        with open(classification_path, 'w', encoding='utf-8') as f:
            json.dump(classified, f, ensure_ascii=False, indent=2)
        logger.info(f"Clasificación guardada en {classification_path}")
        
        # Obtener formato legacy para compatibilidad
        legacy_format = self.classifier.get_legacy_format(classified)
        legacy_path = os.path.join(self.output_dir, f"{output_prefix}_legacy_format.json")
        with open(legacy_path, 'w', encoding='utf-8') as f:
            json.dump(legacy_format, f, ensure_ascii=False, indent=2)
        logger.info(f"Formato legacy guardado en {legacy_path}")
        
        results = {
            'status': 'success',
            'classification': classification_path,
            'legacy_format': legacy_path,
            'stats': {
                'total_urls': len(urls),
                'classification': {
                    cat: len(items) if not isinstance(items, dict) else 
                         sum(len(subitems) for subitems in items.values())
                    for cat, items in classified.items()
                }
            }
        }
        
        # Detectar duplicados si se solicita
        if detect_duplicates:
            logger.info("Detectando duplicados basados en contenido...")
            
            # Convertir clasificación a formato para el deduplicador
            url_categories = {}
            for category, subcats in classified.items():
                if isinstance(subcats, dict):
                    for subcat, items in subcats.items():
                        for item in items:
                            url = item['URL']
                            url_categories[url] = {
                                'category': category,
                                'subcategory': subcat
                            }
                else:
                    for item in subcats:
                        url = item['URL']
                        url_categories[url] = {
                            'category': category,
                            'subcategory': None
                        }
            
            # Encontrar duplicados
            duplicates_result = self.deduplicator.find_duplicates(urls, url_categories)
            
            # Guardar resultados de duplicados
            duplicates_path = os.path.join(self.output_dir, f"{output_prefix}_duplicates.json")
            with open(duplicates_path, 'w', encoding='utf-8') as f:
                json.dump(duplicates_result, f, ensure_ascii=False, indent=2)
            logger.info(f"Análisis de duplicados guardado en {duplicates_path}")
            
            # Obtener URLs filtradas (sin duplicados)
            filtered_urls = self.deduplicator.filter_duplicates(urls, url_categories)
            
            # Guardar lista de URLs filtradas
            filtered_path = os.path.join(self.output_dir, f"{output_prefix}_unique_urls.txt")
            with open(filtered_path, 'w', encoding='utf-8') as f:
                for url in filtered_urls:
                    f.write(f"{url}\n")
            logger.info(f"URLs únicas guardadas en {filtered_path}")
            
            # Actualizar resultados
            results['duplicates'] = duplicates_path
            results['unique_urls'] = filtered_path
            results['stats']['duplicates'] = duplicates_result['stats']
        
        # Guardar estadísticas
        stats_path = os.path.join(self.output_dir, f"{output_prefix}_stats.json")
        with open(stats_path, 'w', encoding='utf-8') as f:
            json.dump(results['stats'], f, ensure_ascii=False, indent=2)
        
        return results
    
    def get_improved_categories(self, links):
        """
        Método compatible con el API existente que devuelve categorías mejoradas
        pero mantiene la estructura esperada por el sistema actual.
        
        Args:
            links: Lista de diccionarios con URLs como se espera en main2.py
            
        Returns:
            dict: Categorías en formato compatible
        """
        # Clasificar con el nuevo sistema
        classified = self.classifier.classify_urls(links)
        
        # Convertir al formato legacy esperado por main2.py
        legacy_format = self.classifier.get_legacy_format(classified)
        
        # Añadir metadatos de clasificación detallada para referencia
        for category, items in legacy_format.items():
            for item in items:
                # Buscar la URL en la clasificación detallada
                url = item.get('URL', '')
                if not url:
                    continue
                
                # Añadir metadatos de clasificación
                item['_enhanced_category'] = None
                item['_enhanced_subcategory'] = None
                
                for cat, subcats in classified.items():
                    if isinstance(subcats, dict):
                        for subcat, subitems in subcats.items():
                            for subitem in subitems:
                                if subitem.get('URL') == url:
                                    item['_enhanced_category'] = cat
                                    item['_enhanced_subcategory'] = subcat
                                    break
                    else:
                        for subitem in subcats:
                            if subitem.get('URL') == url:
                                item['_enhanced_category'] = cat
                                item['_enhanced_subcategory'] = None
                                break
        
        return legacy_format
    
    def filter_duplicate_urls(self, urls, threshold=None):
        """
        Filtra URLs duplicadas basado en contenido real.
        API compatible con el sistema existente.
        
        Args:
            urls: Lista de URLs a filtrar
            threshold: Umbral de similitud (opcional)
            
        Returns:
            list: URLs únicas después del filtrado
        """
        return self.deduplicator.filter_duplicates(urls)


def main():
    """Función principal para ejecución desde línea de comandos"""
    parser = argparse.ArgumentParser(description='Procesador mejorado de URLs')
    parser.add_argument('csv_file', help='Archivo CSV con URLs a procesar')
    parser.add_argument('--no-dedup', dest='dedup', action='store_false', 
                       help='Desactiva detección de duplicados')
    parser.add_argument('--output-prefix', '-o', help='Prefijo para archivos de salida')
    parser.add_argument('--input-dir', default='input', help='Directorio de entrada')
    parser.add_argument('--output-dir', default='output', help='Directorio de salida')
    parser.add_argument('--cache-dir', default='cache', help='Directorio de caché')
    
    args = parser.parse_args()
    
    # Inicializar procesador
    processor = EnhancedURLProcessor(
        input_dir=args.input_dir,
        output_dir=args.output_dir,
        cache_dir=args.cache_dir
    )
    
    # Procesar CSV
    results = processor.process_csv(
        args.csv_file,
        detect_duplicates=args.dedup,
        output_prefix=args.output_prefix
    )
    
    if results['status'] == 'success':
        logger.info("Procesamiento completado exitosamente")
        logger.info(f"Estadísticas: {json.dumps(results['stats'], indent=2)}")
    else:
        logger.error(f"Error en procesamiento: {results['message']}")


if __name__ == '__main__':
    main()
