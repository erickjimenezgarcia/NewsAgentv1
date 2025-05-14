"""
Módulo para clasificación inteligente de URLs y detección de duplicados.
Este módulo mejora el procesamiento de URLs implementando categorización
automática y mecanismos adaptativos de detección de duplicados.
"""

import os
import logging
import json
from urllib.parse import urlparse
from difflib import SequenceMatcher
from datetime import datetime

logger = logging.getLogger(__name__)

class URLClassifier:
    """
    Clase para clasificar URLs por tipo de contenido y detectar duplicados
    mediante análisis de contenido semántico.
    """
    
    # Categorías de URL soportadas
    CATEGORIES = {
        "facebook": ["facebook.com", "fb.com", "fb.watch"],
        "youtube": ["youtube.com", "youtu.be"],
        "news": [
            "andina.pe", "larepublica.pe", "elperuano", "diariocorreo.pe", 
            "rpp.pe", "infobae.com", "eltiempo.pe", "exitosanoticias.pe",
            "lahora.pe", "jornada.com.pe", "sinfronteras"
        ],
        "document": [".pdf", "dispositivo", "normas", "sharepoint"],
        "image": [".jpg", ".jpeg", ".png", ".gif"],
        "media": [".mp3", ".mp4", ".avi", ".mov", ".wav"],
        "government": ["gob.pe", "sunass", "defensoria"],
        "other": []  # Categoría por defecto
    }
    
    # Umbrales de similitud por categoría (valores por defecto)
    DEFAULT_THRESHOLDS = {
        "facebook": 0.85,  # Más permisivo para evitar falsos positivos
        "youtube": 0.95,   # Videos generalmente son únicos
        "news": 0.90,      # Noticias pueden ser similares pero con diferencias editoriales
        "document": 0.98,  # Documentos oficiales con diferente contenido
        "image": 0.99,     # Imágenes son únicas a menos que sean idénticas
        "media": 0.95,     # Audio/video similar umbral a videos
        "government": 0.90, # Sitios de gobierno
        "other": 0.85,     # Categoría genérica
        "default": 0.88    # Umbral global por defecto
    }
    
    def __init__(self, config=None):
        """
        Inicializa el clasificador con configuración personalizada.
        
        Args:
            config: Diccionario de configuración opcional que puede contener:
                   - thresholds: Diccionario de umbrales de similitud por categoría
                   - cache_dir: Directorio para almacenar caché de clasificación
                   - debug: Modo debug para registrar información detallada
        """
        self.config = config or {}
        self.thresholds = {**self.DEFAULT_THRESHOLDS}
        
        # Actualizar umbrales si están en la configuración
        if 'thresholds' in self.config:
            self.thresholds.update(self.config['thresholds'])
        
        self.cache_dir = self.config.get('cache_dir', 'cache')
        self.debug = self.config.get('debug', False)
        
        # Asegurar que exista el directorio de caché
        if not os.path.exists(self.cache_dir):
            os.makedirs(self.cache_dir, exist_ok=True)
            
        logger.info(f"URLClassifier inicializado con umbrales: {self.thresholds}")
    
    def classify_url(self, url):
        """
        Clasifica una URL según su dominio y patrón.
        
        Args:
            url: URL a clasificar
            
        Returns:
            str: Categoría de la URL
        """
        if not url:
            return "invalid"
            
        url_lower = url.lower()
        
        # Primero verificar extensiones de archivo
        for category, patterns in self.CATEGORIES.items():
            if category == "other":
                continue
                
            for pattern in patterns:
                if pattern in url_lower:
                    if self.debug:
                        logger.debug(f"URL {url} clasificada como {category} (patrón: {pattern})")
                    return category
        
        # Si no coincide con ningún patrón, usar "other"
        return "other"
    
    def classify_urls(self, urls):
        """
        Clasifica múltiples URLs y las agrupa por categoría.
        
        Args:
            urls: Lista de URLs a clasificar
            
        Returns:
            dict: Diccionario con URLs agrupadas por categoría
        """
        if not urls:
            return {}
            
        result = {}
        for url in urls:
            category = self.classify_url(url)
            if category not in result:
                result[category] = []
            result[category].append(url)
        
        # Registrar estadísticas
        stats = {category: len(urls_list) for category, urls_list in result.items()}
        logger.info(f"Clasificación de URLs completada: {stats}")
        
        return result
    
    def calculate_similarity(self, text1, text2):
        """
        Calcula la similitud entre dos textos.
        
        Args:
            text1, text2: Textos a comparar
            
        Returns:
            float: Ratio de similitud entre 0.0 y 1.0
        """
        if not text1 or not text2:
            return 0.0
            
        # Normalizar textos
        text1 = ' '.join(text1.lower().split())
        text2 = ' '.join(text2.lower().split())
        
        # Para textos muy largos, utilizamos solo los primeros 5000 caracteres
        if len(text1) > 5000 or len(text2) > 5000:
            text1 = text1[:5000]
            text2 = text2[:5000]
        
        # Calcular similitud
        matcher = SequenceMatcher(None, text1, text2)
        similarity = matcher.ratio()
        
        if self.debug and similarity > 0.7:
            logger.debug(f"Similitud alta ({similarity:.4f}) entre textos")
            
        return similarity
    
    def is_duplicate(self, content1, content2, category=None):
        """
        Determina si dos contenidos son duplicados según su categoría.
        
        Args:
            content1, content2: Contenidos a comparar
            category: Categoría de los contenidos (opcional)
            
        Returns:
            tuple: (es_duplicado, similitud)
        """
        # Determinar umbral según categoría
        threshold = self.thresholds.get(
            category, 
            self.thresholds.get("default")
        )
        
        # Calcular similitud
        similarity = self.calculate_similarity(content1, content2)
        
        # Determinar si es duplicado
        is_dup = similarity >= threshold
        
        if self.debug and is_dup:
            logger.debug(
                f"Contenido duplicado detectado (categoría: {category}, "
                f"similitud: {similarity:.4f}, umbral: {threshold})"
            )
            
        return (is_dup, similarity)
    
    def detect_duplicates(self, content_items):
        """
        Detecta duplicados en una lista de elementos de contenido.
        
        Args:
            content_items: Lista de diccionarios con 'url', 'content' y 'category'
            
        Returns:
            dict: Resultados del análisis de duplicados
        """
        if not content_items:
            return {
                "unique_items": [],
                "duplicates": [],
                "stats": {"total": 0, "unique": 0, "duplicate": 0}
            }
        
        # Agrupar por categoría para aplicar umbral específico
        by_category = {}
        for item in content_items:
            category = item.get('category', 'other')
            if category not in by_category:
                by_category[category] = []
            by_category[category].append(item)
        
        all_unique = []
        all_duplicates = []
        
        # Procesar cada categoría
        for category, items in by_category.items():
            unique_items = []
            duplicates = []
            
            # Obtener umbral para esta categoría
            threshold = self.thresholds.get(category, self.thresholds.get("default"))
            
            # Buscar duplicados
            for item in items:
                is_duplicate = False
                content = item.get('content', '')
                
                # Comparar con elementos previos únicos
                for unique_item in unique_items:
                    unique_content = unique_item.get('content', '')
                    is_dup, similarity = self.is_duplicate(
                        content, unique_content, category
                    )
                    
                    if is_dup:
                        is_duplicate = True
                        duplicate_info = {
                            "original_url": unique_item['url'],
                            "duplicate_url": item['url'],
                            "similarity": similarity,
                            "category": category,
                            "threshold_used": threshold
                        }
                        duplicates.append(duplicate_info)
                        break
                
                # Si no es duplicado, agregar a únicos
                if not is_duplicate:
                    unique_items.append(item)
            
            # Agregar resultados de esta categoría
            all_unique.extend(unique_items)
            all_duplicates.extend(duplicates)
        
        # Resumen de resultados
        stats = {
            "total": len(content_items),
            "unique": len(all_unique),
            "duplicate": len(all_duplicates),
            "duplicate_percentage": round(
                len(all_duplicates) / len(content_items) * 100, 2
            ) if content_items else 0
        }
        
        logger.info(
            f"Análisis de duplicados completado: "
            f"{stats['total']} total, {stats['unique']} únicos, "
            f"{stats['duplicate']} duplicados ({stats['duplicate_percentage']}%)"
        )
        
        return {
            "unique_items": all_unique,
            "duplicates": all_duplicates,
            "stats": stats
        }
    
    def filter_duplicate_urls(self, urls_with_content):
        """
        Filtra URLs duplicadas basadas en el contenido.
        
        Args:
            urls_with_content: Diccionario con URL como clave y contenido como valor
            
        Returns:
            tuple: (urls_unicas, urls_duplicadas, estadisticas)
        """
        # Convertir a formato interno
        content_items = []
        for url, content in urls_with_content.items():
            category = self.classify_url(url)
            content_items.append({
                'url': url,
                'content': content,
                'category': category
            })
        
        # Detectar duplicados
        result = self.detect_duplicates(content_items)
        
        # Extraer URLs únicas
        unique_urls = [item['url'] for item in result['unique_items']]
        
        # Extraer URLs duplicadas
        duplicate_urls = [item['duplicate_url'] for item in result['duplicates']]
        
        # Si se detectaron demasiados duplicados (más del 90%), 
        # aplicar un mecanismo de fallback para evitar eliminar todo
        if (len(duplicate_urls) / len(urls_with_content) > 0.9 and 
                len(urls_with_content) > 10):
            logger.warning(
                f"Porcentaje de duplicados sospechosamente alto: "
                f"{result['stats']['duplicate_percentage']}% ({len(duplicate_urls)}/{len(urls_with_content)}). "
                f"Aplicando mecanismo de fallback."
            )
            
            # Usar un umbral más estricto como fallback
            fallback_items = []
            for url, content in urls_with_content.items():
                category = self.classify_url(url)
                # Aumentar umbral en un 10%
                fallback_items.append({
                    'url': url,
                    'content': content,
                    'category': category,
                    '_fallback': True
                })
            
            # Guardar umbrales originales
            original_thresholds = self.thresholds.copy()
            
            # Aplicar umbrales más estrictos para el fallback
            for category in self.thresholds:
                self.thresholds[category] = min(
                    self.thresholds[category] + 0.1, 0.99
                )
            
            # Detectar duplicados con umbrales más estrictos
            fallback_result = self.detect_duplicates(fallback_items)
            
            # Restaurar umbrales originales
            self.thresholds = original_thresholds
            
            # Usar resultados del fallback si son más razonables
            if (fallback_result['stats']['duplicate_percentage'] < 
                    result['stats']['duplicate_percentage']):
                logger.info(
                    f"Fallback aplicado exitosamente. Duplicados reducidos de "
                    f"{result['stats']['duplicate_percentage']}% a "
                    f"{fallback_result['stats']['duplicate_percentage']}%"
                )
                
                # Actualizar resultado
                result = fallback_result
                unique_urls = [item['url'] for item in result['unique_items']]
                duplicate_urls = [item['duplicate_url'] for item in result['duplicates']]
        
        return unique_urls, duplicate_urls, result['stats']
    
    def process_urls_with_deduplication(self, urls, content_extractor_func):
        """
        Procesa URLs extrayendo contenido y eliminando duplicados.
        
        Args:
            urls: Lista de URLs a procesar
            content_extractor_func: Función que extrae contenido de una URL
                                   Debe recibir una URL y devolver su contenido
            
        Returns:
            dict: Resultados del procesamiento con estadísticas
        """
        if not urls:
            return {
                "unique_urls": [],
                "duplicate_urls": [],
                "processed_content": {},
                "stats": {"total": 0, "unique": 0, "duplicate": 0}
            }
        
        # Clasificar URLs
        classified = self.classify_urls(urls)
        
        # Extraer contenido de cada URL
        urls_with_content = {}
        failed_urls = []
        
        for category, category_urls in classified.items():
            logger.info(f"Procesando {len(category_urls)} URLs de categoría {category}")
            
            for url in category_urls:
                try:
                    content = content_extractor_func(url)
                    if content:
                        urls_with_content[url] = content
                    else:
                        logger.warning(f"Contenido vacío para URL: {url}")
                        failed_urls.append(url)
                except Exception as e:
                    logger.error(f"Error extrayendo contenido de {url}: {e}")
                    failed_urls.append(url)
        
        # Filtrar duplicados
        unique_urls, duplicate_urls, stats = self.filter_duplicate_urls(urls_with_content)
        
        # Crear resultado final
        processed_content = {url: urls_with_content[url] for url in unique_urls}
        
        result = {
            "unique_urls": unique_urls,
            "duplicate_urls": duplicate_urls,
            "failed_urls": failed_urls,
            "processed_content": processed_content,
            "stats": {
                **stats,
                "failed": len(failed_urls),
                "total_input": len(urls)
            }
        }
        
        logger.info(
            f"Procesamiento completo. De {len(urls)} URLs: "
            f"{len(unique_urls)} únicas, {len(duplicate_urls)} duplicadas, "
            f"{len(failed_urls)} fallidas."
        )
        
        return result
