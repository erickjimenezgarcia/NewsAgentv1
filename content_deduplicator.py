"""
Módulo para detección de URLs duplicadas basado en contenido.
Utiliza el SeleniumContentExtractor para comparar contenido real de URLs.
"""

import os
import json
import logging
import hashlib
from datetime import datetime
import re
from difflib import SequenceMatcher
import time
from urllib.parse import urlparse

# Importar el extractor de contenido
from selenium_content_extractor import SeleniumContentExtractor

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("content_deduplicator")

class ContentSimilarityChecker:
    """
    Calcula la similitud entre dos textos usando varios métodos
    """
    
    @staticmethod
    def normalize_text(text):
        """Normaliza el texto para comparación"""
        if not text:
            return ""
        
        # Convertir a minúsculas
        text = text.lower()
        
        # Eliminar URLs
        text = re.sub(r'https?://\S+', '', text)
        
        # Eliminar caracteres especiales y números
        text = re.sub(r'[^\w\s]', ' ', text)
        text = re.sub(r'\d+', ' ', text)
        
        # Eliminar espacios múltiples
        text = re.sub(r'\s+', ' ', text)
        
        return text.strip()
    
    @staticmethod
    def calculate_similarity(text1, text2):
        """
        Calcula la similitud entre dos textos usando SequenceMatcher
        
        Args:
            text1: Primer texto
            text2: Segundo texto
            
        Returns:
            float: Similitud entre 0 y 1
        """
        if not text1 or not text2:
            return 0.0
        
        # Normalizar textos
        norm_text1 = ContentSimilarityChecker.normalize_text(text1)
        norm_text2 = ContentSimilarityChecker.normalize_text(text2)
        
        if not norm_text1 or not norm_text2:
            return 0.0
        
        # Limitar longitud para performance
        max_length = 5000
        if len(norm_text1) > max_length:
            norm_text1 = norm_text1[:max_length]
        if len(norm_text2) > max_length:
            norm_text2 = norm_text2[:max_length]
            
        # Calcular similitud
        return SequenceMatcher(None, norm_text1, norm_text2).ratio()


class ContentDeduplicator:
    """
    Detecta URLs duplicadas basado en su contenido real.
    Utiliza el SeleniumContentExtractor para acceder al contenido
    y métodos de similitud para comparar.
    """
    
    def __init__(self, cache_dir='cache/dedup', thresholds=None):
        """
        Inicializa el deduplicador con configuración.
        
        Args:
            cache_dir: Directorio para caché
            thresholds: Umbrales de similitud por categoría
        """
        self.cache_dir = cache_dir
        self.content_extractor = SeleniumContentExtractor(cache_dir=os.path.join(cache_dir, 'content'))
        self.similarity_checker = ContentSimilarityChecker()
        
        # Umbrales de similitud por defecto para cada categoría
        self.thresholds = thresholds or {
            'social': {
                'facebook': 0.8,  # Más permisivo para Facebook
                'youtube': 0.9,   # Estricto para YouTube
                'twitter': 0.85,
                'other_social': 0.85
            },
            'html': {
                'news': 0.7,      # Más permisivo para noticias (pueden tener variaciones)
                'government': 0.9, # Muy estricto para contenido gubernamental
                'educational': 0.85,
                'documents': 0.95, # Muy estricto para documentos
                'other_html': 0.8
            },
            'images': 0.95,       # Muy estricto para imágenes
            'audio': 0.95,        # Muy estricto para audio
            'default': 0.85       # Umbral por defecto
        }
        
        # Crear directorio de caché si no existe
        if not os.path.exists(cache_dir):
            os.makedirs(cache_dir, exist_ok=True)
            logger.info(f"Directorio de caché creado: {cache_dir}")
        
        # Caché de contenido
        self.content_cache_path = os.path.join(cache_dir, 'content_cache.json')
        self.content_cache = self._load_content_cache()
        
        logger.info(f"ContentDeduplicator inicializado con caché en: {cache_dir}")
    
    def _load_content_cache(self):
        """Carga la caché de contenido si existe"""
        if os.path.exists(self.content_cache_path):
            try:
                with open(self.content_cache_path, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                logger.warning(f"Error cargando caché de contenido: {e}")
        return {}
    
    def _save_content_cache(self):
        """Guarda la caché de contenido"""
        try:
            with open(self.content_cache_path, 'w', encoding='utf-8') as f:
                json.dump(self.content_cache, f, ensure_ascii=False, indent=2)
            logger.debug(f"Caché de contenido guardado en: {self.content_cache_path}")
        except Exception as e:
            logger.warning(f"Error guardando caché de contenido: {e}")
    
    def _get_cache_key(self, url):
        """Genera una clave única para el caché basada en la URL"""
        return hashlib.md5(url.encode()).hexdigest()
    
    def get_threshold_for_url(self, url, category=None, subcategory=None):
        """
        Obtiene el umbral de similitud apropiado para una URL según su categoría
        
        Args:
            url: URL a evaluar
            category: Categoría de la URL (opcional)
            subcategory: Subcategoría de la URL (opcional)
            
        Returns:
            float: Umbral de similitud entre 0 y 1
        """
        if category == 'social' and subcategory in self.thresholds['social']:
            return self.thresholds['social'][subcategory]
        elif category == 'html' and subcategory in self.thresholds['html']:
            return self.thresholds['html'][subcategory]
        elif category in ['images', 'audio'] and category in self.thresholds:
            return self.thresholds[category]
        
        # Default
        return self.thresholds['default']
    
    def extract_content(self, url):
        """
        Extrae contenido de una URL usando el extractor
        
        Args:
            url: URL para extraer contenido
            
        Returns:
            dict: Contenido extraído o None si hay error
        """
        try:
            # Verificar caché de contenido primero
            cache_key = self._get_cache_key(url)
            if cache_key in self.content_cache:
                logger.debug(f"Usando caché de contenido para URL: {url}")
                return self.content_cache[cache_key]
            
            # Verificar extensiones que probablemente no tengan texto útil
            _, ext = os.path.splitext(urlparse(url).path)
            if ext.lower() in ['.jpg', '.jpeg', '.png', '.gif', '.mp3', '.mp4', '.pdf']:
                # Crear un contenido básico para archivos sin texto
                basic_content = {
                    'url': url,
                    'success': True,
                    'type': 'binary',
                    'text': '',
                    'title': os.path.basename(urlparse(url).path),
                    'timestamp': datetime.now().isoformat()
                }
                self.content_cache[cache_key] = basic_content
                self._save_content_cache()
                return basic_content
            
            # Intentar inferir tipo de URL para optimizar extracción
            url_type = self._infer_url_type(url)
            
            # Extraer contenido real con timeout reducido para URLs problemáticas
            try:
                content = self.content_extractor.extract_content(url, url_type=url_type, use_cache=True)
            except Exception as inner_e:
                logger.warning(f"Error en extracción primaria para {url}: {inner_e}. Intentando con timeout reducido.")
                # Registrar error pero continuar con contenido mínimo
                content = {
                    'url': url,
                    'success': False,
                    'type': url_type or 'unknown',
                    'error': str(inner_e),
                    'timestamp': datetime.now().isoformat()
                }
            
            if content and content.get('success'):
                # Guardar en caché solo contenidos exitosos
                self.content_cache[cache_key] = content
                self._save_content_cache()
                return content
            elif content:
                # Devolver el contenido aunque no sea exitoso
                return content
            
            # Crear un contenido básico para URLs fallidas
            fallback_content = {
                'url': url,
                'success': False,
                'type': 'unknown',
                'text': '',
                'timestamp': datetime.now().isoformat()
            }
            return fallback_content
            
        except Exception as e:
            # Error general, registrar solo una vez y continuar
            logger.error(f"Error fatal extrayendo contenido de URL: {url}: {e}")
            return {
                'url': url,
                'success': False,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }
            
    def _infer_url_type(self, url):
        """Infiere el tipo de URL basado en patrones para optimizar extracción"""
        domain = urlparse(url).netloc.lower()
        path = urlparse(url).path.lower()
        
        # Social media
        if 'facebook.com' in domain or 'fb.com' in domain:
            return 'facebook'
        elif 'youtube.com' in domain or 'youtu.be' in domain:
            return 'youtube'
        elif 'twitter.com' in domain or 'x.com' in domain or 't.co' in domain:
            return 'twitter'
        
        # Tipos de contenido comunes
        if any(ext in path for ext in ['.jpg', '.jpeg', '.png', '.gif']):
            return 'image'
        elif any(ext in path for ext in ['.mp3', '.wav', '.ogg']):
            return 'audio'
        elif any(ext in path for ext in ['.mp4', '.avi', '.mov']):
            return 'video'
        elif '.pdf' in path:
            return 'document'
        
        # Dominios de noticias comunes
        news_domains = ['noticia', 'news', 'diario', 'periodico', 'times', 'post']
        if any(news in domain for news in news_domains):
            return 'news'
            
        return None
    
    def are_similar(self, url1, url2, threshold=None):
        """
        Determina si dos URLs tienen contenido similar
        
        Args:
            url1: Primera URL
            url2: Segunda URL
            threshold: Umbral de similitud (opcional)
            
        Returns:
            tuple: (es_similar, puntuación_similitud, mensaje_error)
        """
        # Verificación rápida de dominios diferentes para optimizar
        domain1 = urlparse(url1).netloc
        domain2 = urlparse(url2).netloc
        
        # Si son dominios completamente diferentes, no comparar contenido
        # a menos que sean redes sociales conocidas que podrían compartir contenido
        social_domains = ['facebook.com', 'twitter.com', 'x.com', 'youtube.com', 't.co']
        are_social = any(social in domain1 for social in social_domains) or any(social in domain2 for social in social_domains)
        
        if domain1 != domain2 and not are_social and '.' in domain1 and '.' in domain2:
            # Verificar si son subdominios del mismo dominio
            base_domain1 = '.'.join(domain1.split('.')[-2:])
            base_domain2 = '.'.join(domain2.split('.')[-2:])
            
            if base_domain1 != base_domain2:
                # Dominios completamente diferentes, no comparar contenido
                return False, 0.0, "Dominios diferentes"
        
        # Extraer contenido de ambas URLs
        content1 = self.extract_content(url1)
        if not content1 or not content1.get('success', False):
            return False, 0.0, "Fallo en extracción de primera URL"
            
        content2 = self.extract_content(url2)
        if not content2 or not content2.get('success', False):
            return False, 0.0, "Fallo en extracción de segunda URL"
        
        # Obtener textos principales
        text1 = content1.get('text', '')
        text2 = content2.get('text', '')
        
        # Si no hay texto, usar título
        if not text1 and 'title' in content1:
            text1 = content1['title']
        if not text2 and 'title' in content2:
            text2 = content2['title']
        
        # Si no hay texto ni título, no se puede comparar
        if not text1 and not text2:
            return False, 0.0, "Sin contenido para comparar"
        
        # Si solo una URL tiene contenido, no son similares
        if bool(text1) != bool(text2):
            return False, 0.0, "Solo una URL tiene contenido"
        
        # Calcular similitud
        similarity = self.similarity_checker.calculate_similarity(text1, text2)
        
        # Usar umbral proporcionado o determinar por categoría
        if threshold is None:
            category1 = content1.get('type')
            category2 = content2.get('type')
            
            # Usar la categoría más específica
            if category1 == category2:
                threshold = self.get_threshold_for_url(url1, category1)
            else:
                threshold = self.thresholds['default']
        
        return similarity >= threshold, similarity, None
    
    def find_duplicates(self, urls, url_categories=None):
        """
        Encuentra duplicados en una lista de URLs basado en su contenido
        Implementa optimizaciones para mejorar rendimiento y reducir comparaciones
        
        Args:
            urls: Lista de URLs a analizar
            url_categories: Diccionario con categorías de URLs (opcional)
            
        Returns:
            dict: Resultados con grupos de duplicados y estadísticas
        """
        if not urls:
            return {'groups': [], 'stats': {'total': 0, 'unique': 0, 'duplicate': 0}}
        
        # Inicializar resultados
        duplicate_groups = []
        url_processed = set()
        unique_urls = []
        comparison_count = 0  # Contador para estadísticas
        skipped_count = 0    # Comparaciones evitadas
        error_types = {}     # Registro de tipos de errores
        
        logger.info(f"Analizando {len(urls)} URLs para encontrar duplicados...")
        start_time = time.time()
        
        # Paso 1: Agrupar URLs por dominio para optimizar comparaciones
        url_by_domain = {}
        for url in urls:
            try:
                domain = urlparse(url).netloc.lower()
                if not domain:
                    continue
                    
                # Agrupar por dominio base para manejar subdominios
                base_domain = '.'.join(domain.split('.')[-2:]) if len(domain.split('.')) >= 2 else domain
                
                if base_domain not in url_by_domain:
                    url_by_domain[base_domain] = []
                url_by_domain[base_domain].append(url)
            except Exception:
                # Si hay algún error al parsear, poner en categoría especial
                if 'errors' not in url_by_domain:
                    url_by_domain['errors'] = []
                url_by_domain['errors'].append(url)
        
        # Limitar el número de dominios a procesar si hay demasiados
        domain_count = len(url_by_domain)
        if domain_count > 20:
            logger.warning(f"Demasiados dominios ({domain_count}). Limitando a los 20 más grandes.")
            sorted_domains = sorted(url_by_domain.items(), key=lambda x: len(x[1]), reverse=True)[:20]
            url_by_domain = dict(sorted_domains)
        
        # Paso 2: Procesar cada dominio por separado
        for domain, domain_urls in url_by_domain.items():
            # No analizar dominios con solo 1 URL
            if len(domain_urls) < 2:
                for url in domain_urls:
                    unique_urls.append(url)
                    url_processed.add(url)
                continue
                
            logger.info(f"Procesando {len(domain_urls)} URLs del dominio {domain}")
            
            # Limitar comparaciones en dominios con muchas URLs
            if len(domain_urls) > 50:
                logger.warning(f"Dominio {domain} tiene demasiadas URLs ({len(domain_urls)}). Muestreando para análisis.")
                # Tomar una muestra representativa
                sampled_urls = domain_urls[:50]  # Primeras 50
            else:
                sampled_urls = domain_urls
            
            # Paso 3: Extraer contenido en paralelo para este dominio
            domain_contents = {}
            for url in sampled_urls:
                if url in url_processed:
                    continue
                    
                # Extraer contenido una vez por URL
                content = self.extract_content(url)
                if content:
                    has_text = bool(content.get('text', '')) or bool(content.get('title', ''))
                    domain_contents[url] = {
                        'content': content,
                        'has_text': has_text
                    }
            
            # Paso 4: Comparar URLs dentro del mismo dominio
            domain_urls_list = list(domain_contents.keys())
            for i, url in enumerate(domain_urls_list):
                if url in url_processed:
                    continue
                    
                url_processed.add(url)
                url_content = domain_contents[url]
                
                # Si esta URL no tiene texto, marcarla como única y continuar
                if not url_content['has_text']:
                    unique_urls.append(url)
                    continue
                    
                found_duplicate = False
                current_group = [url]
                
                # Comparar solo con URLs que aún no se han procesado
                for j in range(i+1, len(domain_urls_list)):
                    comparison_url = domain_urls_list[j]
                    
                    if comparison_url in url_processed:
                        continue
                        
                    comparison_content = domain_contents.get(comparison_url)
                    if not comparison_content or not comparison_content['has_text']:
                        skipped_count += 1
                        continue
                    
                    # Obtener categorías si están disponibles
                    url_cat = url_categories.get(url, {}) if url_categories else {}
                    comparison_url_cat = url_categories.get(comparison_url, {}) if url_categories else {}
                    
                    # Determinar umbral según categorías
                    threshold = self.get_threshold_for_url(
                        url, 
                        url_cat.get('category'), 
                        url_cat.get('subcategory')
                    )
                    
                    # Verificar similitud
                    comparison_count += 1
                    is_similar, similarity, error_msg = self.are_similar(url, comparison_url, threshold)
                    
                    if error_msg:
                        error_types[error_msg] = error_types.get(error_msg, 0) + 1
                    
                    if is_similar:
                        # Encontrado duplicado, añadir a grupo
                        if comparison_count % 50 == 0:
                            logger.info(f"Realizadas {comparison_count} comparaciones ({skipped_count} evitadas)")
                        
                        logger.debug(f"Duplicado encontrado: {url} similar a {comparison_url} ({similarity:.2f})")
                        current_group.append(comparison_url)
                        url_processed.add(comparison_url)
                        found_duplicate = True
                
                # Si encontramos duplicados, añadir el grupo
                if found_duplicate and len(current_group) > 1:
                    duplicate_groups.append(current_group)
                # Si no se encontró duplicado, añadir a URLs únicas
                elif not found_duplicate:
                    unique_urls.append(url)
                    
                # Reportar progreso periódicamente
                if len(url_processed) % 20 == 0:
                    elapsed = time.time() - start_time
                    logger.info(f"Progreso: {len(url_processed)}/{len(urls)} URLs procesadas en {elapsed:.2f} seg.")
        
        # Consolidar y procesar grupos
        final_groups = []
        for group in duplicate_groups:
            if len(group) > 1:  # Solo mantener grupos con al menos 2 URLs
                # Verificar si alguna URL del grupo ya está en otro grupo
                existing_group = None
                for g in final_groups:
                    if any(url in g for url in group):
                        existing_group = g
                        break
                        
                if existing_group:
                    # Añadir URLs nuevas al grupo existente
                    for url in group:
                        if url not in existing_group:
                            existing_group.append(url)
                else:
                    # Añadir como nuevo grupo
                    final_groups.append(group)
        
        # Asegurar que cada URL está en un solo grupo
        url_group_mapping = {}
        for i, group in enumerate(final_groups):
            for url in group:
                url_group_mapping[url] = i
                
        # Reconstruir grupos desde el mapping
        clean_groups = [[] for _ in range(len(final_groups))]
        for url, group_idx in url_group_mapping.items():
            clean_groups[group_idx].append(url)
        
        # Eliminar grupos vacíos
        clean_groups = [g for g in clean_groups if g]
        
        # Calcular estadísticas
        url_in_groups = set()
        for group in clean_groups:
            url_in_groups.update(group)
            
        all_processed_urls = url_processed.union(unique_urls)
        unprocessed_count = len(urls) - len(all_processed_urls)
        
        total_urls = len(urls)
        duplicate_count = len(url_in_groups)
        unique_count = total_urls - duplicate_count
        
        # Estadísticas adicionales
        elapsed_time = time.time() - start_time
        comparisons_per_second = comparison_count / elapsed_time if elapsed_time > 0 else 0
        
        logger.info(f"Análisis completado en {elapsed_time:.2f} segundos")
        logger.info(f"URLs totales: {total_urls}, Únicas: {len(unique_urls)}, En grupos: {duplicate_count}")
        logger.info(f"Se realizaron {comparison_count} comparaciones ({comparisons_per_second:.1f}/seg), evitando {skipped_count}")
        logger.info(f"Grupos de duplicados encontrados: {len(clean_groups)}")
        
        if error_types:
            logger.info("Resumen de errores en comparaciones:")
            for error_type, count in error_types.items():
                if count > 10:  # Solo mostrar errores frecuentes
                    logger.info(f" - {error_type}: {count} veces")
        
        return {
            'groups': clean_groups,
            'unique_urls': list(unique_urls),
            'stats': {
                'total': total_urls,
                'unique': unique_count,
                'duplicate': duplicate_count,
                'groups': len(clean_groups),
                'comparisons': comparison_count,
                'skipped': skipped_count,
                'execution_time_seconds': elapsed_time,
                'errors': error_types
            }
        }
    
    def filter_duplicates(self, urls, url_categories=None, keep_first=True):
        """
        Filtra duplicados de una lista de URLs, manteniendo solo una por grupo
        
        Args:
            urls: Lista de URLs a filtrar
            url_categories: Diccionario con categorías de URLs (opcional)
            keep_first: Si mantener la primera URL de cada grupo (True) o la última (False)
            
        Returns:
            list: URLs únicas después del filtrado
        """
        result = self.find_duplicates(urls, url_categories)
        unique_urls = result['unique_urls']
        
        # Para cada grupo, mantener solo una URL
        for group in result['groups']:
            if keep_first:
                unique_urls.append(group[0])  # Mantener la primera
            else:
                unique_urls.append(group[-1])  # Mantener la última
        
        return unique_urls
