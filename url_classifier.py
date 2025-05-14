"""
Módulo para clasificación avanzada de URLs.
Complementa el sistema existente con categorización más detallada.
"""

import os
import re
import logging
from urllib.parse import urlparse, unquote
import tldextract

# Configurar logging
logger = logging.getLogger(__name__)

# Dominios de redes sociales con su categoría
SOCIAL_DOMAINS = {
    # Facebook
    'facebook.com': 'facebook',
    'fb.com': 'facebook',
    'm.facebook.com': 'facebook',
    'web.facebook.com': 'facebook',
    
    # YouTube
    'youtube.com': 'youtube',
    'youtu.be': 'youtube',
    'm.youtube.com': 'youtube',
    
    # Twitter/X
    'twitter.com': 'twitter',
    'x.com': 'twitter',
    't.co': 'twitter',
    'm.twitter.com': 'twitter',
    
    # Otras redes sociales
    'instagram.com': 'instagram',
    'linkedin.com': 'linkedin',
    'tiktok.com': 'tiktok',
    'pinterest.com': 'pinterest',
    'reddit.com': 'reddit',
    'tumblr.com': 'tumblr'
}

# Patrones de URL para sitios de noticias conocidos
NEWS_PATTERNS = [
    r'\/noticias\/',
    r'\/news\/',
    r'\/article\/',
    r'\/articulo\/',
    r'\/noticia\/',
    r'\/politica\/',
    r'\/economia\/',
    r'\/deportes\/'
]

# Patrones de URLs para sitios gubernamentales
GOV_PATTERNS = [
    r'\.gob\.(pe|mx|ar|co|cl|ec|br)',
    r'\.gov\.',
    r'\.mil\.',
    r'\/gobierno\/',
    r'municipalidad',
    r'ministerio'
]

# Patrones para sitios educativos
EDU_PATTERNS = [
    r'\.edu\.',
    r'\.ac\.',
    r'universidad',
    r'institute',
    r'instituto',
    r'school',
    r'colegio',
    r'academy'
]

# Extensiones de documentos
DOCUMENT_EXTENSIONS = [
    '.pdf', '.doc', '.docx', '.ppt', '.pptx', '.xls', '.xlsx',
    '.odt', '.ods', '.odp', '.txt', '.rtf'
]

# Extensiones de imágenes
IMAGE_EXTENSIONS = [
    '.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp', '.svg', '.tiff', '.ico'
]

# Extensiones de audio
AUDIO_EXTENSIONS = [
    '.mp3', '.wav', '.ogg', '.m4a', '.flac', '.aac'
]

class URLClassifier:
    """
    Clasificador avanzado de URLs que categoriza en múltiples niveles.
    Compatible con el sistema de clasificación existente.
    """
    
    def __init__(self):
        """Inicializa el clasificador"""
        logger.info("URLClassifier inicializado")
        self.domain_cache = {}  # Caché para resultados de clasificación por dominio
    
    def classify_url(self, url):
        """
        Clasifica una URL en categorías y subcategorías.
        
        Args:
            url: La URL a clasificar
            
        Returns:
            dict: Diccionario con categoría, subcategoría y metadatos
        """
        if not url or not isinstance(url, str):
            return {
                'category': 'invalid',
                'subcategory': None,
                'valid': False
            }
        
        try:
            parsed = urlparse(url)
            if not parsed.scheme or not parsed.netloc:
                return {
                    'category': 'invalid',
                    'subcategory': None,
                    'valid': False
                }
            
            # Normalizar dominio
            domain = parsed.netloc.lower()
            path = parsed.path.lower()
            query = parsed.query.lower()
            
            # Verificar extensión del archivo en la URL
            _, ext = os.path.splitext(parsed.path)
            ext = ext.lower()
            
            # Comprobar audio (MP3 y otros formatos)
            if ext in AUDIO_EXTENSIONS or 'mp3' in path or 'audio' in path:
                return {
                    'category': 'audio',
                    'subcategory': 'mp3' if ext == '.mp3' or 'mp3' in path else 'other_audio',
                    'valid': True,
                    'extension': ext if ext else None
                }
            
            # Comprobar imágenes
            if ext in IMAGE_EXTENSIONS or any(img_pat in path for img_pat in ['/images/', '/img/', '/static/images/']):
                return {
                    'category': 'images',
                    'subcategory': ext[1:] if ext else 'other_image',
                    'valid': True,
                    'extension': ext if ext else None
                }
            
            # Comprobar documentos
            if ext in DOCUMENT_EXTENSIONS:
                return {
                    'category': 'html',
                    'subcategory': 'documents',
                    'valid': True,
                    'extension': ext,
                    'document_type': ext[1:]
                }
            
            # Comprobar redes sociales (prioridad alta)
            domain_parts = domain.split('.')
            for i in range(len(domain_parts)):
                check_domain = '.'.join(domain_parts[-(min(i+2, len(domain_parts))):])
                if check_domain in SOCIAL_DOMAINS:
                    return {
                        'category': 'social',
                        'subcategory': SOCIAL_DOMAINS[check_domain],
                        'valid': True,
                        'domain': check_domain
                    }
            
            # Extraer componentes del dominio para análisis
            extracted = tldextract.extract(domain)
            domain_without_subdomain = f"{extracted.domain}.{extracted.suffix}"
            
            # Comprobar patrones específicos en la URL para HTML
            
            # Sitios gubernamentales
            if any(re.search(pattern, url, re.IGNORECASE) for pattern in GOV_PATTERNS):
                return {
                    'category': 'html',
                    'subcategory': 'government',
                    'valid': True,
                    'domain': domain
                }
            
            # Sitios educativos
            if any(re.search(pattern, url, re.IGNORECASE) for pattern in EDU_PATTERNS):
                return {
                    'category': 'html',
                    'subcategory': 'educational',
                    'valid': True,
                    'domain': domain
                }
            
            # Sitios de noticias
            if any(re.search(pattern, url, re.IGNORECASE) for pattern in NEWS_PATTERNS):
                return {
                    'category': 'html',
                    'subcategory': 'news',
                    'valid': True,
                    'domain': domain
                }
            
            # Si llegamos aquí, es HTML genérico o sin clasificar
            return {
                'category': 'html',
                'subcategory': 'other_html',
                'valid': True,
                'domain': domain
            }
            
        except Exception as e:
            logger.warning(f"Error clasificando URL '{url}': {e}")
            return {
                'category': 'invalid',
                'subcategory': None,
                'valid': False,
                'error': str(e)
            }
    
    def classify_urls(self, urls):
        """
        Clasifica una lista de URLs según la nueva estructura de categorías.
        Compatible con el formato de salida del url_manager.py existente.
        
        Args:
            urls: Lista de URLs o de diccionarios con clave 'URL'
            
        Returns:
            dict: Categorías y subcategorías con sus URLs
        """
        categorized = {
            'social': {
                'facebook': [],
                'youtube': [],
                'twitter': [],
                'other_social': []
            },
            'html': {
                'news': [],
                'government': [],
                'educational': [],
                'documents': [],
                'other_html': []
            },
            'images': [],
            'audio': [],
            'other': [],
            'invalid': []
        }
        
        # Estadísticas para el log
        stats = {cat: {subcat: 0 for subcat in subcats} if isinstance(subcats, dict) else 0 
                for cat, subcats in categorized.items()}
        
        for item in urls:
            # Determinar la URL (puede ser string o dict con clave 'URL')
            if isinstance(item, dict) and 'URL' in item:
                url = item['URL']
                item_dict = item
            else:
                url = item
                item_dict = {'URL': url}
            
            # Clasificar la URL
            classification = self.classify_url(url)
            category = classification['category']
            subcategory = classification['subcategory']
            
            # Actualizar estadísticas
            if isinstance(categorized[category], dict) and subcategory in categorized[category]:
                categorized[category][subcategory].append(item_dict)
                stats[category][subcategory] += 1
            elif category in categorized:
                categorized[category].append(item_dict)
                stats[category] += 1
            else:
                categorized['other'].append(item_dict)
                stats['other'] += 1
        
        # Registrar estadísticas
        logger.info("Clasificación de URLs completada:")
        for cat, subcats in stats.items():
            if isinstance(subcats, dict):
                total_cat = sum(subcats.values())
                if total_cat > 0:
                    logger.info(f" - {cat.upper()}: {total_cat} URLs")
                    for subcat, count in subcats.items():
                        if count > 0:
                            logger.info(f"   - {subcat}: {count}")
            elif subcats > 0:
                logger.info(f" - {cat.upper()}: {subcats} URLs")
        
        return categorized
    
    def get_legacy_format(self, classified_urls):
        """
        Convierte la clasificación avanzada al formato legacy esperado por main2.py
        
        Args:
            classified_urls: Resultado de classify_urls()
            
        Returns:
            dict: Formato compatible con el sistema existente
        """
        legacy = {
            'html': [],
            'images': [],
            'social': [],
            'other': []
        }
        
        # Mapear a categorías legacy
        # HTML y subcategorías
        for subcat, urls in classified_urls['html'].items():
            legacy['html'].extend(urls)
        
        # Imágenes
        legacy['images'].extend(classified_urls['images'])
        
        # Social y subcategorías
        for subcat, urls in classified_urls['social'].items():
            legacy['social'].extend(urls)
        
        # Audio y otros
        legacy['other'].extend(classified_urls['audio'])
        legacy['other'].extend(classified_urls['other'])
        legacy['other'].extend(classified_urls['invalid'])
        
        return legacy

def extract_domain_info(url):
    """
    Extrae información detallada del dominio para análisis.
    
    Args:
        url: URL a analizar
        
    Returns:
        dict: Información del dominio
    """
    try:
        parsed = urlparse(url)
        domain = parsed.netloc.lower()
        
        extracted = tldextract.extract(domain)
        
        return {
            'domain': domain,
            'subdomain': extracted.subdomain,
            'domain_name': extracted.domain,
            'tld': extracted.suffix,
            'path': parsed.path,
            'query': parsed.query,
            'scheme': parsed.scheme,
            'full_url': url
        }
    except Exception:
        return {
            'domain': None,
            'full_url': url
        }
