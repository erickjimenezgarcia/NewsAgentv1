# codigo/lib/url_manager.py
from urllib.parse import urlparse, unquote
import logging
import re

logger = logging.getLogger(__name__)

# Dominios comunes de redes sociales (lista más completa)
SOCIAL_DOMAINS = {
    'facebook.com', '[www.facebook.com](https://www.facebook.com)',
    'twitter.com', 'x.com', # Incluir x.com
    'instagram.com', '[www.instagram.com](https://www.instagram.com)',
    'linkedin.com', '[www.linkedin.com](https://www.linkedin.com)',
    'youtube.com', '[www.youtube.com](https://www.youtube.com)', 'youtu.be',
    'pinterest.com', '[www.pinterest.com](https://www.pinterest.com)',
    'tiktok.com', '[www.tiktok.com](https://www.tiktok.com)',
    'whatsapp.com', # Enlaces wa.me
    't.me', # Telegram
    'reddit.com', '[www.reddit.com](https://www.reddit.com)',
    # Añadir más si es necesario
}

# Extensiones comunes de imágenes y patrones en URL
IMAGE_EXTENSIONS = {'.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp', '.svg', '.tiff', '.ico'}
IMAGE_PATH_PATTERNS = ['/uploads/', '/images/', '/img/', '/static/images/', '/media/']

def is_valid_url(url):
    """Verifica si una URL tiene esquema y dominio."""
    if not isinstance(url, str) or not url:
        return False
    try:
        parsed = urlparse(url)
        # Requiere esquema (http, https) y netloc (dominio)
        return bool(parsed.scheme) and bool(parsed.netloc)
    except ValueError:
        # URL inválida que causa error en urlparse
        return False

def is_image_url(url):
    """
    Determina si una URL probablemente apunta a una imagen basado en la extensión
    o patrones comunes en la ruta.
    """
    if not is_valid_url(url):
        return False

    try:
        parsed = urlparse(unquote(url)) # Decodificar %20 etc.
        path_lower = parsed.path.lower()

        # Comprobar extensión
        if any(path_lower.endswith(ext) for ext in IMAGE_EXTENSIONS):
            return True

        # Comprobar patrones en la ruta
        if any(pattern in path_lower for pattern in IMAGE_PATH_PATTERNS):
            return True

        # Comprobar parámetros de consulta (menos fiable, pero posible)
        # query_lower = parsed.query.lower()
        # if 'format=jpg' in query_lower or 'format=png' in query_lower:
        #     return True

    except Exception as e:
        logger.warning(f"Error analizando URL '{url}' para imagen: {e}")
        return False

    return False


def is_social_media_url(url):
    """Determina si una URL pertenece a un dominio de red social conocido."""
    if not is_valid_url(url):
        return False
    try:
        parsed = urlparse(url)
        domain = parsed.netloc.lower()
        # Manejar subdominios (e.g., m.facebook.com)
        return any(domain == social_domain or domain.endswith('.' + social_domain) for social_domain in SOCIAL_DOMAINS)
    except Exception as e:
         logger.warning(f"Error analizando URL '{url}' para red social: {e}")
         return False


def classify_urls(links):
    """
    Clasifica una lista de diccionarios de enlaces (con clave 'URL')
    en categorías: 'html', 'images', 'social', 'other'.
    Valida las URLs antes de clasificarlas.
    """
    categories = {'html': [], 'images': [], 'social': [], 'other': []}
    invalid_count = 0
    processed_count = 0

    for link_info in links:
        url = link_info.get("URL")
        processed_count += 1

        if not is_valid_url(url):
            logger.debug(f"URL inválida o vacía omitida: '{url}'")
            invalid_count += 1
            continue

        # Clasificación prioritaria:
        if is_image_url(url):
            categories['images'].append(link_info)
        elif is_social_media_url(url):
            categories['social'].append(link_info)
        # Si no es imagen ni social, asumimos HTML por ahora
        # Podríamos añadir más chequeos aquí (e.g., PDFs, docs, etc.)
        # elif url.lower().endswith(('.pdf', '.doc', '.docx', '.xls', '.xlsx')):
        #    categories['documents'].append(link_info)
        else:
            # Considerar si hay que excluir otros tipos explícitamente
            # Por defecto, va a HTML si no es imagen o social
             if urlparse(url).scheme in ['http', 'https']: # Asegurar que sea web
                categories['html'].append(link_info)
             else: # Otros esquemas (ftp, etc.) o casos no manejados
                categories['other'].append(link_info)


    logger.info(f"Clasificación de {processed_count} URLs:")
    for category, items in categories.items():
        if items: # Solo mostrar si hay elementos
            logger.info(f" - {category.upper()}: {len(items)}")
    if invalid_count > 0:
         logger.warning(f" - Inválidas/Omitidas: {invalid_count}")


    return categories