"""
Módulo para extraer contenido real de URLs usando Puppeteer.
Implementa extracción específica por tipo de contenido.
"""

import os
import json
import logging
import time
import hashlib
from datetime import datetime
from urllib.parse import urlparse
import asyncio

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("puppeteer_extractor")

class ContentExtractor:
    """
    Extractor de contenido usando Puppeteer.
    Implementa métodos específicos para diferentes tipos de URLs.
    """
    
    def __init__(self, cache_dir='cache/puppeteer'):
        """
        Inicializar extractor con configuración básica.
        
        Args:
            cache_dir: Directorio para almacenar caché de extracción
        """
        self.cache_dir = cache_dir
        
        # Crear directorio de caché si no existe
        if not os.path.exists(cache_dir):
            os.makedirs(cache_dir, exist_ok=True)
        
        logger.info(f"ContentExtractor inicializado con caché en: {cache_dir}")
    
    def _get_cache_key(self, url):
        """Genera una clave única para el caché basada en la URL"""
        return hashlib.md5(url.encode()).hexdigest()
    
    def _check_cache(self, url, max_age_days=7):
        """
        Verifica si hay una versión cacheada del contenido de la URL.
        
        Args:
            url: URL a verificar
            max_age_days: Edad máxima del caché en días
            
        Returns:
            dict o None: Contenido cacheado o None si no hay caché válida
        """
        cache_key = self._get_cache_key(url)
        cache_file = os.path.join(self.cache_dir, f"{cache_key}.json")
        
        if not os.path.exists(cache_file):
            return None
        
        try:
            with open(cache_file, 'r', encoding='utf-8') as f:
                cached_data = json.load(f)
            
            # Verificar edad del caché
            cached_timestamp = datetime.fromisoformat(cached_data.get('timestamp', '2000-01-01'))
            now = datetime.now()
            age_days = (now - cached_timestamp).days
            
            if age_days > max_age_days:
                logger.info(f"Caché expirado para URL: {url} (edad: {age_days} días)")
                return None
            
            logger.info(f"Usando caché para URL: {url} (edad: {age_days} días)")
            return cached_data
        
        except Exception as e:
            logger.warning(f"Error leyendo caché para URL {url}: {e}")
            return None
    
    def _save_to_cache(self, url, content_data):
        """
        Guarda el contenido extraído en caché.
        
        Args:
            url: URL del contenido
            content_data: Datos extraídos a guardar
        """
        try:
            # Asegurar que los datos tengan timestamp
            if 'timestamp' not in content_data:
                content_data['timestamp'] = datetime.now().isoformat()
            
            cache_key = self._get_cache_key(url)
            cache_file = os.path.join(self.cache_dir, f"{cache_key}.json")
            
            with open(cache_file, 'w', encoding='utf-8') as f:
                json.dump(content_data, f, ensure_ascii=False, indent=2)
            
            logger.debug(f"Contenido guardado en caché para URL: {url}")
        
        except Exception as e:
            logger.warning(f"Error guardando caché para URL {url}: {e}")
    
    async def extract_content(self, url, url_type=None, use_cache=True):
        """
        Extrae contenido de una URL utilizando Puppeteer.
        
        Args:
            url: URL para extraer contenido
            url_type: Tipo de URL (facebook, youtube, news, etc.)
            use_cache: Si usar el caché o forzar nueva extracción
            
        Returns:
            dict: Datos extraídos con formato normalizado
        """
        # Verificar caché si está habilitado
        if use_cache:
            cached_data = self._check_cache(url)
            if cached_data:
                return cached_data
        
        # Determinar tipo de URL si no se especificó
        if not url_type:
            url_type = self._categorize_url(url)
        
        logger.info(f"Extrayendo contenido de URL: {url} (tipo: {url_type})")
        
        # Usar método específico según tipo de URL
        try:
            if url_type == "facebook":
                content_data = await self._extract_from_facebook(url)
            elif url_type == "youtube":
                content_data = await self._extract_from_youtube(url)
            elif url_type == "news":
                content_data = await self._extract_from_news(url)
            elif url_type == "image":
                content_data = await self._extract_from_image(url)
            elif url_type == "document":
                content_data = await self._extract_from_document(url)
            else:
                content_data = await self._extract_generic(url)
            
            # Guardar en caché
            self._save_to_cache(url, content_data)
            
            return content_data
        
        except Exception as e:
            logger.error(f"Error extrayendo contenido de {url}: {e}")
            return {
                "url": url,
                "type": url_type,
                "success": False,
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }
    
    def _categorize_url(self, url):
        """Categoriza una URL basada en su dominio y patrón"""
        url_lower = url.lower()
        
        if "facebook.com" in url_lower or "fb.com" in url_lower:
            return "facebook"
        elif "youtube.com" in url_lower or "youtu.be" in url_lower:
            return "youtube"
        elif any(x in url_lower for x in ["rpp.pe", "larepublica.pe", "elperuano", 
                                         "diariocorreo", "andina.pe", "infobae.com"]):
            return "news"
        elif any(ext in url_lower for ext in [".jpg", ".jpeg", ".png", ".gif"]):
            return "image"
        elif url_lower.endswith(".pdf") or "dispositivo" in url_lower:
            return "document"
        elif any(ext in url_lower for ext in [".mp3", ".mp4", ".avi", ".mov"]):
            return "media"
        else:
            return "other"
    
    async def _extract_from_facebook(self, url):
        """
        Extrae contenido de una publicación de Facebook.
        Maneja varios tipos de publicaciones.
        
        Args:
            url: URL de Facebook a procesar
            
        Returns:
            dict: Contenido extraído y metadatos
        """
        from mcp_puppeteer_bridge import extract_facebook_content
        
        try:
            # Aquí llamamos a la implementación real con puppeteer
            content = await extract_facebook_content(url)
            
            return {
                "url": url,
                "type": "facebook",
                "success": True,
                "title": content.get("title", ""),
                "text": content.get("text", ""),
                "author": content.get("author", ""),
                "date": content.get("date", ""),
                "has_images": content.get("has_images", False),
                "has_video": content.get("has_video", False),
                "images": content.get("images", []),
                "comments_count": content.get("comments_count", 0),
                "reactions_count": content.get("reactions_count", 0),
                "timestamp": datetime.now().isoformat()
            }
        except Exception as e:
            logger.error(f"Error extrayendo contenido de Facebook {url}: {e}")
            return {
                "url": url,
                "type": "facebook",
                "success": False,
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }
    
    async def _extract_from_youtube(self, url):
        """Extrae contenido de un video de YouTube"""
        from mcp_puppeteer_bridge import extract_youtube_content
        
        try:
            content = await extract_youtube_content(url)
            
            return {
                "url": url,
                "type": "youtube",
                "success": True,
                "title": content.get("title", ""),
                "description": content.get("description", ""),
                "channel": content.get("channel", ""),
                "publish_date": content.get("publish_date", ""),
                "view_count": content.get("view_count", ""),
                "timestamp": datetime.now().isoformat()
            }
        except Exception as e:
            logger.error(f"Error extrayendo contenido de YouTube {url}: {e}")
            return {
                "url": url,
                "type": "youtube",
                "success": False,
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }
    
    async def _extract_from_news(self, url):
        """Extrae contenido de un sitio de noticias"""
        from mcp_puppeteer_bridge import extract_news_content
        
        try:
            content = await extract_news_content(url)
            
            return {
                "url": url,
                "type": "news",
                "success": True,
                "title": content.get("title", ""),
                "summary": content.get("summary", ""),
                "body": content.get("body", ""),
                "author": content.get("author", ""),
                "publish_date": content.get("publish_date", ""),
                "has_images": content.get("has_images", False),
                "timestamp": datetime.now().isoformat()
            }
        except Exception as e:
            logger.error(f"Error extrayendo contenido de noticias {url}: {e}")
            return {
                "url": url,
                "type": "news",
                "success": False,
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }
    
    async def _extract_from_image(self, url):
        """Extrae metadatos de una imagen"""
        # Para imágenes, solo registramos metadatos básicos
        domain = urlparse(url).netloc
        filename = os.path.basename(urlparse(url).path)
        
        return {
            "url": url,
            "type": "image",
            "success": True,
            "domain": domain,
            "filename": filename,
            "timestamp": datetime.now().isoformat()
        }
    
    async def _extract_from_document(self, url):
        """Extrae contenido de un documento (PDF, etc.)"""
        from mcp_puppeteer_bridge import extract_document_content
        
        try:
            content = await extract_document_content(url)
            
            return {
                "url": url,
                "type": "document",
                "success": True,
                "title": content.get("title", ""),
                "text": content.get("text", ""),
                "page_count": content.get("page_count", 0),
                "timestamp": datetime.now().isoformat()
            }
        except Exception as e:
            logger.error(f"Error extrayendo contenido de documento {url}: {e}")
            return {
                "url": url,
                "type": "document",
                "success": False,
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }
    
    async def _extract_generic(self, url):
        """Extrae contenido de una URL genérica"""
        from mcp_puppeteer_bridge import extract_generic_content
        
        try:
            content = await extract_generic_content(url)
            
            return {
                "url": url,
                "type": "other",
                "success": True,
                "title": content.get("title", ""),
                "text": content.get("text", ""),
                "timestamp": datetime.now().isoformat()
            }
        except Exception as e:
            logger.error(f"Error extrayendo contenido genérico {url}: {e}")
            return {
                "url": url,
                "type": "other",
                "success": False,
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }
