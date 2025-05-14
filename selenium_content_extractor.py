"""
Módulo para extraer contenido de URLs usando Selenium.
Implementa extracción específica por tipo de contenido sin dependencia de MCP.
"""

import os
import json
import logging
import time
import hashlib
from datetime import datetime
from urllib.parse import urlparse
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("selenium_extractor")

class SeleniumContentExtractor:
    """
    Extractor de contenido usando Selenium.
    Implementa métodos específicos para diferentes tipos de URLs.
    """
    
    def __init__(self, cache_dir='cache/selenium', headless=True):
        """
        Inicializar extractor con configuración básica.
        
        Args:
            cache_dir: Directorio para almacenar caché de extracción
            headless: Si ejecutar Chrome en modo headless
        """
        self.cache_dir = cache_dir
        self.headless = headless
        
        # Crear directorio de caché si no existe
        if not os.path.exists(cache_dir):
            os.makedirs(cache_dir, exist_ok=True)
        
        logger.info(f"SeleniumContentExtractor inicializado con caché en: {cache_dir}")
    
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
    
    def extract_content(self, url, url_type=None, use_cache=True):
        """
        Extrae contenido de una URL utilizando Selenium.
        
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
        
        # Inicializar driver
        driver = None
        try:
            # Configurar opciones de Chrome
            chrome_options = Options()
            if self.headless:
                chrome_options.add_argument('--headless=new')
            chrome_options.add_argument('--disable-notifications')
            chrome_options.add_argument('--disable-infobars')
            chrome_options.add_argument('--mute-audio')
            chrome_options.add_argument('--disable-extensions')
            chrome_options.add_argument('--disable-popup-blocking')
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--disable-dev-shm-usage')
            
            # Iniciar driver
            driver = webdriver.Chrome(options=chrome_options)
            driver.set_page_load_timeout(30)  # 30 segundos de timeout
            
            # Navegar a la URL
            driver.get(url)
            time.sleep(3)  # Esperar a que cargue la página
            
            # Usar método específico según tipo de URL
            if url_type == "facebook":
                content_data = self._extract_from_facebook(driver, url)
            elif url_type == "youtube":
                content_data = self._extract_from_youtube(driver, url)
            elif url_type == "news":
                content_data = self._extract_from_news(driver, url)
            elif url_type == "image":
                content_data = self._extract_from_image(driver, url)
            elif url_type == "document":
                content_data = self._extract_from_document(driver, url)
            else:
                content_data = self._extract_generic(driver, url)
            
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
        
        finally:
            # Cerrar driver
            if driver:
                try:
                    driver.quit()
                except:
                    pass
    
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
    
    def _extract_from_facebook(self, driver, url):
        """
        Extrae contenido de una publicación de Facebook.
        
        Args:
            driver: WebDriver de Selenium
            url: URL de Facebook
            
        Returns:
            dict: Contenido extraído y metadatos
        """
        try:
            # Verificar si requiere inicio de sesión
            login_elements = driver.find_elements(By.CSS_SELECTOR, 
                'form[action*="login"], input[name="email"], input[placeholder*="correo"], button[name="login"]')
            
            is_login_page = len(login_elements) > 0
            
            # Si es página de login, extraer información básica
            if is_login_page:
                logger.warning(f"Facebook requiere inicio de sesión para URL: {url}")
                
                # Extraer datos básicos disponibles
                title = driver.title
                
                # Intentar obtener descripción de meta tags
                description = ""
                try:
                    meta_desc = driver.find_element(By.CSS_SELECTOR, 'meta[property="og:description"]')
                    description = meta_desc.get_attribute("content") or ""
                except:
                    pass
                
                return {
                    "url": url,
                    "type": "facebook",
                    "success": True,
                    "title": title,
                    "text": description,
                    "content_limited": True,
                    "login_required": True,
                    "timestamp": datetime.now().isoformat()
                }
            
            # Si no requiere login, extraer contenido completo
            # Detectar tipo de contenido
            is_video_post = len(driver.find_elements(By.CSS_SELECTOR, 'video, [data-sigil*="inlineVideo"]')) > 0
            has_images = len(driver.find_elements(By.CSS_SELECTOR, '[data-ft*="photo"], a[href*="photo.php"]')) > 0
            
            # Extraer autor
            author = ""
            author_elements = driver.find_elements(By.CSS_SELECTOR, 'h3, [data-ft*="author"], strong.actor')
            if author_elements:
                author = author_elements[0].text.strip()
            
            # Extraer fecha
            date = ""
            date_elements = driver.find_elements(By.CSS_SELECTOR, 'abbr')
            if date_elements:
                date = date_elements[0].text or date_elements[0].get_attribute("title") or ""
            
            # Extraer texto principal
            post_text = ""
            text_selectors = [
                'div[data-ft*="content_owner_id_new"]', 
                '.userContent', 
                '[data-ad-preview="message"]',
                '[data-testid="post_message"]'
            ]
            
            for selector in text_selectors:
                elements = driver.find_elements(By.CSS_SELECTOR, selector)
                if elements:
                    post_text = elements[0].text.strip()
                    break
            
            # Si no encontramos texto por selectores específicos, intentar con contenido general
            if not post_text:
                content_elements = driver.find_elements(By.CSS_SELECTOR, '#contentArea, article')
                if content_elements:
                    post_text = content_elements[0].text.strip()
            
            # Buscar URLs de imágenes
            image_urls = []
            img_elements = driver.find_elements(By.CSS_SELECTOR, 'a[href*="photo.php"] img, [data-ft*="photo"] img')
            for img in img_elements:
                src = img.get_attribute("src")
                if src and not src.startswith("data:image"):
                    image_urls.append(src)
            
            # Contar comentarios
            comments_count = 0
            comments_elements = driver.find_elements(By.CSS_SELECTOR, '[data-testid="UFI2CommentsCount/root"]')
            if comments_elements:
                comments_text = comments_elements[0].text
                # Extraer número de comentarios del texto
                import re
                comments_match = re.search(r'\d+', comments_text)
                if comments_match:
                    comments_count = int(comments_match.group())
            
            # Contar reacciones
            reactions_count = 0
            reactions_elements = driver.find_elements(By.CSS_SELECTOR, 
                '[data-testid="UFI2TopReactions/tooltip"] span[aria-hidden="true"]')
            if reactions_elements:
                reactions_text = reactions_elements[0].text
                # Extraer número de reacciones del texto
                import re
                reactions_match = re.search(r'\d+', reactions_text)
                if reactions_match:
                    reactions_count = int(reactions_match.group())
            
            return {
                "url": url,
                "type": "facebook",
                "success": True,
                "title": driver.title,
                "text": post_text,
                "author": author,
                "date": date,
                "has_images": has_images,
                "has_video": is_video_post,
                "images": image_urls,
                "comments_count": comments_count,
                "reactions_count": reactions_count,
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
    
    def _extract_from_youtube(self, driver, url):
        """Extrae contenido de un video de YouTube"""
        try:
            # Esperar a que cargue el título
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, 'h1.title, [id="title"] h1, [id="container"] h1'))
            )
            
            # Extraer título
            title_elements = driver.find_elements(By.CSS_SELECTOR, 'h1.title, [id="title"] h1, [id="container"] h1')
            title = title_elements[0].text if title_elements else driver.title
            
            # Extraer descripción
            description = ""
            desc_elements = driver.find_elements(By.CSS_SELECTOR, 
                '#description, #description-text, [id="description"] yt-formatted-string')
            if desc_elements:
                description = desc_elements[0].text
            
            # Extraer canal
            channel = ""
            channel_elements = driver.find_elements(By.CSS_SELECTOR, '#owner-name a, #channel-name, .ytd-channel-name')
            if channel_elements:
                channel = channel_elements[0].text
            
            # Extraer fecha
            publish_date = ""
            date_elements = driver.find_elements(By.CSS_SELECTOR, 
                '#info-strings yt-formatted-string, #upload-info span.date')
            if date_elements:
                publish_date = date_elements[0].text
            
            # Extraer vistas
            view_count = ""
            view_elements = driver.find_elements(By.CSS_SELECTOR, '.view-count, #count .short-view-count')
            if view_elements:
                view_count = view_elements[0].text
            
            return {
                "url": url,
                "type": "youtube",
                "success": True,
                "title": title,
                "description": description,
                "channel": channel,
                "publish_date": publish_date,
                "view_count": view_count,
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
    
    def _extract_from_news(self, driver, url):
        """Extrae contenido de un sitio de noticias"""
        try:
            # Detectar estructura de la página
            is_article = len(driver.find_elements(By.CSS_SELECTOR, 'article, .article, .post, .nota, .entry')) > 0
            
            # Extraer título
            title = driver.title
            title_selectors = [
                'h1', 
                '.article-title', 
                '.post-title', 
                '.entry-title', 
                'article h1',
                '[property="og:title"]'
            ]
            
            for selector in title_selectors:
                elements = driver.find_elements(By.CSS_SELECTOR, selector)
                if elements:
                    title = elements[0].text.strip()
                    break
            
            # Extraer resumen
            summary = ""
            summary_selectors = [
                '.article-summary', 
                '.entry-summary', 
                '.post-excerpt', 
                '.bajada', 
                '.summary',
                '[property="og:description"]'
            ]
            
            for selector in summary_selectors:
                elements = driver.find_elements(By.CSS_SELECTOR, selector)
                if elements:
                    summary = elements[0].text.strip()
                    break
            
            # Extraer cuerpo
            body = ""
            body_selectors = [
                'article .content', 
                '.article-body', 
                '.post-content', 
                '.entry-content', 
                '.article-text'
            ]
            
            for selector in body_selectors:
                elements = driver.find_elements(By.CSS_SELECTOR, selector)
                if elements:
                    body = elements[0].text.strip()
                    break
            
            # Si no encontramos el cuerpo con selectores específicos
            if not body and is_article:
                article_elements = driver.find_elements(By.CSS_SELECTOR, 'article, .article, .post, .nota, .entry')
                if article_elements:
                    body = article_elements[0].text.strip()
            
            # Extraer autor
            author = ""
            author_selectors = ['.author', '.article-author', '.byline', '[rel="author"]']
            for selector in author_selectors:
                elements = driver.find_elements(By.CSS_SELECTOR, selector)
                if elements:
                    author = elements[0].text.strip()
                    break
            
            # Extraer fecha
            publish_date = ""
            date_selectors = ['.date', '.article-date', '.post-date', '[property="article:published_time"]', 'time']
            for selector in date_selectors:
                elements = driver.find_elements(By.CSS_SELECTOR, selector)
                if elements:
                    publish_date = elements[0].text.strip() or elements[0].get_attribute("datetime") or ""
                    break
            
            # Verificar si tiene imágenes
            has_images = len(driver.find_elements(By.CSS_SELECTOR, 'article img, .article img, .post img')) > 0
            
            return {
                "url": url,
                "type": "news",
                "success": True,
                "title": title,
                "summary": summary,
                "body": body,
                "author": author,
                "publish_date": publish_date,
                "has_images": has_images,
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
    
    def _extract_from_image(self, driver, url):
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
    
    def _extract_from_document(self, driver, url):
        """Extrae contenido de un documento (PDF, etc.)"""
        try:
            # Para PDF y documentos, intentar extraer texto visible
            # Detectar si es un PDF incrustado
            is_pdf = len(driver.find_elements(By.CSS_SELECTOR, 
                'embed[type="application/pdf"], object[type="application/pdf"]')) > 0
            
            # Si es PDF incrustado, obtener título y metadatos
            if is_pdf:
                return {
                    "url": url,
                    "type": "document",
                    "success": True,
                    "title": driver.title,
                    "text": "Documento PDF detectado - texto no extraíble directamente",
                    "page_count": 0,
                    "is_pdf": True,
                    "timestamp": datetime.now().isoformat()
                }
            
            # Si no es PDF incrustado, intentar extraer texto visible
            body_text = driver.find_element(By.TAG_NAME, "body").text.strip()
            
            return {
                "url": url,
                "type": "document",
                "success": True,
                "title": driver.title,
                "text": body_text,
                "page_count": 1,
                "is_pdf": False,
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
    
    def _extract_generic(self, driver, url):
        """Extrae contenido de una URL genérica"""
        try:
            # Extraer texto principal, excluyendo cosas como menús, pies de página, etc.
            # Buscar el elemento con más texto (probable contenido principal)
            content_selectors = [
                'main',
                'article',
                '#content',
                '.content',
                '.main',
                '.post',
                '.page'
            ]
            
            main_element = None
            max_length = 0
            
            for selector in content_selectors:
                elements = driver.find_elements(By.CSS_SELECTOR, selector)
                if elements:
                    text = elements[0].text.strip()
                    if len(text) > max_length:
                        max_length = len(text)
                        main_element = elements[0]
            
            # Si no encontramos contenido principal específico, usar todo el body
            if not main_element:
                main_element = driver.find_element(By.TAG_NAME, "body")
            
            main_text = main_element.text.strip()
            
            return {
                "url": url,
                "type": "other",
                "success": True,
                "title": driver.title,
                "text": main_text,
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
