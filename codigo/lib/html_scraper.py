# codigo/lib/html_scraper.py
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
import re
import time
import random
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager # Gestiona driver

# Importar utilidades locales
from .cache_utils import get_cache_key, load_from_cache, save_to_cache
from .file_manager import save_to_json # Para guardar progreso

logger = logging.getLogger(__name__)

# --- Funciones de ayuda ---

def create_session_with_retries(retries=3, backoff_factor=0.5, status_forcelist=(500, 502, 503, 504)):
    """Crea una sesión de Requests con reintentos configurados."""
    session = requests.Session()
    retry_strategy = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

def normalize_text(text):
    """Limpia y normaliza el texto extraído."""
    if not text:
        return ""
    # Reemplaza múltiples espacios/saltos de línea con un solo espacio
    text = re.sub(r'\s+', ' ', text)
    # Elimina espacios al principio/final
    return text.strip()

def calculate_relevance(text, keywords):
    """Calcula una puntuación de relevancia simple basada en palabras clave."""
    if not text or not keywords:
        return 0.0
    text_lower = text.lower()
    score = 0.0
    # Ponderación simple, SUNASS más importante
    weights = {kw.lower(): (0.5 if kw.lower() == "sunass" else 0.2) for kw in keywords}

    found_keywords = set()
    for keyword, weight in weights.items():
        if keyword in text_lower:
             # Contar solo una vez por palabra clave única
            if keyword not in found_keywords:
                 score += weight
                 found_keywords.add(keyword)


    # Normalizar score para que esté entre 0 y 1 (aproximadamente)
    # Podría ser > 1 si hay muchas palabras clave, limitar a 1.
    return min(score, 1.0)


def setup_selenium_driver():
     """Configura e inicializa un driver de Selenium headless."""
     options = Options()
     options.add_argument("--headless")
     options.add_argument("--disable-gpu") # A veces necesario en headless
     options.add_argument("--window-size=1920x1080") # Definir tamaño ventana
     options.add_argument("--no-sandbox") # Necesario en algunos entornos Linux/Docker
     options.add_argument("--disable-dev-shm-usage") # Necesario en algunos entornos Linux/Docker
     # Evitar detección de bot (básico)
     options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36") # Usar el user-agent solicitado
     options.add_experimental_option('excludeSwitches', ['enable-logging']) # Limpiar output consola


     try:
        # Usa webdriver-manager para descargar/gestionar el chromedriver
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
        logger.info("Driver de Selenium (Chrome) inicializado correctamente.")
        return driver
     except Exception as e:
        logger.error(f"Error inicializando Selenium WebDriver: {e}")
        logger.error("Asegúrate de que Chrome esté instalado y accesible.")
        logger.error("O verifica problemas con webdriver-manager (puede requerir conexión a internet la primera vez).")
        return None


def scrape_with_selenium(url, driver):
    """Realiza scraping usando una instancia existente de Selenium WebDriver."""
    if not driver:
         logger.error("Intento de scrape con Selenium sin driver válido.")
         return {"error": "Selenium driver not initialized"}

    try:
        logger.debug(f"Scrapeando con Selenium: {url}")
        driver.get(url)
        # Espera inteligente podría ser mejor, pero simple sleep por ahora
        time.sleep(random.uniform(3, 5)) # Espera para carga JS

        page_source = driver.page_source
        current_url = driver.current_url
        title = driver.title

        soup = BeautifulSoup(page_source, "html.parser")

        # Eliminar tags no deseados (scripts, estilos, etc.)
        for tag in soup(["script", "style", "header", "footer", "nav", "aside", "form"]):
            tag.decompose()

        text = normalize_text(soup.get_text(separator=' ', strip=True))

        content = {
            "metadata": {"title": title, "url": current_url},
            "text": text,
            "content_type": "text/html (selenium)"
        }
        return content

    except Exception as e:
        logger.warning(f"Error scrapeando {url} con Selenium: {e}")
        return {"error": f"Selenium scrape failed: {str(e)}"}


# --- Clase principal del Scraper ---

class HTMLScraper:
    def __init__(self, config):
        self.config = config
        self.session = create_session_with_retries()
        self.headers = config.get('headers', {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/91.0.4472.124'}) # Usa User-Agent de config
        self.cache_dir = config.get('paths', {}).get('cache_dir')
        self.cache_expiry = config.get('cache_expiry')
        self.keywords = config.get('keywords', [])
        self.selenium_driver = None # Inicializar driver solo si se necesita

    def _get_selenium_driver(self):
         """Obtiene o inicializa el driver de Selenium."""
         if self.selenium_driver is None:
             self.selenium_driver = setup_selenium_driver()
         return self.selenium_driver

    def close_selenium_driver(self):
         """Cierra el driver de Selenium si está abierto."""
         if self.selenium_driver:
             try:
                 self.selenium_driver.quit()
                 logger.info("Driver de Selenium cerrado.")
             except Exception as e:
                 logger.warning(f"Error cerrando driver Selenium: {e}")
             finally:
                 self.selenium_driver = None


    def scrape_single_url(self, url_info):
        """
        Realiza el scraping de una única URL (diccionario con 'URL', 'Context', 'Page').
        Gestiona caché y decide si usar Requests o Selenium.
        """
        url = url_info.get("URL")
        context = url_info.get("Context", "")
        page = url_info.get("Page", None)

        if not url:
            return url, {"error": "URL vacía", "context": context, "page": page}

        cache_key = get_cache_key(url)
        if self.cache_dir and self.cache_expiry is not None:
            cached_result = load_from_cache(self.cache_dir, cache_key, self.cache_expiry)
            if cached_result:
                logger.debug(f"Usando caché para {url}")
                # Añadir contexto y página al resultado cacheado si no lo tiene
                if 'context' not in cached_result: cached_result['context'] = context
                if 'page' not in cached_result: cached_result['page'] = page
                return url, cached_result

        # Decidir si usar Selenium (ejemplo simple: para ciertos dominios)
        # Ajusta esta lógica según sea necesario
        use_selenium = False
        if any(domain in url.lower() for domain in ['[example.com/dynamic](https://www.google.com/search?q=https://example.com/dynamic)', 'javascript-heavy.site']):
             use_selenium = True
             logger.info(f"Usando Selenium para: {url}")


        result = {}
        try:
            if use_selenium:
                driver = self._get_selenium_driver()
                if driver:
                     content = scrape_with_selenium(url, driver)
                else:
                     content = {"error": "Selenium driver failed to initialize"}
            else:
                # Usar Requests
                logger.debug(f"Scrapeando con Requests: {url}")
                response = self.session.get(url, headers=self.headers, timeout=20, allow_redirects=True) # Aumentar timeout, permitir redirects
                response.raise_for_status() # Error si no es 2xx

                content_type = response.headers.get('Content-Type', '').lower()
                if 'text/html' not in content_type:
                    logger.info(f"Contenido no es HTML para {url} ({content_type}). Omitiendo body.")
                    content = {"content_type": content_type, "message": "No HTML content", "metadata": {"url": response.url}} # Guardar URL final
                else:
                    # Asegurar codificación correcta
                    response.encoding = response.apparent_encoding if response.apparent_encoding else 'utf-8'
                    soup = BeautifulSoup(response.text, "html.parser")

                    # Extraer metadatos
                    title_tag = soup.find("title")
                    title = title_tag.string.strip() if title_tag else ""
                    description_tag = soup.find("meta", attrs={"name": re.compile(r"description", re.I)})
                    description = description_tag["content"].strip() if description_tag and description_tag.get("content") else ""

                    metadata = {"title": title, "description": description, "url": response.url} # Guardar URL final

                    # Limpiar HTML antes de extraer texto
                    for tag in soup(["script", "style", "header", "footer", "nav", "aside", "form"]):
                         tag.decompose()

                    text = normalize_text(soup.get_text(separator=' ', strip=True))
                    content = {"metadata": metadata, "text": text, "content_type": "text/html"}

            # Añadir contexto, página y calcular relevancia a cualquier resultado exitoso (no error)
            if "error" not in content:
                full_text_for_relevance = f"{content.get('metadata', {}).get('title', '')} {content.get('metadata', {}).get('description', '')} {content.get('text', '')}"
                content["relevance"] = calculate_relevance(full_text_for_relevance, self.keywords)

            # Añadir siempre contexto y página al resultado final
            content["context"] = context
            content["page"] = page
            result = content

            # Guardar en caché si fue exitoso (sin error) y el caché está habilitado
            if "error" not in result and self.cache_dir:
                save_to_cache(self.cache_dir, cache_key, result)

            # Pausa aleatoria para no sobrecargar servidores
            time.sleep(random.uniform(0.5, 1.5))

        except requests.exceptions.Timeout:
            logger.warning(f"Timeout scrapeando {url}")
            result = {"error": "Timeout", "context": context, "page": page}
        except requests.exceptions.HTTPError as e:
             logger.warning(f"Error HTTP {e.response.status_code} scrapeando {url}: {e}")
             result = {"error": f"HTTP Error: {e.response.status_code}", "status_code": e.response.status_code, "context": context, "page": page}
        except requests.exceptions.RequestException as e:
            logger.warning(f"Error de red scrapeando {url}: {e}")
            result = {"error": f"Network Error: {str(e)}", "context": context, "page": page}
        except Exception as e:
            logger.error(f"Error inesperado scrapeando {url}: {e}", exc_info=True) # Log stack trace
            result = {"error": f"Unexpected Error: {str(e)}", "context": context, "page": page}

        return url, result


    def scrape_urls_parallel(self, url_infos, output_json_path):
        """
        Realiza scraping de una lista de URLs (diccionarios) en paralelo.
        Guarda los resultados en un archivo JSON.
        """
        scraped_data = {}
        total_urls = len(url_infos)
        processed_count = 0
        start_time = time.time()

        logger.info(f"Iniciando scraping paralelo de {total_urls} URLs...")

        # Usar context manager para asegurar limpieza del driver Selenium si se usa
        try:
            with ThreadPoolExecutor(max_workers=self.config.get("max_workers", 5)) as executor:
                # Crear futuros
                future_to_url_info = {executor.submit(self.scrape_single_url, url_info): url_info for url_info in url_infos}

                for future in as_completed(future_to_url_info):
                    url_info_orig = future_to_url_info[future]
                    url_orig = url_info_orig.get("URL")
                    processed_count += 1
                    try:
                        url_processed, content = future.result()
                        scraped_data[url_orig] = content # Usar URL original como clave
                        if "error" in content:
                             logger.warning(f"Error procesando {url_orig}: {content['error']}")
                        else:
                             logger.debug(f"Procesada {url_orig} exitosamente.")

                    except Exception as e:
                        logger.error(f"Error procesando futuro para {url_orig}: {e}", exc_info=True)
                        scraped_data[url_orig] = {"error": f"Future processing failed: {str(e)}", "context": url_info_orig.get("Context"), "page": url_info_orig.get("Page")}

                    if processed_count % 20 == 0 or processed_count == total_urls: # Log/Save cada 20 o al final
                         elapsed_time = time.time() - start_time
                         logger.info(f"Progreso: {processed_count}/{total_urls} URLs procesadas en {elapsed_time:.2f} seg.")
                         # Guardar progreso intermedio (opcional, sobrescribe)
                         # save_to_json(scraped_data, output_json_path)

        finally:
            self.close_selenium_driver() # Asegura cerrar el driver

        # Guardado final
        save_to_json(scraped_data, output_json_path)
        end_time = time.time()
        logger.info(f"Scraping completado para {processed_count}/{total_urls} URLs en {end_time - start_time:.2f} segundos.")
        logger.info(f"Resultados guardados en: {output_json_path}")

        return scraped_data