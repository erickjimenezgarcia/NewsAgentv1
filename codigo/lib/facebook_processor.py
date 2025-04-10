# codigo/lib/facebook_processor.py
import os
import json
import base64
import logging
import time
import io
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import random

# Importaciones para Selenium
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import TimeoutException, WebDriverException

# Importar PyPDF2 para extraer texto de PDFs
import PyPDF2

# Importar utilidades locales
from .cache_utils import get_cache_key, load_from_cache, save_to_cache
from .file_manager import ensure_dir_exists

logger = logging.getLogger(__name__)

class FacebookProcessor:
    """Clase para procesar URLs de Facebook y guardarlas como PDF."""
    
    def __init__(self, config):
        self.config = config
        self.paths = config.get('paths', {})
        self.cache_dir = self.paths.get('cache_dir')
        self.cache_expiry = config.get('cache_expiry')
        self.max_workers = min(config.get('max_workers', 5), 5)  # Limitar a 5 para evitar bloqueos
        
        # Configuración de Chrome
        self.chrome_options = Options()
        self.chrome_options.add_argument('--disable-notifications')
        self.chrome_options.add_argument('--disable-infobars')
        self.chrome_options.add_argument('--mute-audio')
        self.chrome_options.add_argument('--disable-extensions')
        self.chrome_options.add_argument('--disable-popup-blocking')
        self.chrome_options.add_argument('--no-sandbox')
        self.chrome_options.add_argument('--disable-dev-shm-usage')
        
        # User agent personalizado (opcional)
        if config.get('headers', {}).get('User-Agent'):
            self.chrome_options.add_argument(f'user-agent={config["headers"]["User-Agent"]}')
        
        # Headless mode (comentado por ahora para debugging, descomentar para producción)
        # self.chrome_options.add_argument('--headless')
        self.chrome_options.add_argument('--headless=new')
    
    def save_page_as_pdf(self, driver, output_path):
        """
        Utiliza el comando Page.printToPDF del DevTools Protocol
        para guardar la página actual como PDF.
        """
        try:
            # Asegurar que el directorio del output_path exista
            output_dir = os.path.dirname(output_path)
            if not os.path.exists(output_dir):
                os.makedirs(output_dir, exist_ok=True)
                logger.info(f"Directorio para PDF creado: {output_dir}")
                
            # Puede personalizar las opciones de impresión
            result = driver.execute_cdp_cmd("Page.printToPDF", {
                "printBackground": True,
                "paperWidth": 8.27,      # A4 en pulgadas
                "paperHeight": 11.7,
                "marginTop": 0.4,
                "marginBottom": 0.4,
                "marginLeft": 0.4,
                "marginRight": 0.4,
                "scale": 0.9,           # Escala para asegurar que todo cabe
            })
            
            # Decodifica y guarda el PDF
            pdf_data = base64.b64decode(result['data'])
            
            with open(output_path, 'wb') as f:
                f.write(pdf_data)
            
            return True
        except Exception as e:
            logger.error(f"Error al guardar PDF: {e}", exc_info=True)
            return False
    
    def process_facebook_url(self, url, date_str, index):
        """
        Procesa una URL de Facebook, capturando la página como PDF.
        
        Args:
            url: URL de Facebook a procesar
            date_str: Fecha en formato ddmmyyyy
            index: Índice para el nombre del archivo
            
        Returns:
            dict: Resultado del procesamiento con metadatos
        """
        # Crear la carpeta de fecha si no existe
        output_dir = os.path.join(self.paths.get('project_root'), 'base', date_str)
        
        # Verificación adicional para asegurar que el directorio exista
        try:
            if not os.path.exists(output_dir):
                os.makedirs(output_dir, exist_ok=True)
                logger.info(f"Directorio creado: {output_dir}")
        except Exception as e:
            logger.error(f"No se pudo crear el directorio {output_dir}: {e}")
            return {"url": url, "timestamp": datetime.now().isoformat(), "pdf_path": None, "success": False, 
                   "error": f"Error creando directorio: {str(e)}"}
        
        # Generar nombre de archivo
        pdf_filename = f"{date_str}-{index}.pdf"
        pdf_path = os.path.join(output_dir, pdf_filename)
        
        # Verificar cache basada en URL
        cache_key = get_cache_key(f"facebook_{url}")
        if self.cache_dir and self.cache_expiry is not None:
            cached_result = load_from_cache(self.cache_dir, cache_key, self.cache_expiry)
            if cached_result:
                # Verificar si el PDF existe en la ruta guardada
                if cached_result.get("pdf_path") and os.path.exists(cached_result["pdf_path"]):
                    logger.debug(f"Usando caché para Facebook URL: {url}")
                    return cached_result
                else:
                    logger.debug(f"Caché encontrada para {url} pero el archivo PDF no existe. Reprocesando.")
        
        result = {
            "url": url,
            "timestamp": datetime.now().isoformat(),
            "pdf_path": None,
            "success": False,
            "error": None
        }
        
        driver = None
        try:
            logger.info(f"Procesando URL de Facebook ({index}): {url}")
            
            # Inicializar el WebDriver
            driver = webdriver.Chrome(options=self.chrome_options)
            driver.set_page_load_timeout(60)  # 60 segundos para cargar
            
            # Navegar a la URL
            driver.get(url)
            
            # Esperar a que la página cargue completamente
            time.sleep(5 + random.uniform(1, 3))  # Espera aleatoria para evitar patrones
            
            # Ajustar tamaño de ventana
            driver.set_window_size(1280, 1600)
            
            # Hacer scroll para cargar más contenido (opcional)
            for _ in range(2):
                driver.execute_script("window.scrollBy(0, 800)")
                time.sleep(1)
            
            # Esperar un poco más para asegurar que cargue todo
            time.sleep(2)
            
            # Configuración de impresión PDF
            result_cdp = driver.execute_cdp_cmd("Page.printToPDF", {
                "printBackground": True,
                "paperWidth": 8.27,      # A4 en pulgadas
                "paperHeight": 11.7,
                "marginTop": 0.4,
                "marginBottom": 0.4,
                "marginLeft": 0.4,
                "marginRight": 0.4,
                "scale": 0.9,           # Escala para asegurar que todo cabe
            })
            
            # Verificar que el directorio existe antes de guardar
            if not os.path.exists(os.path.dirname(pdf_path)):
                os.makedirs(os.path.dirname(pdf_path), exist_ok=True)
            
            # Decodificar y guardar el PDF
            try:
                pdf_data = base64.b64decode(result_cdp['data'])
                with open(pdf_path, 'wb') as f:
                    f.write(pdf_data)
                logger.info(f"PDF guardado exitosamente: {pdf_path}")
                result["pdf_path"] = pdf_path
                result["success"] = True
            except Exception as file_error:
                logger.error(f"Error guardando el archivo PDF {pdf_path}: {file_error}", exc_info=True)
                result["error"] = f"Error guardando PDF: {str(file_error)}"
            
        except TimeoutException:
            logger.warning(f"Timeout alcanzado al cargar: {url}")
            result["error"] = "Timeout"
        except WebDriverException as e:
            logger.warning(f"Error de WebDriver para {url}: {e}")
            result["error"] = f"WebDriver Error: {str(e)}"
        except Exception as e:
            logger.error(f"Error inesperado procesando {url}: {e}", exc_info=True)
            result["error"] = f"Unexpected Error: {str(e)}"
        finally:
            # Cerrar el navegador
            if driver:
                try:
                    driver.quit()
                except Exception:
                    pass  # Ignorar errores al cerrar
            
            # Guardar resultado en caché si es exitoso
            if result["success"] and self.cache_dir:
                save_to_cache(self.cache_dir, cache_key, result)
            
            # Añadir una pausa para evitar sobrecargar el servidor
            time.sleep(random.uniform(1, 3))
        
        return result
    
    def process_facebook_urls_parallel(self, urls, date_str):
        """
        Procesa múltiples URLs de Facebook en paralelo.
        
        Args:
            urls: Lista de URLs de Facebook a procesar
            date_str: Fecha en formato ddmmyyyy
            
        Returns:
            dict: Resultados del procesamiento para cada URL
        """
        if not urls:
            logger.info("No hay URLs de Facebook para procesar.")
            return {}
        
        total_urls = len(urls)
        processed_count = 0
        results = {}
        start_time = time.time()
        
        logger.info(f"Iniciando procesamiento paralelo de {total_urls} URLs de Facebook para la fecha {date_str}...")
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_url = {
                executor.submit(self.process_facebook_url, url, date_str, idx): url
                for idx, url in enumerate(urls, 1)
            }
            
            for future in as_completed(future_to_url):
                url = future_to_url[future]
                processed_count += 1
                
                try:
                    result = future.result()
                    results[url] = result
                    
                    status = "Éxito" if result["success"] else f"Error: {result.get('error')}"
                    logger.debug(f"URL de Facebook procesada ({processed_count}/{total_urls}): {url} - {status}")
                    
                except Exception as e:
                    logger.error(f"Error procesando futuro para {url}: {e}", exc_info=True)
                    results[url] = {
                        "url": url,
                        "timestamp": datetime.now().isoformat(),
                        "pdf_path": None,
                        "success": False,
                        "error": f"Processing failed: {str(e)}"
                    }
                
                # Mostrar progreso periódicamente
                if processed_count % 5 == 0 or processed_count == total_urls:
                    elapsed = time.time() - start_time
                    logger.info(f"Progreso Facebook: {processed_count}/{total_urls} en {elapsed:.2f} seg.")
        
        # Guardar resultados en un archivo JSON
        output_json_path = os.path.join(self.paths.get('project_root'), 'output', f"facebook_results_{date_str}.json")
        ensure_dir_exists(os.path.dirname(output_json_path))
        
        try:
            with open(output_json_path, 'w', encoding='utf-8') as f:
                json.dump(results, f, ensure_ascii=False, indent=2)
            logger.info(f"Resultados de Facebook guardados en: {output_json_path}")
        except Exception as e:
            logger.error(f"Error al guardar resultados de Facebook: {e}", exc_info=True)
        
        # Estadísticas finales
        success_count = sum(1 for result in results.values() if result.get("success", False))
        end_time = time.time()
        logger.info(f"Procesamiento de Facebook completado: {success_count}/{total_urls} exitosos en {end_time - start_time:.2f} segundos.")
        
        return results
    
    def process_facebook_from_json(self, json_file_path, date_str):
        """
        Lee URLs de Facebook desde un archivo JSON y las procesa.
        
        Args:
            json_file_path: Ruta al archivo JSON con URLs
            date_str: Fecha en formato ddmmyyyy
            
        Returns:
            dict: Resultados del procesamiento
        """
        if not os.path.exists(json_file_path):
            logger.warning(f"Archivo JSON no encontrado: {json_file_path}")
            return {}
        
        try:
            with open(json_file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Filtrar solo URLs de Facebook
            fb_urls = []
            for item in data:
                url = item.get("URL", "")
                if url and ("facebook.com" in url.lower() or "fb.com" in url.lower()):
                    fb_urls.append(url)
            
            if not fb_urls:
                logger.info(f"No se encontraron URLs de Facebook en {json_file_path}")
                return {}
            
            logger.info(f"Encontradas {len(fb_urls)} URLs de Facebook en {json_file_path}")
            return self.process_facebook_urls_parallel(fb_urls, date_str)
            
        except json.JSONDecodeError:
            logger.error(f"Error al decodificar JSON desde {json_file_path}")
            return {}
        except Exception as e:
            logger.error(f"Error procesando archivo JSON {json_file_path}: {e}", exc_info=True)
            return {}

    def extract_text_from_pdf(self, pdf_path):
        """
        Extrae texto de un archivo PDF.
        
        Args:
            pdf_path: Ruta al archivo PDF
            
        Returns:
            str: Texto extraído del PDF o cadena vacía si hay error
        """
        if not os.path.exists(pdf_path):
            logger.warning(f"Archivo PDF no encontrado: {pdf_path}")
            return ""
        
        try:
            with open(pdf_path, 'rb') as file:
                # Crear lector de PDF
                pdf_reader = PyPDF2.PdfReader(file)
                
                # Extraer texto de todas las páginas
                text = ""
                for page_num in range(len(pdf_reader.pages)):
                    page = pdf_reader.pages[page_num]
                    text += page.extract_text() + "\n\n"
                
                logger.info(f"Texto extraído exitosamente del PDF: {pdf_path}")
                return text.strip()
        except Exception as e:
            logger.error(f"Error extrayendo texto de PDF {pdf_path}: {e}", exc_info=True)
            return ""
    
    def extract_text_from_all_pdfs(self, facebook_results):
        """
        Extrae texto de todos los PDFs procesados con éxito.
        
        Args:
            facebook_results: Diccionario de resultados del procesamiento de Facebook
            
        Returns:
            dict: Diccionario con URL como clave y texto extraído como valor
        """
        pdf_texts = {}
        
        for url, result in facebook_results.items():
            if result.get("success") and result.get("pdf_path") and os.path.exists(result["pdf_path"]):
                text = self.extract_text_from_pdf(result["pdf_path"])
                if text:
                    pdf_texts[url] = {
                        "extracted_text": text,
                        "pdf_path": result["pdf_path"],
                        "processed_date": datetime.now().strftime('%d%m%Y')
                    }
                    logger.debug(f"Texto extraído para URL {url}: {len(text)} caracteres")
            else:
                logger.debug(f"Omitiendo extracción de texto para URL {url}: PDF no disponible o con error")
        
        logger.info(f"Texto extraído de {len(pdf_texts)} PDFs de Facebook")
        return pdf_texts
