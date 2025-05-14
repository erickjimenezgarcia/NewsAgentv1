"""
Módulo mejorado para procesamiento de Facebook con deduplicación inteligente.
Incorpora clasificación de URLs y detección adaptativa de duplicados.
"""

import os
import json
import base64
import logging
import time
import io
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import random
import traceback

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
from .url_classifier import URLClassifier

logger = logging.getLogger(__name__)

class FacebookProcessorWithDedup:
    """
    Clase mejorada para procesar URLs de Facebook con deduplicación inteligente.
    Incorpora clasificación por tipo de contenido y detección adaptativa de duplicados.
    """
    
    def __init__(self, config):
        """
        Inicializa el procesador con configuración personalizada.
        
        Args:
            config: Diccionario de configuración que debe contener 'paths'
        """
        self.config = config
        self.paths = config.get('paths', {})
        self.cache_dir = self.paths.get('cache_dir')
        self.cache_expiry = config.get('cache_expiry')
        self.max_workers = min(config.get('max_workers', 5), 5)  # Limitar a 5 para evitar bloqueos
        
        # Configuración para deduplicación
        self.dedup_config = config.get('deduplication', {})
        
        # Umbrales de deduplicación por categoría (valores predeterminados si no se especifican)
        default_thresholds = {
            "facebook": 0.85,  # Umbral más permisivo para Facebook
            "youtube": 0.95,
            "news": 0.90,
            "document": 0.98,
            "image": 0.99,
            "media": 0.95,
            "government": 0.90,
            "other": 0.85,
            "default": 0.88
        }
        
        # Actualizar con configuración personalizada si existe
        thresholds = {**default_thresholds}
        if 'thresholds' in self.dedup_config:
            thresholds.update(self.dedup_config['thresholds'])
        
        # Inicializar clasificador de URLs
        self.url_classifier = URLClassifier({
            'thresholds': thresholds,
            'cache_dir': self.cache_dir,
            'debug': config.get('debug', False)
        })
        
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
        
        # Headless mode
        self.chrome_options.add_argument('--headless=new')
        
        logger.info(f"FacebookProcessorWithDedup inicializado con umbrales: {thresholds}")
    
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
                
            # Configurar opciones de impresión
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
            
            # Decodificar y guardar el PDF
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
        date_path = os.path.join(self.paths.get('pdf_output_dir', 'pdfs'), date_str)
        ensure_dir_exists(date_path)
        
        # Generar nombre de archivo
        filename = f"facebook_{date_str}_{index:03d}.pdf"
        output_path = os.path.join(date_path, filename)
        
        # Verificar caché
        cache_key = get_cache_key(url)
        cache_data = load_from_cache(cache_key, self.cache_dir, self.cache_expiry)
        
        if cache_data and os.path.exists(cache_data.get('pdf_path', '')):
            logger.info(f"Usando caché para URL: {url}")
            return {
                "url": url,
                "success": True,
                "pdf_path": cache_data['pdf_path'],
                "from_cache": True,
                "processed_date": cache_data.get('processed_date', date_str)
            }
        
        # Si no hay caché, procesar la URL
        driver = None
        result = {
            "url": url,
            "success": False,
            "pdf_path": "",
            "from_cache": False,
            "error": None,
            "processed_date": date_str
        }
        
        try:
            logger.info(f"Procesando URL de Facebook: {url}")
            
            # Lanzar navegador
            driver = webdriver.Chrome(options=self.chrome_options)
            driver.set_page_load_timeout(30)  # 30 segundos de timeout
            
            # Navegar a la URL
            driver.get(url)
            
            # Esperar a que cargue la página (puedes ajustar según necesidades)
            time.sleep(random.uniform(5, 8))
            
            # Intentar expandir comentarios u otros elementos (opcional)
            try:
                # Este código es solo un ejemplo, ajustar según necesidad
                # Por ejemplo, expandir comentarios o cargar más contenido
                driver.execute_script("""
                    // Intentar expandir botones "Ver más" o similares
                    var verMas = document.querySelectorAll('[role="button"]');
                    for (var i = 0; i < verMas.length; i++) {
                        if (verMas[i].textContent.includes('Ver') || 
                            verMas[i].textContent.includes('See more') ||
                            verMas[i].textContent.includes('Más')) {
                            verMas[i].click();
                        }
                    }
                """)
                time.sleep(1)
            except Exception as e:
                logger.debug(f"Error expandiendo elementos (no crítico): {e}")
            
            # Guardar como PDF
            if self.save_page_as_pdf(driver, output_path):
                result["success"] = True
                result["pdf_path"] = output_path
                
                # Guardar en caché
                save_to_cache(cache_key, {
                    "pdf_path": output_path,
                    "processed_date": date_str
                }, self.cache_dir)
                
                logger.info(f"PDF guardado exitosamente: {output_path}")
            else:
                result["error"] = "Error al guardar PDF"
                logger.error(f"Error al guardar PDF para URL: {url}")
        
        except TimeoutException:
            error_msg = f"Timeout alcanzado para URL: {url}"
            logger.error(error_msg)
            result["error"] = error_msg
        
        except WebDriverException as e:
            error_msg = f"Error de WebDriver: {str(e)}"
            logger.error(error_msg)
            result["error"] = error_msg
        
        except Exception as e:
            error_msg = f"Error procesando URL {url}: {str(e)}"
            logger.error(error_msg, exc_info=True)
            result["error"] = error_msg
        
        finally:
            # Cerrar el navegador
            if driver:
                try:
                    driver.quit()
                except:
                    pass
        
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
            logger.warning("Lista de URLs vacía. Nada que procesar.")
            return {}
        
        logger.info(f"Procesando {len(urls)} URLs de Facebook en paralelo")
        results = {}
        
        # Usar ThreadPoolExecutor para paralelización
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Crear futuro para cada URL
            future_to_url = {
                executor.submit(
                    self.process_facebook_url, url, date_str, i
                ): url for i, url in enumerate(urls)
            }
            
            # Procesar resultados a medida que se completan
            for future in as_completed(future_to_url):
                url = future_to_url[future]
                try:
                    result = future.result()
                    results[url] = result
                    status = "Éxito" if result.get("success") else f"Error: {result.get('error')}"
                    logger.info(f"URL {url} procesada: {status}")
                except Exception as e:
                    logger.error(f"Error en futuro para URL {url}: {e}", exc_info=True)
                    results[url] = {
                        "url": url,
                        "success": False,
                        "error": f"Error en ejecución paralela: {str(e)}",
                        "from_cache": False,
                        "processed_date": date_str
                    }
        
        # Resumen de resultados
        success_count = sum(1 for r in results.values() if r.get("success"))
        logger.info(f"Procesamiento completado: {success_count}/{len(urls)} URLs exitosas")
        
        return results
    
    def process_facebook_urls_with_preselection(self, urls, date_str):
        """
        Procesa URLs de Facebook con preselección para eliminar duplicados
        antes del procesamiento completo.
        
        Args:
            urls: Lista de URLs de Facebook a procesar
            date_str: Fecha en formato ddmmyyyy
            
        Returns:
            dict: Resultados del procesamiento con estadísticas de deduplicación
        """
        if not urls:
            logger.warning("Lista de URLs vacía. Nada que procesar.")
            return {
                "results": {},
                "stats": {
                    "total": 0,
                    "unique": 0,
                    "duplicate": 0,
                    "processed": 0
                }
            }
        
        # Clasificar URLs (para estadísticas y procesamiento diferenciado)
        classified_urls = self.url_classifier.classify_urls(urls)
        
        # Obtener historial de procesamiento si está disponible
        history_file = os.path.join(self.cache_dir, "facebook_history.json")
        history = {}
        
        if os.path.exists(history_file):
            try:
                with open(history_file, 'r', encoding='utf-8') as f:
                    history = json.load(f)
            except Exception as e:
                logger.error(f"Error al cargar historial: {e}")
        
        # Identificar duplicados históricos (primera pasada)
        preselected_urls = []
        duplicate_urls = []
        stats = {"by_category": {}}
        
        for category, category_urls in classified_urls.items():
            # Inicializar estadísticas para esta categoría
            cat_stats = {
                "total": len(category_urls),
                "preselected": 0,
                "historical_duplicate": 0
            }
            
            for url in category_urls:
                cache_key = get_cache_key(url)
                
                # Si la URL ya ha sido procesada anteriormente y tenemos el texto
                if url in history and history[url].get("text"):
                    # Considerar URL como duplicado histórico
                    duplicate_urls.append(url)
                    cat_stats["historical_duplicate"] += 1
                    logger.debug(f"URL {url} identificada como duplicado histórico")
                else:
                    # URL nueva o no tenemos su texto, procesar
                    preselected_urls.append(url)
                    cat_stats["preselected"] += 1
            
            # Guardar estadísticas de esta categoría
            stats["by_category"][category] = cat_stats
        
        # Estadísticas globales de preselección
        stats.update({
            "total": len(urls),
            "preselected": len(preselected_urls),
            "historical_duplicate": len(duplicate_urls)
        })
        
        logger.info(
            f"Preselección completada: {len(preselected_urls)}/{len(urls)} URLs seleccionadas "
            f"({len(duplicate_urls)} duplicados históricos)"
        )
        
        # Procesar URLs preseleccionadas
        results = {}
        
        if preselected_urls:
            logger.info(f"Procesando {len(preselected_urls)} URLs preseleccionadas")
            results = self.process_facebook_urls_parallel(preselected_urls, date_str)
        
        # Extraer texto de PDFs para utilizar en detección de duplicados
        extracted_texts = {}
        for url, result in results.items():
            if result.get("success") and result.get("pdf_path"):
                text = self.extract_text_from_pdf(result["pdf_path"])
                if text:
                    extracted_texts[url] = text
                    
                    # Actualizar historial con este texto
                    history[url] = {
                        "text": text,
                        "pdf_path": result["pdf_path"],
                        "processed_date": result.get("processed_date", date_str)
                    }
        
        # Actualizar estadísticas con resultados del procesamiento
        stats.update({
            "processed": len(results),
            "success": sum(1 for r in results.values() if r.get("success")),
            "text_extracted": len(extracted_texts)
        })
        
        # Guardar historial actualizado
        try:
            ensure_dir_exists(self.cache_dir)
            with open(history_file, 'w', encoding='utf-8') as f:
                json.dump(history, f, ensure_ascii=False, indent=2)
            logger.info(f"Historial actualizado con {len(history)} URLs")
        except Exception as e:
            logger.error(f"Error al guardar historial: {e}")
        
        # Crear resultado final
        final_result = {
            "results": results,
            "duplicate_urls": duplicate_urls,
            "stats": stats
        }
        
        return final_result
    
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
            return self.process_facebook_urls_with_preselection(fb_urls, date_str)
            
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
