# codigo/lib/history_tracker.py
import os
import json
import logging
from .file_manager import ensure_dir_exists # Usar file_manager local

logger = logging.getLogger(__name__)

class HistoryTracker:
    def __init__(self, history_file_path):
        self.history_file = history_file_path
        ensure_dir_exists(self.history_file) # Asegura que el directorio exista
        self.processed_urls = self._load_history()

    def _load_history(self):
        """Carga el historial de URLs procesadas desde el archivo JSON."""
        if os.path.exists(self.history_file):
            try:
                with open(self.history_file, 'r', encoding='utf-8') as f:
                    # Carga como lista y convierte a set para búsqueda rápida
                    history_list = json.load(f)
                    logger.info(f"Historial cargado desde {self.history_file} con {len(history_list)} URLs.")
                    return set(history_list)
            except json.JSONDecodeError:
                logger.error(f"Error al decodificar JSON del historial: {self.history_file}. Se creará uno nuevo.")
                return set()
            except Exception as e:
                 logger.error(f"Error cargando historial desde {self.history_file}: {e}. Se creará uno nuevo.")
                 return set()
        else:
            logger.info(f"Archivo de historial no encontrado en {self.history_file}. Se creará uno nuevo al guardar.")
            return set()

    def _save_history(self):
        """Guarda el historial actual de URLs procesadas en el archivo JSON."""
        try:
            # Convierte el set a lista para poder serializarlo a JSON
            history_list = sorted(list(self.processed_urls))
            with open(self.history_file, 'w', encoding='utf-8') as f:
                json.dump(history_list, f, indent=2) # Usar indent para legibilidad
            logger.info(f"Historial guardado en {self.history_file} con {len(history_list)} URLs.")
        except Exception as e:
            logger.error(f"Error al guardar historial en {self.history_file}: {e}")

    def add_processed_urls(self, urls):
        """
        Añade una colección de URLs al historial y lo guarda.
        Retorna el número de URLs nuevas añadidas.
        """
        new_urls_added = 0
        initial_count = len(self.processed_urls)
        if isinstance(urls, (list, set, tuple)):
            self.processed_urls.update(urls)
        elif isinstance(urls, str):
             self.processed_urls.add(urls)
        else:
             logger.warning(f"Tipo de dato no soportado para añadir al historial: {type(urls)}")
             return 0

        new_urls_added = len(self.processed_urls) - initial_count

        if new_urls_added > 0:
             logger.info(f"Añadidas {new_urls_added} nuevas URLs al historial.")
             self._save_history()
        else:
             logger.debug("No se añadieron nuevas URLs al historial.")

        return new_urls_added


    def is_processed(self, url):
        """Verifica si una URL ya está en el historial."""
        return url in self.processed_urls
    
    def is_url_processed(self, url):
        """Alias para is_processed - verifica si una URL ya está en el historial."""
        return self.is_processed(url)

    def get_unprocessed_links(self, links_list):
         """
         Filtra una lista de diccionarios de enlaces, retornando solo aquellos
         cuya 'URL' no está en el historial.
         """
         unprocessed = [link for link in links_list if not self.is_processed(link.get('URL'))]
         processed_count = len(links_list) - len(unprocessed)
         if processed_count > 0:
             logger.info(f"Filtradas {processed_count} URLs ya procesadas del lote actual.")
         return unprocessed

    def get_history_count(self):
         """Retorna el número total de URLs en el historial."""
         return len(self.processed_urls)