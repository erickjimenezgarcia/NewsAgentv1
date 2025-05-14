import os
import logging
import hashlib
from urllib.parse import urlparse
from collections import defaultdict
from typing import Dict, List, Set, Tuple, Optional
import time
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from codigo.lib.selenium_text_extractor import extract_text_with_selenium

logger = logging.getLogger("optimized_deduplicator")

class OptimizedDeduplicator:
    def __init__(self, cache_dir: str):
        self.cache_dir = cache_dir
        os.makedirs(cache_dir, exist_ok=True)
        self.content_cache = {}
        self.binary_extensions = {'.jpg', '.jpeg', '.png', '.gif', '.mp3', '.mp4', '.pdf', '.zip', '.doc', '.docx'}
        self.domain_samples_limit = 50  # Límite de URLs por dominio para muestreo
        self.similarity_threshold = 0.85
        self.stats = {
            'total_comparisons': 0,
            'skipped_comparisons': 0,
            'binary_urls': 0,
            'failed_extractions': 0,
            'cached_hits': 0,
            'processing_time': 0
        }

    def _get_cache_path(self, url: str) -> str:
        """Genera una ruta de caché única para una URL usando un hash seguro."""
        url_hash = hashlib.sha256(url.encode()).hexdigest()[:16]
        return os.path.join(self.cache_dir, f"{url_hash}.txt")

    def _compute_url_signature(self, url: str) -> str:
        """Genera una firma única para una URL basada en su estructura."""
        parsed = urlparse(url)
        path_parts = parsed.path.split('/')
        # Ignora números y IDs en la URL para agrupar URLs similares
        normalized_parts = [re.sub(r'\d+', 'N', part) for part in path_parts if part]
        return f"{parsed.netloc}:{':'.join(normalized_parts)}"

    def _is_binary_url(self, url: str) -> bool:
        """Detecta si una URL probablemente apunta a contenido binario."""
        _, ext = os.path.splitext(urlparse(url).path.lower())
        return ext in self.binary_extensions

    def _extract_content(self, url: str) -> Optional[str]:
        """Extrae el contenido de una URL con manejo de caché."""
        cache_path = self._get_cache_path(url)
        
        # Verificar caché
        if url in self.content_cache:
            self.stats['cached_hits'] += 1
            return self.content_cache[url]
        
        if os.path.exists(cache_path):
            try:
                with open(cache_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                    self.content_cache[url] = content
                    self.stats['cached_hits'] += 1
                    return content
            except Exception:
                pass

        # Extraer contenido nuevo
        try:
            if self._is_binary_url(url):
                self.stats['binary_urls'] += 1
                return None
            
            content = extract_text_with_selenium(url, timeout=30)
            if content and len(content.strip()) > 0:
                # Guardar en caché
                self.content_cache[url] = content
                try:
                    with open(cache_path, 'w', encoding='utf-8') as f:
                        f.write(content)
                except Exception as e:
                    logger.warning(f"No se pudo guardar en caché: {e}")
                return content
        except Exception as e:
            self.stats['failed_extractions'] += 1
            logger.debug(f"Error extrayendo contenido de {url}: {str(e)}")
        
        return None

    def _calculate_similarity(self, text1: str, text2: str) -> float:
        """Calcula la similitud entre dos textos usando n-gramas."""
        if not text1 or not text2:
            return 0.0
        
        # Usar 3-gramas para comparación
        def get_ngrams(text: str, n: int = 3) -> Set[str]:
            return set(text[i:i+n] for i in range(len(text)-n+1))
        
        ngrams1 = get_ngrams(text1)
        ngrams2 = get_ngrams(text2)
        
        if not ngrams1 or not ngrams2:
            return 0.0
        
        intersection = len(ngrams1.intersection(ngrams2))
        union = len(ngrams1.union(ngrams2))
        
        return intersection / union if union > 0 else 0.0

    def find_duplicates(self, urls: List[str]) -> Dict[str, List[str]]:
        """Encuentra duplicados entre URLs de manera optimizada."""
        start_time = time.time()
        
        # Agrupar URLs por dominio
        domain_groups = defaultdict(list)
        for url in urls:
            domain = urlparse(url).netloc
            domain_groups[domain].append(url)
        
        # Procesar cada grupo de dominio
        duplicates = defaultdict(list)
        processed_contents = {}
        
        for domain, domain_urls in domain_groups.items():
            # Aplicar muestreo si el grupo es muy grande
            if len(domain_urls) > self.domain_samples_limit:
                logger.info(f"Muestreando {self.domain_samples_limit} URLs de {len(domain_urls)} para el dominio {domain}")
                domain_urls = domain_urls[:self.domain_samples_limit]
            
            # Extraer contenido en paralelo
            with ThreadPoolExecutor(max_workers=5) as executor:
                future_to_url = {executor.submit(self._extract_content, url): url for url in domain_urls}
                for future in as_completed(future_to_url):
                    url = future_to_url[future]
                    try:
                        content = future.result()
                        if content:
                            processed_contents[url] = content
                    except Exception as e:
                        logger.debug(f"Error procesando {url}: {str(e)}")
            
            # Buscar duplicados dentro del dominio
            urls_to_check = list(processed_contents.keys())
            for i, url1 in enumerate(urls_to_check):
                for url2 in urls_to_check[i+1:]:
                    self.stats['total_comparisons'] += 1
                    
                    # Verificar similitud de estructura primero
                    if self._compute_url_signature(url1) == self._compute_url_signature(url2):
                        similarity = self._calculate_similarity(processed_contents[url1], processed_contents[url2])
                        if similarity >= self.similarity_threshold:
                            duplicates[url1].append(url2)
                    else:
                        self.stats['skipped_comparisons'] += 1
        
        self.stats['processing_time'] = time.time() - start_time
        
        # Registrar estadísticas
        logger.info(f"Estadísticas de deduplicación:")
        logger.info(f"- Comparaciones totales: {self.stats['total_comparisons']}")
        logger.info(f"- Comparaciones omitidas: {self.stats['skipped_comparisons']}")
        logger.info(f"- URLs binarias detectadas: {self.stats['binary_urls']}")
        logger.info(f"- Extracciones fallidas: {self.stats['failed_extractions']}")
        logger.info(f"- Hits de caché: {self.stats['cached_hits']}")
        logger.info(f"- Tiempo total: {self.stats['processing_time']:.2f} segundos")
        
        return dict(duplicates)
