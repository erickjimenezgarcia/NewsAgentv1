"""
Pipeline principal de RAG para NewsAgent.
Integra todos los componentes: chunking, embeddings, y almacenamiento vectorial.
"""

import os
import json
import time
import logging
import argparse
import yaml
from typing import List, Dict, Any, Optional, Union
from datetime import datetime
from pathlib import Path

# Importar componentes del sistema RAG
from chunker import SmartChunker
from embedding_service import EmbeddingService
from vector_store import VectorDBManager

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/rag_pipeline.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('RAGPipeline')

class MetricsCollector:
    """Recopila métricas de rendimiento y calidad del sistema RAG."""
    
    def __init__(self, log_file: str = 'logs/rag_metrics.log'):
        """
        Inicializa el recopilador de métricas.
        
        Args:
            log_file: Ruta al archivo de log de métricas
        """
        self.log_file = log_file
        self.metrics = {}
        self.timers = {}
        self.start_time = time.time()
        
        # Crear directorio de logs si no existe
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
    
    def record(self, key: str, value: Any) -> None:
        """Registra una métrica."""
        self.metrics[key] = value
    
    def start_timer(self, key: str) -> None:
        """Inicia un temporizador para una métrica de tiempo."""
        self.timers[key] = time.time()
    
    def stop_timer(self, key: str) -> float:
        """
        Detiene un temporizador y registra la duración.
        
        Returns:
            Duración en segundos
        """
        if key not in self.timers:
            return 0.0
            
        duration = time.time() - self.timers[key]
        self.metrics[f"{key}_seconds"] = round(duration, 3)
        return duration
    
    def log_metrics(self) -> None:
        """Registra todas las métricas actuales en el archivo de log."""
        # Añadir timestamp
        self.metrics['timestamp'] = datetime.now().isoformat()
        
        # Registrar en archivo
        with open(self.log_file, 'a', encoding='utf-8') as f:
            f.write(json.dumps(self.metrics) + '\n')
    
    def get_summary(self) -> Dict[str, Any]:
        """Obtiene un resumen de todas las métricas registradas."""
        return self.metrics.copy()


class RAGPipeline:
    """Pipeline completo de RAG para NewsAgent."""
    
    def __init__(self, config_path: str = 'config.yaml'):
        """
        Inicializa el pipeline RAG.
        
        Args:
            config_path: Ruta al archivo de configuración
        """
        # Cargar configuración
        self.config = self._load_config(config_path)
        
        # Inicializar métricas
        self.metrics = MetricsCollector(
            self.config.get('monitoring', {}).get('log_file', 'logs/rag_metrics.log')
        )
        
        # Inicializar componentes
        logger.info("Inicializando componentes del pipeline RAG...")
        self.chunker = SmartChunker(config_path)
        self.embedding_service = EmbeddingService(config_path)
        self.vector_store = VectorDBManager(config_path)
        
        logger.info("Pipeline RAG inicializado")
    
    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Carga configuración desde archivo YAML."""
        # Determinar ruta absoluta si es relativa
        if not os.path.isabs(config_path):
            base_dir = os.path.dirname(os.path.abspath(__file__))
            config_path = os.path.join(base_dir, config_path)
            
        with open(config_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    
    def process_content(self, content_list: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Procesa contenido para indexación RAG.
        
        Args:
            content_list: Lista de documentos con texto y metadatos
            
        Returns:
            Resultados del procesamiento con métricas
        """
        if not content_list:
            return {"error": "No se proporcionó contenido para procesar"}
        
        start_time = time.time()
        self.metrics.start_timer('total_processing')
        
        try:
            # 1. Chunking
            logger.info(f"Generando chunks a partir de {len(content_list)} documentos...")
            self.metrics.start_timer('chunking')
            chunks = self.chunker.process_content(content_list)
            chunking_time = self.metrics.stop_timer('chunking')
            logger.info(f"Se generaron {len(chunks)} chunks en {chunking_time:.2f} segundos")
            
            # 2. Generación de embeddings
            logger.info("Generando embeddings...")
            self.metrics.start_timer('embeddings')
            chunks_with_embeddings = self.embedding_service.get_embeddings(chunks)
            embedding_time = self.metrics.stop_timer('embeddings')
            logger.info(f"Embeddings generados en {embedding_time:.2f} segundos")
            
            # 3. Almacenamiento en vector db
            logger.info("Almacenando documentos en base de datos vectorial...")
            self.metrics.start_timer('storage')
            docs_stored = self.vector_store.upsert_documents(chunks_with_embeddings)
            storage_time = self.metrics.stop_timer('storage')
            logger.info(f"Se almacenaron {docs_stored} documentos en {storage_time:.2f} segundos")
            
            # 4. Registrar métricas finales
            total_time = self.metrics.stop_timer('total_processing')
            self.metrics.record('documents_processed', len(content_list))
            self.metrics.record('chunks_generated', len(chunks))
            self.metrics.record('documents_stored', docs_stored)
            self.metrics.record('api_calls', self.embedding_service.api_calls_count)
            
            self.metrics.log_metrics()
            
            return {
                "success": True,
                "documents_processed": len(content_list),
                "chunks_generated": len(chunks),
                "documents_stored": docs_stored,
                "processing_time_seconds": total_time,
                "metrics": self.metrics.get_summary()
            }
            
        except Exception as e:
            logger.error(f"Error procesando contenido: {e}", exc_info=True)
            return {
                "success": False,
                "error": str(e),
                "documents_processed": 0,
                "processing_time_seconds": time.time() - start_time
            }
    
    def query(self, 
              query_text: str, 
              filters: Optional[Dict[str, Any]] = None,
              limit: int = 10) -> Dict[str, Any]:
        """
        Realiza una consulta en el sistema RAG.
        
        Args:
            query_text: Texto de la consulta
            filters: Filtros para la búsqueda
            limit: Número máximo de resultados
            
        Returns:
            Resultados de la consulta con métricas
        """
        if not query_text:
            return {"error": "No se proporcionó consulta"}
        
        start_time = time.time()
        self.metrics.start_timer('query_total')
        
        try:
            # 1. Generar embedding para la consulta
            self.metrics.start_timer('query_embedding')
            query_embedding = self.embedding_service._get_embedding(query_text)
            query_embedding_time = self.metrics.stop_timer('query_embedding')
            
            # 2. Realizar búsqueda híbrida
            self.metrics.start_timer('query_search')
            
            # Obtener configuración de búsqueda
            retrieval_config = self.config.get('retrieval', {})
            vector_weight = retrieval_config.get('vector_weight', 0.6)
            keyword_weight = retrieval_config.get('keyword_weight', 0.4)
            recall_k = retrieval_config.get('recall_k', 50)
            final_k = min(retrieval_config.get('final_k', 5), limit)
            
            # Realizar búsqueda híbrida
            results = self.vector_store.hybrid_search(
                query=query_text,
                query_embedding=query_embedding,
                vector_weight=vector_weight,
                keyword_weight=keyword_weight,
                filters=filters,
                limit=recall_k
            )
            
            search_time = self.metrics.stop_timer('query_search')
            
            # 3. Preparar resultados
            total_time = self.metrics.stop_timer('query_total')
            
            # Registrar métricas
            self.metrics.record('query_text', query_text)
            self.metrics.record('results_count', len(results))
            self.metrics.record('query_time_seconds', total_time)
            self.metrics.log_metrics()
            
            # Devolver resultados limitados
            return {
                "success": True,
                "query": query_text,
                "results": results[:final_k],
                "total_results": len(results),
                "query_time_seconds": total_time,
                "metrics": {
                    "embedding_time": query_embedding_time,
                    "search_time": search_time,
                    "total_time": total_time
                }
            }
            
        except Exception as e:
            logger.error(f"Error en consulta: {e}", exc_info=True)
            return {
                "success": False,
                "error": str(e),
                "query": query_text,
                "query_time_seconds": time.time() - start_time
            }


def load_rag_json(json_path: str) -> List[Dict[str, Any]]:
    """
    Carga contenido desde un archivo JSON de RAG.
    
    Args:
        json_path: Ruta al archivo JSON
        
    Returns:
        Lista de documentos con texto y metadatos
    """
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # Verificar formato y extraer contenido
    if 'content' in data and isinstance(data['content'], list):
        return data['content']
    elif isinstance(data, list):
        return data
    else:
        raise ValueError("Formato de JSON no reconocido")


def main():
    """Función principal para ejecutar el pipeline desde línea de comandos."""
    parser = argparse.ArgumentParser(description='Pipeline RAG para NewsAgent')
    
    # Comando principal
    subparsers = parser.add_subparsers(dest='command', help='Comando a ejecutar')
    
    # Comando 'process' para procesar documentos
    process_parser = subparsers.add_parser('process', help='Procesar documentos para RAG')
    process_parser.add_argument(
        'json_file', 
        help='Archivo JSON con documentos a procesar'
    )
    
    # Comando 'query' para realizar consultas
    query_parser = subparsers.add_parser('query', help='Realizar consulta en RAG')
    query_parser.add_argument(
        'query_text',
        help='Texto de la consulta'
    )
    query_parser.add_argument(
        '--source',
        help='Filtrar por fuente (html, pdf, image, facebook)'
    )
    query_parser.add_argument(
        '--date',
        help='Filtrar por fecha (formato DDMMYYYY)'
    )
    query_parser.add_argument(
        '--limit',
        type=int,
        default=5,
        help='Número máximo de resultados (default: 5)'
    )
    
    # Parsear argumentos
    args = parser.parse_args()
    
    # Inicializar pipeline
    pipeline = RAGPipeline()
    
    # Ejecutar comando
    if args.command == 'process':
        # Verificar archivo
        if not os.path.exists(args.json_file):
            print(f"Error: No se encuentra el archivo {args.json_file}")
            return 1
            
        # Cargar documentos
        try:
            print(f"Cargando documentos desde {args.json_file}...")
            documents = load_rag_json(args.json_file)
            print(f"Se cargaron {len(documents)} documentos")
            
            # Procesar documentos
            print("Procesando documentos...")
            result = pipeline.process_content(documents)
            
            # Mostrar resultados
            if result.get('success', False):
                print(f"\nProcesamiento completado con éxito:")
                print(f"- Documentos procesados: {result['documents_processed']}")
                print(f"- Chunks generados: {result['chunks_generated']}")
                print(f"- Documentos almacenados: {result['documents_stored']}")
                print(f"- Tiempo total: {result['processing_time_seconds']:.2f} segundos")
            else:
                print(f"\nError en procesamiento: {result.get('error', 'Desconocido')}")
                
        except Exception as e:
            print(f"Error: {e}")
            return 1
            
    elif args.command == 'query':
        # Temporalmente modificado para obtener resultados sin filtros de fecha
        filters = {}
        if args.source:
            filters['source'] = args.source
        # Desactivamos filtro de fecha para ver si hay resultados
        #if args.date:
        #    filters['date'] = args.date
            
        # Realizar consulta
        print(f"Consultando: '{args.query_text}'")
        if filters:
            print(f"Filtros: {filters}")
            
        result = pipeline.query(args.query_text, filters, args.limit)
        
        # Mostrar resultados
        if result.get('success', False):
            print(f"\nConsulta completada en {result['metrics']['total_time']:.3f} segundos")
            print(f"Total de resultados encontrados: {result['total_results']}")
            
            for i, doc in enumerate(result['results']):
                print(f"\n[{i+1}] Puntuación: {doc.get('combined_score', 0):.4f}")
                print(f"Fuente: {doc.get('source', 'Desconocida')}")
                
                if 'title' in doc and doc['title']:
                    print(f"Título: {doc['title']}")
                    
                print(f"Contenido: {doc['content'][:200]}...")
                
                if 'url' in doc and doc['url']:
                    print(f"URL: {doc['url']}")
        else:
            print(f"\nError en consulta: {result.get('error', 'Desconocido')}")
    
    else:
        parser.print_help()
        
    return 0


if __name__ == "__main__":
    # Crear directorios necesarios
    os.makedirs('logs', exist_ok=True)
    os.makedirs('embeddings_cache', exist_ok=True)
    
    # Ejecutar función principal
    main()
