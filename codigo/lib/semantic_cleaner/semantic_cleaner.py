"""
Módulo principal para la limpieza semántica de textos.
"""

import json
import logging
import os
from datetime import datetime
from collections import defaultdict

from .text_similarity import SimilarityAnalyzer

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('semantic_cleaner')

class SemanticCleaner:
    """
    Clase para limpiar semánticamente textos extraídos de diferentes fuentes.
    """
    
    def __init__(self, similarity_threshold=0.7, language='spanish'):
        """
        Inicializa el limpiador semántico.
        
        Args:
            similarity_threshold (float): Umbral de similitud para considerar textos como similares
            language (str): Idioma para el análisis ('spanish' o 'english')
        """
        self.similarity_analyzer = SimilarityAnalyzer(
            language=language,
            similarity_threshold=similarity_threshold
        )
        self.similarity_threshold = similarity_threshold
        
    def _extract_texts_from_pdf(self, pdf_data):
        """
        Extrae textos de datos PDF.
        
        Args:
            pdf_data (dict): Datos de PDF
            
        Returns:
            list: Lista de textos extraídos
        """
        texts = []
        
        if not pdf_data:
            return texts
            
        # Extraer textos de cada fuente en el PDF
        for source_key, paragraphs in pdf_data.items():
            for paragraph in paragraphs:
                text = paragraph.get('text', '')
                if text:
                    texts.append({
                        'source': 'pdf',
                        'source_key': source_key,
                        'text': text,
                        'metadata': {
                            'page': paragraph.get('page', 0),
                            'description': paragraph.get('metadata', {}).get('description', '')
                        },
                        'relevance_score': 1.0  # Los textos PDF se consideran altamente relevantes
                    })
        
        return texts
    
    def _extract_texts_from_html(self, html_data):
        """
        Extrae textos de datos HTML.
        
        Args:
            html_data (dict): Datos de HTML
            
        Returns:
            list: Lista de textos extraídos
        """
        texts = []
        
        if not html_data:
            return texts
            
        # Extraer textos de cada URL en los datos HTML
        for url, page_data in html_data.items():
            text = page_data.get('text', '')
            if text:
                texts.append({
                    'source': 'html',
                    'source_key': url,
                    'text': text,
                    'metadata': {
                        'title': page_data.get('metadata', {}).get('title', ''),
                        'description': page_data.get('metadata', {}).get('description', ''),
                        'url': url
                    },
                    'relevance_score': page_data.get('relevance', 0.0)
                })
        
        return texts
    
    def _extract_texts_from_images(self, image_data):
        """
        Extrae textos de datos de imágenes.
        
        Args:
            image_data (list): Lista de datos de imágenes
            
        Returns:
            list: Lista de textos extraídos
        """
        texts = []
        
        if not image_data:
            return texts
            
        # Extraer textos de cada imagen
        for image_info in image_data:
            text = image_info.get('extracted_text', '')
            if text and not image_info.get('error'):
                texts.append({
                    'source': 'image',
                    'source_key': image_info.get('image_filename'),
                    'text': text,
                    'metadata': {
                        'url': image_info.get('image_filename'),
                        'description': '',
                        'perceptual_hash': image_info.get('perceptual_hash', '')
                    },
                    'relevance_score': 0.5  # Valor predeterminado para textos de imágenes
                })
        
        return texts
    
    def _extract_texts_from_facebook(self, facebook_data):
        """
        Extrae textos de datos de Facebook.
        
        Args:
            facebook_data (dict): Datos de Facebook
            
        Returns:
            list: Lista de textos extraídos
        """
        texts = []
        
        if not facebook_data:
            return texts
            
        # Extraer textos de cada post de Facebook
        for fb_key, fb_info in facebook_data.items():
            text = fb_info.get('extracted_text', '')
            if text:
                texts.append({
                    'source': 'facebook',
                    'source_key': fb_key,
                    'text': text,
                    'metadata': {
                        'url': fb_key,
                        'description': fb_info.get('metadata', {}).get('description', '')
                    },
                    'relevance_score': 0.4  # Valor predeterminado para textos de Facebook
                })
        
        return texts
    
    def _group_similar_texts(self, texts):
        """
        Agrupa textos similares.
        
        Args:
            texts (list): Lista de diccionarios de textos
            
        Returns:
            list: Lista de grupos de textos similares
        """
        if not texts:
            return []
            
        # Inicializar grupos
        groups = []
        processed_indices = set()
        
        # Agrupar textos similares
        for i, text_item in enumerate(texts):
            if i in processed_indices:
                continue
                
            # Crear nuevo grupo
            current_group = [text_item]
            processed_indices.add(i)
            
            # Buscar textos similares
            for j, other_text_item in enumerate(texts):
                if j in processed_indices or i == j:
                    continue
                    
                # Verificar similitud con umbral ajustado para Facebook
                similarity_threshold = self.similarity_threshold
                if text_item['source'] == 'facebook' and other_text_item['source'] == 'facebook':
                    similarity_threshold = 0.85  # Umbral más alto para Facebook
                if self.similarity_analyzer.is_similar(text_item['text'], other_text_item['text'], threshold=similarity_threshold):
                    current_group.append(other_text_item)
                    processed_indices.add(j)
            
            # Agregar grupo a la lista de grupos
            groups.append(current_group)
        
        return groups
    
    def _select_representative_text(self, group):
        """
        Selecciona el texto más representativo de un grupo basándose en su puntaje de relevancia y longitud.
        
        Args:
            group (list): Grupo de textos similares
            
        Returns:
            dict: Texto seleccionado como representativo
        """
        if not group:
            return None
            
        if len(group) == 1:
            return group[0]
            
        # Ordenar por relevancia (descendente) y longitud (descendente)
        sorted_group = sorted(
            group, 
            key=lambda x: (x['relevance_score'], len(x['text'])), 
            reverse=True
        )
        
        # Seleccionar el mejor
        return sorted_group[0]
    
    def _create_cleaned_output(self, representative_texts, original_json):
        """
        Crea un nuevo JSON limpio con solo los textos representativos.
        
        Args:
            representative_texts (list): Lista de textos representativos
            original_json (dict): JSON original
            
        Returns:
            dict: JSON limpio
        """
        # Copiar la estructura básica del JSON original
        cleaned_json = {
            "metadata": original_json.get("metadata", {}),
            "extracted_content": {
                "pdf_paragraphs": {},
                "html_pages": {},
                "image_texts": {},
                "facebook_texts": {}
            }
        }
        
        # Organizar textos representativos por fuente
        for text_item in representative_texts:
            source = text_item['source']
            source_key = text_item['source_key']
            
            if source == 'pdf':
                # Buscar el párrafo original en el JSON original
                for original_key, paragraphs in original_json["extracted_content"]["pdf_paragraphs"].items():
                    if original_key == source_key:
                        if source_key not in cleaned_json["extracted_content"]["pdf_paragraphs"]:
                            cleaned_json["extracted_content"]["pdf_paragraphs"][source_key] = []
                        
                        # Encontrar el párrafo específico
                        for paragraph in paragraphs:
                            if paragraph.get('text') == text_item['text']:
                                cleaned_json["extracted_content"]["pdf_paragraphs"][source_key].append(paragraph)
                                break
            
            elif source == 'html':
                url = source_key
                if url in original_json["extracted_content"]["html_pages"]:
                    cleaned_json["extracted_content"]["html_pages"][url] = original_json["extracted_content"]["html_pages"][url]
            
            elif source == 'image':
                image_key = source_key
                if 'image_texts' in original_json["extracted_content"] and image_key in original_json["extracted_content"]["image_texts"]:
                    if 'image_texts' not in cleaned_json["extracted_content"]:
                        cleaned_json["extracted_content"]["image_texts"] = {}
                    cleaned_json["extracted_content"]["image_texts"][image_key] = original_json["extracted_content"]["image_texts"][image_key]
            
            elif source == 'facebook':
                fb_key = source_key
                if 'facebook_texts' in original_json["extracted_content"] and fb_key in original_json["extracted_content"]["facebook_texts"]:
                    if 'facebook_texts' not in cleaned_json["extracted_content"]:
                        cleaned_json["extracted_content"]["facebook_texts"] = {}
                    cleaned_json["extracted_content"]["facebook_texts"][fb_key] = original_json["extracted_content"]["facebook_texts"][fb_key]
        
        return cleaned_json
    
    def clean_consolidated_json(self, json_data):
        """
        Limpia semánticamente un JSON consolidado.
        
        Args:
            json_data (dict): Datos JSON a limpiar
            
        Returns:
            dict: JSON limpio
        """
        logger.info("Iniciando limpieza semántica del archivo consolidado...")
        
        # Verificar estructura del JSON
        if not isinstance(json_data, dict):
            logger.error("El archivo JSON no tiene la estructura esperada")
            return None
            
        if "extracted_content" not in json_data:
            logger.error("No se encontró la sección 'extracted_content' en el JSON")
            return None
        
        # Extraer textos de todas las fuentes
        all_texts = []
        
        # PDF
        pdf_data = json_data["extracted_content"].get("pdf_paragraphs", {})
        pdf_texts = self._extract_texts_from_pdf(pdf_data)
        all_texts.extend(pdf_texts)
        logger.info(f"Extraídos {len(pdf_texts)} textos de PDF")
        
        # HTML
        html_data = json_data["extracted_content"].get("html_pages", {})
        html_texts = self._extract_texts_from_html(html_data)
        all_texts.extend(html_texts)
        logger.info(f"Extraídos {len(html_texts)} textos de HTML")
        
        # Imágenes
        image_data = json_data["extracted_content"].get("image_texts", {})
        image_texts = self._extract_texts_from_images(image_data)
        all_texts.extend(image_texts)
        logger.info(f"Extraídos {len(image_texts)} textos de imágenes")
        
        # Facebook
        facebook_data = json_data["extracted_content"].get("facebook_texts", {})
        facebook_texts = self._extract_texts_from_facebook(facebook_data)
        all_texts.extend(facebook_texts)
        logger.info(f"Extraídos {len(facebook_texts)} textos de Facebook")
        
        # Agrupar textos similares
        text_groups = self._group_similar_texts(all_texts)
        logger.info(f"Se identificaron {len(text_groups)} grupos de textos similares")
        
        # Seleccionar textos representativos
        representative_texts = []
        for group in text_groups:
            representative = self._select_representative_text(group)
            if representative:
                representative_texts.append(representative)
        
        logger.info(f"Se seleccionaron {len(representative_texts)} textos representativos")
        
        # Crear JSON limpio
        cleaned_json = self._create_cleaned_output(representative_texts, json_data)
        
        # Añadir estadísticas de limpieza
        if "stats_summary" in json_data.get("metadata", {}):
            cleaned_json["metadata"]["stats_summary"] = json_data["metadata"]["stats_summary"]
            
            # Añadir estadísticas de limpieza semántica
            if "stats_summary" not in cleaned_json["metadata"]:
                cleaned_json["metadata"]["stats_summary"] = {}
                
            semantic_stats = {
                "semantic_cleaning": {
                    "original_texts": len(all_texts),
                    "similar_groups": len(text_groups),
                    "representative_texts": len(representative_texts),
                    "reduction_percentage": round((1 - len(representative_texts) / len(all_texts)) * 100, 2) if all_texts else 0,
                    "timestamp": datetime.now().isoformat()
                }
            }
            
            cleaned_json["metadata"]["stats_summary"].update(semantic_stats)
        
        logger.info("Limpieza semántica completada con éxito")
        return cleaned_json
