"""
SemanticCleaner: Clase principal para realizar limpieza semántica de textos consolidados.

Este módulo implementa algoritmos de procesamiento de lenguaje natural para detectar
y eliminar contenido redundante entre diferentes fuentes como PDF, páginas HTML y 
publicaciones de Facebook.
"""

import os
import json
import logging
import nltk
import re
from collections import defaultdict
from datetime import datetime
from typing import Dict, List, Tuple, Set, Any, Optional, Union
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np

# Configuración del logging
logger = logging.getLogger(__name__)

# Descargar recursos de NLTK si no están presentes
try:
    nltk.data.find('tokenizers/punkt')
except LookupError:
    nltk.download('punkt', quiet=True)
    
try:
    nltk.data.find('corpora/stopwords')
except LookupError:
    nltk.download('stopwords', quiet=True)


class SemanticCleaner:
    """
    Clase para limpiar semánticamente contenido redundante de diferentes fuentes
    y generar un documento consolidado limpio.
    """
    
    def __init__(self, similarity_threshold: float = 0.65):
        """
        Inicializa el limpiador semántico.
        
        Args:
            similarity_threshold: Umbral de similitud para considerar textos como redundantes
                                 (valor entre 0 y 1, donde 1 es identidad completa)
        """
        self.similarity_threshold = similarity_threshold
        self.stopwords = set(nltk.corpus.stopwords.words('spanish'))
        # Agregar algunas stopwords adicionales comunes en noticias
        self.stopwords.update(['según', 'indica', 'señala', 'informó', 'dijo', 'añadió',
                              'explicó', 'además', 'también', 'asimismo', 'mientras'])
        
        # Configurar el vectorizador TF-IDF
        self.vectorizer = TfidfVectorizer(
            min_df=2,
            max_df=0.95,
            max_features=5000,
            stop_words=self.stopwords
        )
    
    def clean_text(self, text: str) -> str:
        """
        Limpia el texto eliminando caracteres especiales, múltiples espacios, etc.
        
        Args:
            text: Texto a limpiar
            
        Returns:
            Texto limpio
        """
        if not text:
            return ""
            
        # Eliminar URLs
        text = re.sub(r'https?://\S+|www\.\S+', '', text)
        
        # Eliminar etiquetas HTML si las hubiera
        text = re.sub(r'<.*?>', '', text)
        
        # Reemplazar saltos de línea con espacio
        text = re.sub(r'\n+', ' ', text)
        
        # Eliminar caracteres especiales y dígitos
        text = re.sub(r'[^\w\s]', ' ', text)
        
        # Reemplazar múltiples espacios con un solo espacio
        text = re.sub(r'\s+', ' ', text)
        
        return text.strip().lower()
    
    def extract_content_from_json(self, data: Dict) -> Dict[str, List[Dict[str, Any]]]:
        """
        Extrae contenido relevante del JSON consolidado.
        
        Args:
            data: Diccionario que contiene el JSON consolidado
            
        Returns:
            Diccionario con contenido extraído por tipo de fuente
        """
        extracted_content = {
            "pdf": [],
            "html": [],
            "facebook": []
        }
        
        # Extraer contenido del PDF
        try:
            if "pdf_paragraphs" in data["extracted_content"]:
                for section_name, paragraphs in data["extracted_content"]["pdf_paragraphs"].items():
                    for paragraph in paragraphs:
                        text = paragraph.get("text", "")
                        metadata = paragraph.get("metadata", {})
                        page = paragraph.get("page", 0)
                        
                        if text:
                            extracted_content["pdf"].append({
                                "text": text,
                                "metadata": metadata,
                                "page": page,
                                "source": "pdf",
                                "source_name": section_name,
                                "cleaned_text": self.clean_text(text)
                            })
        except (KeyError, TypeError) as e:
            logger.warning(f"Error extrayendo contenido PDF: {e}")
        
        # Extraer contenido HTML
        try:
            if "html_pages" in data["extracted_content"]:
                for url, page_data in data["extracted_content"]["html_pages"].items():
                    text = page_data.get("text", "")
                    metadata = page_data.get("metadata", {})
                    relevance = page_data.get("relevance", 0)
                    
                    # Solo incluir páginas con cierta relevancia
                    if text and relevance >= 0.3:
                        extracted_content["html"].append({
                            "text": text,
                            "metadata": metadata,
                            "url": url,
                            "relevance": relevance,
                            "source": "html",
                            "cleaned_text": self.clean_text(text)
                        })
        except (KeyError, TypeError) as e:
            logger.warning(f"Error extrayendo contenido HTML: {e}")
        
        # Extraer contenido de Facebook
        try:
            if "facebook_texts" in data["extracted_content"]:
                for url, fb_data in data["extracted_content"]["facebook_texts"].items():
                    text = fb_data.get("extracted_text", "")
                    pdf_path = fb_data.get("pdf_path", "")
                    processed_date = fb_data.get("processed_date", "")
                    
                    if text:
                        extracted_content["facebook"].append({
                            "text": text,
                            "url": url,
                            "pdf_path": pdf_path,
                            "processed_date": processed_date,
                            "source": "facebook",
                            "cleaned_text": self.clean_text(text)
                        })
        except (KeyError, TypeError) as e:
            logger.warning(f"Error extrayendo contenido Facebook: {e}")
        
        return extracted_content
    
    def compute_similarity_matrix(self, texts: List[str]) -> np.ndarray:
        """
        Calcula la matriz de similitud entre textos usando TF-IDF y similitud del coseno.
        
        Args:
            texts: Lista de textos a comparar
            
        Returns:
            Matriz de similitud como array de NumPy
        """
        if not texts:
            return np.array([])
            
        try:
            tfidf_matrix = self.vectorizer.fit_transform(texts)
            return cosine_similarity(tfidf_matrix, tfidf_matrix)
        except Exception as e:
            logger.error(f"Error al calcular matriz de similitud: {e}")
            return np.zeros((len(texts), len(texts)))
    
    def detect_redundant_content(self, content_items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Detecta y elimina contenido redundante entre una lista de ítems.
        
        Args:
            content_items: Lista de diccionarios con contenido extraído
            
        Returns:
            Lista de contenido filtrado sin redundancias
        """
        if not content_items:
            return []
            
        # Extraer textos limpios para procesamiento
        cleaned_texts = [item["cleaned_text"] for item in content_items]
        
        # Calcular matriz de similitud
        similarity_matrix = self.compute_similarity_matrix(cleaned_texts)
        
        # Detectar grupos de contenido similar
        content_groups = []
        processed_indices = set()
        
        for i in range(len(content_items)):
            if i in processed_indices:
                continue
                
            # Crear un nuevo grupo
            similar_group = [i]
            processed_indices.add(i)
            
            # Encontrar ítems similares a este
            for j in range(len(content_items)):
                if j != i and j not in processed_indices:
                    if similarity_matrix[i, j] >= self.similarity_threshold:
                        similar_group.append(j)
                        processed_indices.add(j)
            
            content_groups.append(similar_group)
        
        # Para cada grupo, elegir el representante
        unique_content = []
        
        for group in content_groups:
            if len(group) == 1:
                # Si solo hay un ítem en el grupo, incluirlo directamente
                unique_content.append(content_items[group[0]])
            else:
                # Si hay múltiples ítems similares, seleccionar el mejor representante
                # Priorizar por relevancia, longitud del texto o fuente
                best_idx = self._select_best_representative(group, content_items)
                representative = content_items[best_idx]
                
                # Conservar información de qué contenido está fusionado
                representative["merged_from"] = [content_items[idx]["source"] for idx in group]
                unique_content.append(representative)
        
        return unique_content
    
    def _select_best_representative(self, group: List[int], 
                                  content_items: List[Dict[str, Any]]) -> int:
        """
        Selecciona el mejor representante de un grupo de ítems similares.
        
        Args:
            group: Lista de índices de ítems similares
            content_items: Lista completa de ítems de contenido
            
        Returns:
            Índice del ítem seleccionado como representante
        """
        best_score = -1
        best_idx = group[0]  # Por defecto, el primero
        
        for idx in group:
            item = content_items[idx]
            
            # Iniciar con puntaje base
            score = 0
            
            # Dar prioridad según fuente
            if item["source"] == "html":
                # Priorizar HTML con alta relevancia
                score += item.get("relevance", 0) * 10
            elif item["source"] == "pdf":
                # Los PDF suelen tener buen contenido
                score += 5
            else:  # Facebook
                score += 3
                
            # Favorecer textos más largos (pero no demasiado)
            text_length = len(item["text"])
            if 100 <= text_length <= 1000:
                score += 3
            elif text_length > 1000:
                score += 2
            else:
                score += 1
                
            # Actualizar si es mejor
            if score > best_score:
                best_score = score
                best_idx = idx
                
        return best_idx
    
    def organize_by_topics(self, unique_content: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        """
        Organiza el contenido por temas para una mejor estructura.
        
        Args:
            unique_content: Lista de contenido sin redundancias
            
        Returns:
            Diccionario de contenido organizado por temas
        """
        # Definir categorías/temas comunes
        topics = {
            "noticias_sunass": [],
            "agua_saneamiento": [],
            "politica_economia": [],
            "otros": []
        }
        
        # Palabras clave para cada tema
        topic_keywords = {
            "noticias_sunass": ["sunass", "superintendencia", "nacional", "servicios", "saneamiento", 
                              "módulo", "atención", "orientación", "ciudadano", "mac", "regulador"],
            "agua_saneamiento": ["agua", "potable", "desagüe", "alcantarillado", "eps", "sedapal", 
                               "sedalib", "epsel", "ptar", "planta", "tratamiento", "residuales"],
            "politica_economia": ["gobierno", "ministerio", "economía", "vivienda", "proyecto", 
                                "inversión", "millones", "presupuesto", "soles", "financiamiento"]
        }
        
        # Clasificar cada contenido
        for item in unique_content:
            text = item["text"].lower()
            
            # Determinar tema según palabras clave
            max_matches = 0
            best_topic = "otros"
            
            for topic, keywords in topic_keywords.items():
                matches = sum(1 for keyword in keywords if keyword in text)
                if matches > max_matches:
                    max_matches = matches
                    best_topic = topic
            
            # Asignar a la categoría apropiada
            topics[best_topic].append(item)
        
        return topics
    
    def process_consolidated_json(self, input_path: str, output_path: str) -> Dict:
        """
        Procesa el archivo JSON consolidado para eliminar redundancias y organizarlo.
        
        Args:
            input_path: Ruta al archivo JSON consolidado de entrada
            output_path: Ruta para guardar el JSON limpio
            
        Returns:
            Diccionario con datos limpios y organizados
        """
        # Cargar datos del archivo JSON
        try:
            with open(input_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
        except Exception as e:
            logger.error(f"Error cargando archivo JSON {input_path}: {e}")
            return {}
            
        # Extraer contenido
        extracted_content = self.extract_content_from_json(data)
        
        # Combinar contenido de todas las fuentes para procesamiento
        all_content = (
            extracted_content["pdf"] + 
            extracted_content["html"] + 
            extracted_content["facebook"]
        )
        
        # Detectar y eliminar redundancias
        unique_content = self.detect_redundant_content(all_content)
        
        # Organizar por temas
        organized_content = self.organize_by_topics(unique_content)
        
        # Crear salida limpia
        clean_data = {
            "metadata": {
                "source_pdf": data.get("metadata", {}).get("source_pdf", ""),
                "processing_date": data.get("metadata", {}).get("processing_date", ""),
                "cleaning_date": datetime.now().isoformat(),
                "stats_summary": data.get("metadata", {}).get("stats_summary", {})
            },
            "organized_content": organized_content
        }
        
        # Guardar resultado
        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(clean_data, f, ensure_ascii=False, indent=2)
            logger.info(f"Datos limpios guardados en: {output_path}")
        except Exception as e:
            logger.error(f"Error guardando archivo JSON limpio: {e}")
            
        return clean_data

