"""
Módulo para analizar la similitud entre textos utilizando diferentes técnicas NLP.
"""

import re
import string
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.stem import SnowballStemmer

# Descargar recursos necesarios de NLTK (si no están ya descargados)
def download_nltk_resources():
    resources = ['punkt', 'stopwords']
    for resource in resources:
        try:
            if resource == 'punkt':
                nltk.data.find('tokenizers/punkt')
            elif resource == 'stopwords':
                nltk.data.find('corpora/stopwords')
        except LookupError:
            print(f"Descargando recurso NLTK: {resource}")
            nltk.download(resource)

# Descargar los recursos necesarios
download_nltk_resources()

class SimilarityAnalyzer:
    """
    Clase para analizar la similitud semántica entre textos.
    """
    
    def __init__(self, language='spanish', similarity_threshold=0.7):
        """
        Inicializa el analizador de similitud.
        
        Args:
            language (str): Idioma para stopwords y stemming ('spanish' o 'english')
            similarity_threshold (float): Umbral de similitud para considerar textos como similares
        """
        self.language = language
        self.similarity_threshold = similarity_threshold
        self.stemmer = SnowballStemmer(language)
        self.stop_words = set(stopwords.words(language))
        self.vectorizer = TfidfVectorizer(stop_words=self.stop_words)
        
    def preprocess_text(self, text):
        """
        Preprocesa el texto para análisis de similitud.
        
        Args:
            text (str): Texto a preprocesar
            
        Returns:
            str: Texto preprocesado
        """
        if not text or not isinstance(text, str):
            return ""
            
        # Convertir a minúsculas
        text = text.lower()
        
        # Eliminar puntuación
        text = text.translate(str.maketrans('', '', string.punctuation))
        
        # Tokenizar de manera simple, evitando nltk.word_tokenize por el error de punkt_tab
        tokens = text.split()
        
        # Eliminar stopwords y aplicar stemming
        tokens = [self.stemmer.stem(token) for token in tokens if token not in self.stop_words]
        
        # Reconvertir a texto
        return ' '.join(tokens)
    
    def compute_similarity(self, text1, text2):
        """
        Calcula la similitud coseno entre dos textos.
        
        Args:
            text1 (str): Primer texto
            text2 (str): Segundo texto
            
        Returns:
            float: Valor de similitud entre 0 y 1
        """
        if not text1 or not text2:
            return 0.0
            
        # Preprocesar textos
        processed_text1 = self.preprocess_text(text1)
        processed_text2 = self.preprocess_text(text2)
        
        if not processed_text1 or not processed_text2:
            return 0.0
            
        # Crear matriz TF-IDF
        try:
            tfidf_matrix = self.vectorizer.fit_transform([processed_text1, processed_text2])
            
            # Calcular similitud coseno
            similarity = cosine_similarity(tfidf_matrix[0:1], tfidf_matrix[1:2])[0][0]
            return similarity
        except Exception:
            # Fallback para textos muy cortos o idénticos
            if processed_text1 == processed_text2:
                return 1.0
            return 0.0
    
    def is_similar(self, text1, text2, threshold=None):
        """
        Determina si dos textos son similares según el umbral configurado.
        
        Args:
            text1 (str): Primer texto
            text2 (str): Segundo texto
            threshold (float, optional): Umbral de similitud personalizado. Si no se proporciona,
                                        se usa el umbral por defecto.
            
        Returns:
            bool: True si los textos son similares, False en caso contrario
        """
        similarity = self.compute_similarity(text1, text2)
        threshold = threshold if threshold is not None else self.similarity_threshold
        return similarity >= threshold
    
    def find_similar_paragraphs(self, source_paragraphs, target_paragraphs):
        """
        Encuentra párrafos similares entre dos conjuntos de textos.
        
        Args:
            source_paragraphs (list): Lista de textos fuente
            target_paragraphs (list): Lista de textos objetivo
            
        Returns:
            list: Lista de tuplas (índice_fuente, índice_objetivo, similitud)
        """
        similarities = []
        
        for i, source in enumerate(source_paragraphs):
            for j, target in enumerate(target_paragraphs):
                sim = self.compute_similarity(source, target)
                if sim >= self.similarity_threshold:
                    similarities.append((i, j, sim))
        
        return similarities
