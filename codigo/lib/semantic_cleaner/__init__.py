"""
Módulo de limpieza semántica para textos extraídos de diferentes fuentes.

Este módulo proporciona funcionalidades para identificar y eliminar textos similares 
o redundantes de diferentes fuentes (PDF, HTML, imágenes, Facebook), realizando un 
análisis semántico para conservar la información más relevante y evitar duplicaciones.

Desarrollado para: SUNASS
Fecha: Mayo 2025
"""

from .text_similarity import SimilarityAnalyzer
from .semantic_cleaner import SemanticCleaner
from .markdown_converter import MarkdownConverter
