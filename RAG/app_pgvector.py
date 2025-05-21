"""
Aplicación web para consultar el sistema RAG de SUNASS usando PostgreSQL con pgvector.
"""

import os
import sys
import yaml
import json
import streamlit as st
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional

# Añadir directorio actual al path para importar módulos
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Importar componentes del sistema RAG
from vector_store import VectorDBManager
from config_api import is_api_configured, configure_api_key, get_api_key
from embedding_service import EmbeddingService

class RAGQueryApp:
    """Aplicación RAG para consultar noticias usando PostgreSQL y pgvector."""
    
    def __init__(self, config_path: str = 'config.yaml'):
        """
        Inicializa la aplicación RAG.
        
        Args:
            config_path: Ruta al archivo de configuración
        """
        self.config_path = config_path
        self.load_config()
        
        # Verificar configuración de API
        if not is_api_configured('google'):
            st.error("⚠️ API key de Google no configurada.")
            st.info("Por favor configura la API key para usar el sistema.")
            if st.button("Configurar API key ahora"):
                api_key = st.text_input("Ingresa tu API key de Google:", type="password")
                if api_key and st.button("Guardar"):
                    os.environ["GOOGLE_API_KEY"] = api_key
                    configure_api_key('google', interactive=False)
                    st.success("✅ API key configurada correctamente.")
                    st.experimental_rerun()
            return
        
        # Inicializar servicios
        self.init_services()
    
    def load_config(self):
        """Carga la configuración desde el archivo YAML."""
        try:
            # Determinar ruta absoluta si es relativa
            if not os.path.isabs(self.config_path):
                base_dir = os.path.dirname(os.path.abspath(__file__))
                config_path = os.path.join(base_dir, self.config_path)
            else:
                config_path = self.config_path
                
            with open(config_path, 'r', encoding='utf-8') as f:
                self.config = yaml.safe_load(f)
        except Exception as e:
            st.error(f"Error cargando configuración: {e}")
            self.config = {}
    
    def init_services(self):
        """Inicializa los servicios necesarios para la aplicación."""
        try:
            # Inicializar servicio de embeddings
            self.embedding_service = EmbeddingService(self.config_path)
            
            # Inicializar gestor de base de datos vectorial
            self.vector_store = VectorDBManager(self.config_path)
            
            # Obtener fechas disponibles
            self.available_dates = self.get_available_dates()
            
        except Exception as e:
            st.error(f"Error inicializando servicios: {e}")
            st.exception(e)
    
    def get_available_dates(self) -> List[str]:
        """
        Obtiene las fechas disponibles en la base de datos.
        
        Returns:
            Lista de fechas en formato DDMMYYYY
        """
        try:
            conn = self.vector_store._get_connection()
            try:
                with conn.cursor() as cur:
                    cur.execute(f"SELECT DISTINCT date FROM {self.vector_store.full_table_name} ORDER BY date DESC")
                    dates = [row[0] for row in cur.fetchall()]
                    return dates
            finally:
                conn.close()
        except Exception as e:
            st.error(f"Error obteniendo fechas: {e}")
            return []
    
    def search(self, query: str, limit: int = 5, filter_date: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Realiza una búsqueda en la base de datos vectorial.
        
        Args:
            query: Consulta a buscar
            limit: Número máximo de resultados
            filter_date: Fecha para filtrar resultados (formato DDMMYYYY)
            
        Returns:
            Lista de resultados con texto y metadatos
        """
        try:
            # Generar embedding para la consulta
            query_embedding = []
            query_chunks = [{"text": query}]
            
            with st.spinner("Generando embeddings para la consulta..."):
                query_with_embedding = self.embedding_service.get_embeddings(query_chunks)
                query_embedding = query_with_embedding[0]["embedding"]
            
            # Preparar filtros
            filters = {}
            if filter_date:
                filters["date"] = filter_date
            
            # Realizar búsqueda
            with st.spinner("Buscando resultados..."):
                results = self.vector_store.hybrid_search(
                    query=query,
                    query_embedding=query_embedding,
                    filters=filters,
                    limit=limit
                )
            
            return results
            
        except Exception as e:
            st.error(f"Error en búsqueda: {e}")
            st.exception(e)
            return []

# Configuración de la página
st.set_page_config(
    page_title="SUNASS News Agent - RAG",
    page_icon="📰",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Título y descripción
st.title("📰 SUNASS News Agent - Sistema RAG")
st.markdown("""
Este sistema permite consultar noticias relacionadas con SUNASS utilizando RAG 
(Retrieval Augmented Generation) con PostgreSQL y pgvector.
""")

# Inicializar la aplicación RAG
@st.cache_resource
def initialize_app():
    return RAGQueryApp()

app = initialize_app()

# Sidebar para configuración
st.sidebar.header("Configuración")

# Verificar si la aplicación se inicializó correctamente
if not hasattr(app, 'vector_store') or not hasattr(app, 'embedding_service'):
    st.error("No se pudo inicializar la aplicación RAG correctamente.")
    st.stop()

# Filtro por fecha
use_date_filter = st.sidebar.checkbox("Filtrar por fecha", value=False)
filter_date = None

if use_date_filter:
    if app.available_dates:
        filter_date = st.sidebar.selectbox(
            "Fecha",
            app.available_dates,
            format_func=lambda x: f"{x[:2]}/{x[2:4]}/{x[4:]}" if len(x) >= 8 else x
        )
    else:
        st.sidebar.warning("No hay fechas disponibles en la base de datos.")

# Número de resultados
limit = st.sidebar.slider("Número de resultados", 1, 20, 5)

# Consulta
query = st.text_input("Ingresa tu consulta", placeholder="Ejemplo: problemas de agua en Piura")

# Botón de búsqueda
search_button = st.button("Buscar")

# Realizar búsqueda cuando se presiona el botón
if search_button and query:
    results = app.search(query, limit, filter_date)
    
    if results:
        st.success(f"Se encontraron {len(results)} resultados")
        
        # Mostrar cada resultado con un expander
        for i, result in enumerate(results):
            # Formatear el score para mostrarlo como porcentaje
            score_percent = f"{result.get('combined_score', 0) * 100:.1f}%"
            
            # Formatear fecha si está disponible
            date_str = result.get('date', '')
            if date_str and len(date_str) >= 8:
                date_formatted = f"{date_str[:2]}/{date_str[2:4]}/{date_str[4:]}"
            else:
                date_formatted = date_str
            
            with st.expander(f"Resultado {i+1} - Score: {score_percent}"):
                if date_formatted:
                    st.markdown(f"**Fecha:** {date_formatted}")
                
                title = result.get('title', '')
                if title:
                    st.markdown(f"**Título:** {title}")
                
                url = result.get('url', '')
                if url:
                    st.markdown(f"**URL:** [{url}]({url})")
                
                st.markdown("**Texto:**")
                st.markdown(result.get('content', ''))
                
                st.markdown("---")
                source = result.get('source', 'Desconocido')
                st.markdown(f"**Fuente:** {source}")
                
                # Mostrar scores individuales
                col1, col2 = st.columns(2)
                with col1:
                    st.markdown(f"**Score vectorial:** {result.get('vector_score', 0):.3f}")
                with col2:
                    st.markdown(f"**Score keywords:** {result.get('keyword_score', 0):.3f}")
    else:
        st.warning("No se encontraron resultados para la consulta")

# Información adicional
st.sidebar.markdown("---")
st.sidebar.header("Información")
st.sidebar.markdown("""
Este sistema utiliza:
- Google AI para embeddings
- PostgreSQL con pgvector para búsqueda vectorial
- Búsqueda híbrida (vectorial + keywords) para mejorar resultados
""")

# Estadísticas de la base de datos
st.sidebar.markdown("---")
st.sidebar.header("Estadísticas")
try:
    conn = app.vector_store._get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {app.vector_store.table_name}")
            doc_count = cur.fetchone()[0]
            st.sidebar.metric("Documentos indexados", doc_count)
            
            cur.execute(f"SELECT COUNT(DISTINCT source) FROM {app.vector_store.table_name}")
            source_count = cur.fetchone()[0]
            st.sidebar.metric("Fuentes únicas", source_count)
    finally:
        conn.close()
except Exception as e:
    st.sidebar.warning("No se pudieron cargar estadísticas")

# Footer
st.markdown("---")
st.markdown("SUNASS News Agent - Sistema RAG © 2025")
