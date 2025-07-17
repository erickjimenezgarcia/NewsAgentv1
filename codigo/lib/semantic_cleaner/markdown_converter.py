"""
Módulo para convertir datos JSON limpios a formato Markdown.
"""

import json
import logging
from datetime import datetime
import os
import re

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('markdown_converter')

class MarkdownConverter:
    """
    Clase para convertir datos JSON a formato Markdown.
    """
    
    def __init__(self):
        """
        Inicializa el convertidor Markdown.
        """
        pass
        
    def _sanitize_text(self, text):
        """
        Sanitiza el texto para formato Markdown.
        
        Args:
            text (str): Texto a sanitizar
            
        Returns:
            str: Texto sanitizado
        """
        if not text or not isinstance(text, str):
            return ""
            
        # Reemplazar caracteres especiales de Markdown
        text = text.replace('#', '\\#')
        text = text.replace('*', '\\*')
        text = text.replace('_', '\\_')
        text = text.replace('`', '\\`')
        text = text.replace('>', '\\>')
        text = text.replace('<', '\\<')
        
        # Eliminar múltiples espacios en blanco
        text = re.sub(r'\s+', ' ', text).strip()
        
        return text
    
    def _format_date(self, date_str):
        """
        Formatea una fecha para mostrarla en Markdown.
        
        Args:
            date_str (str): Fecha en formato ISO
            
        Returns:
            str: Fecha formateada
        """
        try:
            # Convertir de formato ISO a formato legible
            date_obj = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
            return date_obj.strftime("%d/%m/%Y %H:%M:%S")
        except (ValueError, AttributeError):
            return date_str
    
    def _generate_metadata_section(self, metadata):
        """
        Genera la sección de metadatos para el Markdown.
        
        Args:
            metadata (dict): Metadatos del JSON
            
        Returns:
            str: Texto Markdown para la sección de metadatos
        """
        md_text = "# Informe de Noticias SUNASS\n\n"
        
        # Información de fecha y procesamiento
        if "stats_summary" in metadata:
            stats = metadata["stats_summary"]
            
            md_text += "## Información General\n\n"
            
            if "run_timestamp" in stats:
                md_text += f"**Fecha de procesamiento:** {self._format_date(stats['run_timestamp'])}\n\n"
            
            if "date_processed" in stats:
                md_text += f"**Fecha de datos:** {stats['date_processed']}\n\n"
            
            # Estadísticas
            md_text += "## Estadísticas\n\n"
            
            # URLs
            if "total_urls_in_pdf" in stats:
                md_text += f"**Total de URLs en PDF:** {stats['total_urls_in_pdf']}\n\n"
                
            if "new_urls_processed_count" in stats:
                md_text += f"**Nuevas URLs procesadas:** {stats['new_urls_processed_count']}\n\n"
                
            if "history_total_urls" in stats:
                md_text += f"**Historial total de URLs:** {stats['history_total_urls']}\n\n"
            
            # Categorías
            if "categories" in stats:
                md_text += "### Distribución por Categoría\n\n"
                for category, count in stats["categories"].items():
                    md_text += f"- **{category.capitalize()}:** {count}\n"
                md_text += "\n"
            
            # Estadísticas de procesamiento HTML
            if "html_processing" in stats:
                md_text += "### Procesamiento HTML\n\n"
                html_stats = stats["html_processing"]
                for key, value in html_stats.items():
                    md_text += f"- **{key.replace('_', ' ').capitalize()}:** {value}\n"
                md_text += "\n"
            
            # Estadísticas de procesamiento de imágenes
            if "image_processing" in stats:
                md_text += "### Procesamiento de Imágenes\n\n"
                img_stats = stats["image_processing"]
                for key, value in img_stats.items():
                    md_text += f"- **{key.replace('_', ' ').capitalize()}:** {value}\n"
                md_text += "\n"
            
            # Estadísticas de procesamiento de Facebook
            if "facebook_processing" in stats:
                md_text += "### Procesamiento de Facebook\n\n"
                fb_stats = stats["facebook_processing"]
                for key, value in fb_stats.items():
                    md_text += f"- **{key.replace('_', ' ').capitalize()}:** {value}\n"
                md_text += "\n"
            
            # Tiempos de procesamiento
            if "timings_seconds" in stats:
                md_text += "### Tiempos de Procesamiento (segundos)\n\n"
                timings = stats["timings_seconds"]
                for key, value in timings.items():
                    md_text += f"- **{key.replace('_', ' ').capitalize()}:** {value}\n"
                md_text += "\n"
                
            # Estadísticas de limpieza semántica
            if "semantic_cleaning" in stats:
                md_text += "### Limpieza Semántica\n\n"
                semantic_stats = stats["semantic_cleaning"]
                for key, value in semantic_stats.items():
                    if key != "timestamp":
                        md_text += f"- **{key.replace('_', ' ').capitalize()}:** {value}\n"
                md_text += "\n"
        
        return md_text
    
    def _generate_pdf_section(self, pdf_data):
        """
        Genera la sección de contenido PDF para el Markdown.
        
        Args:
            pdf_data (dict): Datos de PDF del JSON
            
        Returns:
            str: Texto Markdown para la sección de PDF
        """
        if not pdf_data:
            return ""
            
        md_text = "## Contenido de PDF\n\n"
        
        for source_key, paragraphs in pdf_data.items():
            md_text += f"### {source_key}\n\n"
            
            for paragraph in paragraphs:
                text = paragraph.get('text', '')
                page = paragraph.get('page', '')
                
                if text:
                    sanitized_text = self._sanitize_text(text)
                    md_text += f"{sanitized_text}\n\n"
                    if page:
                        md_text += f"*Página: {page}*\n\n"
                    md_text += "---\n\n"
        
        return md_text
    
    def _generate_html_section(self, html_data):
        """
        Genera la sección de contenido HTML para el Markdown.
        
        Args:
            html_data (dict): Datos de HTML del JSON
            
        Returns:
            str: Texto Markdown para la sección de HTML
        """
        if not html_data:
            return ""
            
        md_text = "## Contenido de Páginas Web\n\n"
        
        for url, page_data in html_data.items():
            title = page_data.get('metadata', {}).get('title', 'Sin título')
            text = page_data.get('text', '')
            relevance = page_data.get('relevance', 0)
            
            md_text += f"### {title}\n\n"
            md_text += f"**URL:** [{url}]({url})\n\n"
            
            if relevance:
                md_text += f"**Relevancia:** {relevance}\n\n"
                
            if text:
                sanitized_text = self._sanitize_text(text)
                md_text += f"{sanitized_text}\n\n"
                
            md_text += "---\n\n"
        
        return md_text
    
    def _generate_image_section(self, image_data):
        """
        Genera la sección de contenido de imágenes para el Markdown.
        
        Args:
            image_data (dict): Datos de imágenes del JSON
            
        Returns:
            str: Texto Markdown para la sección de imágenes
        """
        if not image_data:
            return ""
            
        md_text = "## Contenido de Imágenes\n\n"
        
        for image_key, image_info in image_data.items():
            extracted_text = image_info.get('extracted_text', '')
            url = image_info.get('url', image_key)

            md_text += f"### Imagen: {image_info.get('image_filename', image_key)}\n\n"
            md_text += f"**URL:** [{url}]({url})\n\n"
                
            if extracted_text:
                sanitized_text = self._sanitize_text(extracted_text)
                md_text += f"{sanitized_text}\n\n"
                
            md_text += "---\n\n"
        
        return md_text

    def _generate_facebook_section(self, facebook_data):
        """
        Genera la sección de contenido de Facebook para el Markdown.
        
        Args:
            facebook_data (dict): Datos de Facebook del JSON
            
        Returns:
            str: Texto Markdown para la sección de Facebook
        """
        if not facebook_data:
            return ""
            
        md_text = "## Contenido de Redes Sociales\n\n"
        
        for fb_key, fb_info in facebook_data.items():
            text = fb_info.get('extracted_text', '')
            
            md_text += f"### Publicación\n\n"
            md_text += f"**Fuente:** {fb_key}\n\n"
                
            if text:
                sanitized_text = self._sanitize_text(text)
                md_text += f"{sanitized_text}\n\n"
                
            md_text += "---\n\n"
        
        return md_text
    
    def convert_to_markdown(self, json_data, output_path=None):
        """
        Convierte datos JSON a formato Markdown.
        
        Args:
            json_data (dict): Datos JSON a convertir
            output_path (str, optional): Ruta del archivo de salida
            
        Returns:
            str: Contenido en formato Markdown
        """
        if not isinstance(json_data, dict):
            logger.error("Los datos JSON no tienen el formato esperado")
            return ""
            
        # Generar secciones de Markdown
        md_content = ""
        
        # Sección de metadatos
        if "metadata" in json_data:
            md_content += self._generate_metadata_section(json_data["metadata"])
        
        # Sección de contenido
        if "extracted_content" in json_data:
            content = json_data["extracted_content"]
            
            # PDF
            if "pdf_paragraphs" in content and content["pdf_paragraphs"]:
                md_content += self._generate_pdf_section(content["pdf_paragraphs"])
            
            # HTML
            if "html_pages" in content and content["html_pages"]:
                md_content += self._generate_html_section(content["html_pages"])
            
            # Imágenes
            if "image_texts" in content and content["image_texts"]:
                md_content += self._generate_image_section(content["image_texts"])
            
            # Facebook
            if "facebook_texts" in content and content["facebook_texts"]:
                md_content += self._generate_facebook_section(content["facebook_texts"])
        
        # Guardar en archivo si se proporciona una ruta
        if output_path:
            try:
                with open(output_path, 'w', encoding='utf-8') as f:
                    f.write(md_content)
                logger.info(f"Archivo Markdown guardado en: {output_path}")
            except Exception as e:
                logger.error(f"Error al guardar el archivo Markdown: {str(e)}")
        
        return md_content
