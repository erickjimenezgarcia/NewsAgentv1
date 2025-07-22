"""
Script de limpieza avanzada para preparar datos para RAG.
Este script procesa los archivos JSON de salida del sistema NewsAgent,
eliminando contenido no relevante y preparando los datos para chunking y embedding.

Uso:
    python clean_data.py DDMMYYYY
    
Ejemplo:
    python clean_data.py 16052025

Los archivos JSON se buscan por defecto en la carpeta 'output/clean'.
Los archivos limpios se guardan en la carpeta 'RAG/data'.

Se generan tres archivos de salida:
1. rag_*.json - Versión completa con metadata mínima
2. simple_*.json - Versión simplificada con solo los textos
3. rag_*.txt - Versión en texto plano para inspección
"""

import os
import json
import re
import sys
from pathlib import Path
import argparse
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime


class AdvancedCleaner:
    """Clase para limpieza avanzada de datos para RAG."""
    
    def __init__(self, input_dir: str, output_dir: str, date_str: str):
        """
        Inicializa el limpiador avanzado.
        
        Args:
            input_dir: Directorio donde se encuentran los archivos JSON a limpiar
            output_dir: Directorio donde se guardarán los archivos limpios
            date_str: Fecha en formato DDMMYYYY para procesar solo archivos de esa fecha
        """
        self.input_dir = Path(input_dir)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.date_str = date_str
        
        # Validar formato de fecha
        try:
            datetime.strptime(date_str, '%d%m%Y')
        except ValueError:
            raise ValueError(f"Formato de fecha incorrecto: {date_str}. Use DDMMYYYY (ejemplo: 16052025)")
        
        # Patrones para eliminar texto no deseado
        self.unwanted_patterns = [
            r"\d+ vez(es)? compartido",
            r"Iniciar sesión",
            r"Me gusta",
            r"Comentar",
            r"Compartir",
            r"\d+ comentarios?",
            r"Más pertinentes",
            r"Autor",
            r"@seguidores",
            r"Seguir",
            r"Más populares",
            r"Publicación de",
            r"Buscar",
            r"Contraseña",
            r"Crear una cuenta",
            r"Registrarse",
            r"Recuperación de contraseña",
            r"Recupera tu contraseña",
            r"tu correo electrónico",
            r"Portada",
            r"Política",
            r"Nacional",
            r"Mundo",
            r"Buscar",
            r"Publicado por",
            r"Fecha:",
            r"Facebook Twitter Pinterest WhatsApp",
            r"- Publicidad -",
            r"Tags",
            r"Artículo anterior",
            r"Artículo siguiente",
            r"RELACIONADOS",
            r"Popular",
            r"Recien leídos",
            r"Recomendado",
            r"Más Noticias",
            r"Últimas noticias",
            r"Convierta a Diario .* en su fuente de noticias aquí",
            r"© \d+ Todos los Derechos Reservados",
            r"Debes Saber",
            r"Puedes leer",
            r"LEE MÁS",
            r"VER MÁS",
            r"LE PUEDE INTERESAR",
            r"TAGS RELACIONADOS",
            r"NO TE PIERDAS",
            r"Contenido de",
            r"Siguiente artículo",
            r"Saltar a contenido principal",
            r"reproducciones",
        ]
        
        # Compilar patrones para mejor rendimiento
        self.compiled_patterns = [re.compile(pattern, re.IGNORECASE) for pattern in self.unwanted_patterns]
        
    def clean_text(self, text: str) -> str:
        """
        Limpia el texto eliminando patrones no deseados y normalizando espacios.
        
        Args:
            text: Texto a limpiar
            
        Returns:
            Texto limpio o cadena vacía si el texto no es relevante
        """
        if not text:
            return ""
        
        # Verificar si el texto es relevante antes de procesarlo
        if self.is_irrelevant_text(text):
            return ""
            
        # Aplicar todos los patrones de limpieza
        for pattern in self.compiled_patterns:
            text = pattern.sub("", text)
        
        # Eliminar múltiples espacios y saltos de línea
        text = re.sub(r'\s+', ' ', text)
        
        # Eliminar espacios al inicio y final
        text = text.strip()
        
        # Verificar longitud mínima significativa después de la limpieza
        if len(text) < 20:  # Textos muy cortos probablemente no son informativos
            return ""
            
        return text
        
    def is_irrelevant_text(self, text: str) -> bool:
        """
        Determina si un texto es irrelevante y debe ser descartado.
        
        Args:
            text: Texto a evaluar
            
        Returns:
            True si el texto debe ser descartado, False si es relevante
        """
        # Lista de patrones para textos irrelevantes
        irrelevant_patterns = [
            # Referencias a imágenes o archivos
            r'^image\d+\.\w{3}\s+\d+K?$',  # Ej: "image001.jpg 17K"
            r'^https?:\/\/',  # Textos que son solo URLs
            r'^OneDrive$',  # Texto genérico de OneDrive
            r'^Este cont?[ e]?nido no está disponible$',  # Contenido no disponible
            r'^PDF\s+HTML\s+Cuadernillo$',
            r'^Saltar (a|al) contenido',
            r'^(PDF|HTML|XML|CSV|XLS|XLSX|DOC|DOCX|JSON)$',  # Solo extensiones de archivo
            r'^\d+ veces compartida$',  # Metadata de compartidos
            r'^Me [Gg]usta\s+Comentar\s+Compartir$',  # Botones de Facebook
            r'^\s*Iniciar sesión\s*$',  # Texto de inicio de sesión
            
            # Otros textos sin valor informativo
            r'^Ver (más|todo)$',
            r'^Leer más$',
            r'^Siguiente$',
            r'^Anterior$',
            r'^Volver$',
            r'^(Cerrar|Aceptar|Cancelar)$',
            r'^Mostrar\s+\d+\s+(comentarios?|respuestas?)$',
            r'^\d+\s+reproducciones$'
        ]
        
        # Comprobar si el texto coincide con algún patrón irrelevante
        for pattern in irrelevant_patterns:
            if re.search(pattern, text, re.IGNORECASE):
                return True
                
        # Verificar si el texto es demasiado corto y parece ser sólo una referencia
        if len(text) < 15 and re.search(r'^[\w\s\.]+$', text):
            return True
            
        return False
    
    def clean_json_file(self, file_path: Path) -> Dict[str, Any]:
        """
        Limpia un archivo JSON completo.
        
        Args:
            file_path: Ruta al archivo JSON
            
        Returns:
            Diccionario con datos limpios
        """
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Crear una estructura nueva para los datos limpios con metadata mínima
            clean_data = {
                "fecha": self.date_str,
                "fecha_procesamiento": datetime.now().isoformat(),
                "content": []
            }
            
            # Procesar contenido del PDF
            if "extracted_content" in data and "pdf_paragraphs" in data["extracted_content"]:
                pdf_paragraphs = data["extracted_content"]["pdf_paragraphs"]
                
                # Verificar si pdf_paragraphs es un diccionario o una lista
                if isinstance(pdf_paragraphs, dict):
                    # Caso 1: Es un diccionario con secciones
                    for section, paragraphs in pdf_paragraphs.items():
                        if isinstance(paragraphs, list):
                            for paragraph in paragraphs:
                                if "text" in paragraph and paragraph["text"]:
                                    clean_text = self.clean_text(paragraph["text"])
                                    if clean_text:
                                        clean_data["content"].append({
                                            "source": "pdf",
                                            "section": section,
                                            "text": clean_text,
                                            "page": paragraph.get("page", 0),
                                            "url": paragraph.get("metadata", {}).get("url", "")
                                        })
                elif isinstance(pdf_paragraphs, list):
                    # Caso 2: Es una lista directa de párrafos
                    for paragraph in pdf_paragraphs:
                        if "text" in paragraph and paragraph["text"]:
                            clean_text = self.clean_text(paragraph["text"])
                            if clean_text:
                                clean_data["content"].append({
                                    "source": "pdf",
                                    "section": "general",
                                    "text": clean_text,
                                    "page": paragraph.get("page", 0),
                                    "url": paragraph.get("metadata", {}).get("url", "")
                                })
            
            # Procesar contenido HTML
            if "extracted_content" in data and "html_pages" in data["extracted_content"]:
                html_pages = data["extracted_content"]["html_pages"]
                
                if isinstance(html_pages, dict):
                    # Caso normal: Es un diccionario de URLs
                    for url, content in html_pages.items():
                        if "text" in content and content["text"]:
                            # Obtener metadatos importantes
                            metadata = content.get("metadata", {})
                            title = metadata.get("title", "")
                            description = metadata.get("description", "")
                            
                            # Verificar si el título y el texto son relevantes
                            if title == "OneDrive" or len(title) < 5:
                                continue  # Saltar entradas con títulos genéricos o muy cortos
                                
                            clean_text = self.clean_text(content["text"])
                            
                            # Solo procesar si hay texto limpio relevante
                            if clean_text:
                                # Añadir descripción al principio del texto si existe y añade valor
                                if description and len(description) > 20 and description != title:
                                    clean_text = f"{description}\n\n{clean_text}"
                                
                                clean_data["content"].append({
                                    "source": "html",
                                    "text": clean_text,
                                    "url": url,
                                    "title": title,
                                    "relevance": content.get("relevance", 0)
                                })
                            
                elif isinstance(html_pages, list):
                    # Caso alternativo: Es una lista de contenidos HTML
                    for content in html_pages:
                        if isinstance(content, dict) and "text" in content and content["text"]:
                            # Verificar relevancia del título
                            title = content.get("title", "")
                            if title == "OneDrive" or len(title) < 5:
                                continue
                            
                            clean_text = self.clean_text(content["text"])
                            if clean_text:
                                # Añadir descripción si existe
                                description = content.get("description", "")
                                if description and len(description) > 20 and description != title:
                                    clean_text = f"{description}\n\n{clean_text}"
                                    
                                clean_data["content"].append({
                                    "source": "html",
                                    "text": clean_text,
                                    "url": content.get("url", ""),
                                    "title": title,
                                    "relevance": content.get("relevance", 0)
                                })
            
            # Procesar contenido de imágenes
            if "extracted_content" in data and "image_texts" in data["extracted_content"]:
                image_texts = data["extracted_content"]["image_texts"]
                
                if isinstance(image_texts, dict):
                    # Caso normal: Es un diccionario de IDs de imagen
                    for img_id, content in image_texts.items():
                        # Buscar el texto en diferentes campos posibles
                        text_content = ""
                        
                        # Prioridad 1: Campo extracted_text 
                        if "extracted_text" in content and content["extracted_text"]:
                            text_content = content["extracted_text"]
                        # Prioridad 2: Campo text
                        elif "text" in content and content["text"]:
                            text_content = content["text"]
                            
                        if text_content:
                            clean_text = self.clean_text(text_content)
                            # Solo incluir textos que son realmente informativos
                            if clean_text and len(clean_text) > 50:  # Filtro adicional para textos muy cortos
                                clean_data["content"].append({
                                    "source": "image",
                                    "text": clean_text,
                                    "image_id": img_id
                                })
                elif isinstance(image_texts, list):
                    # Caso alternativo: Es una lista de contenidos de imagen
                    for i, content in enumerate(image_texts):
                        if not isinstance(content, dict):
                            continue
                            
                        # Buscar texto en diferentes campos posibles
                        text_content = ""
                        
                        # Prioridad 1: extracted_text
                        if "extracted_text" in content and content["extracted_text"]:
                            text_content = content["extracted_text"]
                        # Prioridad 2: text
                        elif "text" in content and content["text"]:
                            text_content = content["text"]
                        
                        if text_content:
                            clean_text = self.clean_text(text_content)
                            # Solo incluir textos que son realmente informativos
                            if clean_text and len(clean_text) > 50:  # Filtro adicional para textos muy cortos
                                # Obtener un identificador para la imagen
                                image_id = content.get("image_filename", 
                                             content.get("image_id", 
                                                       content.get("url", f"img_{i}")))
                                
                                clean_data["content"].append({
                                    "source": "image",
                                    "text": clean_text,
                                    "image_id": image_id
                                })
            
            # Procesar contenido de Facebook
            if "extracted_content" in data and "facebook_texts" in data["extracted_content"]:
                facebook_texts = data["extracted_content"]["facebook_texts"]
                
                if isinstance(facebook_texts, dict):
                    # Caso normal: Es un diccionario de URLs
                    for url, content in facebook_texts.items():
                        # Revisar si hay texto extraído y es válido (no genérico)
                        if "extracted_text" in content and content["extracted_text"]:
                            text = content["extracted_text"]
                            
                            # Descartar textos irrelevantes de Facebook
                            if text.strip() in ["Este contenido no está disponible", "Este cont enido no está disponible"]:
                                continue
                                
                            clean_text = self.clean_text(text)
                            
                            # Solo incluir si el texto es significativo
                            if clean_text and len(clean_text) > 50:  # Asegurar que tiene contenido valioso
                                fb_item = {
                                    "source": "facebook",
                                    "text": clean_text,
                                    "url": url
                                }
                                
                                # Solo incluir pdf_path si realmente es necesario para el RAG
                                # (normalmente no es necesario para chunking/embeddings)
                                #if content.get("pdf_path"):
                                #    fb_item["pdf_path"] = content.get("pdf_path")
                                    
                                clean_data["content"].append(fb_item)
                        
                elif isinstance(facebook_texts, list):
                    # Caso alternativo: Es una lista de contenidos de Facebook
                    for content in facebook_texts:
                        if not isinstance(content, dict):
                            continue
                            
                        # Revisar si hay texto extraído válido
                        if "extracted_text" in content and content["extracted_text"]:
                            text = content["extracted_text"]
                            
                            # Filtrar textos irrelevantes
                            if text.strip() in ["Este contenido no está disponible", "Este cont enido no está disponible"]:
                                continue
                                
                            clean_text = self.clean_text(text)
                            
                            # Solo incluir si el texto es significativo
                            if clean_text and len(clean_text) > 50:
                                fb_item = {
                                    "source": "facebook",
                                    "text": clean_text,
                                    "url": content.get("url", "")
                                }
                                
                                # Solo incluir pdf_path si realmente es necesario
                                #if content.get("pdf_path"):
                                #    fb_item["pdf_path"] = content.get("pdf_path")
                                    
                                clean_data["content"].append(fb_item)
            
            return clean_data
        
        except Exception as e:
            print(f"Error procesando {file_path}: {e}")
            return {}
    
    def process_directory(self):
        """Procesa los archivos JSON en el directorio de entrada para la fecha especificada."""
        # Buscar archivos JSON en el directorio de entrada que coincidan con la fecha
        json_files = list(self.input_dir.glob(f"clean_{self.date_str}.json"))
        
        if not json_files:
            json_files = list(self.input_dir.glob(f"consolidated_{self.date_str}.json"))
        
        if not json_files:
            print(f"No se encontraron archivos JSON para la fecha {self.date_str} en {self.input_dir}")
            return
        
        for json_file in json_files:
            print(f"Procesando {json_file.name}...")
            
            # Limpiar el archivo JSON
            clean_data = self.clean_json_file(json_file)
            
            if not clean_data:
                print(f"No se pudo procesar {json_file.name}")
                continue
            
            # Guardar archivo JSON limpio
            output_json = self.output_dir / f"rag_{json_file.stem}.json"
            with open(output_json, 'w', encoding='utf-8') as f:
                json.dump(clean_data, f, ensure_ascii=False, indent=2)
            
            print(f"Archivo limpio guardado en {output_json}")
            
            # También guardar una versión simplificada con solo los textos
            simplified_data = {
                "fecha": self.date_str,
                "textos": [item["text"] for item in clean_data["content"]]
            }
            
            output_simplified = self.output_dir / f"simple_{json_file.stem}.json"
            with open(output_simplified, 'w', encoding='utf-8') as f:
                json.dump(simplified_data, f, ensure_ascii=False, indent=2)
            
            print(f"Versión simplificada guardada en {output_simplified}")
            
            # También guardar una versión en formato de texto para inspección
            output_txt = output_json.with_suffix('.txt')
            with open(output_txt, 'w', encoding='utf-8') as f:
                f.write(f"# Datos limpios para RAG - {json_file.stem}\n\n")
                
                for item in clean_data["content"]:
                    f.write(f"## Fuente: {item['source']}\n")
                    
                    if 'url' in item and item['url']:
                        f.write(f"URL: {item['url']}\n")
                    
                    if 'title' in item and item['title']:
                        f.write(f"Título: {item['title']}\n")
                    
                    f.write(f"\n{item['text']}\n\n")
                    f.write("-" * 80 + "\n\n")
            
            print(f"Versión de texto guardada en {output_txt}")


def main():
    """Función principal."""
    parser = argparse.ArgumentParser(description="Limpieza avanzada de datos para RAG")
    parser.add_argument("date", type=str,
                        help="Fecha en formato DDMMYYYY (ejemplo: 16052025)")
    parser.add_argument("--input", "-i", default="output/clean", 
                        help="Directorio de entrada con archivos JSON")
    parser.add_argument("--output", "-o", default="RAG/data", 
                        help="Directorio de salida para archivos limpios")
    
    args = parser.parse_args()
    
    try:
        cleaner = AdvancedCleaner(args.input, args.output, args.date)
        cleaner.process_directory()
    except ValueError as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
