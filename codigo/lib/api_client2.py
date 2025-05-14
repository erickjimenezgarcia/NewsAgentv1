# codigo/lib/api_client.py
import logging
import os
import json
from datetime import datetime
from PIL import Image
from dotenv import load_dotenv
import google.generativeai as genai

logger = logging.getLogger(__name__)

# Predefined prompts for image text extraction
PREDEFINED_PROMPTS = {
    "simple": (
        "Extrae únicamente el texto legible que encuentres en esta imagen de documento escaneado. "
        "Ignora por completo cualquier logotipo, gráfico, figura, fotografía, diagrama o elemento visual similar. "
        "No describas la imagen, solo transcribe el texto."
    ),
    "detallado": (
        "Realiza una transcripción OCR precisa del contenido textual de esta imagen. Captura todo el texto visible incluyendo:"
        "- Titulares y subtítulos (respeta mayúsculas/minúsculas)"
        "- Párrafos completos (mantén la estructura original)"
        "- Pies de foto y leyendas (solo el texto)"
        "- Marcadores de viñetas y numeración"
        "- Información en cuadros de texto y destacados"
        "Preserva el formato original en cuanto a:"
        "- Separación entre párrafos"
        "- Estructura de columnas (transcribe de izquierda a derecha, columna por columna)"
        "- Jerarquía visual (títulos, subtítulos, cuerpo)"
        "Excluye elementos no textuales como imágenes, gráficos, bordes decorativos y logotipos."
        "No interpretes, resumas ni reorganices el contenido; transcribe fielmente como aparece."
        "Si algún texto es ilegible o dudoso, indícalo con [ilegible]."
    ),
    "estructurado": (
        "Analiza la estructura de esta página (probablemente un diario o documento similar) y extrae todo el texto de los artículos, titulares y bloques de texto. "
        "Omite deliberadamente cualquier imagen, publicidad gráfica, gráfico estadístico, o logotipo. "
        "Conserva los saltos de párrafo si es posible, pero enfócate en obtener solo el contenido escrito."
    ),
    "anti-ruido": (
        "Transcribe el texto principal de este documento. Presta especial atención a ignorar elementos visuales distractores como manchas, sellos superpuestos (si no son texto claro), firmas (si son ilegibles o puramente gráficas), y cualquier tipo de gráfico o ilustración. "
        "Devuelve solo el texto puro."
    )
}

class ImageTextExtractorAPI:
    def __init__(self, api_key=None, model_name='gemini-1.5-pro-latest', prompt_key='detallado'):
        """
        Inicializa el cliente de API para Gemini.
        
        Args:
            api_key: API key para Gemini (si es None, intentará cargar de .env)
            model_name: Nombre del modelo de Gemini a usar
            prompt_key: Clave del prompt predefinido a usar (detallado por defecto)
        """
        self.model_name = model_name
        
        # Intentar cargar API key si no se proporcionó
        if api_key is None:
            # Buscar archivo .env en varias ubicaciones posibles
            script_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # lib -> codigo
            project_root = os.path.abspath(os.path.join(script_dir, os.pardir))  # codigo -> scr1403
            dotenv_paths = [
                os.path.join(project_root, 'credentials', '.env'),
                os.path.join(project_root, '.env'),
                os.path.join(script_dir, '.env')
            ]
            
            for dotenv_path in dotenv_paths:
                if os.path.exists(dotenv_path):
                    load_dotenv(dotenv_path=dotenv_path)
                    logger.info(f"Cargado archivo .env desde: {dotenv_path}")
                    break
            
            api_key = os.getenv("GOOGLE_API_KEY")
            if not api_key:
                raise ValueError("No se encontró GOOGLE_API_KEY en variables de entorno o .env")
        
        # Configurar Gemini API
        try:
            genai.configure(api_key=api_key)
            logger.info(f"API Gemini configurada con modelo: {model_name}")
        except Exception as e:
            logger.error(f"Error configurando API Gemini: {e}")
            raise ValueError(f"Error al configurar la API de Gemini: {e}")
        
        # Guardar el prompt a usar
        if prompt_key in PREDEFINED_PROMPTS:
            self.prompt = PREDEFINED_PROMPTS[prompt_key]
        else:
            logger.warning(f"Prompt key '{prompt_key}' no encontrada, usando 'detallado'")
            self.prompt = PREDEFINED_PROMPTS["detallado"]

    def extract_text_from_image(self, image_path):
        """
        Envía una imagen a la API de Gemini y extrae el texto.
        
        Args:
            image_path: Ruta al archivo de imagen
            
        Returns:
            dict: Diccionario con los resultados formateados
        """
        if not os.path.exists(image_path):
            logger.error(f"Archivo de imagen no encontrado: {image_path}")
            return {
                "image_filename": os.path.basename(image_path),
                "processed_date": datetime.today().strftime('%d%m%Y'),
                "extracted_text": "",
                "error": "File not found"
            }

        result = {
            "image_filename": os.path.basename(image_path),
            "processed_date": datetime.today().strftime('%d%m%Y'),
            "extracted_text": "",
            "error": None
        }

        try:
            # Verificar la imagen antes de enviarla
            try:
                with Image.open(image_path) as img:
                    img.verify()  # Verificar que la imagen es válida
                img = Image.open(image_path)  # Reabrir para uso
                
                # Verificar tamaño para evitar errores con imágenes muy grandes
                width, height = img.size
                if width * height > 25000000:  # ~25 megapíxeles
                    logger.warning(f"Imagen muy grande ({width}x{height}), redimensionando")
                    ratio = (25000000 / (width * height)) ** 0.5
                    new_width = int(width * ratio)
                    new_height = int(height * ratio)
                    img = img.resize((new_width, new_height), Image.LANCZOS)
                    
                    # Guardar versión redimensionada en carpeta temp si existe
                    temp_dir = os.path.join(os.path.dirname(image_path), "temp")
                    if not os.path.exists(temp_dir):
                        os.makedirs(temp_dir, exist_ok=True)
                    temp_path = os.path.join(temp_dir, f"resized_{os.path.basename(image_path)}")
                    img.save(temp_path)
                    logger.info(f"Imagen redimensionada guardada en: {temp_path}")
                    image_path = temp_path  # Usar la versión redimensionada
            except Exception as img_err:
                logger.warning(f"Error verificando imagen {os.path.basename(image_path)}: {img_err}")
                # Intentaremos procesar de todos modos
            
            # Crear modelo y enviar la solicitud
            logger.debug(f"Enviando imagen {os.path.basename(image_path)} a Gemini API")
            model = genai.GenerativeModel(self.model_name)
            
            # Abrir imagen para enviarla a la API
            img = Image.open(image_path)
            
            # Enviar solicitud a la API con timeout generoso
            response = model.generate_content([self.prompt, img], request_options={'timeout': 180})
            
            # Procesar respuesta
            if response.parts:
                if hasattr(response, 'text') and response.text:
                    result["extracted_text"] = response.text.strip()
                    logger.info(f"Texto extraído de {os.path.basename(image_path)} ({len(result['extracted_text'])} chars).")
                else:
                    logger.warning(f"Respuesta sin texto para {os.path.basename(image_path)}")
                    result["error"] = "No text in response"
            else:
                reason = "Razón desconocida"
                try:
                    if response.prompt_feedback and response.prompt_feedback.block_reason:
                        reason = f"Bloqueado por: {response.prompt_feedback.block_reason}"
                except Exception:
                    pass
                logger.warning(f"Respuesta sin partes de texto. {reason}")
                result["error"] = f"No parts in response: {reason}"

        except Exception as e:
            logger.error(f"Error procesando {os.path.basename(image_path)} con Gemini API: {type(e).__name__} - {e}", exc_info=True)
            result["error"] = f"API error: {type(e).__name__} - {e}"

        return result