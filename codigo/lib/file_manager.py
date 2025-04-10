# codigo/lib/file_manager.py
import os
import json
import pandas as pd
import logging

logger = logging.getLogger(__name__)

def ensure_dir_exists(file_path):
    """Asegura que el directorio para un archivo exista."""
    directory = os.path.dirname(file_path)
    if directory and not os.path.exists(directory):
        try:
            os.makedirs(directory, exist_ok=True)
            logger.debug(f"Directorio creado: {directory}")
        except OSError as e:
            logger.error(f"Error creando directorio {directory}: {e}")
            raise # Relanzar la excepción para manejarla arriba si es necesario

def save_to_csv(data, output_csv_path):
    """Guarda una lista de diccionarios en un archivo CSV."""
    if not data:
        logger.warning(f"No hay datos para guardar en {output_csv_path}")
        return
    try:
        ensure_dir_exists(output_csv_path)
        df = pd.DataFrame(data)
        df.to_csv(output_csv_path, index=False, encoding='utf-8')
        logger.info(f"Datos guardados en CSV: {output_csv_path}")
    except Exception as e:
        logger.error(f"Error al guardar CSV en {output_csv_path}: {e}")

def load_from_csv(input_csv_path):
    """Carga datos desde un archivo CSV a un DataFrame de Pandas."""
    if not os.path.exists(input_csv_path):
        logger.warning(f"Archivo CSV no encontrado: {input_csv_path}")
        return pd.DataFrame() # Retorna DataFrame vacío si no existe
    try:
        df = pd.read_csv(input_csv_path, encoding='utf-8')
        logger.info(f"Datos cargados desde CSV: {input_csv_path}")
        return df
    except Exception as e:
        logger.error(f"Error al cargar CSV desde {input_csv_path}: {e}")
        return pd.DataFrame()

def save_to_json(data, output_json_path, indent=4):
    """Guarda datos (diccionario o lista) en un archivo JSON."""
    try:
        ensure_dir_exists(output_json_path)
        with open(output_json_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=indent)
        logger.info(f"Datos guardados en JSON: {output_json_path}")
    except Exception as e:
        logger.error(f"Error al guardar JSON en {output_json_path}: {e}")

def load_from_json(input_json_path):
    """Carga datos desde un archivo JSON."""
    if not os.path.exists(input_json_path):
        logger.warning(f"Archivo JSON no encontrado: {input_json_path}")
        return None # O {} o [] según el caso de uso esperado
    try:
        with open(input_json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        logger.info(f"Datos cargados desde JSON: {input_json_path}")
        return data
    except json.JSONDecodeError as e:
        logger.error(f"Error de decodificación JSON en {input_json_path}: {e}")
        return None
    except Exception as e:
        logger.error(f"Error al cargar JSON desde {input_json_path}: {e}")
        return None

def save_stats(stats_data, stats_path):
    """Guarda las estadísticas de procesamiento en un archivo JSON."""
    save_to_json(stats_data, stats_path)
    logger.info(f"Estadísticas guardadas: {stats_path}")
    logger.info(f"Contenido estadísticas: {stats_data}")