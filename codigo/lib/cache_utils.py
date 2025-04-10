# codigo/lib/cache_utils.py
import os
import json
import hashlib
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

def get_cache_key(input_data):
    """Genera una clave MD5 para una cadena o bytes."""
    if isinstance(input_data, str):
        return hashlib.md5(input_data.encode('utf-8')).hexdigest()
    elif isinstance(input_data, bytes):
        return hashlib.md5(input_data).hexdigest()
    else:
        # Intenta convertir a string si no es str ni bytes
        try:
            return hashlib.md5(str(input_data).encode('utf-8')).hexdigest()
        except Exception as e:
            logger.error(f"No se pudo generar la clave de caché para el tipo {type(input_data)}: {e}")
            raise TypeError("El input para get_cache_key debe ser string o bytes")

def load_from_cache(cache_dir, cache_key, cache_expiry_seconds):
    """
    Carga datos desde un archivo de caché si existe y no ha expirado.
    Retorna el contenido cacheado o None.
    """
    cache_file = os.path.join(cache_dir, f"{cache_key}.json")
    if os.path.exists(cache_file):
        try:
            with open(cache_file, 'r', encoding='utf-8') as f:
                cache_data = json.load(f)
            
            timestamp = cache_data.get('timestamp')
            content = cache_data.get('content')

            if timestamp and content is not None:
                cache_time = datetime.fromtimestamp(timestamp)
                # Usar expiración personalizada si existe, o la predeterminada
                expiry_secs = cache_data.get('expiry_seconds', cache_expiry_seconds)
                if datetime.now() < cache_time + timedelta(seconds=expiry_secs):
                    logger.debug(f"Cache HIT para clave {cache_key} (expiración: {expiry_secs} seg)")
                    return content
                else:
                    logger.debug(f"Cache EXPIRED para clave {cache_key} (expiración: {expiry_secs} seg)")
            else:
                 logger.warning(f"Formato de caché inválido en {cache_file}")

        except json.JSONDecodeError:
            logger.warning(f"Error al decodificar JSON de caché: {cache_file}. Se tratará como MISS.")
        except Exception as e:
            logger.warning(f"Error al cargar caché desde {cache_file}: {e}")
    else:
        logger.debug(f"Cache MISS para clave {cache_key}")
    return None

def save_to_cache(cache_dir, cache_key, content, expiry_seconds=None):
    """Guarda contenido en un archivo de caché con timestamp y opcionalmente una expiración personalizada."""
    # expiry_seconds es opcional y se usa solo para almacenar en metadata
    if not os.path.exists(cache_dir):
        try:
            os.makedirs(cache_dir, exist_ok=True)
            logger.debug(f"Directorio de caché creado: {cache_dir}")
        except OSError as e:
            logger.error(f"Error creando directorio de caché {cache_dir}: {e}")
            return # No intentar guardar si no se puede crear el dir

    cache_file = os.path.join(cache_dir, f"{cache_key}.json")
    cache_data = {
        'timestamp': datetime.now().timestamp(),
        'content': content
    }
    
    # Agregar expiración personalizada si se proporciona
    if expiry_seconds is not None:
        cache_data['expiry_seconds'] = expiry_seconds
        # Calcular y almacenar tiempo de expiración para facilitar la depuración
        expiry_date = datetime.now() + timedelta(seconds=expiry_seconds)
        cache_data['expires_at'] = expiry_date.strftime('%Y-%m-%d %H:%M:%S')
    try:
        with open(cache_file, 'w', encoding='utf-8') as f:
            json.dump(cache_data, f, ensure_ascii=False)
        logger.debug(f"Contenido guardado en caché: {cache_file}")
    except TypeError as e:
         logger.error(f"Error de tipo al serializar contenido para caché (clave {cache_key}): {e}. Contenido: {str(content)[:100]}...")
    except Exception as e:
        logger.warning(f"Error al guardar caché en {cache_file}: {e}")

def clear_cache(cache_dir):
     """Elimina todos los archivos del directorio de caché."""
     if not os.path.isdir(cache_dir):
         logger.warning(f"El directorio de caché {cache_dir} no existe o no es un directorio.")
         return False
     try:
         count = 0
         for filename in os.listdir(cache_dir):
             file_path = os.path.join(cache_dir, filename)
             if os.path.isfile(file_path):
                 os.remove(file_path)
                 count += 1
         logger.info(f"Se eliminaron {count} archivos de caché de {cache_dir}")
         return True
     except Exception as e:
         logger.error(f"Error al limpiar el caché en {cache_dir}: {e}")
         return False