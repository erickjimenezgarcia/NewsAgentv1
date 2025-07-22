"""
Módulo para configurar y gestionar claves API de forma segura.
Permite configurar la API key de Google localmente sin exponerla en el código.
"""

import os
import json
import getpass
from pathlib import Path
from typing import Dict, Optional, Any

# Ruta al archivo de configuración local
CONFIG_DIR = Path(__file__).parent.parent / "credentials"
CONFIG_FILE = CONFIG_DIR / "api_config.json"

def load_api_config() -> Dict[str, Any]:
    """
    Carga la configuración de API desde el archivo local.
    
    Returns:
        Dict con la configuración de API
    """
    if not CONFIG_FILE.exists():
        return {}
    
    try:
        with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"Error cargando configuración API: {e}")
        return {}

def save_api_config(config: Dict[str, Any]) -> bool:
    """
    Guarda la configuración de API en el archivo local.
    
    Args:
        config: Diccionario con la configuración a guardar
        
    Returns:
        True si se guardó correctamente, False en caso contrario
    """
    # Asegurar que el directorio existe
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    
    try:
        with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
            json.dump(config, f, indent=2)
        
        # Establecer permisos restrictivos (solo lectura para el usuario)
        os.chmod(CONFIG_FILE, 0o600)
        return True
    except Exception as e:
        print(f"Error guardando configuración API: {e}")
        return False

def get_api_key(service: str = "google") -> Optional[str]:
    """
    Obtiene la clave API para el servicio especificado.
    Primero intenta obtenerla de variables de entorno, luego del archivo local.
    
    Args:
        service: Nombre del servicio (default: "google")
        
    Returns:
        Clave API o None si no está configurada
    """
    # Primero intentar desde variables de entorno (prioridad más alta)
    env_var = f"{service.upper()}_API_KEY"
    api_key = os.environ.get(env_var)
    
    if api_key:
        return api_key
    
    # Si no está en variables de entorno, intentar desde archivo local
    config = load_api_config()
    return config.get(service, {}).get("api_key")

def configure_api_key(service: str = "google", interactive: bool = True) -> bool:
    """
    Configura la clave API para el servicio especificado.
    
    Args:
        service: Nombre del servicio (default: "google")
        interactive: Si es True, solicita la clave al usuario; si es False,
                     espera que esté en variables de entorno
    
    Returns:
        True si se configuró correctamente, False en caso contrario
    """
    # Cargar configuración actual
    config = load_api_config()
    
    if service not in config:
        config[service] = {}
    
    # Intentar obtener de variables de entorno
    env_var = f"{service.upper()}_API_KEY"
    api_key = os.environ.get(env_var)
    
    # Si no está en variables de entorno y es interactivo, solicitarla
    if not api_key and interactive:
        print(f"\nConfiguración de API key para {service.capitalize()}")
        print("-" * 50)
        print(f"La clave API no está configurada en variables de entorno ({env_var}).")
        api_key = getpass.getpass(f"Ingrese su clave API de {service.capitalize()}: ")
    
    if not api_key:
        print(f"No se pudo obtener la clave API para {service}.")
        return False
    
    # Guardar en configuración
    config[service]["api_key"] = api_key
    
    # Guardar configuración
    success = save_api_config(config)
    
    if success:
        print(f"✅ Clave API de {service.capitalize()} configurada correctamente.")
    else:
        print(f"❌ Error al guardar la clave API de {service.capitalize()}.")
    
    return success

def is_api_configured(service: str = "google") -> bool:
    """
    Verifica si la API está configurada para el servicio especificado.
    
    Args:
        service: Nombre del servicio (default: "google")
        
    Returns:
        True si está configurada, False en caso contrario
    """
    return get_api_key(service) is not None

if __name__ == "__main__":
    # Si se ejecuta directamente, configurar la API key de Google
    configure_api_key("google", interactive=True)
