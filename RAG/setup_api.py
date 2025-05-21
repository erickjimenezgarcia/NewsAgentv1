#!/usr/bin/env python
"""
Script para configurar la clave API de Google de forma interactiva.
Ejecutar este script antes de usar el sistema RAG.
"""

import os
import sys
import argparse
from pathlib import Path

# Añadir directorio raíz al path para importar módulos
sys.path.append(str(Path(__file__).parent.parent))

# Importar desde el mismo directorio
from config_api import configure_api_key, is_api_configured, get_api_key

def main():
    """Función principal para configurar la API key desde línea de comandos."""
    parser = argparse.ArgumentParser(description='Configurar clave API para el sistema RAG')
    
    parser.add_argument(
        '--service',
        default='google',
        choices=['google'],
        help='Servicio para el que configurar la API key (default: google)'
    )
    
    parser.add_argument(
        '--key',
        help='Clave API (si no se proporciona, se solicitará de forma interactiva)'
    )
    
    parser.add_argument(
        '--check',
        action='store_true',
        help='Solo verificar si la API key está configurada'
    )
    
    # Parsear argumentos
    args = parser.parse_args()
    
    # Si solo se quiere verificar
    if args.check:
        if is_api_configured(args.service):
            print(f"✅ La clave API de {args.service.capitalize()} está configurada.")
            # Mostrar los primeros y últimos 4 caracteres de la clave para verificación
            key = get_api_key(args.service)
            if key:
                masked_key = f"{key[:4]}...{key[-4:]}" if len(key) > 8 else "****"
                print(f"   Clave configurada: {masked_key}")
            return 0
        else:
            print(f"❌ La clave API de {args.service.capitalize()} NO está configurada.")
            print(f"   Ejecute: python RAG/setup_api.py --service {args.service}")
            return 1
    
    # Si se proporciona la clave como argumento
    if args.key:
        os.environ[f"{args.service.upper()}_API_KEY"] = args.key
        interactive = False
    else:
        interactive = True
    
    # Configurar la clave
    success = configure_api_key(args.service, interactive)
    
    if success:
        print("\nPara usar la clave API en sus scripts:")
        print("```python")
        print("from RAG.config_api import get_api_key")
        print("api_key = get_api_key('google')  # Reemplazar 'google' por el servicio deseado")
        print("```")
        return 0
    else:
        return 1

if __name__ == "__main__":
    sys.exit(main())
