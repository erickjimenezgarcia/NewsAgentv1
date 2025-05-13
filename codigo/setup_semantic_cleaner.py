#!/usr/bin/env python
"""
Script para instalar y configurar todos los requisitos del limpiador semántico.

Este script instala las bibliotecas necesarias y descarga los recursos de NLTK
requeridos para el funcionamiento del módulo de limpieza semántica.

Desarrollado para: SUNASS
Fecha: Mayo 2025
"""

import os
import sys
import subprocess
import importlib.util

# Lista de paquetes requeridos
REQUIRED_PACKAGES = [
    "nltk",
    "scikit-learn",
    "numpy"
]

def check_package(package_name):
    """
    Verifica si un paquete está instalado.
    
    Args:
        package_name (str): Nombre del paquete
        
    Returns:
        bool: True si está instalado, False en caso contrario
    """
    return importlib.util.find_spec(package_name) is not None

def install_package(package_name):
    """
    Instala un paquete usando pip.
    
    Args:
        package_name (str): Nombre del paquete
        
    Returns:
        bool: True si la instalación fue exitosa, False en caso contrario
    """
    print(f"Instalando {package_name}...")
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", package_name])
        return True
    except subprocess.CalledProcessError:
        print(f"Error al instalar {package_name}")
        return False

def download_nltk_resources():
    """
    Descarga los recursos necesarios de NLTK.
    
    Returns:
        bool: True si la descarga fue exitosa, False en caso contrario
    """
    try:
        import nltk
        
        # Recursos a descargar
        resources = ["punkt", "stopwords"]
        
        for resource in resources:
            print(f"Descargando recurso NLTK: {resource}")
            nltk.download(resource)
            
        return True
    except Exception as e:
        print(f"Error al descargar recursos NLTK: {str(e)}")
        return False

def main():
    """
    Función principal del script.
    """
    print("=== Configuración del Limpiador Semántico ===")
    
    # Verificar e instalar paquetes requeridos
    print("\n1. Verificando e instalando paquetes requeridos...")
    for package in REQUIRED_PACKAGES:
        if check_package(package):
            print(f"  ✓ {package} ya está instalado")
        else:
            if install_package(package):
                print(f"  ✓ {package} instalado correctamente")
            else:
                print(f"  ✕ Error al instalar {package}")
    
    # Descargar recursos NLTK
    print("\n2. Descargando recursos NLTK...")
    if download_nltk_resources():
        print("  ✓ Recursos NLTK descargados correctamente")
    else:
        print("  ✕ Error al descargar recursos NLTK")
    
    print("\n=== Configuración completada ===")
    print("\nAhora puede ejecutar el limpiador semántico:")
    print("  python clean_news.py --date DDMMYYYY")
    print("  python demo_semantic_cleaner.py")

if __name__ == "__main__":
    main()
