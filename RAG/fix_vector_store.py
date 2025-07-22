"""
Corregir problemas de tipo de datos en las consultas de vector_store.py
"""

import os
import re
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('FixVectorStore')

def fix_vector_store():
    """Aplicar correcciones a vector_store.py para arreglar problemas de tipo y búsqueda"""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    vector_store_path = os.path.join(script_dir, "vector_store.py")
    
    if not os.path.exists(vector_store_path):
        logger.error(f"No se encontró el archivo {vector_store_path}")
        return False
    
    # Hacer backup
    backup_path = vector_store_path + ".bak"
    with open(vector_store_path, 'r', encoding='utf-8') as f:
        original_content = f.read()
        
    with open(backup_path, 'w', encoding='utf-8') as f:
        f.write(original_content)
    
    logger.info(f"Backup creado en {backup_path}")
    
    # Aplicar correcciones
    modified_content = original_content
    
    # 1. Corregir el cast a vector para que sea compatible con pgvector
    modified_content = modified_content.replace(
        "1 - (embedding <=> %s::vector) AS similarity",
        "1 - (embedding <=> CAST(%s AS vector)) AS similarity"
    )
    
    # 2. Asegurar que todas las consultas SQL usen self.full_table_name
    modified_content = modified_content.replace(
        "FROM {self.table_name}",
        "FROM {self.full_table_name}"
    )
    
    # Guardar cambios
    with open(vector_store_path, 'w', encoding='utf-8') as f:
        f.write(modified_content)
    
    logger.info("Correcciones aplicadas correctamente a vector_store.py")
    logger.info("Ejecuta ahora: python RAG/rag_pipeline.py query \"regulación de agua potable en Piura\" --date 14032025")
    
    return True

if __name__ == "__main__":
    fix_vector_store()
