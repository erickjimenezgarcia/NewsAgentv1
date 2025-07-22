"""
Script integral para resolver los problemas del sistema RAG.
1. Verifica y crea el esquema y tablas necesarios
2. Corrige el método de procesamiento de chunks
3. Adapta la tabla para manejar tanto 'text' como 'content'
"""

import os
import sys
import json
import yaml
import logging
import psycopg2
from psycopg2.extras import execute_values
from typing import Dict, Any, List, Optional

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('fix_rag')

def load_config(config_path: str = 'config.yaml') -> Dict[str, Any]:
    """Carga la configuración desde el archivo YAML."""
    # Determinar ruta absoluta si es relativa
    if not os.path.isabs(config_path):
        base_dir = os.path.dirname(os.path.abspath(__file__))
        config_path = os.path.join(base_dir, config_path)
        
    with open(config_path, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    
    return config

def get_connection_params(config: Dict[str, Any] = None) -> Dict[str, str]:
    """Obtiene los parámetros de conexión a la base de datos."""
    if config is None:
        config = load_config()
    
    connection_params = config.get('vector_store', {}).get('connection', {})
    
    return {
        "host": connection_params.get('host', 'localhost'),
        "port": connection_params.get('port', 5432),
        "dbname": connection_params.get('database', 'newsagent'),
        "user": connection_params.get('user', 'postgres'),
        "password": connection_params.get('password', 'postgres')
    }

def get_connection():
    """Crea una conexión a la base de datos."""
    conn_params = get_connection_params()
    return psycopg2.connect(**conn_params)

def fix_database_schema():
    """Crea o modifica el esquema y tabla para el sistema RAG."""
    try:
        conn = get_connection()
        conn.autocommit = True
        
        try:
            with conn.cursor() as cur:
                # 1. Crear esquema rag si no existe
                cur.execute("CREATE SCHEMA IF NOT EXISTS rag")
                logger.info("Esquema 'rag' creado o ya existente")
                
                # 2. Verificar si pgvector está instalado
                cur.execute("CREATE EXTENSION IF NOT EXISTS vector")
                logger.info("Extensión pgvector creada o ya existente")
                
                # 3. Verificar si la tabla existe y modificarla si es necesario
                cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'rag' AND table_name = 'chunks'
                )
                """)
                table_exists = cur.fetchone()[0]
                
                if table_exists:
                    # Verificar si la columna 'text' existe
                    cur.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.columns 
                        WHERE table_schema = 'rag' AND table_name = 'chunks' AND column_name = 'text'
                    )
                    """)
                    text_column_exists = cur.fetchone()[0]
                    
                    if not text_column_exists:
                        # Añadir columna 'text' y copiar los datos de 'content'
                        logger.info("Añadiendo columna 'text' a la tabla 'rag.chunks'")
                        cur.execute("""
                        ALTER TABLE rag.chunks ADD COLUMN text TEXT;
                        UPDATE rag.chunks SET text = content WHERE text IS NULL;
                        """)
                else:
                    # Crear la tabla desde cero con todas las columnas necesarias
                    logger.info("Creando tabla 'rag.chunks'")
                    cur.execute("""
                    CREATE TABLE rag.chunks (
                        id SERIAL PRIMARY KEY,
                        chunk_id TEXT UNIQUE NOT NULL,
                        text TEXT NOT NULL,
                        content TEXT NOT NULL,
                        embedding vector(768),
                        metadata JSONB,
                        source TEXT,
                        url TEXT,
                        title TEXT,
                        date TEXT,
                        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                    )
                    """)
                
                # 4. Crear índices necesarios
                logger.info("Creando índices para la tabla 'rag.chunks'")
                cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_chunks_chunk_id ON rag.chunks(chunk_id);
                CREATE INDEX IF NOT EXISTS idx_chunks_source ON rag.chunks(source);
                CREATE INDEX IF NOT EXISTS idx_chunks_date ON rag.chunks(date);
                CREATE INDEX IF NOT EXISTS idx_chunks_text_gin ON rag.chunks USING GIN (to_tsvector('spanish', text));
                """)
                
                # 5. Crear índice vectorial si no existe
                try:
                    cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_chunks_embedding ON rag.chunks 
                    USING hnsw (embedding vector_l2_ops)
                    WITH (
                        ef_construction=128,
                        m=16
                    );
                    """)
                    logger.info("Índice vectorial creado o ya existente")
                except Exception as e:
                    logger.warning(f"No se pudo crear el índice vectorial: {e}")
                    logger.warning("Esto es normal si aún no hay datos o si el índice ya existe")
                
                # 6. Verificar si hay datos en la tabla
                cur.execute("SELECT COUNT(*) FROM rag.chunks")
                count = cur.fetchone()[0]
                logger.info(f"La tabla 'rag.chunks' contiene {count} registros")
                
                return True
                
        except Exception as e:
            conn.rollback()
            logger.error(f"Error creando esquema y tabla: {e}")
            return False
        finally:
            conn.close()
    except Exception as e:
        logger.error(f"Error conectando a la base de datos: {e}")
        return False

def update_vector_store_file():
    """Actualiza el archivo vector_store.py para corregir los problemas detectados."""
    file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "vector_store.py")
    
    if not os.path.exists(file_path):
        logger.error(f"No se encontró el archivo {file_path}")
        return False
    
    # Crear backup
    backup_path = file_path + ".bak"
    with open(file_path, 'r', encoding='utf-8') as f:
        original_content = f.read()
    
    with open(backup_path, 'w', encoding='utf-8') as f:
        f.write(original_content)
    
    logger.info(f"Se creó un backup en {backup_path}")
    
    # Buscar y modificar el método _process_batch
    fixed = False
    with open(file_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    
    new_lines = []
    in_process_batch = False
    in_values = False
    
    for line in lines:
        if "def _process_batch" in line:
            in_process_batch = True
            new_lines.append(line)
        elif in_process_batch and "unique_chunks = {}" in line:
            # Reemplazar la forma de manejar chunks duplicados
            new_lines.append("        unique_chunks = {}\n")
            new_lines.append("        for chunk in batch:\n")
            new_lines.append("            chunk_id = chunk.get('chunk_id')\n")
            new_lines.append("            if chunk_id:\n")
            new_lines.append("                # Si ya existe, loguear la advertencia\n")
            new_lines.append("                if chunk_id in unique_chunks:\n")
            new_lines.append("                    logger.warning(f\"ID duplicado encontrado: {chunk_id}. Se usará la última ocurrencia.\")\n")
            new_lines.append("                unique_chunks[chunk_id] = chunk\n")
            new_lines.append("        deduplicated_batch = list(unique_chunks.values())\n")
            new_lines.append("\n")
            new_lines.append("        if len(deduplicated_batch) < len(batch):\n")
            new_lines.append("            logger.warning(f\"Se encontraron {len(batch) - len(deduplicated_batch)} chunks con IDs duplicados. Solo se conserva la última ocurrencia de cada ID\")\n")
            fixed = True
            # Saltar las siguientes líneas hasta después de deduplicated_batch
            in_process_batch = False  # Reiniciamos para no duplicar
            continue
        elif "chunk.get('content', '')" in line:
            # Cambiar 'content' por 'text'
            line = line.replace("chunk.get('content', '')", "text")
            fixed = True
        elif "values.append" in line:
            in_values = True
            new_lines.append("                    # Preparar valores para inserción\n")
            new_lines.append("                    values = []\n")
            new_lines.append("                    skipped = 0\n")
            new_lines.append("                    for chunk in deduplicated_batch:\n")
            new_lines.append("                        # Verificar que tenga texto\n")
            new_lines.append("                        text = chunk.get('text', '')\n")
            new_lines.append("                        if not text:\n")
            new_lines.append("                            logger.warning(f\"Chunk {chunk.get('chunk_id', 'unknown')} no tiene texto. Será omitido.\")\n")
            new_lines.append("                            skipped += 1\n")
            new_lines.append("                            continue\n")
            new_lines.append("                            \n")
            new_lines.append("                        # Metadatos a JSON\n")
            new_lines.append("                        metadata_json = json.dumps(chunk.get('metadata', {}))\n")
            new_lines.append("                        \n")
            new_lines.append("                        # Preparar valores - usar 'text' como campo principal\n")
            new_lines.append("                        # y también duplicarlo en 'content' para mantener compatibilidad\n")
            new_lines.append("                        values.append((\n")
            new_lines.append("                            chunk.get('chunk_id', ''),\n")
            new_lines.append("                            text,  # Campo text (correcto)\n")
            new_lines.append("                            text,  # Duplicamos en content para compatibilidad\n")
            new_lines.append("                            self._vector_to_pg_array(chunk.get('embedding', [])),\n")
            new_lines.append("                            metadata_json,\n")
            new_lines.append("                            chunk.get('metadata', {}).get('source', ''),\n")
            new_lines.append("                            chunk.get('metadata', {}).get('url', ''),\n")
            new_lines.append("                            chunk.get('metadata', {}).get('title', ''),\n")
            new_lines.append("                            chunk.get('metadata', {}).get('date', '')\n")
            new_lines.append("                        ))\n")
            new_lines.append("\n")
            new_lines.append("                    if skipped > 0:\n")
            new_lines.append("                        logger.warning(f\"Se omitieron {skipped} chunks sin texto\")\n")
            new_lines.append("                        \n")
            new_lines.append("                    if not values:\n")
            new_lines.append("                        logger.warning(\"No hay valores válidos para insertar\")\n")
            new_lines.append("                        return 0\n")
            fixed = True
            # Saltar hasta la consulta SQL
            continue
        elif "(chunk_id, content, embedding" in line:
            # Modificar la consulta SQL para incluir text
            line = line.replace("(chunk_id, content, embedding", "(chunk_id, text, content, embedding")
            fixed = True
        elif "content = EXCLUDED.content" in line:
            # Añadir actualización de text también
            new_lines.append("                            text = EXCLUDED.text,\n")
            new_lines.append(line)
            fixed = True
            continue
        elif "Error procesando batch" in line:
            # Añadir stack trace completo
            line = line.replace("Error procesando batch: {e}", "Error procesando batch: {e}\", exc_info=True")
            fixed = True
        
        if in_values and ")" in line and not any(x in line for x in ["metadata", "title", "date"]):
            # Estamos aún en la definición de values.append, saltarla
            continue
        
        if in_values and "])" in line:
            in_values = False
        
        new_lines.append(line)
    
    if fixed:
        with open(file_path, 'w', encoding='utf-8') as f:
            f.writelines(new_lines)
        logger.info(f"Se ha actualizado el archivo {file_path}")
        return True
    else:
        logger.warning("No se encontraron secciones a modificar en el archivo")
        return False

def check_chunker():
    """Verifica el chunker.py para asegurar que los IDs sean únicos."""
    file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "chunker.py")
    
    if not os.path.exists(file_path):
        logger.error(f"No se encontró el archivo {file_path}")
        return False
    
    # Buscar y modificar la generación de chunk_id
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Verificar si ya usa UUID
    if "import uuid" in content and "uuid.uuid4()" in content:
        logger.info("chunker.py ya utiliza UUIDs para generar IDs únicos")
        return True
    
    # Agregar generación de UUID
    modified = content.replace(
        "import json",
        "import json\nimport uuid"
    )
    
    modified = modified.replace(
        "chunk_id = f\"{metadata['source']}_{int(time.time())}_{i}\"",
        "chunk_id = f\"{metadata['source']}_{int(time.time())}_{i}_{uuid.uuid4().hex[:8]}\"  # Añadimos componente aleatorio para garantizar unicidad"
    )
    
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(modified)
    
    logger.info(f"Se ha actualizado el archivo {file_path} para generar IDs más únicos")
    return True

def run():
    """Ejecuta todas las correcciones."""
    print("=" * 80)
    print(" CORRIGIENDO SISTEMA RAG ".center(80, "="))
    print("=" * 80)
    
    print("\n1. Creando/actualizando esquema y tabla en la base de datos...")
    db_fixed = fix_database_schema()
    
    print("\n2. Actualizando vector_store.py para manejar correctamente 'text' y 'content'...")
    vs_fixed = update_vector_store_file()
    
    print("\n3. Mejorando generación de IDs en chunker.py...")
    chunker_fixed = check_chunker()
    
    print("\n" + "=" * 80)
    print(" RESULTADOS ".center(80, "="))
    print("=" * 80)
    
    if db_fixed and vs_fixed and chunker_fixed:
        print("\n✅ ¡Todas las correcciones han sido aplicadas correctamente!")
        print("\nAhora puedes procesar documentos diariamente sin problemas de persistencia.")
        print("Para probar el sistema, ejecuta:")
        print("\n1. Procesar documentos: python RAG/process_clean_data.py DDMMYYYY")
        print("2. Realizar consultas: python RAG/rag_pipeline.py query \"tu consulta aquí\"")
        return 0
    else:
        print("\n⚠️ Algunas correcciones no pudieron aplicarse:")
        print(f"- Base de datos: {'✅ Corregido' if db_fixed else '❌ Error'}")
        print(f"- vector_store.py: {'✅ Corregido' if vs_fixed else '❌ Error'}")
        print(f"- chunker.py: {'✅ Corregido' if chunker_fixed else '❌ Error'}")
        print("\nRevisa los logs para más detalles.")
        return 1

if __name__ == "__main__":
    sys.exit(run())
