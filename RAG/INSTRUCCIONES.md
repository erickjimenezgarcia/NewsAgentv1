# Sistema RAG para SUNASS News Agent

Este directorio contiene los scripts necesarios para implementar un sistema RAG (Retrieval Augmented Generation) sobre los datos procesados por el sistema SUNASS News Agent.

## Estructura del Proyecto

- `clean_data.py`: Script para limpiar los datos de los archivos JSON generados por el sistema.
- `embedding_generator.py`: Script para generar embeddings a partir de los datos limpios.
- `app.py`: Aplicación web para consultar el sistema RAG.

## Flujo de Trabajo

El proceso completo para implementar el sistema RAG consta de tres pasos:

1. **Limpieza de datos**: Elimina contenido no relevante de los archivos JSON.
2. **Generación de embeddings**: Realiza chunking de los textos y genera embeddings.
3. **Consulta**: Interfaz web para realizar consultas sobre los datos procesados.

## Requisitos

- Python 3.8 o superior
- Bibliotecas: sentence-transformers, qdrant-client, streamlit, pandas

## Instalación

```bash
pip install sentence-transformers qdrant-client streamlit pandas
```

## Uso

### 1. Limpieza de Datos

```bash
python clean_data.py --input "../output/clean" --output "./data"
```

Opciones:
- `--input` o `-i`: Directorio donde se encuentran los archivos JSON a limpiar (por defecto: "../output/clean")
- `--output` o `-o`: Directorio donde se guardarán los archivos limpios (por defecto: "./data")

### 2. Generación de Embeddings

```bash
python embedding_generator.py --input "./data" --output "./embeddings" --model "paraphrase-multilingual-MiniLM-L12-v2" --collection "sunass_news"
```

Opciones:
- `--input` o `-i`: Directorio donde se encuentran los archivos JSON limpios (por defecto: "./data")
- `--output` o `-o`: Directorio donde se guardarán los embeddings (por defecto: "./embeddings")
- `--model` o `-m`: Modelo de SentenceTransformers a utilizar (por defecto: "paraphrase-multilingual-MiniLM-L12-v2")
- `--collection` o `-c`: Nombre de la colección de Qdrant (por defecto: "sunass_news")

### 3. Ejecución de la Aplicación Web

```bash
streamlit run app.py
```

La aplicación web se abrirá automáticamente en tu navegador predeterminado.

## Consideraciones Importantes

- Los archivos JSON de entrada deben tener la estructura generada por el sistema SUNASS News Agent.
- El script de limpieza elimina contenido no relevante como "Iniciar sesión", "Me gusta", "Comentar", etc.
- El chunking se realiza respetando las oraciones para mantener la coherencia del texto.
- La base de datos vectorial (Qdrant) se almacena localmente en el directorio de embeddings.

## Personalización

- Puedes ajustar el tamaño de los chunks y el solapamiento en el archivo `embedding_generator.py`.
- Para usar un modelo de embeddings diferente, especifica el nombre del modelo en el parámetro `--model`.
- La interfaz web permite filtrar por fecha y ajustar el número de resultados.

## Solución de Problemas

- Si encuentras problemas con la instalación de dependencias, intenta instalarlas manualmente:
  ```bash
  pip install sentence-transformers qdrant-client streamlit pandas
  ```

- Si la aplicación web no encuentra la base de datos Qdrant, asegúrate de haber ejecutado correctamente el script `embedding_generator.py`.

- Para problemas con la memoria durante la generación de embeddings, reduce el tamaño del lote (`batch_size`) en el archivo `embedding_generator.py`.
