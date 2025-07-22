# Limpiador Semántico de Textos

## Descripción

Este módulo proporciona funcionalidades para realizar una limpieza semántica de los textos extraídos de diferentes fuentes (PDF, HTML, imágenes, Facebook) en los archivos JSON consolidados. Su objetivo principal es eliminar la redundancia de información mediante la identificación y fusión de textos similares semánticamente.

## Características

- Detección de similitud textual utilizando técnicas de procesamiento de lenguaje natural (NLP)
- Análisis de similitud semántica entre textos de diferentes fuentes
- Selección de textos representativos basados en relevancia y longitud
- Conversión del JSON limpio a formato Markdown para una mejor legibilidad
- Integración con el flujo de trabajo diario de procesamiento de noticias

## Estructura del Módulo

El módulo consta de los siguientes componentes:

- `text_similarity.py`: Implementa las funcionalidades para analizar la similitud entre textos
- `semantic_cleaner.py`: Clase principal para realizar la limpieza semántica del JSON consolidado
- `markdown_converter.py`: Convierte los datos JSON limpios a formato Markdown
- `run_semantic_cleaner.py`: Script para ejecutar la limpieza semántica desde la línea de comandos

## Requisitos

- Python 3.8 o superior
- Bibliotecas:
  - nltk
  - scikit-learn
  - numpy

## Instalación de Dependencias

Se proporciona un script de configuración que instala todas las dependencias necesarias y descarga los recursos de NLTK:

```bash
python setup_semantic_cleaner.py
```

Alternativamente, puedes instalarlo manualmente:

```bash
pip install nltk scikit-learn numpy
```

Y descargar los recursos de NLTK:

```python
import nltk
nltk.download('punkt')
nltk.download('stopwords')
```

## Uso

### Como Parte del Proceso Diario

El script `clean_news.py` en el directorio principal se ha diseñado para integrarse con el flujo de trabajo diario de procesamiento de noticias:

```bash
python clean_news.py [--date DDMMYYYY] [--threshold 0.7] [--language spanish]
```

Parámetros opcionales:
- `--date`: Fecha de los datos a procesar en formato DDMMYYYY (por defecto: fecha de ayer)
- `--input-dir`: Directorio que contiene los archivos JSON consolidados
- `--output-dir`: Directorio donde se guardarán los archivos de salida
- `--threshold`: Umbral de similitud (entre 0.0 y 1.0, por defecto 0.7)
- `--language`: Idioma para el análisis ('spanish' o 'english', por defecto 'spanish')

### Uso del Módulo Directamente

También se puede utilizar el módulo de limpieza semántica directamente desde código Python:

```python
from lib.semantic_cleaner import SemanticCleaner, MarkdownConverter

# Cargar JSON de entrada
with open('ruta/al/archivo/consolidado.json', 'r', encoding='utf-8') as f:
    json_data = json.load(f)

# Inicializar limpiador semántico
cleaner = SemanticCleaner(similarity_threshold=0.7, language='spanish')

# Realizar limpieza semántica
cleaned_json = cleaner.clean_consolidated_json(json_data)

# Guardar JSON limpio
with open('ruta/al/archivo/limpio.json', 'w', encoding='utf-8') as f:
    json.dump(cleaned_json, f, ensure_ascii=False, indent=2)

# Convertir a Markdown
markdown_converter = MarkdownConverter()
markdown_converter.convert_to_markdown(cleaned_json, 'ruta/al/archivo/limpio.md')
```

## Ajuste de Parámetros

El principal parámetro a ajustar es el umbral de similitud (`similarity_threshold`):

- Valores más altos (cercanos a 1.0) requerirán que los textos sean muy similares para considerarse equivalentes
- Valores más bajos (cercanos a 0.0) permitirán agrupar textos con similitudes más tenues
- El valor recomendado es 0.7, que proporciona un buen equilibrio

## Flujo de Procesamiento

1. Extracción de textos de todas las fuentes (PDF, HTML, imágenes, Facebook)
2. Análisis de similitud y agrupamiento de textos similares
3. Selección del texto más representativo de cada grupo
4. Creación de un nuevo JSON con solo los textos representativos
5. Conversión a formato Markdown para mejor legibilidad

## Salida

El proceso genera dos archivos:

1. Un archivo JSON limpio (`clean_DDMMYYYY.json`)
2. Un archivo Markdown (`clean_DDMMYYYY.md`) con la información estructurada

## Mantenimiento

- Verificar periódicamente la calidad de la limpieza ajustando el umbral de similitud
- Actualizar las bibliotecas de NLP cuando estén disponibles nuevas versiones
- Considerar el entrenamiento de modelos específicos para mejorar la detección de similitud en el dominio de SUNASS
######

revisa C:\Jerson\SUNASS\2025\5_May\NewsAgent
leelo y crea un modulo Continuaré con la implementación del script clean_news.py:
lee codigo\lib\README.md
Ahora, para asegurar que el módulo se integre correctamente con el resto del sistema, vamos a crear un archivo README.md con instrucciones para su uso:

También crearemos un script de demostración para mostrar cómo usar el módulo con un ejemplo específico:

Vamos a asegurarnos de que el directorio de logs exista:

También vamos a crear el directorio de salida para los archivos limpios:

### Enfoque de Limpieza Semántica

1. **Análisis de Similitud Textual**:
   - Utilicé técnicas de procesamiento de lenguaje natural (NLP) para medir la similitud semántica entre textos
   - Implementé el cálculo de similitud coseno con vectorización TF-IDF
   - Apliqué preprocesamiento de texto: tokenización, eliminación de stopwords y stemming

2. **Detección y Agrupamiento de Textos Similares**:
   - Desarrollé un algoritmo para agrupar textos similares basado en un umbral configurable
   - Implementé comparaciones cruzadas entre todas las fuentes de texto

3. **Selección de Textos Representativos**:
   - Creé un mecanismo para seleccionar el texto más representativo de cada grupo
   - Implementé criterios de selección basados en relevancia y longitud del texto

4. **Conversión a Markdown**:
   - Diseñé un convertidor que transforma los datos JSON limpios a un formato Markdown estructurado
   - Incluí secciones para metadatos, estadísticas y cada tipo de contenido

### Componentes Desarrollados

1. **Módulo de Limpieza Semántica** (`lib/semantic_cleaner/`):
   - `text_similarity.py`: Análisis de similitud textual
   - `semantic_cleaner.py`: Limpieza semántica del JSON
   - `markdown_converter.py`: Conversión a formato Markdown
   - `run_semantic_cleaner.py`: Script para ejecución desde línea de comandos
   - `README.md`: Documentación completa del módulo

2. **Scripts de Ejecución**:
   - `clean_news.py`: Script principal para la ejecución diaria
   - `demo_semantic_cleaner.py`: Script de demostración

3. **Estructura de Directorios**:
   - `codigo/lib/semantic_cleaner/`: Módulo de limpieza semántica
   - `codigo/logs/`: Registros de la ejecución
   - `output/clean/`: Archivos limpios generados
   - `output/demo/`: Archivos de demostración

### Características principales

- **Escalabilidad**: El módulo está diseñado para manejar grandes volúmenes de datos
- **Configurabilidad**: Los parámetros clave como el umbral de similitud son ajustables
- **Mantenibilidad**: Código modular con buena documentación y pruebas
- **Integración**: Se integra fácilmente con el flujo de trabajo diario existente
- **Usabilidad**: Proporciona tanto interfaz de línea de comandos como API para uso programático

El módulo ya está listo para ser utilizado en el proceso diario de limpieza de los archivos JSON consolidados. Para ejecutarlo, simplemente se debe correr el script `clean_news.py`, que procesa automáticamente el archivo consolidado más reciente y genera las versiones limpias en JSON y Markdown.