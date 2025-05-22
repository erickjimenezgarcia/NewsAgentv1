# NewsAgent

Herramienta para la extracción, procesamiento y análisis de noticias desde PDFs y fuentes en línea.

## Características

- Extracción de enlaces de PDFs
- Clasificación automática de URLs por tipo (HTML, imágenes, redes sociales)
- Descarga y procesamiento de imágenes
- Scraping de contenido HTML
- Captura de publicaciones de Facebook
- Análisis de texto e imágenes mediante IA (Google Gemini)
- Consolidación de resultados

## Requisitos

- Python 3.8 - 3.11
- Dependencias en `requirements.txt`
- Credenciales de API (Google Gemini)

## Configuración

1. Clona el repositorio
2. Instala las dependencias: `pip install -r requirements.txt`
3. Configura las credenciales en la carpeta `credentials`
   - Crea un archivo `api_keys.yaml` con tu clave de Google Gemini

## Estructura de directorios

```
NewsAgent/
├── base/               # PDFs de entrada
├── codigo/             # Código fuente
│   ├── lib/            # Módulos de la biblioteca
│   ├── main.py         # Orquestador principal
│   └── otros scripts
├── input/              # Directorio de entrada de datos
│   ├── Images/         # Imágenes descargadas
│   ├── In/             # Datos de entrada
│   ├── Out/            # Datos procesados
│   ├── Social/         # Enlaces de redes sociales
│   └── Stats/          # Estadísticas
├── logs/               # Registros de ejecución
├── output/             # Resultados consolidados
├── credentials/        # Credenciales (no incluidas en Git)
├── config.yaml         # Configuración
└── NewsAg.ipynb        # Notebook para ejecutar el orquestador
```

## Uso

### Mediante Jupyter Notebook

La forma más sencilla de ejecutar el orquestador es usando el notebook `NewsAg.ipynb`:

1. Abre `NewsAg.ipynb` en Jupyter
2. Ejecuta la primera celda para cargar el script
3. Ejecuta `ejecutar_orquestador("DDMMYYYY")` con la fecha deseada

### Mediante terminal

```bash
cd codigo
python main.py DDMMYYYY
```

## Contribuciones

Las contribuciones son bienvenidas. Para cambios importantes, abre primero un issue para discutir lo que te gustaría cambiar.

## Licencia

[MIT](LICENSE)
