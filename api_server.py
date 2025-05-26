# importando librerías necesarias
import os
import sys
import csv
import asyncio
from fastapi import FastAPI, UploadFile, WebSocket, File
from fastapi.responses import JSONResponse,FileResponse
from pydantic import BaseModel


project_root = os.path.dirname(os.path.abspath(__file__))
code_dir = os.path.join(project_root, "codigo")
if code_dir not in sys.path:
    sys.path.append(code_dir)

from codigo.notebook_utils import setup_environment
from codigo.main3 import run_pipeline

app = FastAPI(title="NewsAgent API")



#--------ENDPOINTS DE PDF-------------------
#---- upload pdf
@app.post("/upload_pdf/")
async def upload_pdf(file: UploadFile = File(...)):
    """
    Endpoint upload_pdf.
    Sube un archivo PDF al servidor y lo guarda temporalmente.

    Args:
        file (UploadFile): Archivo PDF enviado desde el cliente.

    Returns:
        dict: Un diccionario con el nombre del archivo guardado.
    """
    # Guarda el archivo temporalmente
    file_location = f"base/{file.filename}"
    with open(file_location, "wb") as f:
        f.write(await file.read())
    # Retorna el nombre o un ID para referencia posterior
    return {"filename": file.filename}


#-----process pdf
class ProcesarPDFRequest(BaseModel):
    filename: str
    prompt: str
    batchSize: int
    pauseSeconds: int

@app.post("/procesar_pdf/")
async def procesar_pdf(req: ProcesarPDFRequest):
    """
    Procesa el PDF subido usando los parámetros personalizados y retorna el análisis en JSON.
    """
    # Ruta del archivo PDF subido
    file_path = os.path.join("base", req.filename + ".pdf")
    if not os.path.exists(file_path):
        return JSONResponse(status_code=404, content={"error": "Archivo no encontrado"})

    try:
        # Aquí debes adaptar run_pipeline o tu función orquestadora para aceptar estos parámetros
        setup_environment(project_root)
        resultado = run_pipeline(
            req.filename
        )
        # Si run_pipeline guarda el JSON en disco, puedes cargarlo y devolverlo:
        output_path = os.path.join("output/clean", f"clean_{req.filename}.json")
        if os.path.exists(output_path):
            import json
            with open(output_path, "r", encoding="utf-8") as f:
                resultado_json = json.load(f)
            return JSONResponse(content=resultado_json)
        else:
            # Si run_pipeline retorna el resultado directamente
            return JSONResponse(content=resultado)
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})

#----download markdown file
@app.get("/download_md/{filename}")
async def download_md(filename: str):
    md_path = os.path.join("output/clean", f"clean_{filename}.md")
    if not os.path.exists(md_path):
        return JSONResponse(status_code=404, content={"error": "Archivo .md no encontrado"})
    return FileResponse(md_path, media_type="text/markdown", filename=f"{filename}.md")


#---- send to URLs extracted
import csv

@app.get("/urls_extraidas/{namefile}")
async def urls_extraidas(namefile: str):
    """
    Devuelve la lista de URLs extraídas leyendo el archivo CSV correspondiente.
    La URL está en la segunda columna del CSV.
    """
    csv_path = os.path.join("input", "in", f"links_extracted_{namefile}.csv")
    if not os.path.exists(csv_path):
        return JSONResponse(status_code=404, content={"error": "Archivo CSV no encontrado"})
    try:
        urls = []
        with open(csv_path, newline='', encoding="utf-8") as csvfile:
            reader = csv.reader(csvfile)
            next(reader, None)  # Saltar encabezado
            for row in reader:
                if len(row) > 1 and row[1].startswith("http"):
                    urls.append(row[1])
        return JSONResponse(content={"urls": urls})
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})

#---- checking status of the process of the PDF
@app.websocket("/ws/status/{filename}")
async def websocket_progreso(websocket: WebSocket, filename: str):
    await websocket.accept()
    try:
        for progress, status in [(0, "processing"), (10, "processing"), (40, "processing"), (60, "processing"), (90, "processing"), (100, "done")]:
            await websocket.send_json({"progress": progress, "status": status})
        await websocket.close()
    except Exception as e:
        await websocket.close()

#----agregando CORS para permitir peticiones desde cualquier origen
from fastapi.middleware.cors import CORSMiddleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Cambia esto a los dominios permitidos
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
