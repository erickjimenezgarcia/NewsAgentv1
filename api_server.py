# importando librerías necesarias
import os
import sys
import csv
import asyncio
from fastapi import FastAPI, UploadFile, WebSocket, File
from fastapi.responses import JSONResponse,FileResponse
from pydantic import BaseModel
from RAG.embedding_open_ia import main as run_embeddings
from RAG.chatbot import responder_chatbot  
from qdrant_client.models import Filter, FieldCondition, MatchValue
from qdrant_client import QdrantClient
from datetime import datetime
from zoneinfo import ZoneInfo
from qdrant_client.http.models import MatchAny

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
        input_dir = "output/clean"
        fechas = [req.filename]
        run_embeddings(input_dir, fechas)        
        
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
        print(e)
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
    csv_path = os.path.join("input", "In", f"links_extracted_{namefile}.csv")
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
    
class ChatRequest(BaseModel):
    pregunta:str
    
@app.post("/chatbot/")
async def preguntar_chatbot(data: ChatRequest):
    respuesta = responder_chatbot(data.pregunta)
    return {"respuesta": respuesta}


@app.get("/resumen_stats/global")
def get_resumen_global():
    qdrant = QdrantClient(url="http://142.93.196.168:6333", timeout=30.0)
    filtro = Filter(
        must=[
            FieldCondition(key="type", match=MatchValue(value="resumen_estadistico"))
        ]
    )
    html_url_filter = Filter(
    must=[
        FieldCondition(key="source_type", match=MatchValue(value="html")),
        FieldCondition(key="chunk_index", match=MatchValue(value=0))
    ]
)
    
    noticias_sunasss = Filter(
        must=[
            FieldCondition(key="source_type", match=MatchValue(value="pdf_paragraph")),
            FieldCondition(key="chunk_index", match=MatchValue(value=0))
        ]
    )
    
    supervision_filter = Filter(
        must=[
            FieldCondition(key="event_type", match=MatchValue(value="supervision")),
            FieldCondition(key="chunk_index", match=MatchValue(value=0))
        ]
    )
    
    interrup_denun_filter = Filter(
        must= [
            FieldCondition(
            key="event_type",
            match=MatchAny(any=["interrupcion", "denuncia"])
        ),
            FieldCondition(key="chunk_index", match=MatchValue(value=0))            
        ]
    )
    
    supervision_data = qdrant.search(
        collection_name="sunass_news_openai",
        query_vector=[0.0] * 1536,
        query_filter=supervision_filter,
        limit=1000,
        with_payload=True,
        with_vectors=False
    )
    
    
    interrup_denun_data = qdrant.search(
        collection_name="sunass_news_openai",
        query_vector=[0.0] * 1536,
        query_filter=interrup_denun_filter,
        limit=1000,
        with_payload=True,
        with_vectors=False
    )
    

    resultados = qdrant.search(
        collection_name="sunass_news_openai",
        query_vector=[0.0] * 1536,
        query_filter=filtro,
        limit=1000,
        with_payload=True,
        with_vectors=False
    )
    
    noticias_pdf_chunks = qdrant.search(
        collection_name="sunass_news_openai",
        query_vector=[0.0] * 1536,
        query_filter=noticias_sunasss,
        limit=1000,
        with_payload=True,
        with_vectors=False
    )
    
    html_chunks  = qdrant.search(
        collection_name="sunass_news_openai",
        query_vector=[0.0] * 1536,
        query_filter=html_url_filter,
        limit=1000,
        with_payload=True,
        with_vectors=False
    )
    
    supervision_data_resumen = []
    vistos_supervision = set()
    for punto in supervision_data:
        payload = punto.payload
        clave = (payload.get("section", ""), payload.get("event_type", ""))
        if clave in vistos_supervision:
            continue
        vistos_supervision.add(clave)
        section = payload.get("section", "")

        if section.lower().endswith(".jpg"):
            titulo = "No se encontró título"
        else:
            titulo = section
        supervision_data_resumen.append({
            "titulo": titulo ,  # el title
            "resumen": payload.get("text", ""), 
            "event_type": payload.get("event_type", ""),
            "pagina": payload.get("page") or 1,
        })
    
    
    interrup_denun_data_resumen = []    
    for punto in interrup_denun_data:
        payload = punto.payload
        interrup_denun_data_resumen.append({
            "titulo": payload.get("section", ""),  # el title
            "resumen": payload.get("text", ""), 
            "pagina": payload.get("page") or 1,
            "event_type": payload.get("event_type", "")
        }) 
    
    noticias_con_resumen = []
    vistos = set()
    for punto in noticias_pdf_chunks:
        payload = punto.payload
        clave = (payload.get("section", ""), payload.get("page", 0))
        if clave in vistos:
            continue
        vistos.add(clave)
        noticias_con_resumen.append({
            "titulo": payload.get("section", ""),  # el title
            "resumen": payload.get("text", ""),     # primer chunk
            "pagina": payload.get("page", 0)
        })
    
    urls_con_resumen = []
    vistos_urls = set()
    for punto in html_chunks:
        payload = punto.payload
        url = payload.get("url")
        if url in vistos_urls:
            continue
        vistos_urls.add(url)
        urls_con_resumen.append({
            "url": payload.get("url"),
            "titulo": payload.get("section", ""),  # el title
            "resumen": payload.get("text", "")     # primer chunk
        })

    totales = {
        "total_urls": 0,
        "html_processed": 0,
        "html_successful": 0,
        "image_attempted": 0,
        "image_downloaded": 0,
        "facebook_extracted": 0,
        "semantic_chunks": 0,
        "archivos_cargados": 0,
        "time_pdf_extraction": 0,
        "time_text_extraction_pdf": 0,
        "time_image_download": 0,
        "time_image_api": 0,
        "time_html_scraping": 0,
        "time_facebook_processing": 0,
        "ultima_actualizacion": "",
        
    }
    
    max_timestamp = None

    for punto in resultados:
        p = punto.payload
        totales["archivos_cargados"] += 1
        totales["total_urls"] += p.get("total_urls", 0)
        totales["html_processed"] += p.get("html_processed", 0)
        totales["html_successful"] += p.get("html_successful", 0)
        totales["image_attempted"] += p.get("image_attempted", 0)
        totales["image_downloaded"] += p.get("image_downloaded", 0)
        totales["facebook_extracted"] += p.get("facebook_extracted", 0)
        totales["semantic_chunks"] += p.get("semantic_chunks", 0)
        totales["time_pdf_extraction"] += p.get("time_pdf_extraction", 0)
        totales["time_text_extraction_pdf"] += p.get("time_text_extraction_pdf", 0)
        totales["time_image_download"] += p.get("time_image_download", 0)
        totales["time_image_api"] += p.get("time_image_api", 0)
        totales["time_html_scraping"] += p.get("time_html_scraping", 0)
        totales["time_facebook_processing"] += p.get("time_facebook_processing", 0)
        
        ts_str = p.get("run_timestamp")
        if ts_str:
            ts = datetime.fromisoformat(ts_str).replace(tzinfo=ZoneInfo("UTC"))
            if not max_timestamp or ts > max_timestamp:
                max_timestamp = ts
        
    archivos_cargados = totales["archivos_cargados"] or 1
    
    totales["time_pdf_extraction"] = round(totales["time_pdf_extraction"] / archivos_cargados, 2)
    totales["time_text_extraction_pdf"] = round(totales["time_text_extraction_pdf"] / archivos_cargados, 2)
    totales["time_image_download"] = round(totales["time_image_download"] / archivos_cargados, 2)
    totales["time_image_api"] = round(totales["time_image_api"] / archivos_cargados, 2)
    totales["time_html_scraping"] = round(totales["time_html_scraping"] / archivos_cargados, 2)
    totales["time_facebook_processing"] = round(totales["time_facebook_processing"] / archivos_cargados, 2)
    
    
    # Convertir a zona Lima y formatear
    if max_timestamp:
        lima_time = max_timestamp.astimezone(ZoneInfo("America/Lima"))
        totales["ultima_actualizacion"] = lima_time.strftime("%Y-%m-%d %H:%M:%S")

    return {"global_stats": totales,"urls_resumen": urls_con_resumen, "noticias_con_resumen": noticias_con_resumen, "interrup_denun_data_resumen": interrup_denun_data_resumen, "supervision_data_resumen": supervision_data_resumen }





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

