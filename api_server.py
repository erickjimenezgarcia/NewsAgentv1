# importando librerías necesarias
import json
import os
import re
import unicodedata
import sys
from collections import defaultdict
import csv
from enum import Enum, auto
import re, unicodedata
import asyncio
from fastapi import FastAPI, UploadFile, WebSocket, File
from fastapi.responses import JSONResponse,FileResponse, StreamingResponse
from pydantic import BaseModel
from RAG.embedding_open_ia import main as run_embeddings
from RAG.chatbot import buscar_por_ventanas, clasificar_tipo_pregunta, deduplicar_payloads, extraer_fecha, extraer_tipo_evento, filtrar_eventos_por_similitud, get_event_type_embeddings, pregunta_es_conteo, puntuar_chunks, responder_chatbot, buscar_contexto_openai, responder_llm, responder_llm_gemini, responder_llm_groq, split_in_windows  
from qdrant_client.models import Filter, FieldCondition, MatchValue
from qdrant_client import QdrantClient
from datetime import datetime
from zoneinfo import ZoneInfo
from qdrant_client.http.models import MatchAny
from math import ceil
from RAG.utils import load_openai_api_key
from openai import OpenAI

os.environ["OPENAI_API_KEY"] = load_openai_api_key()

MODEL_EMBEDDING = "text-embedding-3-small"
COLLECTION_NAME = "sunass_news_openai"
client = OpenAI(api_key=load_openai_api_key())  # reemplaza con tu API key
qdrant = QdrantClient( url="http://142.93.196.168:6333",)

project_root = os.path.dirname(os.path.abspath(__file__))
code_dir = os.path.join(project_root, "codigo")
if code_dir not in sys.path:
    sys.path.append(code_dir)

from codigo.notebook_utils import setup_environment
from codigo.main3 import run_pipeline

app = FastAPI(title="NewsAgent API")

class Intent(Enum):
    SMALLTALK = auto()
    PRESENTACION = auto()
    AYUDA = auto()
    COUNT_QUERY = auto()
    DATE_QUERY = auto()
    STATS_QUERY = auto()
    DOMAIN_QUERY = auto()      # Pregunta de negocio general
    OTHER = auto()
    
    
_LEET_MAP = str.maketrans({
    "0": "o","1": "i","3": "e","4": "a","@":"a","$":"s","5":"s","7":"t",
    "¡":"", "!":"", "?":"", "¿":""
})

SMALLTALK = {
    "hola","holi","holaa","ola","buenas","buenos dias","buenas tardes","buenas noches",
    "hey","hi","hello","que tal","qué tal","gracias","de nada","saludos"
}

PRESENTACION = {
    "presentate","presentate por favor","preséntate","quien eres","quién eres",
    "como te llamas","cómo te llamas","cual es tu nombre","cuál es tu nombre"
}

PRESENTACION_PATTERNS = [
    re.compile(r"\bpres[eé]ntate\b"),
    re.compile(r"\bpres[eé]ntese\b"),
    re.compile(r"\bqu[ií]en eres\b"),
    re.compile(r"\bc[oó]mo te llamas\b"),
    re.compile(r"\bcu[aá]l es tu nombre\b"),
    re.compile(r"\b(puedes|podrias|podr[ií]as|me puedes|me podr[ií]as)\s+present(ar(se|te)?|arte|arse)\b"),
    re.compile(r"\bte\s+puedes\s+present(ar(te)?)\b"),
]

HELP_TERMS = {"ayuda","help","como funciona","cómo funciona","que puedes hacer","qué puedes hacer"}

COUNT_TERMS = {"cuantas","cuántas","cuantos","cuántos","numero de","número de","conteo","total","cantidad"}
DATE_TERMS  = {"fecha","fechas","hoy","ayer","manana","mañana","semana","mes","periodo","período","rango"}
STATS_TERMS = {"promedio","tendencia","variacion","variación","porcentaje","proyeccion","proyección"}


DOMAIN_HINTS = {
    "sunass","eps","interrupcion","interrupciones","corte","cortes","abastecimiento",
    "servicio de agua","suspension","suspensión","alerta","reporte","incidencia"
}

def _is_presentacion(text: str) -> bool:
    # text debe venir ya normalizado por _normalize()
    for pat in PRESENTACION_PATTERNS:
        if pat.search(text):
            return True
    # fallback por si llega literal
    return any(t in text for t in ("presentate","presentese","presentarte","presentarse"))

def _normalize(s: str) -> str:
    s = s.lower()
    s = "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")
    s = s.translate(_LEET_MAP)
    # colapsa repeticiones largas: holaaaa -> holaa
    s = re.sub(r"(.)\1{2,}", r"\1\1", s)
    s = re.sub(r"[^a-z0-9ñ ]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def _contains_any(text: str, vocab: set[str]) -> bool:
    for t in vocab:
        if t in text:
            return True
    return False

def intent_router(pregunta: str) -> tuple[Intent, dict]:
    p = _normalize(pregunta or "")
    tokens = p.split()

    # 1) Smalltalk ultra-rápido (mensajes cortos y en set)
    if (p in SMALLTALK) or (len(tokens) <= 3 and _contains_any(p, SMALLTALK)):
        return Intent.SMALLTALK, dict(should_retrieve=False, should_partition=False)

    # 2) Presentación  ✅ (ahora con patrones)
    if _is_presentacion(p):
        return Intent.PRESENTACION, dict(should_retrieve=False, should_partition=False)

    # 3) AYUDA
    if _contains_any(p, HELP_TERMS):
        return Intent.AYUDA, dict(should_retrieve=False, should_partition=False)

    # 4) Intentos especializados
    if _contains_any(p, COUNT_TERMS):
        return Intent.COUNT_QUERY, dict(should_retrieve=True, should_partition=False)
    if _contains_any(p, DATE_TERMS):
        return Intent.DATE_QUERY, dict(should_retrieve=True, should_partition=False)
    if _contains_any(p, STATS_TERMS):
        return Intent.STATS_QUERY, dict(should_retrieve=True, should_partition=False)

    # 5) Dominio SUNASS
    if _contains_any(p, DOMAIN_HINTS):
        return Intent.DOMAIN_QUERY, dict(should_retrieve=True, should_partition=True)

    # 6) Heurística de longitud
    if len(tokens) <= 3:
        return Intent.OTHER, dict(should_retrieve=False, should_partition=False)

    return Intent.DOMAIN_QUERY, dict(should_retrieve=True, should_partition=False)




def _norm(s: str) -> str:
    if not s:
        return ""
    s = s.strip().lower()
    s = "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")
    return re.sub(r"\s+", " ", s)


SMALLTALK_PATTERNS = [
    r"^hola(?:\s+que tal)?$",
    r"^buen[oa]s(?:\s+dias|\s+tardes|\s+noches)?$",
    r"^gracias(?:\s+muchas|\s+por.*)?$",
    r"^muchas\s+gracias$",
    r"^ok$",
    r"^listo$",
    r"^entendido$",
    r"^de acuerdo$",
    r"^presentate$",
    r"^quien eres\??$",
    r"^qu[ée]\s+puedes\s+hacer\??$",
    r"^ayuda$",
    r"^thanks?$",
    r"^hello$",
]


_LEET_MAP = str.maketrans({
    "0": "o",
    "1": "i",  # también podría mapear a 'l'
    "3": "e",
    "4": "a",
    "@": "a",
    "$": "s",
    "5": "s",
    "7": "t",
    "¡": "",
    "!": "",
    "?": "",
    "¿": "",
})

_SMALLTALK_TERMS = {
    "hola", "holi", "holaa", "ola", "buenas",
    "buenos dias", "buenas tardes", "buenas noches",
    "que tal", "qué tal", "como estas", "cómo estas", "como esta", "cómo esta",
    "gracias", "muchas gracias", "de nada",
    "hey", "hi", "hello", "sup"
}

_MIN_LEN = 2
_MAX_LEN_QUICK = 5

MIN_PAYLOADS_TO_PARTITION = 7

def normalize_text(s: str) -> str:
    # 1) a minúsculas
    s = s.lower()
    # 2) quitar acentos/diacríticos
    s = "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")
    # 3) mapear leetspeak y quitar signos comunes
    s = s.translate(_LEET_MAP)
    # 4) colapsar repeticiones (holaaa -> hola)
    s = re.sub(r"(.)\1{2,}", r"\1\1", s)  # deja como máximo 2 repeticiones
    # 5) reemplazar cualquier separador/no alfabético por un espacio
    s = re.sub(r"[^a-zA-Záéíóúñ]+", " ", s)  # tras quitar diacríticos ya no quedan tildes, pero por si acaso
    # 6) espacios limpios
    s = re.sub(r"\s+", " ", s).strip()
    return s


def es_smalltalk(pregunta: str) -> bool:
    
    if not pregunta or not pregunta.strip():
        return False
    p = normalize_text(pregunta)
    
    t = _norm(pregunta)
    
    # Coincidencia exacta corta
    
    if _MIN_LEN <= len(p) <= _MAX_LEN_QUICK and p in _SMALLTALK_TERMS:
        return True

    # Coincidencia por frases (e.g., "buenos dias", "buenas tardes")
    if p in _SMALLTALK_TERMS:
        return True

    # Heurística: si todo son palabras muy cortas y todas están en el set
    tokens = p.split()
    if tokens and all(t in _SMALLTALK_TERMS for t in tokens):
        return True

    # Heurística flexible: saludo + algo muy corto (e.g., "hola!", "holaa que tal")
    for term in _SMALLTALK_TERMS:
        if p.startswith(term) and len(p) <= len(term) + 5:
            return True
    
    if not t:
        return False
    # muy corto y sin verbos “informativos”
    if len(t.split()) <= 3 and not re.search(r"(cu[aá]nt|c[oó]mo|qu[eé]|por qu[eé]|d[oó]nde|cu[aá]l|cuando)", t):
        # si coincide saludos o cierres comunes
        for pat in SMALLTALK_PATTERNS:
            if re.fullmatch(pat, t):
                return True
    # patrones específicos
    for pat in SMALLTALK_PATTERNS:
        if re.fullmatch(pat, t):
            return True
    return False



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
    Procesa el PDF subido usando los parámetros personalizados y retorna el análisis en JSON, Además se añadió la funcionalidad de hacer el embeding una vez termindo el proceso del pipeline.
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
    ultimo_mensaje_bot: str = ""
    
@app.post("/chatbot/")
async def preguntar_chatbot(data: ChatRequest):
    """
    Endpoint encargado de hacer las consultas al chatbot
    """
    respuesta = responder_chatbot(data.pregunta)
    return {"respuesta": respuesta}



from collections import defaultdict
import asyncio
import json
from datetime import datetime, timedelta
from dataclasses import dataclass


@dataclass
class IntentDecision:
    intent: Intent
    should_retrieve: bool
    should_partition: bool
    score: float  # confianza


def stream_final(texto: str):
    async def gen():
        # opcional: avisa "escribiendo" para que el frontend quite el placeholder de forma uniforme
        yield json.dumps({"rol": "bot", "tipo": "escribiendo"}) + "\n"
        # si quieres un micro-delay visual
        await asyncio.sleep(0.05)
        # mensaje final
        yield json.dumps({"rol": "bot", "tipo": "final", "texto": texto}) + "\n"
    return StreamingResponse(gen(), media_type="text/event-stream")


@app.post("/chatbot/stream/")
async def preguntar_chatbot_stream(data: ChatRequest):
    def agrupar_payloads_por_dia(payloads):
        agrupados = defaultdict(list)
        for p in payloads:
            dia = p.get("date_day") or p.get("date")
            if dia:
                agrupados[dia].append(p)
        return agrupados

    def seleccionar_payloads_balanceados(payloads, max_total=21):
        por_dia = agrupar_payloads_por_dia(payloads)
        seleccionados = []
        for dia in sorted(por_dia.keys()):
            for p in por_dia[dia]:
                if len(seleccionados) >= max_total:
                    return seleccionados
                seleccionados.append(p)
        return seleccionados

    # ===== Router de intención: decide si hacemos RAG/partición =====
    pregunta = data.pregunta
    intent, flags = intent_router(pregunta)
    should_retrieve = flags["should_retrieve"]
    should_partition = flags["should_partition"]

    # --- Respuestas directas (sin RAG ni particionado) ---
    # --- Respuestas directas (sin RAG ni particionado) ---
    if intent == Intent.SMALLTALK:
        return stream_final("¡Hola! Soy tu asistente de SUNASS. ¿En qué te ayudo?")

    if intent == Intent.PRESENTACION:
        return stream_final("Soy un asistente de SUNASS. Puedo responder sobre interrupciones, fechas y conteos.")

    if intent == Intent.AYUDA:
        return stream_final("Puedes preguntarme: '¿cuántas interrupciones hubo en julio 2025?' o 'eventos del 12-08-2025'.")

    # Si NO toca RAG, responde genérico sin retrieval ni partición
    if not should_retrieve:
        return stream_final("¿Podrías detallar un poco más para buscar en los reportes?")


    # ===== A partir de aquí, tu flujo existente =====
    ref_embeddings = get_event_type_embeddings()

    # --- NUEVO: detecta si es RANGO para usar el flujo por ventanas ---
    fecha_detectada = extraer_fecha(pregunta)
    es_rango = bool(fecha_detectada and fecha_detectada.get("tipo") == "rango")

    # Si NO es rango, dejamos TODO tu flujo tal cual
    if not es_rango:
        contexto_completo, payloads = buscar_contexto_openai(pregunta)

        # 🔍 Lógica para preguntas de conteo (igual que la tuya)
        if pregunta_es_conteo(pregunta):
            tipo_evento = extraer_tipo_evento(pregunta)
            if not tipo_evento:
                return StreamingResponse(
                    iter([json.dumps({
                        "rol": "bot",
                        "tipo": "final",
                        "texto": "❌ No se pudo identificar el tipo de evento a contar."
                    }) + "\n"]),
                    media_type="text/event-stream"
                )

            eventos = filtrar_eventos_por_similitud(payloads, ref_embeddings, tipo_evento)
            plural = {
                "interrupcion": "interrupciones",
                "denuncia": "denuncias",
                "supervision": "supervisiones"
            }.get(tipo_evento, tipo_evento + "s")

            if not eventos:
                return StreamingResponse(
                    iter([json.dumps({
                        "rol": "bot",
                        "tipo": "final",
                        "texto": responder_llm_groq(pregunta, contexto_completo, payloads)
                    }) + "\n"]),
                    media_type="text/event-stream"
                )

            conteo = len(eventos)
            resumen_eventos = "\n\n".join([
                f"[fecha: {p.get('date')}] [sección: {p.get('section')}] {p.get('text')}"
                for p in eventos
            ])
            contexto_eventos = f"Se encontraron {conteo} {plural} en el contexto analizado.\n\n{resumen_eventos}"
            respuesta = responder_llm_gemini(pregunta, contexto_eventos, "1")

            return StreamingResponse(
                iter([json.dumps({
                    "rol": "bot",
                    "tipo": "final",
                    "texto": f"📌 Conclusión final:\n{respuesta}"
                }) + "\n"]),
                media_type="text/event-stream"
            )

        # --- Tu flujo existente para 1/2/3 ---
        tipo_pregunta = clasificar_tipo_pregunta(pregunta, len(payloads), ultimo_mensaje=data.ultimo_mensaje_bot)
        print("================================= TIPO PREGUNTA", tipo_pregunta)
        MAX_PAYLOADS = 21
        payloads = seleccionar_payloads_balanceados(payloads, MAX_PAYLOADS)

        # ✅ Regla dura: no fragmentar si hay poco contexto
        if len(payloads) <= 6 and tipo_pregunta == "2":
            tipo_pregunta = "1"

        async def generador_respuestas():
            yield json.dumps({"rol": "bot", "tipo": "escribiendo"}) + "\n"
            await asyncio.sleep(1)

            if tipo_pregunta == "1":
                contexto_breve = "\n\n".join([
                    f"[fecha: {p.get('date')}] [fuente: {p.get('source_type')}] [sección: {p.get('section')}]\n{p.get('text')}"
                    for p in payloads
                ])
                texto = responder_llm_gemini(pregunta, contexto_breve, "1")
                yield json.dumps({
                    "rol": "bot",
                    "tipo": "final",
                    "texto": f"\n{texto}"
                }) + "\n"
                return

            elif tipo_pregunta == "2":
                bloque_size = 3
                bloques = [payloads[i:i + bloque_size] for i in range(0, len(payloads), bloque_size)]
                total = len(bloques)
                respuestas_parciales = []

                for i, bloque in enumerate(bloques):
                    contexto_bloque = "\n\n".join([
                        f"[fecha: {p.get('date')}] [fuente: {p.get('source_type')}] [sección: {p.get('section')}]\n{p.get('text')}"
                        for p in bloque
                    ])
                    texto = responder_llm_gemini(pregunta, contexto_bloque, "2")
                    respuestas_parciales.append(texto)

                    yield json.dumps({
                        "rol": "bot",
                        "tipo": "parcial",
                        "analisis_parcial": i + 1,
                        "analisis_total": total,
                        "texto": f"{texto}"
                    }) + "\n"
                    await asyncio.sleep(1)

                conclusion = responder_llm_gemini(pregunta, "\n\n".join(respuestas_parciales), "3")
                yield json.dumps({
                    "rol": "bot",
                    "tipo": "final",
                    "texto": f"📌 Conclusión final:\n{conclusion}"
                }) + "\n"

            elif tipo_pregunta == "3":
                texto = responder_llm_gemini(pregunta, data.ultimo_mensaje_bot or "", "4")
                yield json.dumps({
                    "rol": "bot",
                    "tipo": "final",
                    "texto": f"📌 Resumen:\n{texto}"
                }) + "\n"

        return StreamingResponse(generador_respuestas(), media_type="text/event-stream")

    # ======================= FLUJO NUEVO SOLO PARA RANGO =======================
    inicio_ddmmyyyy = fecha_detectada["inicio"]
    fin_ddmmyyyy = fecha_detectada["fin"]
    tipo_evento = extraer_tipo_evento(pregunta)

    # Si tienes embeddings OpenAI ya en buscar_contexto, reúsalos:
    vector = client.embeddings.create(model=MODEL_EMBEDDING, input=pregunta).data[0].embedding

    window_size_days = 10
    ini_dt = datetime.strptime(inicio_ddmmyyyy, "%d%m%Y")
    fin_dt = datetime.strptime(fin_ddmmyyyy, "%d%m%Y")

    ventanas = list(split_in_windows(ini_dt, fin_dt, window_size_days))
    analisis_total = len(ventanas)

    stream_gen = buscar_por_ventanas(
        qdrant=qdrant,
        collection=COLLECTION_NAME,
        query_vector=vector,
        inicio_ddmmyyyy=inicio_ddmmyyyy,
        fin_ddmmyyyy=fin_ddmmyyyy,
        tipo_evento=tipo_evento,
        k_total=50,
        window_size_days=10,
        use_numeric_field="auto",
        per_day_cap=3,
        stop_on_k=False,
        logger=print,
    )

    async def generador_respuestas_rango():
        yield json.dumps({"rol": "bot", "tipo": "escribiendo"}) + "\n"
        await asyncio.sleep(1)

        respuestas_parciales = []
        payloads_globales = []

        for idx, bloque in enumerate(stream_gen):
            if not bloque:
                continue

            payloads_bloque = [p.payload for p in bloque]
            payloads_globales.extend(payloads_bloque)

            payloads_balanceados = seleccionar_payloads_balanceados(payloads_bloque, max_total=21)

            contexto_bloque = "\n\n".join([
                f"[fecha: {p.get('date')}] [fuente: {p.get('source_type')}] [sección: {p.get('section')}]\n{p.get('text')}"
                for p in payloads_balanceados
            ])

            texto_parcial = responder_llm_gemini(pregunta, contexto_bloque, "2")
            respuestas_parciales.append(texto_parcial)

            yield json.dumps({
                "rol": "bot",
                "tipo": "parcial",
                "analisis_parcial": idx + 1,
                "analisis_total": analisis_total,
                "texto": texto_parcial
            }) + "\n"
            await asyncio.sleep(0.6)

        # CONTEO sobre todo el rango
        if pregunta_es_conteo(pregunta):
            tipo_evento = extraer_tipo_evento(pregunta)
            if not tipo_evento:
                yield json.dumps({
                    "rol": "bot",
                    "tipo": "final",
                    "texto": "❌ No se pudo identificar el tipo de evento a contar."
                }) + "\n"
                return

            eventos = filtrar_eventos_por_similitud(payloads_globales, ref_embeddings, tipo_evento)
            plural = {
                "interrupcion": "interrupciones",
                "denuncia": "denuncias",
                "supervision": "supervisiones"
            }.get(tipo_evento, tipo_evento + "s")

            if not eventos:
                conclusion = responder_llm_gemini(pregunta, "\n\n".join(respuestas_parciales) or "", "3")
                yield json.dumps({
                    "rol": "bot",
                    "tipo": "final",
                    "texto": f"📌 Conclusión final:\n{conclusion}"
                }) + "\n"
                return

            conteo = len(eventos)
            resumen_eventos = "\n\n".join([
                f"[fecha: {p.get('date')}] [sección: {p.get('section')}] {p.get('text')}"
                for p in eventos
            ])
            contexto_eventos = f"Se encontraron {conteo} {plural} en el contexto analizado.\n\n{resumen_eventos}"
            respuesta = responder_llm_gemini(pregunta, contexto_eventos, "1")

            yield json.dumps({
                "rol": "bot",
                "tipo": "final",
                "texto": f"📌 Conclusión final:\n{respuesta}"
            }) + "\n"
            return

        # Conclusión normal con TODO el rango
        try:
            payloads_glob = deduplicar_payloads(payloads_globales)
            if payloads_glob and isinstance(payloads_glob[0], dict) and "embedding" in payloads_glob[0]:
                payloads_glob = puntuar_chunks(payloads_glob, vector)[:60]

            payloads_finales = seleccionar_payloads_balanceados(payloads_glob, max_total=42)

            contexto_final = "\n\n".join([
                f"[fecha: {p.get('date')}] [fuente: {p.get('source_type')}] [sección: {p.get('section')}]\n{p.get('text')}"
                for p in payloads_finales
            ])

            notas_parciales = "\n\n".join(respuestas_parciales) if respuestas_parciales else ""

            prompt_conclusion = (
                "Elabora una conclusión global que cubra TODO el rango temporal indicado (sin sesgarte "
                "solo a los últimos días). Prioriza variedad temporal y agrupa por temas.\n\n"
                "=== CONTEXTO DEL RANGO ===\n" + contexto_final +
                ("\n\n=== NOTAS PARCIALES ===\n" + notas_parciales if notas_parciales else "")
            )

            conclusion = responder_llm_gemini(pregunta, prompt_conclusion, "3")
            print(f"🧮 Final rango -> payloads_glob={len(payloads_glob)} | payloads_finales={len(payloads_finales)} | parciales={len(respuestas_parciales)}")

            yield json.dumps({
                "rol": "bot",
                "tipo": "final",
                "texto": f"📌 Conclusión final:\n{conclusion}"
            }) + "\n"

        except Exception as e:
            print("⚠️ Fallback conclusión por error:", e)
            conclusion = responder_llm_gemini(pregunta, "\n\n".join(respuestas_parciales) or "", "3")
            yield json.dumps({
                "rol": "bot",
                "tipo": "final",
                "texto": f"📌 Conclusión final:\n{conclusion}"
            }) + "\n"

    return StreamingResponse(generador_respuestas_rango(), media_type="text/event-stream")


@app.get("/fechamax_fechamin_cargadas")
def get_fechamax_fechamin():
    qdrant = QdrantClient(url="http://142.93.196.168:6333", timeout=30.0)
    resultados = qdrant.search(
        collection_name="sunass_news_openai",
        query_vector=[0.0] * 1536,
        with_payload=True,
        limit=1000,
        with_vectors=False
    )
    
    fechas = []
    for punto in resultados:
        date_day = punto.payload.get("date_day")
        if not date_day:
            continue
        try:
            # ddmmYYYY → datetime
            fecha_obj = datetime.strptime(date_day, "%d%m%Y")
            fechas.append(fecha_obj)
        except ValueError:
            # Si el formato viene mal, lo ignoramos
            continue
        
    
    if not fechas:
        return {"fechamax": None, "fechamin": None}
    
    fechamin = min(fechas)
    fechamax = max(fechas)
    
    
    return {
        "fechamax": fechamax.strftime("%Y-%m-%d"),
        "fechamin": fechamin.strftime("%Y-%m-%d")
    }


@app.get("/resumen_stats/global")
def get_resumen_global():
    """
    Endpoint para obtener el resumen estadístico global utilizado en la sección del dashboard de la web
    """
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

