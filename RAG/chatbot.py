import re
from openai import OpenAI
from qdrant_client import QdrantClient
from qdrant_client.http.models import Filter, FieldCondition, MatchValue, Range
from qdrant_client.http import models
import httpx
from utils import load_openai_api_key,load_llama3_api_key
from numpy import dot
from numpy.linalg import norm
import time
from collections import defaultdict
from langchain.chains import RetrievalQA
from langchain_community.vectorstores import Qdrant
from langchain_openai import OpenAIEmbeddings  
from langchain_core.language_models.chat_models import SimpleChatModel
from langchain_core.messages import AIMessage, HumanMessage
import os

os.environ["OPENAI_API_KEY"] = load_openai_api_key()


# Configuracion con llama3 api
GROQ_API_KEY = load_llama3_api_key()
GROQ_MODEL = "llama3-8b-8192"

# Configuración
MODEL_EMBEDDING = "text-embedding-3-small"
COLLECTION_NAME = "sunass_news_openai"
client = OpenAI(api_key=load_openai_api_key())  # reemplaza con tu API key
qdrant = QdrantClient(path="./embeddings/qdrant_db")

USAR_LANGCHAIN = True

class GroqChat(SimpleChatModel):
    def _call(self, messages, **kwargs):
        prompt = "\n".join(f"{'Human' if isinstance(m, HumanMessage) else 'AI'}: {m.content}" for m in messages)
        import requests

        res = requests.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {GROQ_API_KEY}",
                "Content-Type": "application/json"
            },
            json={
                "model": GROQ_MODEL,
                "messages": [{"role": "user", "content": prompt}],
                "temperature": 0.3
            }
        )
        response_json = res.json()
        content = response_json.get("choices", [{}])[0].get("message", {}).get("content")
        if not isinstance(content, str):
            raise ValueError(f"❌ Respuesta inesperada de Groq API: {response_json}")
        return AIMessage(content=content)


    @property
    def _llm_type(self) -> str:
        return "groq-chat"

    
qdrant_store = Qdrant(
    client=qdrant,
    collection_name="sunass_news_openai",
    embeddings=OpenAIEmbeddings(model="text-embedding-3-small"),
    content_payload_key="text"
)

def truncar_contexto(contexto, max_tokens=5500):
    max_chars = max_tokens * 4  # estimación generosa
    return contexto[:max_chars]

def responder_llm_langchain(pregunta, contexto, payloads=None):
    print("💬 Generando respuesta con LangChain + LLaMA3...")

    if len(contexto) > 8000 and payloads:
        print("⚠️ Contexto muy largo, aplicando resumen por día...")
        contexto = resumir_por_dia(payloads, max_eventos_por_dia=2)
        contexto = truncar_contexto(contexto, max_tokens=5500)

    chat = GroqChat()
    respuesta = chat._call([HumanMessage(content=f"Contexto:\n{contexto}\n\nPregunta: {pregunta}")])
    return respuesta if isinstance(respuesta, str) else respuesta.content


def deduplicar_chunks(chunks: list[str]) -> list[str]:
    vistos = set()
    unicos = []
    for chunk in chunks:
        texto_limpio = chunk.strip().lower()
        if texto_limpio not in vistos:
            unicos.append(chunk)
            vistos.add(texto_limpio)
    return unicos


def deduplicar_payloads(payloads: list[dict]) -> list[dict]:
    vistos = set()
    unicos = []
    for p in payloads:
        clave = p.get("text", "").strip().lower()
        if clave not in vistos:
            unicos.append(p)
            vistos.add(clave)
    return unicos


def puntuar_chunks(payloads: list[dict], query_embedding: list[float], alpha=0.8) -> list[tuple[float, dict]]:
    scored = []
    for p in payloads:
        embedding = p.get("embedding", None)
        if embedding:
            sim = cosine_similarity(query_embedding, embedding)
            score = alpha * sim
            scored.append((score, p))
    return sorted(scored, key=lambda x: x[0], reverse=True)



def cosine_similarity(a, b):
    return dot(a, b) / (norm(a) * norm(b))

def get_event_type_embeddings():
    ejemplos = {
        "interrupcion": "interrupción del servicio de agua potable por obras o fallas",
        "denuncia": "denuncia o reclamo por mal servicio o atención",
        "supervision": "actividad de supervisión, fiscalización o monitoreo de servicios"
    }

    embeddings = {}
    for tipo, texto in ejemplos.items():
        emb = client.embeddings.create(
            model="text-embedding-3-small",
            input=[texto]
        ).data[0].embedding
        embeddings[tipo] = emb
    return embeddings

# Detectar el tipo de evento del texto por similitud
def detectar_evento_por_embedding(texto: str, ref_embeddings: dict, threshold=0.80):
    emb_text = client.embeddings.create(
        model="text-embedding-3-small",
        input=[texto]
    ).data[0].embedding

    mejores = [
        (tipo, cosine_similarity(emb_text, emb_ref))
        for tipo, emb_ref in ref_embeddings.items()
    ]

    tipo, score = max(mejores, key=lambda x: x[1])
    return tipo if score >= threshold else None

# ----------------------------
# 🔍 Utilidades de análisis de la pregunta
# ----------------------------
def extraer_fecha(pregunta: str) -> dict | None:
    pregunta = pregunta.lower()
    meses = {
        "enero": "01", "febrero": "02", "marzo": "03", "abril": "04",
        "mayo": "05", "junio": "06", "julio": "07", "agosto": "08",
        "setiembre": "09", "septiembre": "09", "octubre": "10",
        "noviembre": "11", "diciembre": "12"
    }

    # 🔁 Detectar rango de fechas (varios formatos)
    match = re.search(
        r"(?:entre\s+)?el\s+(\d{1,2})\s+de\s+(\w+)\s+(?:y|al)\s+el?\s*(\d{1,2})\s+de\s+(\w+)(?:\s+de\s+(\d{4}))?",
        pregunta
    )
    if match:
        d1, m1, d2, m2, a = match.groups()
        m1 = meses.get(m1.lower())
        m2 = meses.get(m2.lower())
        a = a or "2025"
        if m1 and m2:
            return {
                "tipo": "rango",
                "inicio": f"{int(d1):02}{m1}{a}",
                "fin": f"{int(d2):02}{m2}{a}"
            }

    # 📅 Fecha exacta: "15 de julio de 2025"
    match = re.search(r"(\d{1,2})\s+de\s+(\w+)(?:\s+de\s*(\d{4}))?", pregunta)
    if match:
        d, m_nombre, a = match.groups()
        m = meses.get(m_nombre.lower())
        if m:
            return {"tipo": "exacta", "valor": f"{int(d):02}{m}{a or '2025'}"}

    # 📆 Solo mes y año: "julio de 2025"
    # búsqueda más robusta: ¿existe un mes en la pregunta?
    for m_nombre, m_num in meses.items():
        if re.search(rf"\b{m_nombre}\b.*(?:\d{{4}})?", pregunta):
            a_match = re.search(r"(\d{4})", pregunta)
            a = a_match.group(1) if a_match else "2025"
            return {"tipo": "mes", "valor": f"{m_num}{a}"}


    # 📆 Fecha tipo numérica: "10/07/2025"
    match = re.search(r"(\d{1,2})[/-](\d{1,2})[/-](\d{4})", pregunta)
    if match:
        d, m, a = match.groups()
        return {"tipo": "exacta", "valor": f"{int(d):02}{int(m):02}{a}"}

    return None

def pregunta_es_conteo(pregunta: str) -> bool:
    patrones = [
        r"cu[aá]ntas?", r"cu[aá]ntos?", r"n[uú]mero de", r"total de", r"hubo.*(interrupciones|denuncias|supervisiones)"
    ]
    return any(re.search(p, pregunta.lower()) for p in patrones)

def extraer_tipo_evento(pregunta: str):
    pregunta = pregunta.lower()

    if any(palabra in pregunta for palabra in ["interrupción", "interrupciones", "corte de agua", "suspensión", "sin agua"]):
        return "interrupcion"
    elif any(palabra in pregunta for palabra in ["denuncia", "denuncias", "reclamo", "reclamos"]):
        return "denuncia"
    elif any(palabra in pregunta for palabra in ["fiscalización", "supervisión", "monitoreo"]):
        return "supervision"
    
    return None

def resumir_por_dia(payloads, max_eventos_por_dia=3, min_texto_valido=30):
    from collections import defaultdict

    dias = defaultdict(list)

    for p in payloads:
        dia = p.get("date", "desconocido")
        texto = p.get("text", "").strip()
        if texto and len(texto) > min_texto_valido:
            resumen = texto[:300].replace("\n", " ").strip()
            fuente = p.get("source_type", "")
            seccion = p.get("section", "")
            dias[dia].append(f"[fuente: {fuente}] [sección: {seccion}] {resumen}...")

    if not dias:
        return "No hay información detallada disponible para las fechas consultadas."

    resumen_dias = []
    for fecha in sorted(dias.keys()):
        eventos = dias[fecha][:max_eventos_por_dia]
        if eventos:
            resumen_dias.append(f"[fecha: {fecha}] ({len(eventos)} eventos)\n" + "\n".join([f"- {e}" for e in eventos]))

    return "\n\n".join(resumen_dias)


# ----------------------------
# 📥 Embedding y búsqueda
# ----------------------------
def buscar_contexto_openai(pregunta: str, k=50):
    fecha = extraer_fecha(pregunta)
    print("🧪 Fecha extraída:", fecha)
    tipo_evento = extraer_tipo_evento(pregunta)

    vector = client.embeddings.create(
        model=MODEL_EMBEDDING,
        input=pregunta
    ).data[0].embedding

    puntos = []

    if fecha:
        if fecha["tipo"] == "rango":
            from datetime import datetime, timedelta

            inicio = datetime.strptime(fecha["inicio"], "%d%m%Y")
            fin = datetime.strptime(fecha["fin"], "%d%m%Y")
            dias = (fin - inicio).days + 1
            fechas_rango = [
                (inicio + timedelta(days=i)).strftime("%d%m%Y") for i in range(dias)
            ]

            k_por_dia = max(k // dias, 3)

            for dia in fechas_rango:
                filtro_dia = Filter(must=[
                    FieldCondition(key="date_day", match=MatchValue(value=dia))
                ])
                if tipo_evento:
                    filtro_dia.must.append(FieldCondition(
                        key="event_type", match=MatchValue(value=tipo_evento)
                    ))

                res = qdrant.query_points(
                    collection_name=COLLECTION_NAME,
                    query=vector,
                    query_filter=filtro_dia,
                    limit=k_por_dia,
                    with_payload=True
                )
                puntos.extend(res.points)
        else:
            must_conditions = []

            if fecha["tipo"] == "exacta":
                must_conditions.append(FieldCondition(
                    key="date_day", match=MatchValue(value=fecha["valor"])
                ))
            elif fecha["tipo"] == "mes":
                from datetime import datetime, timedelta
                from calendar import monthrange
                print("📆 Buscando por mes -------------------------:", fecha["valor"])
                mes = int(fecha["valor"][:2])
                anio = int(fecha["valor"][2:])
                dias_en_mes = monthrange(anio, mes)[1]

                fechas_mes = [
                    f"{day:02}{mes:02}{anio}" for day in range(1, dias_en_mes + 1)
                ]

                k_por_dia = max(k // dias_en_mes, 3)

                for dia in fechas_mes:
                    filtro_dia = Filter(must=[FieldCondition(
                        key="date_day", match=MatchValue(value=dia)
                    )])
                    if tipo_evento:
                        filtro_dia.must.append(FieldCondition(
                            key="event_type", match=MatchValue(value=tipo_evento)
                        ))

                    res = qdrant.query_points(
                        collection_name=COLLECTION_NAME,
                        query=vector,
                        query_filter=filtro_dia,
                        limit=k_por_dia,
                        with_payload=True
                    )
                    puntos.extend(res.points)

            if tipo_evento:
                must_conditions.append(FieldCondition(
                    key="event_type", match=MatchValue(value=tipo_evento)
                ))

            filtro = Filter(must=must_conditions) if must_conditions else None

            resultados = qdrant.query_points(
                collection_name=COLLECTION_NAME,
                query=vector,
                query_filter=filtro,
                limit=k,
                with_payload=True
            )
            puntos = resultados.points
            
            
    else:
        # Sin fecha, búsqueda general
        print("no hay fecha")
        must_conditions = []
        if tipo_evento:
            must_conditions.append(FieldCondition(
                key="event_type", match=MatchValue(value=tipo_evento)
            ))

        filtro = Filter(must=must_conditions) if must_conditions else None

        resultados = qdrant.query_points(
            collection_name=COLLECTION_NAME,
            query=vector,
            query_filter=filtro,
            limit=k,
            with_payload=True
        )
        puntos = resultados.points

    # Resultado vacío
    if not puntos:
        print("⚠️ No se encontraron puntos para esta consulta.")
        return "No se encontró contexto relacionado.", []

    payloads = [p.payload for p in puntos]

    # Paso 2: deduplicar
    payloads = deduplicar_payloads(payloads)

    # Paso 3: puntuar y limitar si tienen embedding
    if payloads and "embedding" in payloads[0]:
        payloads = puntuar_chunks(payloads, vector)
        payloads = payloads[:k]

    # Procesamiento de contexto y payloads
    contextos = []
    for punto in puntos:
        payload = punto.payload
        texto = payload.get("text", "")
        fecha = payload.get("date", "")
        fuente = payload.get("source_type", "")
        seccion = payload.get("section", "")

        payloads.append(payload)
        contextos.append(f"[fecha: {fecha}] [fuente: {fuente}] [sección: {seccion}]\n{texto}")

        print("📌 Fecha encontrada:", fecha, "| Texto:", texto[:80])

    return "\n\n".join(contextos), payloads

# ----------------------------
# 🤖 Chat y generación
# ----------------------------
def responder_conteo(pregunta, payloads):
    tipo_evento = extraer_tipo_evento(pregunta)
    print(f"[DEBUG] Tipo de evento: {tipo_evento}")
    if not tipo_evento:
        return "No se pudo identificar el tipo de evento a contar."

    print(f"[DEBUG] Total de payloads: {len(payloads)}")
    palabras_clave = {
        "interrupcion": ["interrupción", "interrupciones", "corte de agua", "sin agua", "suspensión"],
        "denuncia": ["denuncia", "reclamo"],
        "supervision": ["fiscalización", "supervisión", "monitoreo"]
    }

    claves = palabras_clave.get(tipo_evento, [])
    eventos_detectados = []

    for p in payloads:
        texto = p.get("text", "").lower()
        if any(k in texto for k in claves):
            eventos_detectados.append(p)

    conteo = len(eventos_detectados)

    if conteo == 0:
        return f"No se encontraron {tipo_evento}s en el contexto."

    plural = {
        "interrupcion": "interrupciones",
        "denuncia": "denuncias",
        "supervision": "supervisiones"
    }.get(tipo_evento, tipo_evento + "s")

    resumen = f"Se encontraron {conteo} {plural} en el contexto.\n"
    resumen += "\n".join([
        f"📝 {i+1}. {p.get('text', '')[:200].strip()}..."  # Solo los primeros 200 caracteres
        for i, p in enumerate(eventos_detectados[:5])     # Solo los primeros 5
    ])

    return resumen



def responder_llm_groq(pregunta, contexto,payloads=None, intentos=3):
    print("💬 Generando respuesta con LLAMA3...")
    
    if len(contexto) > 8000 and payloads:
        print("⚠️ Contexto muy largo, aplicando resumen por día...")
        contexto = resumir_por_dia(payloads, max_eventos_por_dia=3)
        if len(contexto) > 8000:
            contexto = contexto[:8000]
        
    url = "https://api.groq.com/openai/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {GROQ_API_KEY}",
        "Content-Type": "application/json"
    }
    data = {
        "model": GROQ_MODEL,
        "messages": [
            {"role": "system", "content": (
                "Eres un asistente experto de SUNASS. Usa únicamente el contexto proporcionado para responder. "
                "Cuando la pregunta sea sobre cuántos eventos ocurrieron (como interrupciones o denuncias), responde indicando la cantidad y "
                "resumiendo brevemente los eventos: dónde, cuándo y por qué sucedieron, si la información está disponible. "
                "Si no hay datos suficientes, dilo explícitamente."
            )},
            {"role": "user", "content": f"Contexto:\n{contexto}\n\nPregunta: {pregunta}"}
        ],
        "temperature": 0.3
    }

    for i in range(intentos):
        try:
            response = httpx.post(url, json=data, headers=headers)
            response.raise_for_status()
            return response.json()["choices"][0]["message"]["content"]
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 429:
                espera = 2 ** i
                print(f"⏳ Esperando {espera} segundos por límite de uso...")
                time.sleep(espera)
            else:
                print("❌ Error en llamada:", e.response.text[:1000])
                raise

    return "Se alcanzó el límite de uso de la API. Intenta más tarde."

def responder_llm(pregunta, contexto):
    print("💬 Generando respuesta con OPEN-IA-4o...")
    system_prompt = (
        "Eres un asistente de SUNASS. Usa el contexto con precisión para responder. "
        "Si no se identifican eventos específicos como interrupciones o denuncias,brinda un resumen general de los temas mencionados ese día."
    )

    respuesta = client.chat.completions.create(
        model="gpt-4o",
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": f"Pregunta: {pregunta}\n\nContexto:\n{contexto}"}
        ]
    )
    return respuesta.choices[0].message.content

def filtrar_eventos_por_similitud(payloads, ref_embeddings, tipo_evento, threshold=0.80):
    eventos_detectados = []

    for p in payloads:
        texto = p.get("text", "")
        tipo_detectado = detectar_evento_por_embedding(texto, ref_embeddings, threshold)
        if tipo_detectado == tipo_evento:
            eventos_detectados.append(p)

    return eventos_detectados



def chat():
    print("🤖 Chatbot SUNASS con Embedding de OpenAI (escribe 'salir' para terminar)\n")
    # Carga una sola vez los embeddings de referencia
    ref_embeddings = get_event_type_embeddings()
    
    while True:
        pregunta = input("Tú: ")
        if pregunta.lower() in {"salir", "exit"}:
            break

        print("📡 Buscando contexto...")
        contexto, payloads = buscar_contexto_openai(pregunta)

        if pregunta_es_conteo(pregunta):
            print("🔢 Contando eventos relevantes...")
            tipo_evento = extraer_tipo_evento(pregunta)
            if not tipo_evento:
                respuesta = "No se pudo identificar el tipo de evento a contar."
                print(f"\n🧠 Respuesta: {respuesta}\n")
                continue

            eventos = filtrar_eventos_por_similitud(payloads, ref_embeddings, tipo_evento)

            if not eventos:
                plural = {
                "interrupcion": "interrupciones",
                "denuncia": "denuncias",
                "supervision": "supervisiones"
                }.get(tipo_evento, tipo_evento + "s")
                respuesta = f"No se encontraron {plural} en el contexto."
            else:
                # 🔥 NUEVO: Arma el contexto con el conteo incluido
                plural = {
                    "interrupcion": "interrupciones",
                    "denuncia": "denuncias",
                    "supervision": "supervisiones"
                }.get(tipo_evento, tipo_evento + "s")

                conteo = len(eventos)
                resumen_eventos = "\n\n".join([
                    f"[fecha: {p.get('date')}] [sección: {p.get('section')}] {p.get('text')}"
                    for p in eventos
                ])

                contexto_eventos = (
                    f"Se encontraron {conteo} {plural} en el contexto analizado.\n\n"
                    f"{resumen_eventos}"
                )

                if USAR_LANGCHAIN:
                    respuesta = responder_llm_langchain(pregunta, contexto, payloads)
                else:
                    respuesta = responder_llm_groq(pregunta, contexto, payloads)


        else:           
            if USAR_LANGCHAIN:
                respuesta = responder_llm_langchain(pregunta, contexto, payloads)
            else:
                respuesta = responder_llm_groq(pregunta, contexto, payloads)

        print(f"\n🧠 Respuesta: {respuesta}\n")

if __name__ == "__main__":
    chat()
