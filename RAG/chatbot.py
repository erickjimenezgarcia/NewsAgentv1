import re
from openai import OpenAI
from qdrant_client import QdrantClient
from qdrant_client.http.models import Filter, FieldCondition, MatchValue, Range
from qdrant_client.http import models

from RAG.utils import load_openai_api_key


# Configuración
MODEL_EMBEDDING = "text-embedding-3-small"
COLLECTION_NAME = "sunass_news_openai"
client = OpenAI(api_key=load_openai_api_key())  # reemplaza con tu API key
qdrant = QdrantClient(path="./embeddings/qdrant_db")


# ----------------------------
# 🔍 Utilidades de análisis de la pregunta
# ----------------------------
def extraer_fecha(pregunta: str) -> dict | None:
    meses = {
        "enero": "01", "febrero": "02", "marzo": "03", "abril": "04",
        "mayo": "05", "junio": "06", "julio": "07", "agosto": "08",
        "setiembre": "09", "septiembre": "09", "octubre": "10",
        "noviembre": "11", "diciembre": "12"
    }

    pregunta = pregunta.lower()
    match = re.search(r"(\d{1,2})\s+de\s+([a-zA-Z]+)(?:\s+de[l]?\s*(\d{4}))?", pregunta)
    if match:
        d, m_nombre, a = match.groups()
        m = meses.get(m_nombre.lower())
        if m:
            return {"tipo": "exacta", "valor": f"{int(d):02}{m}{a or '2025'}"}

    match = re.search(r"(?:en\s+)?([a-zA-Z]+)(?:\s+de[l]?\s*(\d{4}))?", pregunta)
    if match:
        m_nombre, a = match.groups()
        m = meses.get(m_nombre.lower())
        if m:
            return {"tipo": "mes", "valor": f"{m}{a or '2025'}"}

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

def extraer_tipo_evento(pregunta: str) -> str | None:
    pregunta = pregunta.lower()
    if "interrupción" in pregunta or "sin agua" in pregunta or "corte de agua" in pregunta:
        return "interrupcion"
    if "denuncia" in pregunta:
        return "denuncia"
    if "fiscalización" in pregunta or "supervisión" in pregunta:
        return "supervision"
    return None

# ----------------------------
# 📥 Embedding y búsqueda
# ----------------------------
def buscar_contexto_openai(pregunta: str, k=5):
    fecha = extraer_fecha(pregunta)
    tipo_evento = extraer_tipo_evento(pregunta)

    vector = client.embeddings.create(
        model=MODEL_EMBEDDING,
        input=pregunta
    ).data[0].embedding

    must_conditions = []
    if fecha:
        if fecha["tipo"] == "exacta":
            must_conditions.append(FieldCondition(
                key="date", match=MatchValue(value=fecha["valor"])
            ))
        elif fecha["tipo"] == "mes":
            must_conditions.append(FieldCondition(
                key="date", range=Range(gte=f"01{fecha['valor']}", lte=f"31{fecha['valor']}")
            ))

    if tipo_evento:
        must_conditions.append(FieldCondition(
            key="event_type", match=MatchValue(value=tipo_evento)
        ))

    # Solo aquí generamos el filtro
    filtro = Filter(must=must_conditions) if must_conditions else None


    resultados = qdrant.search(
        collection_name=COLLECTION_NAME,
        query_vector=vector,
        limit=k,
        query_filter=filtro
    )

    for r in resultados:
        print("📌 Fecha encontrada:", r.payload.get("date"), "| Texto:", r.payload.get("text")[:80])


    contextos = []
    payloads = []
    for r in resultados:
        payload = r.payload
        texto = payload.get("text", "")
        fecha = payload.get("date", "")
        fuente = payload.get("source_type", "")
        seccion = payload.get("section", "")
        
        payloads.append(payload)
        contextos.append(f"[fecha: {fecha}] [fuente: {fuente}] [sección: {seccion}]\n{texto}")


    return "\n\n".join(contextos), payloads

# ----------------------------
# 🤖 Chat y generación
# ----------------------------
def responder_conteo(pregunta, payloads):
    tipo_evento = extraer_tipo_evento(pregunta)
    if not tipo_evento:
        return "No se pudo identificar el tipo de evento a contar."

    palabras_clave = {
        "interrupcion": ["interrupción","interrupciones", "corte de agua", "sin agua", "suspensión"],
        "denuncia": ["denuncia", "reclamo"],
        "supervision": ["fiscalización", "supervisión", "monitoreo"]
    }

    claves = palabras_clave.get(tipo_evento, [])
    conteo = 0
    for p in payloads:
        texto = p.get("text", "").lower()
        if any(k in texto for k in claves):
            conteo += 1

    if conteo == 0:
        return f"No se encontraron {tipo_evento}s en el contexto."
    elif conteo == 1:
        return f"Se encontró 1 {tipo_evento} en el contexto."
    else:
        return f"Se encontraron {conteo} {tipo_evento}s en el contexto."


def responder_llm(pregunta, contexto):
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


def chat():
    print("🤖 Chatbot SUNASS con Embedding de OpenAI (escribe 'salir' para terminar)\n")
    while True:
        pregunta = input("Tú: ")
        if pregunta.lower() in {"salir", "exit"}:
            break

        print("📡 Buscando contexto...")
        contexto, payloads = buscar_contexto_openai(pregunta)

        if pregunta_es_conteo(pregunta):
            print("🔢 Contando eventos relevantes...")
            respuesta = responder_conteo(pregunta, payloads)
        else:
            print("💬 Generando respuesta con GPT-4o...")
            respuesta = responder_llm(pregunta, contexto)

        print(f"\n🧠 Respuesta: {respuesta}\n")

if __name__ == "__main__":
    chat()
