import re
from openai import OpenAI
from qdrant_client import QdrantClient
from qdrant_client.http.models import Filter, FieldCondition, MatchValue, Range, MatchAny
from qdrant_client.http import models
import httpx
from RAG.utils import load_openai_api_key,load_llama3_api_key,load_api_gemini_key
from numpy import dot
from numpy.linalg import norm
from calendar import monthrange
import time
from collections import defaultdict
from langchain.chains import RetrievalQA
from langchain_community.vectorstores import Qdrant
from langchain_openai import OpenAIEmbeddings  
from langchain_core.language_models.chat_models import SimpleChatModel
from langchain_core.messages import AIMessage, HumanMessage
import os
from google.generativeai.types import HarmCategory, HarmBlockThreshold
from qdrant_client.models import Range
from datetime import datetime, timedelta
from math import ceil
import google.generativeai as genai


os.environ["OPENAI_API_KEY"] = load_openai_api_key()
genai.configure(api_key=load_api_gemini_key())


# Configuracion con llama3 api
GROQ_API_KEY = load_llama3_api_key()
GROQ_MODEL = "llama3-8b-8192"

# Configuración
MODEL_EMBEDDING = "text-embedding-3-small"
COLLECTION_NAME = "sunass_news_openai"
client = OpenAI(api_key=load_openai_api_key())  # reemplaza con tu API key
qdrant = QdrantClient( url="http://142.93.196.168:6333",)

USAR_LANGCHAIN = False

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


# Utilidades

def ddmmyyyy_to_yyyymmdd_int(s: str) -> int:
    # "ddmmyyyy" -> 20250107
    return int(f"{s[4:]}{s[2:4]}{s[:2]}")

def iso_to_ddmmyyyy(iso: str) -> str:
    # "YYYY-MM-DD" -> "ddmmyyyy"
    dt = datetime.strptime(iso, "%Y-%m-%d")
    return dt.strftime("%d%m%Y")

def daterange_days(inicio: datetime, fin: datetime):
    dias = (fin - inicio).days + 1
    for i in range(dias):
        yield inicio + timedelta(days=i)

def split_in_windows(inicio: datetime, fin: datetime, window_size_days: int = 10):
    """
    Genera ventanas [ini, fin] de window_size_days (1-10, 11-20, ...).
    La última ventana puede ser más corta.
    """
    current = inicio
    while current <= fin:
        end = min(current + timedelta(days=window_size_days - 1), fin)
        yield (current, end)
        current = end + timedelta(days=1)
        
        
def buscar_por_ventanas(
    qdrant, collection, query_vector,
    inicio_ddmmyyyy: str, fin_ddmmyyyy: str,
    tipo_evento: str | None,
    k_total: int = 50,
    window_size_days: int = 10,
    use_numeric_field: bool | str = "auto",   # True / False / "auto"
    per_day_cap: int = 3,
    stop_on_k: bool = False,                  # ⬅️ NO cortar al llegar a k_total (por defecto False)
    logger=print,
):
    inicio = datetime.strptime(inicio_ddmmyyyy, "%d%m%Y")
    fin = datetime.strptime(fin_ddmmyyyy, "%d%m%Y")

    total_days = (fin - inicio).days + 1
    k_por_dia = max(k_total // max(total_days, 1), per_day_cap)
    k_por_ventana = max(1, k_por_dia * window_size_days)

    ventanas = list(split_in_windows(inicio, fin, window_size_days))
    seen_ids = set()
    acumulados = 0

    for idx, (win_ini, win_fin) in enumerate(ventanas, start=1):
        gte = int(win_ini.strftime("%Y%m%d"))
        lte = int(win_fin.strftime("%Y%m%d"))
        ddmm_list = [d.strftime("%d%m%Y") for d in daterange_days(win_ini, win_fin)]

        def filtro_numerico():
            f = Filter(must=[FieldCondition(key="date_day_num", range=Range(gte=gte, lte=lte))])
            if tipo_evento:
                f.must.append(FieldCondition(key="event_type", match=MatchValue(value=tipo_evento)))
            return f

        def filtro_matchany():
            f = Filter(must=[FieldCondition(key="date_day", match=MatchAny(any=ddmm_list))])
            if tipo_evento:
                f.must.append(FieldCondition(key="event_type", match=MatchValue(value=tipo_evento)))
            return f

        resultados = []
        intento = None

        # ---------- Selección / fallback ----------
        if use_numeric_field is True:
            intento = "NUM"
            filtro = filtro_numerico()
            logger(f"🔎 Ventana {idx}/{len(ventanas)} NUM {win_ini:%d-%m-%Y}..{win_fin:%d-%m-%Y} "
                   f"gte={gte} lte={lte} limit={k_por_ventana}")
            resultados = qdrant.search(
                collection_name=collection,
                query_vector=query_vector,
                query_filter=filtro,
                limit=k_por_ventana,
                with_payload=True,
            )

        elif use_numeric_field is False:
            intento = "ANY"
            filtro = filtro_matchany()
            logger(f"🔎 Ventana {idx}/{len(ventanas)} ANY {win_ini:%d-%m-%Y}..{win_fin:%d-%m-%Y} "
                   f"days={len(ddmm_list)} limit={k_por_ventana}")
            resultados = qdrant.search(
                collection_name=collection,
                query_vector=query_vector,
                query_filter=filtro,
                limit=k_por_ventana,
                with_payload=True,
            )

        else:  # "auto": probar NUM y si viene vacío, caer a ANY
            intento = "AUTO>NUM"
            filtro = filtro_numerico()
            logger(f"🔎 Ventana {idx}/{len(ventanas)} AUTO(NUM) {win_ini:%d-%m-%Y}..{win_fin:%d-%m-%Y} "
                   f"gte={gte} lte={lte} limit={k_por_ventana}")
            resultados = qdrant.search(
                collection_name=collection,
                query_vector=query_vector,
                query_filter=filtro,
                limit=k_por_ventana,
                with_payload=True,
            )
            if not resultados:
                intento = "AUTO>ANY"
                filtro = filtro_matchany()
                logger(f"↩️  Ventana {idx}/{len(ventanas)} fallback ANY days={len(ddmm_list)} limit={k_por_ventana}")
                resultados = qdrant.search(
                    collection_name=collection,
                    query_vector=query_vector,
                    query_filter=filtro,
                    limit=k_por_ventana,
                    with_payload=True,
                )

        # ---------- Dedup por ID de puntos ----------
        nuevos = []
        for p in resultados:
            if p.id not in seen_ids:
                seen_ids.add(p.id)
                nuevos.append(p)
        acumulados += len(nuevos)

        logger(f"✅ Ventana {idx}/{len(ventanas)} [{intento}] resultados={len(resultados)} nuevos={len(nuevos)} "
               f"acumulados={acumulados}")

        yield nuevos  # bloque de esta ventana (para streaming)

        # Sólo si quieres cortar al llegar a k_total
        if stop_on_k and acumulados >= k_total:
            logger(f"⛔ Corte por k_total={k_total} en ventana {idx}")
            return



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
    """
    Función para eliminar duplicados en una lista de payloads.
    """
    vistos = set()
    unicos = []
    for p in payloads:
        clave = p.get("text", "").strip().lower()
        if clave not in vistos:
            unicos.append(p)
            vistos.add(clave)
    return unicos


def puntuar_chunks(payloads: list[dict], query_embedding: list[float], alpha=0.8) -> list[tuple[float, dict]]:
    """
    Función para puntuar un conjunto de payloads en base a su similitud con un embedding de consulta.
    """
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
    s = pregunta.lower().strip()
    meses = {
        "enero": "01", "febrero": "02", "marzo": "03", "abril": "04",
        "mayo": "05", "junio": "06", "julio": "07", "agosto": "08",
        "setiembre": "09", "septiembre": "09", "octubre": "10",
        "noviembre": "11", "diciembre": "12"
    }

    def to_ddmmyyyy(d, m, a):
        return f"{int(d):02}{m}{a}"

    # ---------- 1) Rangos con texto ----------
    # a) "del D de MES (de AAAA)? al D de MES (de AAAA)?"
    m = re.search(
        r"\bdel\s+(\d{1,2})\s+de\s+(\w+)(?:\s+de\s+(\d{4}))?\s+al\s+(\d{1,2})\s+de\s+(\w+)(?:\s+de\s+(\d{4}))?",
        s
    )
    if m:
        d1, m1n, a1, d2, m2n, a2 = m.groups()
        m1 = meses.get(m1n, None)
        m2 = meses.get(m2n, None)
        if m1 and m2:
            a1 = a1 or "2025"
            a2 = a2 or a1
            return {"tipo": "rango", "inicio": to_ddmmyyyy(d1, m1, a1), "fin": to_ddmmyyyy(d2, m2, a2)}

    # b) "desde el D de MES (de AAAA)? hasta el D de MES (de AAAA)?"
    m = re.search(
        r"\bdesde\s+el\s+(\d{1,2})\s+de\s+(\w+)(?:\s+de\s+(\d{4}))?\s+hasta\s+el\s+(\d{1,2})\s+de\s+(\w+)(?:\s+de\s+(\d{4}))?",
        s
    )
    if m:
        d1, m1n, a1, d2, m2n, a2 = m.groups()
        m1 = meses.get(m1n, None)
        m2 = meses.get(m2n, None)
        if m1 and m2:
            a1 = a1 or "2025"
            a2 = a2 or a1
            return {"tipo": "rango", "inicio": to_ddmmyyyy(d1, m1, a1), "fin": to_ddmmyyyy(d2, m2, a2)}

    # c) "entre el D de MES (de AAAA)? y el D de MES (de AAAA)?"
    m = re.search(
        r"\bentre\s+el\s+(\d{1,2})\s+de\s+(\w+)(?:\s+de\s+(\d{4}))?\s+(?:y|al)\s+el?\s*(\d{1,2})\s+de\s+(\w+)(?:\s+de\s+(\d{4}))?",
        s
    )
    if m:
        d1, m1n, a1, d2, m2n, a2 = m.groups()
        m1 = meses.get(m1n, None)
        m2 = meses.get(m2n, None)
        if m1 and m2:
            a1 = a1 or "2025"
            a2 = a2 or a1
            return {"tipo": "rango", "inicio": to_ddmmyyyy(d1, m1, a1), "fin": to_ddmmyyyy(d2, m2, a2)}
        
        
    # d) Rango de MESES con texto: "desde el mes de enero (de 2025)? (hasta|a|al) (el )?mes de febrero (de 2025)?"
    m = re.search(
        r"\bdesde\s+el\s+mes\s+de\s+(\w+)(?:\s+de\s+(\d{4}))?\s+(?:hasta|a|al)\s+(?:el\s+)?mes\s+de\s+(\w+)(?:\s+de\s+(\d{4}))?",
        s
    )
    if m:
        m1n, a1, m2n, a2 = m.groups()
        m1 = meses.get(m1n, None)
        m2 = meses.get(m2n, None)
        if m1 and m2:
            a1 = a1 or "2025"
            a2 = a2 or a1  # si solo hay un año, se aplica a ambos
            # inicio: día 01 del primer mes
            inicio = f"01{m1}{a1}"
            # fin: último día del segundo mes
            last_day = monthrange(int(a2), int(m2))[1]
            fin = f"{last_day:02}{m2}{a2}"
            return {"tipo": "rango", "inicio": inicio, "fin": fin}

    # e) Rango de MESES sin la palabra "mes": "enero (de 2025)? (hasta|a|al) febrero (de 2025)?"
    m = re.search(
        r"\b(\w+)(?:\s+de\s+(\d{4}))?\s+(?:hasta|a|al)\s+(\w+)(?:\s+de\s+(\d{4}))?\b",
        s
    )
    if m:
        m1n, a1, m2n, a2 = m.groups()
        # Solo acepta si ambos son nombres de mes válidos (evita confundir con palabras sueltas)
        if m1n in meses and m2n in meses:
            m1 = meses[m1n]; m2 = meses[m2n]
            a1 = a1 or "2025"
            a2 = a2 or a1
            inicio = f"01{m1}{a1}"
            last_day = monthrange(int(a2), int(m2))[1]
            fin = f"{last_day:02}{m2}{a2}"
            return {"tipo": "rango", "inicio": inicio, "fin": fin}

    # f) Rango numérico de MESES: "mm/yyyy (hasta|a|al) mm/yyyy"
    m = re.search(
        r"\b(\d{1,2})[\/\-](\d{4})\s+(?:hasta|a|al)\s+(\d{1,2})[\/\-](\d{4})\b",
        s
    )
    if m:
        mo1, a1, mo2, a2 = m.groups()
        mo1 = int(mo1); mo2 = int(mo2)
        if 1 <= mo1 <= 12 and 1 <= mo2 <= 12:
            inicio = f"01{mo1:02}{a1}"
            last_day = monthrange(int(a2), mo2)[1]
            fin = f"{last_day:02}{mo2:02}{a2}"
            return {"tipo": "rango", "inicio": inicio, "fin": fin}

    # ---------- 2) Rangos numéricos ----------
    # "dd/mm/aaaa (al|a|hasta|-) dd/mm/aaaa"  (acepta / - .)
    m = re.search(
        r"(\d{1,2})[\/\-\.\s](\d{1,2})[\/\-\.\s](\d{4})\s*(?:al|a|hasta|–|-|—)\s*(\d{1,2})[\/\-\.\s](\d{1,2})[\/\-\.\s](\d{4})",
        s
    )
    if m:
        d1, mo1, a1, d2, mo2, a2 = m.groups()
        return {"tipo": "rango",
                "inicio": f"{int(d1):02}{int(mo1):02}{a1}",
                "fin": f"{int(d2):02}{int(mo2):02}{a2}"}

    # ---------- 3) Fecha exacta (si NO hubo rango) ----------
    m = re.search(r"(\d{1,2})\s+de\s+(\w+)(?:\s+de\s*(\d{4}))?", s)
    if m:
        d, m_nombre, a = m.groups()
        mm = meses.get(m_nombre, None)
        if mm:
            return {"tipo": "exacta", "valor": to_ddmmyyyy(d, mm, a or "2025")}

    # ---------- 4) Solo mes y año ----------
    for m_nombre, m_num in meses.items():
        if re.search(rf"\b{m_nombre}\b", s):
            a_match = re.search(r"(\d{4})", s)
            a = a_match.group(1) if a_match else "2025"
            return {"tipo": "mes", "valor": f"{m_num}{a}"}

    # ---------- 5) Fecha exacta numérica ----------
    m = re.search(r"(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{4})", s)
    if m:
        d, mo, a = m.groups()
        return {"tipo": "exacta", "valor": f"{int(d):02}{int(mo):02}{a}"}

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
    """
    Función para resumir por dia
    """
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
# ---------- Tu función principal ----------
def buscar_contexto_openai(pregunta: str, k=50):
    fecha = extraer_fecha(pregunta)
    print("🧪 Fecha extraída:", fecha)
    tipo_evento = extraer_tipo_evento(pregunta)

    vector = client.embeddings.create(
        model=MODEL_EMBEDDING,
        input=pregunta
    ).data[0].embedding

    puntos = []            # Para acumular resultados finales cuando NO es rango
    puntos_acumulados = [] # Para el caso rango (acumula todos los bloques para la conclusión)

    if fecha:
        if fecha.get("tipo") == "rango":
            # === NUEVO: flujo por ventanas (stream-ready) ===
            use_numeric = True  # ponlo en False si aún no guardas date_day_num

            stream_gen = buscar_por_ventanas(
                qdrant=qdrant,
                collection=COLLECTION_NAME,
                query_vector=vector,
                inicio_ddmmyyyy=fecha["inicio"],
                fin_ddmmyyyy=fecha["fin"],
                tipo_evento=tipo_evento,
                k_total=k,
                window_size_days=10,  # ajusta 1 (día a día), 10, 15, 30, etc.
                use_numeric_field=use_numeric,
                per_day_cap=3,
            )

            # IMPORTANTE: aquí puedes emitir por streaming cada bloque
            for bloque in stream_gen:
                if not bloque:
                    continue
                # 👉 En tu endpoint /chatbot/stream/ envía una "respuesta parcial" con estos payloads:
                payloads_bloque = [p.payload for p in bloque]
                # Si quieres, dedup de texto aquí (entre bloques) antes de mandar.
                # emitir_respuesta_parcial(payloads_bloque)  # <-- tu función de streaming
                puntos_acumulados.extend(bloque)

            # Conclusión final después del streaming por ventanas
            payloads = [p.payload for p in puntos_acumulados]
            payloads = deduplicar_payloads(payloads)
            if payloads and isinstance(payloads[0], dict) and "embedding" in payloads[0]:
                payloads = puntuar_chunks(payloads, vector)[:k]

            # Construir contexto para retorno (si esta función devuelve algo al caller)
            contextos = []
            for punto in puntos_acumulados:
                payload = punto.payload
                texto = payload.get("text", "")
                fch = payload.get("date", "")
                fuente = payload.get("source_type", "")
                seccion = payload.get("section", "")
                contextos.append(f"[fecha: {fch}] [fuente: {fuente}] [sección: {seccion}]\n{texto}")

            print("📦 Total puntos (rango/ventanas):", len(puntos_acumulados))
            print("🧹 Payloads después de dedup + rank:", len(payloads))

            return "\n\n".join(contextos), payloads

        else:
            # === Mantén tu lógica para exacta/mes ===
            must_conditions = []

            if fecha["tipo"] == "exacta":
                must_conditions.append(FieldCondition(
                    key="date_day", match=MatchValue(value=fecha["valor"])
                ))

            elif fecha["tipo"] == "mes":
                print("📆 Buscando por mes -------------------------:", fecha["valor"])
                mes = int(fecha["valor"][:2])
                anio = int(fecha["valor"][2:])
                dias_en_mes = monthrange(anio, mes)[1]

                fechas_mes = [f"{day:02}{mes:02}{anio}" for day in range(1, dias_en_mes + 1)]
                # Usar MatchAny para no disparar N queries
                filtro_mes = Filter(must=[FieldCondition(key="date_day", match=MatchAny(any=fechas_mes))])
                if tipo_evento:
                    filtro_mes.must.append(FieldCondition(key="event_type", match=MatchValue(value=tipo_evento)))

                try:
                    print("🧪 Ejecutando query_points (mes) con MatchAny:")
                    res = qdrant.search(
                        collection_name=COLLECTION_NAME,
                        query_vector=vector,
                        query_filter=filtro_mes,
                        limit=k,
                        with_payload=True
                    )
                    puntos.extend(res)
                except Exception as e:
                    print("❌ Error en query_points (mes):", e)
                    print("📋 Filtro usado:", filtro_mes)
                    raise

            if tipo_evento and fecha["tipo"] != "mes":
                must_conditions.append(FieldCondition(
                    key="event_type", match=MatchValue(value=tipo_evento)
                ))

            filtro = Filter(must=must_conditions) if must_conditions else None

            try:
                print("🧪 Ejecutando query_points (no rango):")
                resultados = qdrant.search(
                    collection_name=COLLECTION_NAME,
                    query_vector=vector,
                    query_filter=filtro,
                    limit=k,
                    with_payload=True
                )
                puntos.extend(resultados)
            except Exception as e:
                print("❌ Error en query_points (no rango):", e)
                print("📋 Filtro usado:", filtro)
                raise

    else:
        # Sin fecha, búsqueda general
        print("no hay fecha")
        must_conditions = []
        if tipo_evento:
            must_conditions.append(FieldCondition(
                key="event_type", match=MatchValue(value=tipo_evento)
            ))
        filtro = Filter(must=must_conditions) if must_conditions else None

        try:
            resultados = qdrant.search(
                collection_name=COLLECTION_NAME,
                query_vector=vector,
                query_filter=filtro,
                limit=k,
                with_payload=True
            )
            puntos.extend(resultados)
        except Exception as e:
            print("❌ Error en query_points (general):", e)
            print("📋 Filtro usado:", filtro)
            raise

    # ---------- Post-proceso común (no rango) ----------
    if not puntos:
        print("⚠️ No se encontraron puntos para esta consulta.")
        return "No se encontró contexto relacionado.", []

    payloads = [p.payload for p in puntos]
    payloads = deduplicar_payloads(payloads)

    if payloads and isinstance(payloads[0], dict) and "embedding" in payloads[0]:
        payloads = puntuar_chunks(payloads, vector)[:k]

    contextos = []
    for punto in puntos:
        payload = punto.payload
        texto = payload.get("text", "")
        fch = payload.get("date", "")
        fuente = payload.get("source_type", "")
        seccion = payload.get("section", "")
        contextos.append(f"[fecha: {fch}] [fuente: {fuente}] [sección: {seccion}]\n{texto}")

    print("📦 Total de puntos encontrados (sin deduplicar):", len(puntos))
    print("🧹 Total de payloads después de deduplicar:", len(payloads))

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

def responder_llm_gemini(pregunta: str, contexto: str, tipoPregunta: str) -> str:
    model = genai.GenerativeModel("models/gemini-1.5-pro")
    
    if tipoPregunta == "1":
        
        """"
        Tipo de prompt para cuando es normal
        """       
        
        prompt = f"""
        Eres un asistente experto de SUNASS.
        Con base en la siguiente información de contexto, responde la pregunta de manera clara y específica.

        📚 CONTEXTO:
        {contexto}

        ❓ PREGUNTA:
        {pregunta}
        """
        
        
    elif tipoPregunta == "2":
        """
        Tipo de prompt para cuando es particionada
        """
        
        prompt = f"""
        Eres un asistente experto de SUNASS.
        Esto es una pregunta particionada de un contexto particionado, se te irá pasando una por una los contextos no des una conclución de la pregunta, es importante que la respuesta sea clara y especifica.
        
        📚 CONTEXTO:
        {contexto}

        ❓ PREGUNTA:
        {pregunta}
        
        """
        
    elif tipoPregunta == "3":
        
        """
        Tipo de pregunta para cuando es final de una particionada
        """
        
        prompt = f"""
        Eres un asistente experto de SUNASS.

        Esto es prompt para la pregunta final, se te pasará el contexto pasado generado pero para hacer exactamente lo que se pide en la pregunta, es importante que la respuesta sea de acorde a lo que dice en la pregunta.

        📚 CONTEXTO:
        {contexto}

        ❓ PREGUNTA:
        {pregunta}
        """
        
    elif tipoPregunta == "4":
        
        """
        Tipo de pregunta para generar respuestas pasadas
        """
        
        prompt = f"""
        Eres un asistente experto de SUNASS.

        Esto es prompt para la respuesta en el contexto de la respuesta pasada o la ultima respuesta generada, aca ya sea pregunta o una orden como generar o hacer un resumen debes hacerlo lo mas preciso a lo que se te envía la ordenanza

        📚 CONTEXTO:
        {contexto}

        Ordenanza:
        {pregunta}
        """

    try:
        response = model.generate_content(prompt)
        return response.text.strip() if response.text else "⚠️ No se generó respuesta."
    except Exception as e:
        return f"❌ Error al generar respuesta con Gemini: {str(e)}"



def clasificar_tipo_pregunta(pregunta: str, cantidad_payloads: int, umbral: int = 6, ultimo_mensaje: str = "") -> str:
    """
    Devuelve "1", "2" o "3" según el tipo de prompt:
    - "1": pregunta normal
    - "2": pregunta con partición de contexto
    - "3": pregunta que hace referencia a una respuesta previa
    """
    pregunta = pregunta.lower()

    #  Detectar si hace referencia a una respuesta previa
    referencias = [
        r"\b(respuesta pasada|respuesta anterior|de lo anterior|según lo anterior)\b",
        r"\b(resúmelo|resumen|tabla de resumen|hazme una tabla|puedes resumir)\b",
        r"\b(según el contexto anterior|analiza lo anterior)\b"
    ]

    hace_referencia = any(re.search(pat, pregunta) for pat in referencias)

    print(" Hace referencia a una respuesta previa:", hace_referencia)
    print("ultimo mensaje:", ultimo_mensaje)
    
    if hace_referencia and ultimo_mensaje.strip():
        return "3"  # Conclusión o resumen posterior

    if cantidad_payloads > umbral:
        return "2"  # Particionada

    return "1"  # Normal



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

def responder_chatbot(pregunta: str) -> str:
    ref_embeddings = get_event_type_embeddings()
    contexto, payloads = buscar_contexto_openai(pregunta)

    if pregunta_es_conteo(pregunta):
        tipo_evento = extraer_tipo_evento(pregunta)
        if not tipo_evento:
            return "No se pudo identificar el tipo de evento a contar."

        eventos = filtrar_eventos_por_similitud(payloads, ref_embeddings, tipo_evento)
        plural = {
            "interrupcion": "interrupciones",
            "denuncia": "denuncias",
            "supervision": "supervisiones"
        }.get(tipo_evento, tipo_evento + "s")

        if not eventos:
            return f"No se encontraron {plural} en el contexto."

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
            return responder_llm_langchain(pregunta, contexto, payloads)
        else:
            return responder_llm_groq(pregunta, contexto, payloads)

    else:
        if USAR_LANGCHAIN:
            return responder_llm_langchain(pregunta, contexto, payloads)
        else:
            return responder_llm_groq(pregunta, contexto, payloads)
        
    


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
