from utils import load_api_gemini_key
import google.generativeai as genai


genai.configure(api_key=load_api_gemini_key())


model = genai.GenerativeModel("models/gemini-2.5-pro")

model = genai.GenerativeModel("models/gemini-1.5-pro")

# Pregunta de prueba
prompt = "¿Qué pasó en julio con las interrupciones del servicio de agua según SUNASS?"

response = model.generate_content(prompt)

# Muestra la respuesta
print(response.text)