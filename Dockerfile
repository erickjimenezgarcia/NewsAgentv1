# Usa una imagen base oficial de Python
FROM python:3.11-slim

# Establece directorio de trabajo
WORKDIR /app

# Copia el código del proyecto
COPY . .

# Instala dependencias
RUN pip install --no-cache-dir -r requirements.txt

# Expone el puerto (Railway usará esto)
EXPOSE 8000

# Comando de inicio
CMD ["uvicorn", "api_server:app", "--host", "0.0.0.0", "--port", "8000"]
