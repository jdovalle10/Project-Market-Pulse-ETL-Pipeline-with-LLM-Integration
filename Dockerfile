# Dockerfile

# 1. Imagen base: Usamos una imagen oficial de Python que ya tiene las herramientas base.
FROM python:3.11-slim

# 2. Establecer el directorio de trabajo dentro del contenedor
WORKDIR /app

# 3. Copiar el archivo de requisitos e instalar las dependencias
# Esto se hace primero para aprovechar el caching de Docker.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 4. Copiar el script principal y otros archivos necesarios (como los datos de prueba, si quieres que estén dentro)
# Copia el script principal
COPY main.py .

# 5. Definir el comando de entrada por defecto
# Usaremos 'python' como punto de entrada. El operador de orquestación (Airflow/Batch) 
# definirá los argumentos (clean, enrich, load) en tiempo de ejecución.

ENTRYPOINT ["python", "main.py"]

# 6. Comando por defecto (Opcional, pero útil para correr la imagen sin argumentos)
# Puedes borrar esta línea si prefieres no tener un comando por defecto.
# CMD ["--help"]