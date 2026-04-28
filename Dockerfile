# Usamos una versión ligera de Python
FROM python:3.10-slim

# Definimos la carpeta de trabajo dentro del contenedor
WORKDIR /app

# Instalamos las librerías necesarias
RUN pip install --no-cache-dir polars==0.20.10 redis==5.0.3 numpy==1.26.4

# Copiamos todo tu código al contenedor
COPY . .

# Agregamos esta variable para que Python encuentre tus subcarpetas fácilmente
ENV PYTHONPATH=/app

# El comando que arranca el sistema
CMD ["python", "generador_trafico/main_trafico.py"]