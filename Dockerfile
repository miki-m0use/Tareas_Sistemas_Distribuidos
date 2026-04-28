
FROM python:3.10-slim


WORKDIR /app


RUN pip install --no-cache-dir polars==0.20.10 redis==5.0.3 numpy==1.26.4


COPY . .


ENV PYTHONPATH=/app


CMD ["python", "-m", "generador_trafico.main_trafico"]