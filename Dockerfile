FROM python:3.10-slim


WORKDIR /app


RUN pip install --no-cache-dir \
    polars==0.20.10 \
    redis==5.0.3 \
    numpy==1.26.4 \
    fastapi==0.110.0 \
    uvicorn==0.34.0 \
    confluent-kafka==2.2.0 \
    requests==2.31.0 

COPY . .


ENV PYTHONPATH=/app