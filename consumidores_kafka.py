import json
import time
import random
import requests
import redis
import sys

from confluent_kafka import Consumer, Producer, KafkaError
from metricas.metricas import registrar_metrica, calcular_estadisticas

# CONFIGURACIÓN

MAX_REINTENTOS = 3
TTL = 300
FAILURE_RATE = 0.1  # 10% de probabilidad de fallo simulado

# URL del generador de respuestas (FastAPI)
GENERADOR_URL = "http://generador_respuestas:8000/procesar"

# Conexión a Redis
r = redis.Redis(host="redis", port=6379, db=0, decode_responses=True)

# Configuración del consumidor Kafka
# group_id igual para todos los workers --- Kafka reparte las particiones automáticamente
conf_consumer = {
    'bootstrap.servers': 'kafka:29092',
    'group.id': 'workers-group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False  # commit manual para no perder mensajes ante fallos
}

# Configuración del producer (para reintentos y DLQ)
conf_producer = {
    'bootstrap.servers': 'kafka:29092'
}

consumer = Consumer(conf_consumer)
producer = Producer(conf_producer)

# Suscribimos al tópico principal Y al de reintentos
consumer.subscribe(['consultas-principales', 'consultas-reintentos'])

# FUNCIONES AUXILIARES

def obtener_key_cache(consulta):
    """
    Construye la clave de Redis para una consulta.
    Igual que en cache_main.py de la tarea 1.
    """
    q = consulta["query"]
    conf = consulta.get("confidence", 0.0)

    if q == "Q1":
        return f"count:{consulta['zona']}:conf={conf}"
    elif q == "Q2":
        return f"area:{consulta['zona']}:conf={conf}"
    elif q == "Q3":
        return f"density:{consulta['zona']}:conf={conf}"
    elif q == "Q4":
        return f"compare:density:{consulta['zona_a']}:{consulta['zona_b']}:conf={conf}"
    elif q == "Q5":
        bins = consulta.get("bins", 5)
        return f"confidence_dist:{consulta['zona']}:bins={bins}"


def obtener_evictions():
    """Obtiene el número de claves evictadas desde Redis."""
    info = r.info("stats")
    return info.get("evicted_keys", 0)


def mandar_a_reintento(payload):
    """
    Manda una consulta fallida al tópico de reintentos,
    incrementando el retry_count.
    """
    payload["retry_count"] += 1
    producer.produce(
        'consultas-reintentos',
        key=payload.get("id", "sin-id"),
        value=json.dumps(payload)
    )
    producer.poll(0)
    print(f"  ↩ Consulta {payload['id']} → reintentos (intento {payload['retry_count']})")


def mandar_a_dlq(payload, motivo="max reintentos alcanzado"):
    """
    Manda una consulta a la Dead Letter Queue.
    Se llega acá cuando retry_count >= MAX_REINTENTOS.
    """
    payload["motivo_dlq"] = motivo
    producer.produce(
        'consultas-dlq',
        key=payload.get("id", "sin-id"),
        value=json.dumps(payload)
    )
    producer.poll(0)
    print(f"  ✗ Consulta {payload['id']} → DLQ ({motivo})")


def simular_fallo():
    """
    Simula un fallo aleatorio con probabilidad FAILURE_RATE.
    Sirve para testear el sistema de reintentos.
    """
    return random.random() < FAILURE_RATE


# PROCESAMIENTO DE UNA CONSULTA

def procesar_mensaje(payload):
    """
    Lógica principal de procesamiento:
    1. Revisar caché (Redis)
    2. Si HIT → retornar resultado
    3. Si MISS → llamar al generador de respuestas via HTTP
    4. Si falla → lanzar excepción para que el caller maneje el reintento
    """
    consulta = payload["consulta_data"]
    inicio = time.perf_counter()

    # ── 1. Intentar obtener desde caché ──
    key = obtener_key_cache(consulta)
    cached = r.get(key)

    if cached is not None:
        # CACHE HIT
        resultado = json.loads(cached)
        latencia = time.perf_counter() - inicio
        registrar_metrica("HIT", latencia, obtener_evictions())
        print(f"  ✓ HIT  | {consulta['query']} | latencia: {round(latencia, 4)}s")
        return resultado

    # ── 2. Cache MISS: llamar al generador de respuestas ──
    # Simulamos fallo aleatorio antes de llamar al generador
    if simular_fallo():
        raise Exception("Fallo simulado aleatorio")

    try:
        response = requests.post(GENERADOR_URL, json=consulta, timeout=10)
        response.raise_for_status()
        resultado = response.json()["result"]
    except Exception as e:
        # Si el generador falla (caído, timeout, error), lanzamos excepción
        raise Exception(f"Error en generador de respuestas: {e}")

    # ── 3. Guardar en caché y registrar métrica ──
    r.setex(key, TTL, json.dumps(resultado))
    latencia = time.perf_counter() - inicio
    registrar_metrica("MISS", latencia, obtener_evictions())
    print(f"  ✓ MISS | {consulta['query']} | latencia: {round(latencia, 4)}s")

    return resultado

# LOOP PRINCIPAL DEL WORKER

print("=" * 60)
print("WORKER KAFKA INICIADO")
print(f"Suscrito a: consultas-principales, consultas-reintentos")
print(f"MAX_REINTENTOS: {MAX_REINTENTOS} | FAILURE_RATE: {FAILURE_RATE*100}%")
print("=" * 60)

mensajes_procesados = 0
shutdown = False

try:
    while not shutdown:
        # Esperamos un mensaje hasta 1 segundo
        msg = consumer.poll(timeout=1.0)

        if msg is None:
            continue

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(f"Error Kafka: {msg.error()}", file=sys.stderr)
                continue

        # Deserializamos el mensaje
        try:
            payload = json.loads(msg.value().decode('utf-8'))
        except Exception as e:
            print(f"Error deserializando mensaje: {e}", file=sys.stderr)
            consumer.commit(message=msg)
            continue

        consulta_data = payload.get("consulta_data", {})

        # ── Poison Pill: señal de fin de simulación ──
        if consulta_data.get("query") == "SHUTDOWN":
            print("\nSeñal de cierre recibida. Terminando worker...")
            shutdown = True
            consumer.commit(message=msg)
            break

        # ── Procesar el mensaje ──
        try:
            procesar_mensaje(payload)
            mensajes_procesados += 1

        except Exception as e:
            # Algo falló al procesar
            retry_count = payload.get("retry_count", 0)

            if retry_count >= MAX_REINTENTOS:
                # Ya se reintentó demasiadas veces --- DLQ
                mandar_a_dlq(payload, motivo=str(e))
            else:
                # Todavía tiene reintentos disponibles
                mandar_a_reintento(payload)

        # Commit manual: solo confirmamos que procesamos este mensaje
        consumer.commit(message=msg)

except KeyboardInterrupt:
    print("\nWorker interrumpido manualmente.")

finally:
    consumer.close()
    producer.flush()

    print("\n" + "=" * 60)
    print("MÉTRICAS FINALES DEL WORKER")
    print("=" * 60)
    print(f"Mensajes procesados exitosamente: {mensajes_procesados}")
    estadisticas = calcular_estadisticas()
    if isinstance(estadisticas, dict):
        for clave, valor in estadisticas.items():
            print(f"{clave}: {valor}")
    else:
        print(estadisticas)