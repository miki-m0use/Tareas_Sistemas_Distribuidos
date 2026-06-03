import json
import time
import random
import requests
import redis
import sys
import threading

# pyrefly: ignore [missing-import]
from confluent_kafka import Consumer, Producer, KafkaError, TopicPartition
from metricas import metricas as metricas_module  # FIX: importar el módulo, no variables sueltas
from metricas.metricas import (
    registrar_metrica, calcular_estadisticas,
    registrar_reintento, registrar_recuperada,
    registrar_dlq, registrar_backlog,
    registrar_inicio_recuperacion
)


# ============================================================
# CONFIGURACIÓN GENERAL DEL WORKER
# ============================================================

# Si supera este número, se manda a la DLQ.
MAX_REINTENTOS = 2

# Tiempo que una respuesta queda guardada en Redis antes de expirar.
TTL = 128 # es la cantida mas comun que he  visto



# URL del servicio FastAPI del generador de respuestas.
# Este worker llamará a esta URL cuando tenga un cache MISS.
GENERADOR_URL = "http://generador_respuestas:8000/procesar"

# Conexión a Redis.
# "redis" es el nombre del servicio dentro de docker-compose.
r = redis.Redis(host="redis", port=6379, db=0, decode_responses=True)

# Lock para que las métricas finales no se entrelacen visualmente
# cuando hay múltiples workers terminando al mismo tiempo.
_print_lock = threading.Lock()


# ============================================================
# CONFIGURACIÓN DE KAFKA
# ============================================================

conf_consumer = {
    # Dirección interna del servicio Kafka en Docker
    'bootstrap.servers': 'kafka:29092',

    # Todos los consumers con este mismo grupo se reparten los mensajes.
    # Si levantas 3 workers, Kafka divide la carga entre ellos
    'group.id': 'workers-group',

    # Si no hay offset guardado, empieza desde el mensaje más antiguo
    'auto.offset.reset': 'earliest',

    # Desactivamos commit automático
    # Así confirmamos manualmente cuando ya procesamos un mensaje
    'enable.auto.commit': False
}

# Producer usado por este worker para reenviar mensajes a retry o DLQ.
conf_producer = {
    'bootstrap.servers': 'kafka:29092'
}

consumer = Consumer(conf_consumer)
producer = Producer(conf_producer)

# Este worker escucha dos tópicos:
# 1. consultas-principales: consultas nuevas
# 2. consultas-reintentos: consultas que fallaron y se vuelven a intentar
consumer.subscribe(['consultas-principales', 'consultas-reintentos'])


# ============================================================
# FUNCIONES AUXILIARES
# ============================================================

def obtener_key_cache(consulta):
    """
    Construye la clave que se usará en Redis.

    La idea es que una misma consulta siempre genere la misma key.
    Si esa key existe en Redis, entonces es cache HIT.
    Si no existe, es cache MISS.
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
    """
    Pregunta a Redis cuántas claves han sido expulsadas por evicción.
    Esto sirve para métricas.
    """

    info = r.info("stats")
    return info.get("evicted_keys", 0)


def obtener_backlog():
    """
    Calcula cuántos mensajes están pendientes en el tópico principal.

    Lo hace comparando el offset más alto disponible (high watermark)
    con el offset que el consumer ya confirmó (committed offset).
    La diferencia es la cantidad de mensajes que aún no se han procesado.
    """
    try:
        metadata = consumer.list_topics(topic='consultas-principales', timeout=2)
        particiones = metadata.topics['consultas-principales'].partitions
        total_pendientes = 0

        for p_id in particiones:
            tp = TopicPartition('consultas-principales', p_id)
            low, high = consumer.get_watermark_offsets(tp, timeout=2)
            committed = consumer.committed([tp], timeout=2)
            offset_actual = committed[0].offset if committed[0].offset >= 0 else low
            pendientes = max(0, high - offset_actual)
            total_pendientes += pendientes

        return total_pendientes
    except Exception:
        # Si no se puede calcular, devolvemos 0 para no interrumpir el flujo
        return 0


def mandar_a_reintento(payload):
    """
    Envía una consulta fallida al tópico de reintentos.

    Se aumenta retry_count para saber cuántas veces se ha intentado procesar.
    """

    payload["retry_count"] += 1

    # FIX: guardamos el timestamp de cuándo se puede procesar este reintento.
    # El worker esperará hasta ese momento antes de procesarlo, evitando que
    # el reintento llegue inmediatamente cuando el generador aún está caído.
    # Delay exponencial: 2^retry_count segundos (intento 1 → 2s, intento 2 → 4s)
    delay_segundos = 2 ** payload["retry_count"]
    payload["retry_not_before"] = time.time() + delay_segundos

    producer.produce(
        'consultas-reintentos',
        key=payload.get("id", "sin-id"),
        value=json.dumps(payload)
    )

    # poll(0) permite que Kafka procese internamente el envío.
    producer.poll(0)

    # Registramos en métricas que hubo un reintento
    registrar_reintento()

    print(f"  ↩ Consulta {payload['id']} → reintentos (intento {payload['retry_count']}, delay {delay_segundos}s)")


def mandar_a_dlq(payload, motivo="max reintentos alcanzado"):
    """
    Envía una consulta a la DLQ.

    La DLQ guarda mensajes que ya no se pudieron resolver después de varios intentos.
    """

    payload["motivo_dlq"] = motivo

    producer.produce(
        'consultas-dlq',
        key=payload.get("id", "sin-id"),
        value=json.dumps(payload)
    )

    producer.poll(0)

    # Registramos en métricas que hubo un envío a DLQ
    registrar_dlq()

    print(f"  ✗ Consulta {payload['id']} → DLQ ({motivo})")





# ============================================================
# PROCESAMIENTO DE UNA CONSULTA
# ============================================================

def procesar_mensaje(payload):
    """
    Procesa una consulta completa.

    Flujo:
    1. Extrae la consulta desde el mensaje Kafka.
    2. Construye la key de Redis.
    3. Busca si la respuesta ya está en caché.
    4. Si hay HIT, devuelve el resultado guardado.
    5. Si hay MISS, llama al generador de respuestas por HTTP.
    6. Guarda el resultado en Redis.
    7. Registra métricas.
    """

    consulta = payload["consulta_data"]
    inicio = time.perf_counter()

    # FIX: si el mensaje tiene un delay de reintento, esperamos hasta que sea el momento.
    # Esto implementa el backoff exponencial real: el worker espera en vez de procesar de inmediato.
    retry_not_before = payload.get("retry_not_before")
    if retry_not_before is not None:
        espera = retry_not_before - time.time()
        if espera > 0:
            print(f"  ⏳ Esperando {round(espera, 1)}s antes de reintentar {payload.get('id', '?')}...")
            time.sleep(espera)

    # Revisamos si este mensaje viene del tópico de reintentos.
    # Si retry_count > 0, significa que ya falló al menos una vez antes.
    es_reintento = payload.get("retry_count", 0) > 0

    # 1. Construir key para buscar en Redis
    key = obtener_key_cache(consulta)

    # 2. Preguntar a Redis si ya existe esa respuesta
    cached = r.get(key)

    if cached is not None:
        # Si existe, es cache HIT.
        resultado = json.loads(cached)

        latencia = time.perf_counter() - inicio
        registrar_metrica("HIT", latencia, obtener_evictions())

        # Si era un reintento y salió HIT, significa que se recuperó exitosamente.
        if es_reintento:
            registrar_recuperada()

        print(f"  ✓ HIT  | {consulta['query']} | latencia: {round(latencia, 4)}s")

        return resultado, False

    # Si llegamos aquí, no estaba en caché.
    # Por lo tanto, es cache MISS.



    try:
        # Llamada HTTP al generador de respuestas.
        # Se manda la consulta como JSON.
        response = requests.post(GENERADOR_URL, json=consulta, timeout=10)

        # Si FastAPI responde con error HTTP, esto lanza excepción.
        response.raise_for_status()

        # Se obtiene el resultado real calculado por el generador de respuestas.
        resultado = response.json()["result"]

    except Exception as e:
        # Si el generador está caído, lento o responde mal,
        # se lanza una excepción para activar retry/DLQ.
        raise Exception(f"Error en generador de respuestas: {e}")

    # Guardar resultado en Redis.
    # setex guarda con TTL, o sea, la key expira después de cierto tiempo.
    r.setex(key, TTL, json.dumps(resultado))

    latencia = time.perf_counter() - inicio
    registrar_metrica("MISS", latencia, obtener_evictions())

    # Si era un reintento y salió MISS pero se procesó bien, también se recuperó.
    if es_reintento:
        registrar_recuperada()

    print(f"  ✓ MISS | {consulta['query']} | latencia: {round(latencia, 4)}s")

    return resultado, True


# ============================================================
# LOOP PRINCIPAL DEL WORKER
# ============================================================

print("=" * 60)
print("WORKER KAFKA INICIADO")
print("Suscrito a: consultas-principales, consultas-reintentos")
print(f"MAX_REINTENTOS: {MAX_REINTENTOS}")
print("=" * 60)

mensajes_procesados = 0
shutdown = False
shutdown_pendiente = False  # FIX: True cuando recibimos SHUTDOWN pero aún hay reintentos pendientes

# Indica si el sistema está actualmente en un período de fallas.
# Se activa con el primer fallo y se desactiva cuando se procesa exitosamente.
en_periodo_falla = False

# Contador para medir el backlog cada cierta cantidad de mensajes.
# No lo medimos en cada mensaje para no agregar demasiada latencia.
contador_backlog = 0

try:
    while not shutdown:

        # consumer.poll espera un mensaje desde Kafka.
        # Si no llega nada en 1 segundo, devuelve None.
        msg = consumer.poll(timeout=1.0)

        if msg is None:
            # Si la cola se vació, no llegarán mensajes (msg es None). Si estamos en recuperación,
            # medimos el backlog para registrar que llegó a 0 y cerrar el cálculo del recovery_time.
            # FIX: acceder al módulo, no a la variable importada (que siempre sería None)
            if metricas_module.tiempo_inicio_recuperacion is not None:
                backlog_actual = obtener_backlog()
                registrar_backlog(backlog_actual)

            # FIX: si recibimos SHUTDOWN y ya no hay más mensajes (incluido consultas-reintentos),
            # es seguro apagarse. El timeout de 1s en poll garantiza que esperamos lo suficiente.
            if shutdown_pendiente:
                print("Cola de reintentos vaciada. Terminando worker.")
                shutdown = True
                break

            continue

        # Revisión de errores del mensaje.
        # UNKNOWN_TOPIC_OR_PART es normal al inicio (los tópicos aún no existen).
        # _PARTITION_EOF indica que se llegó al final de la partición.
        if msg.error():
            if msg.error().code() in (KafkaError._PARTITION_EOF, KafkaError.UNKNOWN_TOPIC_OR_PART):
                continue
            else:
                print(f"Error Kafka: {msg.error()}", file=sys.stderr)
                continue

        # Convertimos el mensaje desde bytes/string JSON a diccionario Python.
        try:
            payload = json.loads(msg.value().decode('utf-8'))

        except Exception as e:
            print(f"Error deserializando mensaje: {e}", file=sys.stderr)

            # Como este mensaje no se puede leer, se confirma para no quedar atrapados.
            consumer.commit(message=msg)
            continue

        consulta_data = payload.get("consulta_data", {})

        # Poison Pill:
        # mensaje especial para apagar el worker ordenadamente.
        if consulta_data.get("query") == "SHUTDOWN":
            print("\nSeñal de cierre recibida.")
            consumer.commit(message=msg)

            # FIX: antes de apagarse, verificamos si quedan mensajes en consultas-reintentos.
            # Si hay mensajes pendientes ahí, el worker los debe procesar antes de terminar
            # para que no se pierdan ni queden sin llegar a DLQ.
            print("Verificando si quedan mensajes en consultas-reintentos...")
            try:
                meta = consumer.list_topics(topic='consultas-reintentos', timeout=3)
                partes = meta.topics['consultas-reintentos'].partitions
                pendientes_retry = 0
                for p_id in partes:
                    tp = TopicPartition('consultas-reintentos', p_id)
                    low, high = consumer.get_watermark_offsets(tp, timeout=2)
                    committed = consumer.committed([tp], timeout=2)
                    offset_actual = committed[0].offset if committed[0].offset >= 0 else low
                    pendientes_retry += max(0, high - offset_actual)
            except Exception:
                pendientes_retry = 0

            if pendientes_retry > 0:
                print(f"  Hay {pendientes_retry} mensajes en consultas-reintentos. Procesándolos antes de cerrar...")
                # Seguimos corriendo el loop; shutdown se activará cuando se vacíe
                # el tópico de reintentos (msg is None por 3 segundos seguidos).
                shutdown_pendiente = True
            else:
                print("Terminando worker (no hay reintentos pendientes).")
                shutdown = True

            if metricas_module.tiempo_inicio_recuperacion is not None:
                backlog_actual = obtener_backlog()
                registrar_backlog(backlog_actual)

            if shutdown:
                break
            continue

        # Medimos el backlog. Querying Kafka offsets (list_topics, committed, get_watermark_offsets)
        # requiere llamadas de red costosas. Para no enlentecer el worker, medimos cada 50 mensajes
        # durante la recuperación y cada 200 en funcionamiento normal.
        contador_backlog += 1
        intervalo_backlog = 50 if metricas_module.tiempo_inicio_recuperacion is not None else 200
        if contador_backlog % intervalo_backlog == 0:
            backlog_actual = obtener_backlog()
            registrar_backlog(backlog_actual)

        # Procesamos el mensaje normalmente.
        try:
            resultado, was_miss = procesar_mensaje(payload)
            mensajes_procesados += 1

            # Si estábamos en período de falla y el generador volvió a contestar (MISS exitoso),
            # comienza la fase de recuperación de la cola (drenado del backlog).
            if en_periodo_falla and was_miss:
                registrar_inicio_recuperacion()
                en_periodo_falla = False
                print("  ✓ Generador recuperado. Iniciando vaciado de la cola (backlog)...")

        except Exception as e:
            # Si algo falla, marcamos que el sistema está en falla
            en_periodo_falla = True

            # Si algo falla, revisamos cuántos reintentos lleva.
            retry_count = payload.get("retry_count", 0)

            if retry_count >= MAX_REINTENTOS:
                # Si ya falló demasiadas veces, se manda a DLQ.
                mandar_a_dlq(payload, motivo=str(e))
            else:
                # Si todavía puede reintentarse, se manda al tópico de retry.
                mandar_a_reintento(payload)

        # Commit manual:
        # se confirma que este mensaje ya fue atendido
        # aunque haya terminado en retry o DLQ.
        consumer.commit(message=msg)

except KeyboardInterrupt:
    print("\nWorker interrumpido manualmente.")

finally:
    # Cerramos conexiones al terminar.
    consumer.close()
    producer.flush()

    # Usamos un lock para que si hay múltiples workers terminando al mismo tiempo,
    # las métricas de cada uno se impriman de forma ordenada y no entrelazada.
    with _print_lock:
        time.sleep(0.5)  # pequeño delay para que otros workers terminen sus prints en curso
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

        print("=" * 60)