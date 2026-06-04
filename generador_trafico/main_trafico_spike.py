import json
import time
import sys
# pyrefly: ignore [missing-import]
from confluent_kafka import Producer
# pyrefly: ignore [missing-import]
from confluent_kafka.admin import AdminClient, NewTopic
from generador_trafico.traffic_generator import generar_consulta

# ============================================================
# CONFIGURACIÓN — edita estos valores antes de cada corrida
# ============================================================

N            = 5000   # Cambia a 5000, 10000 o 15000 según la prueba
modo_trafico = "zipf"

SPIKE_ACTIVO = True    # False = prueba normal | True = con spike
SPIKE_INICIO = int(N * 0.4)   # El spike empieza al 32% de los mensajes
SPIKE_FIN    = int(N * 0.7)   # El spike termina al 48% de los mensajes
DELAY_NORMAL = 0.002
DELAY_SPIKE  = 0.00002  # 100x más rápido

# ============================================================
conf_kafka = {'bootstrap.servers': "kafka:29092"}
admin_client = AdminClient(conf_kafka)
topicos_a_crear = [
    NewTopic('consultas-principales', num_partitions=2, replication_factor=1),
    NewTopic('consultas-reintentos',  num_partitions=2, replication_factor=1),
    NewTopic('consultas-dlq',         num_partitions=1, replication_factor=1)
]
fs = admin_client.create_topics(topicos_a_crear)


for topic, f in fs.items():
    try:
        f.result()
        print(f"Tópico '{topic}' creado.")
    except Exception as e:
        print(f"Aviso '{topic}': {e}")

producer = Producer(conf_kafka)

def delivery_report(err, msg):
    if err is not None:
        print(f"Error Kafka: {err}", file=sys.stderr)

modo_str = "CON SPIKE" if SPIKE_ACTIVO else "SIN SPIKE"
print(f"\nPRODUCER — N={N} | {modo_trafico.upper()} | {modo_str}")
if SPIKE_ACTIVO:
    print(f"  Spike: mensajes {SPIKE_INICIO}–{SPIKE_FIN}")
print("-" * 60)

en_spike = False

for i in range(N): # la N es la cantidad de consultas que se van a generar
    if SPIKE_ACTIVO:
        if i == SPIKE_INICIO and not en_spike:
            en_spike = True
            print(f"\n⚡ SPIKE INICIADO (mensaje {i})\n")
        elif i == SPIKE_FIN and en_spike:
            en_spike = False
            print(f"\n SPIKE TERMINADO (mensaje {i})\n")

    consulta = generar_consulta(modo_trafico)
    payload = {
        "id": f"req_{int(time.time()*1000)}_{i}",
        "timestamp_creacion": time.time(),
        "retry_count": 0,
        "consulta_data": consulta,
        "en_spike": en_spike
    }
    producer.produce('consultas-principales', key=payload["id"],
                     value=json.dumps(payload), callback=delivery_report)
    producer.poll(0)

    if (i + 1) % 500 == 0:
        print(f"  [{'⚡ SPIKE' if en_spike else 'normal'}] {i+1} mensajes...")

    time.sleep(DELAY_SPIKE if en_spike else DELAY_NORMAL)

producer.flush()
print("-" * 60)
print("Enviando Poison Pills...")

for _ in range(3):
    for p in range(2): #el 3 es el numero de particiones
        producer.produce('consultas-principales',
                         value=json.dumps({"consulta_data": {"query": "SHUTDOWN"}}),
                         partition=p)
producer.flush()
print(f"COMPLETADO: {N} consultas ({modo_str}).")