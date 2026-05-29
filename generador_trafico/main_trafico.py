import json
import time
import sys
# pyrefly: ignore [missing-import]
from confluent_kafka import Producer
# pyrefly: ignore [missing-import]
from confluent_kafka.admin import AdminClient, NewTopic # Importamos el cliente de administración
from generador_trafico.traffic_generator import generar_consulta

N = 5000
modo_trafico = "zipf"

conf_kafka = {'bootstrap.servers': "kafka:29092"}

admin_client = AdminClient(conf_kafka)
topicos_a_crear = [
    NewTopic('consultas-principales', num_partitions=3, replication_factor=1),
    NewTopic('consultas-reintentos', num_partitions=3, replication_factor=1),
    NewTopic('consultas-dlq', num_partitions=1, replication_factor=1)
]

# Intentamos crear los tópicos en el broker KRaft
fs = admin_client.create_topics(topicos_a_crear)
for topic, f in fs.items():
    try:
        f.result() # Espera a que termine la creacion
        print(f"Tópico '{topic}' creado exitosamente con múltiples particiones.")
    except Exception as e:
        # Si ya existe, Kafka tiraun aviso que podemos ignorar
        print(f"Aviso sobre tópico '{topic}': {e}")

# Inicializamos el productor normal
producer = Producer(conf_kafka)

def delivery_report(err, msg):
    if err is not None:
        print(f"Error al entregar mensaje en Kafka: {err}", file=sys.stderr)

print(f"\nPRODUCER KAFKA INICIALIZADO - GENERANDO TRAFICO EN MODO: {modo_trafico.upper()}")
print("-" * 60)

for i in range(N):
    consulta = generar_consulta(modo_trafico)
    
    payload = {
        "id": f"req_{int(time.time()*1000)}_{i}",
        "timestamp_creacion": time.time(),
        "retry_count": 0,
        "consulta_data": consulta
    }
    
    # Al pasar la clave 'key=payload["id"]', Kafka le aplica un algoritmo Hash 
    # para repartir las consultas equitativamente entre las 4 particiones
    producer.produce(
        'consultas-principales', 
        key=payload["id"], 
        value=json.dumps(payload), 
        callback=delivery_report
    )
    
    producer.poll(0)
    
    #esto en el grafico de bloques es la primera parte 
    if (i + 1) % 500 == 0:
        print(f"-> Inyectados exitosamente {i + 1} mensajes JSON en Apache Kafka...") 
        
    time.sleep(0.002)

producer.flush()
print("-" * 60)
print("Enviando señal de término (Poison Pill) a los Workers...")

# Enviamos la señal SHUTDOWN a todas las particiones para que los 4 workers se enteren
for p in range(3):
    payload_termino = {"consulta_data": {"query": "SHUTDOWN"}}
    producer.produce('consultas-principales', value=json.dumps(payload_termino), partition=p)

producer.flush()

print("=" * 60)
print(f"PROCESO COMPLETADO: {N} consultas inyectadas y señal de cierre emitida.")
print("=" * 60)