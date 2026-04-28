import redis
import json
import time

from metricas.metricas import registrar_metrica
from generador_respuestas.main import q1_count, q2_area, q3_density, q4_compare, q5_confidence_dist
from generador_trafico.main import ejecutar_consulta as calcular_consulta # me di cuenta hiciste una wea parecida pompom, asi que lo estoy probando

r = redis.Redis(host='localhost', port=6379, db=0) 

TTL = 60

""""
En cache tendremos que hacer las sigueintes cosas:
 - pedir del cache (sacar los datos) 
 - ingresar a cache (osea ingresar datos)
 - preguntarle si estan los datos buscados

 como estamos trabajando con redis el sistema se basa en una clave-valor
 etnonces debemos obtener la key primero
"""


def obtener_key(consulta):

    """
    Obtiene la clave de cache para una consulta específica.
    """
    q = consulta["query"]  # esto sirve para obtener la consulta de la peticion
    conf = consulta.get("confidence", 0.0) # esto sirve para obtener la confianza de la peticion, como diccionario, "confidence" es la clave y 0.0 es su valor por defecto

    if q == "Q1":
        return f"count: {consulta['zona']}:conf={consulta['confidence']}"
    elif q == "Q2":
        return f"area:{consulta['zona']}:conf={consulta['confidence']}"
    elif q == "Q3":
        return f"density:{consulta['zona']}:conf={consulta['confidence']}"
    elif q == "Q4":
        return f"compare:density:{consulta['zona_a']}:{consulta['zona_b']}:conf={consulta['confidence']}"
    elif q == "Q5":
        bins = consulta.get("bins", 5)
        return f"confidence_dist:{consulta['zona']}:bins={bins}"



def resolver_consulta(consulta):

    key = cache_key(consulta)
    inicio = time.time()

    cached = r.get(key)

    if cached is not None:
        resultado = json.loads(cached)# esto retorna los datos que estan en el cache, se pone json.loads 
        #para que se convierta en diccionario, que es un tipo de dato que podemos usar facilement
        latencia = time.time() - inicio
        return resultado, "HIT", latencia

    else: #cache MISS
        #como no esta en cache hay que enviarla al generador de respuestas y luego ingresarla al cache
        # esto deberia comunicarse con el generador de respuestas, con esa respuesta guardamos en la latencia en metricas
        # y la respuesta se guarda en el cache (redis) por TTL segundos 

        resultado = procesar_consulta(consulta)
        #agora hay que guardarlo en el cache, se pone json.dump para convertirlo en diccionario
        r.setex(key, TTL, json.dumps(resultado))

        #ahora guardamos en metricas el miss y la latencia
        registrar_metrica("MISS", latencia)
        
        

        

    

    
