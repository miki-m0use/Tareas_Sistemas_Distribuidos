from generador_trafico.traffic_generator import generar_consulta 
from cache.cache_main import preguntarle_al_cache
from metricas.metricas import registrar_metrica

# Aquí se elige el número de consultas a generar
N = 100

# IMPORTANTE. Aquí se cambia el modo de tráfico/la distribución (uniforme o zipf)
modo_trafico = "uniforme"
# modo_trafico = "zipf"

print(f"GENERADOR DE TRÁFICO - MODO {modo_trafico.upper()}")
print("-" * 60)

for i in range(N):
    consulta = generar_consulta(modo_trafico)
    respuesta, estado_cache, latencia = preguntarle_al_cache(consulta)

    print(f"Consulta {i + 1}")
    print("Consulta generada:", consulta)
    print("Cache:", respuesta["cache"])
    print("Key:", respuesta["key"])
    print("Resultado:", respuesta["resultado"])
    print("-" * 60)

