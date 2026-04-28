from generador_trafico.traffic_generator import generar_consulta
from cache.cache_main import procesar_consulta
from metricas.metricas import obtener_metricas

# Aquí se elige el número de consultas a generar
N = 10

# IMPORTANTE. Aquí se cambia el modo de tráfico/la distribución (uniforme o zipf)
modo_trafico = "uniforme"
# modo_trafico = "zipf"

print(f"GENERADOR DE TRÁFICO - MODO {modo_trafico.upper()}")
print("-" * 60)

for i in range(N):
    consulta = generar_consulta(modo_trafico)
    respuesta = procesar_consulta(consulta)

    print(f"Consulta {i + 1}")
    print("Consulta generada:", consulta)
    print("Cache:", respuesta["cache"])
    print("Key:", respuesta["key"])
    print("Resultado:", respuesta["resultado"])
    print("-" * 60)


print("\nMÉTRICAS FINALES")
print(obtener_metricas())
