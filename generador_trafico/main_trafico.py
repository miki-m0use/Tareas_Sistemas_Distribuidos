from generador_trafico.traffic_generator import generar_consulta 
from cache.cache_main import preguntarle_al_cache
from metricas.metricas import calcular_estadisticas

# Aquí se elige el número de consultas a generar
N = 5000
# IMPORTANTE. Aquí se cambia el modo de tráfico/la distribución (uniforme o zipf)
#modo_trafico = "uniforme"
modo_trafico = "zipf"

print(f"GENERADOR DE TRÁFICO - MODO {modo_trafico.upper()}")
print("-" * 60)

for i in range(N):
    consulta = generar_consulta(modo_trafico)
    respuesta, estado_cache, latencia = preguntarle_al_cache(consulta)

    print(f"Consulta {i + 1}")
    print("Consulta generada:", consulta)
    print("Estado Cache:", estado_cache)
    print("Latencia:", round(latencia, 4), "segundos")
    print("Resultado matemático:", respuesta)
    print("-" * 60)


print("\n" + "=" * 40)
print("MÉTRICAS FINALES DE LA SIMULACIÓN")
print("=" * 40)
estadisticas = calcular_estadisticas()
if isinstance(estadisticas, dict):
    for clave, valor in estadisticas.items():
        print(f"{clave}: {valor}")
else:
    print(estadisticas)

