from generador_trafico.traffic_generator import generar_consulta
from generador_respuestas.main import q1_count, q2_area, q3_density, q4_compare, q5_confidence_dist

# Aquí se elige el número de consultas a generar
N = 10

# IMPORTANTE. Aquí se cambia el modo de tráfico/la distribución (uniforme o zipf)
modo_trafico = "uniforme"
# modo_trafico = "zipf"

print(f"TRÁFICO {modo_trafico.upper()}\n")

for i in range(N):
    consulta = generar_consulta(modo_trafico)
    resultado = ejecutar_consulta(consulta)

    print(f"Consulta {i + 1}:")
    print("Datos consulta:", consulta)
    print("Resultado:", resultado)
    print("-" * 40)
