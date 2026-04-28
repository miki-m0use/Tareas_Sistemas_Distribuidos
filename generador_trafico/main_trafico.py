from generador_trafico.traffic_generator import generar_consulta
from generador_respuestas.main import q1_count, q2_area, q3_density, q4_compare, q5_confidence_dist

# Aquí se elige el número de consultas a generar
N = 10

# IMPORTANTE. Aquí se cambia el modo de tráfico/la distribución (uniforme o zipf)
modo_trafico = "uniforme"
# modo_trafico = "zipf"
    
def ejecutar_consulta(c):
    """
    Recibe una consulta generada por el generador de tráfico
    y llama a la función correspondiente del generador de respuestas.
    """

    query = c["query"]
    confidence = c["confidence"]

    if query == "Q1":
        return q1_count(c["zona"], confidence)

    elif query == "Q2":
        return q2_area(c["zona"], confidence)

    elif query == "Q3":
        return q3_density(c["zona"], confidence)

    elif query == "Q4":
        return q4_compare(c["zona_a"], c["zona_b"], confidence)

    elif query == "Q5":
        return q5_confidence_dist(c["zona"])

    else:
        return "Consulta no válida"

print(f"TRÁFICO {modo_trafico.upper()}\n")

for i in range(N):
    consulta = generar_consulta(modo_trafico)
    resultado = ejecutar_consulta(consulta)

    print(f"Consulta {i + 1}:")
    print("Datos consulta:", consulta)
    print("Resultado:", resultado)
    print("-" * 40)
