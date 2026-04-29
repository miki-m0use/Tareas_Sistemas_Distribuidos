import random

# Lista de las zonas disponibles
zonas = ["Zona_1", "Zona_2", "Zona_3", "Zona_4", "Zona_5"]

# Tipos de consultas posibles
queries = ["Q1", "Q2", "Q3", "Q4", "Q5"]


def elegir_zona(modo="uniforme"):
    """
    Selecciona una zona según el tipo de tráfico.

    - modo "uniforme": todas las zonas tienen la misma probabilidad (en esta función está por defecto el modo uniforme, por si se llama la función sin argumento)
    - modo "zipf": algunas zonas aparecen de manera más frecuente (o sea que simula popularidad)
    """
    if modo == "uniforme":
        # Selección completamente aleatoria
        return random.choice(zonas)

    elif modo == "zipf":
        # Algunas zonas son más consultadas/probables que otras
        # Dejamos Zona_1 como la más consultada/probable
        probabilidades = [0.5, 0.2, 0.15, 0.1, 0.05]

        # random.choices permite elegir con pesos, o sea que elige una zona al azar pero con probabilidades distintas
        return random.choices(zonas, weights=probabilidades)[0]


def elegir_query():
    """
    Selecciona aleatoriamente el tipo de consulta (Q1 a Q5).
    """
    return random.choice(queries)


def generar_consulta(modo):
    """
    Genera una consulta completa simulando una petición de usuario.

    Incluye:
    - tipo de query (Q1–Q5)
    - zona geográfica
    - parámetro de confianza (random entre 0 y 1)
    """

    # Elegimos zona según el tipo de tráfico
    zona = elegir_zona(modo)

    # Elegimos tipo de consulta
    query = elegir_query()

    # Generamos un confidence con pocos valores posibles 
    confidence = round(random.uniform(0.0, 1.0), 2)

    # Caso especial: Q4 necesita dos zonas distintas
    if query == "Q4":
        zona_b = elegir_zona(modo)

        # aseguramos que no sean iguales
        while zona_b == zona:
            zona_b = elegir_zona(modo)

        return {
            "query": query,
            "zona_a": zona,
            "zona_b": zona_b,
            "confidence": confidence
        }

    # resto de queries
    return {
        "query": query,
        "zona": zona,
        "confidence": confidence
    }

