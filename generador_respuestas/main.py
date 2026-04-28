import polars as pl
import time
import generador_respuestas.bounding_boxes as bbox

#cargamos el dataset de los edificios
dataset = pl.read_csv('./967_buildings/967_buildings.csv')
#print(dataset.head())

#las zonas estan definida en otro archivo
# por lo tanto ahora queda solo mostar las consultas de ejemplo

def filtrar_por_zona(zone_id, confidence_min=0.0):
    """
    Filtra el dataset según la zona geográfica y la confianza mínima (se usará en Q5).
    """
    zona = bbox.zonas[zone_id]

    filtro = (
        (dataset["latitude"] >= zona["lat"][0]) &
        (dataset["latitude"] <= zona["lat"][1]) &
        (dataset["longitude"] >= zona["lon"][0]) &
        (dataset["longitude"] <= zona["lon"][1]) &
        (dataset["confidence"] >= confidence_min)
    )

    return dataset.filter(filtro)

#CONSULTA 1: Conteo de edificios de una zona
def q1_count(zone_id, confidence_min = 0.0):
    """
    se pide que se simule un tiempo de procesamiento, pero no se si se refiero al tiempo que el dataset tarda en cargar
    o si hay que poner un sleep para simular el tiempo de procesamiento, ai que por las dudas ocuparemos un time.sleep()
    #simulamos un tiempo de procesamiento
    """
    time.sleep(0.1)

    zona = bbox.zonas[zone_id]
    filtrado = (
        ( dataset['latitude'] >= zona['lat'][0]) & (dataset['latitude'] <= zona['lat'][1]) &
        ( dataset['longitude'] >= zona['lon'][0]) & (dataset['longitude'] <= zona['lon'][1]) & 
        ( dataset['confidence'] >= confidence_min)
    )

    contando = int(dataset.filter(filtrado).shape[0])
    return contando


# CONSULTA 2: Área total y promedio de los edificios de una zona
def q2_area(zone_id, confidence_min=0.0):

    time.sleep(0.1)

    zona = bbox.zonas[zone_id]

    filtro = (
        (dataset['latitude'] >= zona['lat'][0]) & (dataset['latitude'] <= zona['lat'][1]) &
        (dataset['longitude'] >= zona['lon'][0]) & (dataset['longitude'] <= zona['lon'][1]) &
        (dataset['confidence'] >= confidence_min)
    )

    filtrado = dataset.filter(filtro)

    n = filtrado.height

    if n == 0:
        return {
            "n": 0,
            "total_area": 0,
            "avg_area": 0
        }

    area_total = float(filtrado["area_in_meters"].sum())
    promedio_area = area_total / n

    return {
        "n": n,
        "total_area": area_total,
        "avg_area": promedio_area
    }


#CONSULTA 3: Densidad de edificions por km^2
def q3_density(zone_id, confidence_min = 0.0):
    time.sleep(0.1)
    zona = bbox.zonas[zone_id]

    filtro = (  
        (dataset['latitude']>= zona['lat'][0]) & (dataset['latitude'] <= zona['lat'][1]) &
        (dataset['longitude']>= zona['lon'][0]) & (dataset['longitude'] <= zona['lon'][1]) &
        ( dataset['confidence'] >= confidence_min)
    )

    filtrado = dataset.filter(filtro)

    conteo = filtrado.height

    # CÁLCULO DEL ÁREA DE LA ZONA (Bounding Box)
    # Aproximación: 1 grado lat ≈ 111.1 km | 1 grado lon ≈ 111.1 * cos(lat) km
    lat_mid = (zona['lat'][0] + zona['lat'][1]) / 2
    import math
    
    ancho_km = (zona['lon'][1] - zona['lon'][0]) * 111.1 * math.cos(math.radians(lat_mid))
    alto_km = (zona['lat'][1] - zona['lat'][0]) * 111.1
    
    area_km2 = abs(ancho_km * alto_km)
    
    densidad = conteo / area_km2
    return densidad


#CONSULTA 4: Comparación de densidad entre zonas
def q4_compare(zone_a, zone_b, confidence_min = 0.0):

    zone_a_densidad = q3_density(zone_a, confidence_min)
    zone_b_densidad = q3_density(zone_b, confidence_min)

    if zone_a_densidad > zone_b_densidad:
        return zone_a
    
    else:
        return zone_b



#CONSULTA 5: Distribución de confianza en una zona
"""
Calcula la distribución del score de confianza de detección en una zona, agrupado en intervalos.
Permite evaluar la calidad del dato geoespacial antes de tomar decisiones operativas.
Parámetros: bbox, número de intervalos bins (por defecto 5)
"""

def q5_confidence_dist(zone_id, bins=5):
    """
    Q5: Distribución del score de confianza en una zona.

    Lo que hace esta función es:
    -Tomar todos los valores de 'confidence' de los edificios en una zona
    -Dividir el rango [0, 1] en 'bins' intervalos iguales
    -Contar cuántos valores caen dentro de cada intervalo
    
    O sea construir un histograma de la variable 'confidence'.

    Traducción ultra simple: dividir el rango 0-1, meter los datos en cajitas, contar cuántos hay en cada cajita.
    """

    # Simula tiempo de procesamiento
    time.sleep(0.1)

    # Filtrar el dataset para quedarse solo con los edificios de la zona
    # IMPORTANTE: no se filtra por confidence mínima porque la idea es ver la distribución completa
    filtrado = filtrar_por_zona(zone_id, confidence_min=0.0)

    # Si no hay edificios en esa zona, no hay nada que analizar
    # Entonces se retorna un diccionario vacío
    if filtrado.height == 0:
        return {}

    # Extraer la columna 'confidence' como lista de Python
    # Ejemplo: [0.12, 0.45, 0.9, 0.33, ...]
    scores = filtrado["confidence"].to_list()

    # Crear los límites de los intervalos (bins)
    # Si bins = 5, se divide [0,1] en 5 partes iguales, o sea:
    # [0.0, 0.2, 0.4, 0.6, 0.8, 1.0]
    intervalos = [i / bins for i in range(bins + 1)]

    # Aquí se guarda el resultado final
    # De esta manera:
    # {
    #   "0.0-0.2": 10,
    #   "0.2-0.4": 25,
    #   ...
    # }
    distribucion = {}

    # Recorrer cada intervalo
    # Si bins = 5, pues hay 5 intervalos :p
    for i in range(bins):

        # Límite inferior y superior del intervalo actual
        lower = intervalos[i]
        upper = intervalos[i + 1]

        # Contar cuántos valores caen en este intervalo

        # Para todos los intervalos menos el último:
        # usamos lower <= s < upper
        # para evitar contar dos veces valores en los bordes
        if i == bins - 1:
            # Último intervalo incluye el 1.0 
            count = sum(1 for s in scores if lower <= s <= upper)
        else:
            # Intervalos normales
            count = sum(1 for s in scores if lower <= s < upper)

        # Hacer una etiqueta para el intervalo
        # Que se vea tipo: "0.0-0.2"
        key = f"{round(lower, 2)}-{round(upper, 2)}"

        # Guardar el conteo en el diccionario
        distribucion[key] = count

    # Retornar el resultado final
    return distribucion


def procesar_consulta(consulta):
    
    q = consulta["query"] 
    conf = consulta.get("confidence", 0.0)
    
    if q == "Q1":
        return q1_count(consulta["zona"], conf)
    elif q == "Q2":
        return q2_area(consulta["zona"], conf)
    elif q == "Q3":
        return q3_density(consulta["zona"], conf)
    elif q == "Q4":
        return q4_compare(consulta["zona_a"], consulta["zona_b"], conf)
    elif q == "Q5":
        return q5_confidence_dist(consulta["zona"], consulta.get("bins", 5))
    