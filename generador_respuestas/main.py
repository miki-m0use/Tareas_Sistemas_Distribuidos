
import polars as pl
import time
import generador_respuestas.bounding_boxes as bbox

#cargamos el dataset de los edificios
dataset = pl.read_csv('./967_buildings/967_buildings.csv')
#print(dataset.head())

#las zonas estan definida en otro archivo
# por lo tanto ahora queda solo mostar las consultas de ejemplo

#CONSULTA 1: Conteo de edificios de una zona
def q1_count(zone_id, confidence_min = 0.0):
    """
    se pide que se simule un tiempo de procesamiento, pero no se si se refiero al tiempo que el dataset tarda en cargar
    o si hay que poner un sleep para simular el tiempo de procesamiento, ai que por las dudas ocuparemos un time.sleep()
    #simulamos un tiempo de procesamiento
    """
    time.sleep(0.5)

    zona = bbox.zonas[zone_id]
    filtrado = (
        ( dataset['latitude'] >= zona['lat'][0]) & (dataset['latitude'] <= zona['lat'][1]) &
        ( dataset['longitude'] >= zona['lon'][0]) & (dataset['longitude'] <= zona['lon'][1]) & 
        ( dataset['confidence'] >= confidence_min)
    )

    contando = int(dataset.filter(filtrado).shape[0])
    return contando


#CONSULTA 2: Area total y promedio de los edificios de una zona
def q2_area(zone_id, confidence_min = 0.0):
    
    time.sleep(0.5)

    zona = bbox.zonas[zone_id]

    filtro = (  
        (dataset['latitude']>= zona['lat'][0]) & (dataset['latitude'] <= zona['lat'][1]) &
        (dataset['longitude']>= zona['lon'][0]) & (dataset['longitude'] <= zona['lon'][1]) &
        ( dataset['confidence'] >= confidence_min)
    )

    filtrado = dataset.filter(filtro)

    area_total= 0
    area = filtrado['area_in_meters']

    for i in range(filtrado.height):
        area_total += float(area[i])
        promedio_area = area_total / filtrado.height

    return print(f"n: ", filtrado.height, f"Total: ",area_total, f"Promedio: ", promedio_area)



#CONSULTA 3: Densidad de edificions por km^2
def q3_density(zone_id, confidence_min = 0.0):
    time.sleep(0.5)
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

# CONSULTA 5: Distribución de confianza en una zona
def q5_confidence_dist(zone_id, bins=5):
    time.sleep(0.5) # Simulación de tiempo real [cite: 40]
    
    zona = bbox.zonas[zone_id]
    
    # Filtramos por zona (sin filtrar por confianza mínima, ya que queremos ver la distribución total)
    filtro = (  
        (dataset['latitude'] >= zona['lat'][0]) & (dataset['latitude'] <= zona['lat'][1]) &
        (dataset['longitude'] >= zona['lon'][0]) & (dataset['longitude'] <= zona['lon'][1])
    )
    
    filtrado = dataset.filter(filtro)
    scores = filtrado['confidence']
    
    return 




print(q1_count('Zona_1', 0.5))
print(q4_compare('Zona_1', 'Zona_2', 0.5))                               