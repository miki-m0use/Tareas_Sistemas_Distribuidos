import pandas as pd
import time
import bounding_boxes as bbox

#cargamos el dataset de los edificios
dataset = pd.read_csv('./967_buildings/967_buildings.csv')
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

    contando = int(dataset[filtrado].shape[0])
    return contando


def q2_area(zone_id, confidence_min = 0.0):
    return 0.0