# Sistema de Monitoreo y Resiliencia de Consultas Turísticas

Este proyecto implementa una arquitectura distribuida utilizando **Apache Kafka** y **Redis** para gestionar y procesar consultas turísticas en tiempo real, garantizando alta disponibilidad y resiliencia ante fallos.

## Características Principales

### 1. Productores de Consultas (Producers)
- **Generador de Tráfico (generador_trafico.py)**: Simula la carga de consultas con diferentes distribuciones (Uniforme o Zipf).
- **Soporte para Spikes**: Capacidad de simular picos de tráfico alterando la tasa de generación de consultas.
- **Integración con Kafka**: Envía consultas a topics específicos (`consultas-principales`, `consultas-reintentos`, `consultas-dlq`).

### 2. Base de Datos de Respuestas (Generador de Respuestas)
- **FastAPI**: Servicio rápido y ligero que actúa como base de datos de respuestas pre-generadas.
- **Almacenamiento en Memoria**: Utiliza un diccionario en memoria para servir las respuestas a los consumidores.
- **Cache Redis**: Almacena las respuestas más recientes para un acceso casi instantáneo (reduce latencia y carga de red).

### 3. Consumidores (Workers)
- **Workers Múltiples**: Arquitectura escalable con múltiples instancias de consumidores (`consumidor_worker`).
- **Consumer Group**: Los workers se reparten la carga automáticamente gracias al `group.id` de Kafka.
- **Lógica de Cache**: Verifica si la respuesta existe en Redis (`Cache HIT`) antes de consultar al servicio de respuestas (`Cache MISS`).
- **Retries con Delay Exponencial**: Si un worker falla al procesar una consulta, esta se envía de nuevo al tópico de reintentos con un delay incremental (2s, 4s, 8s...) para evitar sobrecargar el sistema en recuperación.
- **Dead Letter Queue (DLQ)**: Las consultas que superan el máximo de reintentos se envían a una cola de descarte para análisis posterior.
- **Recuperación Automática**: Los workers detectan el inicio de la recuperación (cuando el servicio de respuestas vuelve a estar disponible) y reintentan procesar las consultas retenidas.

### 4. Métricas y Monitoreo
- **Metricas en Tiempo Real**: Cada worker registra métricas internas (latencia, reintentos, evicciones de Redis).
- **Reporte Final Consolidado**: Al finalizar, cada worker imprime un reporte detallado de su actividad.
- **Apis de Estado**:endpoints para verificar el estado de los servicios.

## Despliegue

### Requisitos Previos
- **Docker** y **Docker Compose** instalados.

- **Clonar repositorio**: 
    ```bash
    git clone https://github.com/miki-m0use/Tareas_Sistemas_Distribuidos.git
    ```

- **Entrar al directorio**:
    ```bash
    cd Tareas_Sistemas_Distribuidos
    ```
### Comandos de Ejecución

1.  **Construir yLevantar el Sistema**: 
    ```bash
    sudo docker-compose up --build
    ```

2.  **Escalar para Pruebas de Carga**: 
    Para simular múltiples consumidores y probar la distribución de la carga:
    ```bash
    sudo docker-compose up --build --scale consumidor_worker=3
    ```
    tener en cuenta que en nuestro caso, la cantidad de particiones y de consumidores fueron iguales en todas las simulaciones, es decir, 3 particiones y 3 consumidores. Si la cantidad de consumidores fuera menor a la cantidad de particiones, no todas las particiones serian consumidas. 4 consumidores, 4 particiones, etc.

    si desea modificar parametros debe modificar los que se encuentran en main_trafico.py para ejecuciones normales y si se desea ejecutar con picos debe modificar los parametros de main_trafico_spike.py las particiones. y en docker.compose.yml el archivo que se leera. 

    ```
    generador_trafico:
    build: .
    command: python -m generador_trafico.main_trafico_spike # este parametro
    volumes:
      - .:/app
    environment:
      - PYTHONUNBUFFERED=1
    # El generador arranca último, cuando todo esté listo
    depends_on:
      kafka:
        condition: service_healthy
      consumidor_worker:
        condition: service_started
    ```
    y luego ejecutar docker-compose up --build --scale consumidor_worker=3 o la cantidad de consumidores que desee. 

3.  **Detener el Sistema**: 
    CTRL+C

    ```bash
    sudo docker-compose down
    ```

4.  **Detener y Limpiar Volúmenes**: 
    Para eliminar datos de Kafka y Redis persistidos:
    ```bash
    sudo docker-compose down -v
    ```

## Estructura del Proyecto

```
Tareas_Sistemas_Distribuidos/
├── README.md                      # Documentación del proyecto
├── docker-compose.yml             # Orquestación de contenedores (Kafka, Zookeeper, servicios)
├── Dockerfile                     # Receta de construcción para tus contenedores
├── consumidores_kafka.py          # Script principal de los workers/consumidores de Kafka
├── generador_graficos.py          # Script para generar las gráficas (latencia, backlog, etc.)

├── generador_respuestas/          # Servicio generador de respuestas
│   ├── main.py
│   └── bounding_boxes.py
│
├── generador_trafico/             # Servicio generador de tráfico/clientes
│   ├── main_trafico.py            # Generador de tráfico normal
│   ├── main_trafico_spike.py      # Generador de tráfico con picos (spikes) para estres
│   └── traffic_generator.py       # Lógica base de generación de tráfico
│
├── metricas/                      # Recolección y gestión de métricas
│   └── metricas.py                # Lógica para registrar los resutlados
│
├── graficos/                      # Directorio donde guardamos los grafico procesados de metricas
├── cache/                         # Directorio de caché
├── 967_buildings/                 # dataset de los edificios
├── Tarea_1_Sistemas_Distribuidos_2026_1.pdf  # Enunciados de las tareas
├── Tarea_2_Sistemas_Distribuidos.pdf
```

## Testing

### Verificar Métricas
Después de ejecutar con tráfico, puedes verificar las métricas:
1.  Abrir la UI de Kafka: [http://localhost:8080](http://localhost:8080)
2.  Ver logs de los workers: `sudo docker-compose logs consumidor_worker=n` donde n es la cantidad de consumidores, por ejemplo `sudo docker-compose logs consumidor_worker=3`
3.  Verificar estado de los servicios: [http://localhost:8000/status](http://localhost:8000/status)


