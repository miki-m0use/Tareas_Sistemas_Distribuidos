import time
import numpy as np

# Registro de eventos del sistema
log_eventos = []
tiempo_inicio = time.perf_counter()

# Contador de evicciones
evictions = 0


def registrar_metrica(evento, latencia, evictions_actuales=0):
    """
    Guarda el evento de caché y su latencia.

    evento: "HIT" o "MISS"
    latencia: tiempo que demoró la consulta
    evictions_actuales: cantidad de evicciones detectadas en Redis
    """
    global evictions

    log_eventos.append({
        "evento": evento,
        "latencia": latencia
    })

    evictions = evictions_actuales


def calcular_estadisticas():
    """
    Calcula métricas del sistema:
    -Hit rate
    -Miss rate
    -Throughput
    -Latencia p50/p95
    -Eviction rate
    -Cache efficiency
    """

    if not log_eventos:
        return "No hay métricas registradas."

    hits = sum(1 for e in log_eventos if e["evento"] == "HIT")
    misses = sum(1 for e in log_eventos if e["evento"] == "MISS")
    total = hits + misses

    hit_rate = hits / total if total > 0 else 0
    miss_rate = misses / total if total > 0 else 0

    tiempo_total = time.perf_counter() - tiempo_inicio
    throughput = total / tiempo_total if tiempo_total > 0 else 0

    latencias = [e["latencia"] for e in log_eventos]
    p50 = np.percentile(latencias, 50)
    p95 = np.percentile(latencias, 95)

    # Eviction rate = evictions por minuto
    tiempo_total_min = tiempo_total / 60
    eviction_rate = evictions / tiempo_total_min if tiempo_total_min > 0 else 0

    # Latencia promedio por tipo de evento
    latencias_hit = [e["latencia"] for e in log_eventos if e["evento"] == "HIT"]
    latencias_miss = [e["latencia"] for e in log_eventos if e["evento"] == "MISS"]

    t_cache = np.mean(latencias_hit) if latencias_hit else 0
    t_db = np.mean(latencias_miss) if latencias_miss else 0

    # Cache efficiency = (hits * t_cache - misses * t_db) / total
    cache_efficiency = ((hits * t_cache) - (misses * t_db)) / total if total > 0 else 0

    return {
        "Total Consultas": total,
        "Hits": hits,
        "Misses": misses,
        "Hit Rate": round(hit_rate, 4),
        "Miss Rate": round(miss_rate, 4),
        "Throughput (req/s)": round(throughput, 2),
        "Latencia P50 (s)": round(p50, 4),
        "Latencia P95 (s)": round(p95, 4),
        "Evictions": evictions,
        "Eviction Rate (evictions/min)": round(eviction_rate, 4),
        "t_cache promedio (s)": round(t_cache, 4),
        "t_db promedio (s)": round(t_db, 4),
        "Cache Efficiency": round(cache_efficiency, 4)
    }