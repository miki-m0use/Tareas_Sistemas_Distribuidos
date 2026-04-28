import time
import numpy as np

# Variables en memoria para guardar el registro de todo el tráfico
log_eventos = []
tiempo_inicio = time.time()

def registrar_metrica(evento, latencia):
    """Guarda si fue HIT o MISS y cuánto se demoró."""
    log_eventos.append({
        "evento": evento,
        "latencia": latencia
    })

def calcular_estadisticas():
    """Calcula todo lo necesario para tu informe (Hit rate, Throughput, p50, p95)"""
    if not log_eventos:
        return "No hay métricas registradas."
    
    hits = sum(1 for e in log_eventos if e["evento"] == "HIT")
    misses = sum(1 for e in log_eventos if e["evento"] == "MISS")
    total = hits + misses
    
    hit_rate = hits / total if total > 0 else 0
    
    tiempo_total = time.time() - tiempo_inicio
    throughput = total / tiempo_total if tiempo_total > 0 else 0
    
    latencias = [e["latencia"] for e in log_eventos]
    p50 = np.percentile(latencias, 50)
    p95 = np.percentile(latencias, 95)
    
    return {
        "Total Consultas": total,
        "Hits": hits,
        "Misses": misses,
        "Hit Rate": round(hit_rate, 4),
        "Throughput (req/s)": round(throughput, 2),
        "Latencia P50 (s)": round(p50, 4),
        "Latencia P95 (s)": round(p95, 4)
    }