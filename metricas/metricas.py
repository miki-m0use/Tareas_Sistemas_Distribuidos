import time
import numpy as np

# ─────────────────────────────────────────
# ESTADO GLOBAL
# ─────────────────────────────────────────

log_eventos = []
tiempo_inicio = time.perf_counter()
evictions = 0

# Métricas nuevas tarea 2
total_reintentos = 0        # consultas enviadas al tópico de reintentos
total_recuperadas = 0       # consultas que fallaron pero se procesaron exitosamente en reintento
total_dlq = 0               # consultas enviadas a la DLQ
log_backlog = []            # registro de backlog a lo largo del tiempo
tiempo_inicio_recuperacion = None  # momento en que el generador vuelve a responder (inicia drenado de cola)
recovery_time = None        # tiempo que tardó en vaciarse la cola tras la falla


# ─────────────────────────────────────────
# FUNCIONES DE REGISTRO
# ─────────────────────────────────────────

def resetear_metricas():
    global log_eventos, tiempo_inicio, evictions
    global total_reintentos, total_recuperadas, total_dlq
    global log_backlog, tiempo_inicio_falla, recovery_time

    log_eventos = []
    tiempo_inicio = time.perf_counter()
    evictions = 0
    total_reintentos = 0
    total_recuperadas = 0
    total_dlq = 0
    log_backlog = []
    tiempo_inicio_recuperacion = None
    recovery_time = None


def registrar_metrica(evento, latencia, evictions_actuales=0):
    """Registra un evento HIT o MISS con su latencia."""
    global evictions
    log_eventos.append({
        "evento": evento,
        "latencia": latencia
    })
    evictions = evictions_actuales


def registrar_reintento():
    """Registra que una consulta fue enviada al tópico de reintentos."""
    global total_reintentos
    total_reintentos += 1


def registrar_recuperada():
    """Registra que una consulta fue recuperada exitosamente tras un fallo."""
    global total_recuperadas
    total_recuperadas += 1


def registrar_dlq():
    """Registra que una consulta fue enviada a la DLQ."""
    global total_dlq
    total_dlq += 1


def registrar_backlog(cantidad):
    """Registra el tamaño del backlog en un momento dado."""
    global recovery_time, tiempo_inicio_recuperacion
    log_backlog.append({
        "timestamp": time.perf_counter() - tiempo_inicio,
        "backlog": cantidad
    })
    
    # Si la falla ya se resolvió y el backlog finalmente llega a 0,
    # calculamos cuánto tardó en vaciarse la cola desde que se recuperó el generador.
    if tiempo_inicio_recuperacion is not None and cantidad == 0:
        recovery_time = time.perf_counter() - tiempo_inicio_recuperacion
        tiempo_inicio_recuperacion = None  # Reseteamos para que no calcule múltiples veces


def registrar_inicio_recuperacion():
    """Marca el momento en que el generador se recupera y empieza a drenar el backlog."""
    global tiempo_inicio_recuperacion
    # Solo registramos el inicio si no hay uno activo.
    if tiempo_inicio_recuperacion is None:
        tiempo_inicio_recuperacion = time.perf_counter()


# ─────────────────────────────────────────
# CÁLCULO DE ESTADÍSTICAS
# ─────────────────────────────────────────

def calcular_estadisticas():
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

    tiempo_total_min = tiempo_total / 60
    eviction_rate = evictions / tiempo_total_min if tiempo_total_min > 0 else 0

    latencias_hit = [e["latencia"] for e in log_eventos if e["evento"] == "HIT"]
    latencias_miss = [e["latencia"] for e in log_eventos if e["evento"] == "MISS"]
    t_cache = np.mean(latencias_hit) if latencias_hit else 0
    t_db = np.mean(latencias_miss) if latencias_miss else 0

    cache_efficiency = ((hits * t_cache) - (misses * t_db)) / total if total > 0 else 0

    # Métricas nuevas tarea 2
    retry_rate = total_reintentos / (total + total_reintentos) if (total + total_reintentos) > 0 else 0
    recovery_rate = total_recuperadas / total_reintentos if total_reintentos > 0 else 0
    dlq_rate = total_dlq / (total + total_dlq) if (total + total_dlq) > 0 else 0
    backlog_max = max((b["backlog"] for b in log_backlog), default=0)

    return {
        # Métricas tarea 1
        "Total Consultas": total,
        "Hits": hits,
        "Misses": misses,
        "Hit Rate": round(hit_rate, 4),
        "Miss Rate": round(miss_rate, 4),
        "Throughput (req/s)": round(throughput, 2),
        "Latencia P50 (s)": round(p50, 6),
        "Latencia P95 (s)": round(p95, 6),
        "Evictions": evictions,
        "Eviction Rate (evictions/min)": round(eviction_rate, 4),
        "t_cache promedio (s)": round(t_cache, 6),
        "t_db promedio (s)": round(t_db, 6),
        "Cache Efficiency": round(cache_efficiency, 6),
        # Métricas tarea 2
        "Total Reintentos": total_reintentos,
        "Total Recuperadas": total_recuperadas,
        "Total DLQ": total_dlq,
        "Retry Rate": round(retry_rate, 4),
        "Recovery Rate": round(recovery_rate, 4),
        "DLQ Rate": round(dlq_rate, 4),
        "Backlog Maximo": backlog_max,
        "Recovery Time (s)": round(recovery_time, 2) if recovery_time is not None else "N/A"
    }