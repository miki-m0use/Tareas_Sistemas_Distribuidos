"""
generar_graficos.py
====================
1. Lee todos los archivos metricas_*.json de la carpeta actual.
2. Los agrupa automáticamente por nombre de experimento.
3. Genera todos los gráficos en la carpeta graficos/

CÓMO NOMBRAR LOS ARCHIVOS DE MÉTRICAS:
  Cada archivo debe tener este nombre exacto (lo pones tú al renombrar):

    metricas_1c_5k_sin.json
    metricas_1c_5k_con.json
    metricas_1c_10k_sin.json
    metricas_1c_10k_con.json
    metricas_1c_15k_sin.json
    metricas_1c_15k_con.json
    metricas_3c_5k_sin.json
    ... etc

  Formato: metricas_{consumers}c_{n_consultas}k_{sin|con}.json

  El script lee el nombre y arma la etiqueta automáticamente:
    "1c_5k_sin"  →  "1 Consumer | 5k | Sin Spike"
"""

import json, os, glob, re
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np

OUTPUT_DIR = "graficos"
os.makedirs(OUTPUT_DIR, exist_ok=True)

COLORES_BASE = {"sin": "#2196F3", "con": "#FF5722"}
COLOR_SPIKE  = "#FF9800"

# ─────────────────────────────────────────────────────────────────────────────
# CARGA Y PARSEO DE ARCHIVOS
# ─────────────────────────────────────────────────────────────────────────────

def parsear_nombre(filename):
    """
    'metricas_1c_5k_sin.json' → {'consumers': '1', 'n': '5k', 'spike': 'sin',
                                   'label': '1 Consumer | 5k | Sin Spike'}
    """
    base = os.path.basename(filename).replace("metricas_", "").replace(".json", "")
    m = re.match(r"(\d+)c_(\d+k)_(sin|con)", base)
    if not m:
        print(f"  [SKIP] No entiendo el nombre: {filename}  (formato esperado: metricas_Nc_Xk_sin|con.json)")
        return None
    consumers, n, spike = m.groups()
    label = f"{consumers} Consumer{'s' if int(consumers)>1 else ''} | {n} | {'Sin Spike' if spike=='sin' else 'Con Spike'}"
    return {"consumers": consumers, "n": n, "spike": spike, "label": label, "file": filename}


def cargar_experimentos():
    archivos = sorted(glob.glob("metricas_*.json"))
    if not archivos:
        print("[ERROR] No se encontraron archivos metricas_*.json en esta carpeta.")
        print("        Asegúrate de correr este script junto a los JSONs exportados por el worker.")
        raise SystemExit(1)

    experimentos = []
    for f in archivos:
        meta = parsear_nombre(f)
        if meta is None:
            continue
        with open(f) as fh:
            datos = json.load(fh)
        meta["stats"]       = datos.get("stats", {})
        meta["log_backlog"] = datos.get("log_backlog", [])
        # spike_inicio_t / spike_fin_t: estimado a partir del % de N si no viene en el JSON
        meta["spike_inicio_t"] = datos.get("spike_inicio_t", None)
        meta["spike_fin_t"]    = datos.get("spike_fin_t", None)
        experimentos.append(meta)
        print(f"  ✓ Cargado: {meta['label']}")
    return experimentos


def get_stat(exp, key, default=0):
    val = exp.get("stats", {}).get(key, default)
    return val if val != "N/A" else default

def guardar(fig, nombre):
    path = os.path.join(OUTPUT_DIR, nombre)
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  → {path}")

# ─────────────────────────────────────────────────────────────────────────────
# GRÁFICO 1 — Backlog superpuesto sin vs con spike, por consumers y N
# ─────────────────────────────────────────────────────────────────────────────

def grafico_backlog_superpuesto(experimentos):
    # Agrupar por (consumers, n)
    grupos = {}
    for exp in experimentos:
        key = (exp["consumers"], exp["n"])
        grupos.setdefault(key, {})
        grupos[key][exp["spike"]] = exp

    for (consumers, n), par in grupos.items():
        if "sin" not in par and "con" not in par:
            continue

        fig, ax = plt.subplots(figsize=(11, 4.5))

        for spike_key, color, label_suffix in [("sin", "#2196F3", "Sin Spike"),
                                                ("con", "#FF5722", "Con Spike")]:
            if spike_key not in par:
                continue
            exp  = par[spike_key]
            log  = exp.get("log_backlog", [])
            if not log:
                continue
            ts   = [p["timestamp"] for p in log]
            bk   = [p["backlog"]   for p in log]

            # Zona spike
            if spike_key == "con" and exp.get("spike_inicio_t"):
                ax.axvspan(exp["spike_inicio_t"], exp["spike_fin_t"],
                           alpha=0.12, color=COLOR_SPIKE)
                ax.axvline(exp["spike_inicio_t"], color=COLOR_SPIKE, lw=1.2, ls="--", alpha=0.8)
                ax.axvline(exp["spike_fin_t"],    color=COLOR_SPIKE, lw=1.2, ls="--", alpha=0.8)
                ax.text(exp["spike_inicio_t"] + 0.3, max(bk)*0.92,
                        "⚡ spike", color=COLOR_SPIKE, fontsize=8)

            ax.plot(ts, bk, color=color, lw=2, label=label_suffix)
            ax.fill_between(ts, bk, alpha=0.1, color=color)

        c_label = f"{consumers} Consumer{'s' if int(consumers)>1 else ''}"
        ax.set_title(f"Backlog en el tiempo — {c_label} | {n} consultas", fontsize=13)
        ax.set_xlabel("Tiempo (s)")
        ax.set_ylabel("Mensajes pendientes")
        ax.legend(loc="upper right")
        ax.grid(True, alpha=0.3)
        guardar(fig, f"backlog_{consumers}c_{n}.png")


# ─────────────────────────────────────────────────────────────────────────────
# GRÁFICO 2 — Pico de backlog: barras agrupadas por N, sin vs con spike
# ─────────────────────────────────────────────────────────────────────────────

def grafico_pico_backlog(experimentos):
    # Agrupar por (consumers, n) → pico sin y con spike
    for consumers in sorted(set(e["consumers"] for e in experimentos)):
        exps_c = [e for e in experimentos if e["consumers"] == consumers]
        ns = sorted(set(e["n"] for e in exps_c), key=lambda x: int(x.replace("k","")))

        sin_picos = []
        con_picos = []
        for n in ns:
            grupo = {e["spike"]: e for e in exps_c if e["n"] == n}
            sin_picos.append(max((p["backlog"] for p in grupo.get("sin", {}).get("log_backlog", [])), default=0))
            con_picos.append(max((p["backlog"] for p in grupo.get("con", {}).get("log_backlog", [])), default=0))

        x = np.arange(len(ns))
        w = 0.35
        fig, ax = plt.subplots(figsize=(8, 5))
        b1 = ax.bar(x - w/2, sin_picos, w, label="Sin Spike", color="#2196F3")
        b2 = ax.bar(x + w/2, con_picos, w, label="Con Spike", color="#FF5722")
        for bar in list(b1) + list(b2):
            h = bar.get_height()
            if h > 0:
                ax.text(bar.get_x() + bar.get_width()/2, h + 1,
                        str(int(h)), ha="center", va="bottom", fontsize=9)

        c_label = f"{consumers} Consumer{'s' if int(consumers)>1 else ''}"
        ax.set_title(f"Pico máximo de Backlog — {c_label}", fontsize=13)
        ax.set_xticks(x)
        ax.set_xticklabels([f"{n} consultas" for n in ns])
        ax.set_ylabel("Mensajes pendientes (pico)")
        ax.legend()
        ax.grid(True, axis="y", alpha=0.3)
        guardar(fig, f"pico_backlog_{consumers}c.png")


# ─────────────────────────────────────────────────────────────────────────────
# GRÁFICO 3 — Throughput: 1c vs 3c, por N, sin y con spike
# ─────────────────────────────────────────────────────────────────────────────

def grafico_throughput(experimentos):
    ns = sorted(set(e["n"] for e in experimentos), key=lambda x: int(x.replace("k","")))
    configs = sorted(set((e["consumers"], e["spike"]) for e in experimentos))

    x = np.arange(len(ns))
    w = 0.8 / len(configs)
    COLORES = ["#2196F3","#1565C0","#FF5722","#B71C1C","#4CAF50","#1B5E20","#FF9800","#E65100"]

    fig, ax = plt.subplots(figsize=(11, 5))
    for i, (consumers, spike) in enumerate(configs):
        vals = []
        for n in ns:
            match = next((e for e in experimentos if e["consumers"]==consumers and e["spike"]==spike and e["n"]==n), None)
            vals.append(get_stat(match, "Throughput (req/s)") if match else 0)
        c_label = f"{consumers}c {'sin' if spike=='sin' else 'con'} spike"
        bars = ax.bar(x + i*w - (len(configs)-1)*w/2, vals, w,
                      label=c_label, color=COLORES[i % len(COLORES)])
        for bar, v in zip(bars, vals):
            if v > 0:
                ax.text(bar.get_x()+bar.get_width()/2, bar.get_height()+0.3,
                        f"{v:.0f}", ha="center", va="bottom", fontsize=7)

    ax.set_title("Throughput (consultas/s) por configuración", fontsize=13)
    ax.set_xticks(x)
    ax.set_xticklabels([f"{n} consultas" for n in ns])
    ax.set_ylabel("Consultas por segundo")
    ax.legend(fontsize=8)
    ax.grid(True, axis="y", alpha=0.3)
    guardar(fig, "throughput_comparativo.png")


# ─────────────────────────────────────────────────────────────────────────────
# GRÁFICO 4 — Latencias P50 y P95, por N y consumers
# ─────────────────────────────────────────────────────────────────────────────

def grafico_latencias(experimentos):
    ns = sorted(set(e["n"] for e in experimentos), key=lambda x: int(x.replace("k","")))

    for consumers in sorted(set(e["consumers"] for e in experimentos)):
        exps_c = [e for e in experimentos if e["consumers"] == consumers]
        labels, p50s, p95s = [], [], []
        for n in ns:
            for spike in ["sin", "con"]:
                match = next((e for e in exps_c if e["n"]==n and e["spike"]==spike), None)
                if match:
                    labels.append(f"{n}\n{'sin' if spike=='sin' else 'con'} spike")
                    p50s.append(get_stat(match, "Latencia P50 (s)") * 1000)
                    p95s.append(get_stat(match, "Latencia P95 (s)") * 1000)

        if not labels:
            continue

        x = np.arange(len(labels))
        w = 0.35
        fig, ax = plt.subplots(figsize=(11, 5))
        ax.bar(x - w/2, p50s, w, label="P50", color="#2196F3")
        ax.bar(x + w/2, p95s, w, label="P95", color="#FF5722")
        ax.set_yscale("log")
        c_label = f"{consumers} Consumer{'s' if int(consumers)>1 else ''}"
        ax.set_title(f"Latencia P50 y P95 (ms, escala log) — {c_label}", fontsize=13)
        ax.set_xticks(x)
        ax.set_xticklabels(labels, fontsize=8)
        ax.set_ylabel("Latencia (ms)")
        ax.legend()
        ax.grid(True, axis="y", alpha=0.3, which="both")
        guardar(fig, f"latencias_{consumers}c.png")


# ─────────────────────────────────────────────────────────────────────────────
# GRÁFICO 5 — Reintentos / Recuperadas / DLQ (solo si hay fallas)
# ─────────────────────────────────────────────────────────────────────────────

def grafico_tolerancia(experimentos):
    relevantes = [e for e in experimentos if get_stat(e, "Total Reintentos") > 0]
    if not relevantes:
        print("  [SKIP tolerancia] Sin reintentos registrados.")
        return

    labels     = [e["label"] for e in relevantes]
    reintentos = [get_stat(e, "Total Reintentos")  for e in relevantes]
    recuperadas= [get_stat(e, "Total Recuperadas") for e in relevantes]
    dlq        = [get_stat(e, "Total DLQ")         for e in relevantes]

    x = np.arange(len(labels))
    w = 0.25
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.bar(x - w,   reintentos,  w, label="Reintentos",  color="#FF9800")
    ax.bar(x,       recuperadas, w, label="Recuperadas", color="#4CAF50")
    ax.bar(x + w,   dlq,         w, label="DLQ",         color="#F44336")
    ax.set_title("Tolerancia a fallos: reintentos, recuperadas y DLQ", fontsize=13)
    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=12, ha="right")
    ax.set_ylabel("Consultas")
    ax.legend()
    ax.grid(True, axis="y", alpha=0.3)
    guardar(fig, "tolerancia_fallos.png")


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("GENERADOR DE GRÁFICOS — Tarea 2 Sistemas Distribuidos")
    print("=" * 60)

    experimentos = cargar_experimentos()
    print(f"\n{len(experimentos)} experimentos cargados.\n")

    print("1. Backlog superpuesto (sin vs con spike)...")
    grafico_backlog_superpuesto(experimentos)

    print("2. Pico máximo de backlog...")
    grafico_pico_backlog(experimentos)

    print("3. Throughput comparativo...")
    grafico_throughput(experimentos)

    print("4. Latencias P50/P95...")
    grafico_latencias(experimentos)

    print("5. Tolerancia a fallos...")
    grafico_tolerancia(experimentos)

    print(f"\nListo. Gráficos en '{OUTPUT_DIR}/'")

if __name__ == "__main__":
    main()