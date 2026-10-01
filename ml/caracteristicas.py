"""Características de una ventana de lecturas.

Calcula lo mismo que spark/procesar.py, para que el modelo reciba en vivo
exactamente los mismos datos con los que se entrenó.
"""

import numpy as np

SENSORES = ["temperatura", "vibracion", "presion", "rpm"]
METRICAS = ["media", "desv", "min", "max", "tendencia"]
COLUMNAS = [f"{s}_{m}" for s in SENSORES for m in METRICAS]
# Una ventana con muy pocas lecturas no es representativa (por ejemplo, el
# borde de una ventana cuando recién arranca una máquina).
MIN_LECTURAS = 10


def calcular(segundos, valores):
    """Características de una ventana.

    segundos: array con el momento de cada lectura (epoch en segundos).
    valores: dict sensor -> array con sus valores, en el mismo orden.
    """
    t = np.asarray(segundos, dtype=float)
    fila = {}
    for s in SENSORES:
        x = np.asarray(valores[s], dtype=float)
        fila[f"{s}_media"] = x.mean()
        fila[f"{s}_desv"] = x.std(ddof=1)  # igual que stddev() de Spark (muestral)
        fila[f"{s}_min"] = x.min()
        fila[f"{s}_max"] = x.max()
        # Pendiente de la recta que mejor ajusta = covarianza / varianza,
        # igual que covar_pop / var_pop en Spark.
        fila[f"{s}_tendencia"] = ((t - t.mean()) * (x - x.mean())).mean() / t.var()
    return fila
