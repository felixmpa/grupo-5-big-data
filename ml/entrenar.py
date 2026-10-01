"""Entrena los modelos de predicción de fallas.

Lee las características por ventana que calculó Spark (PR 5) desde HDFS y
entrena dos modelos:

1. Clasificador (Random Forest, supervisado): aprende de las etiquetas de la
   simulación a reconocer si una máquina está normal, en degradación o en
   falla. La probabilidad de "degradación + falla" es el RIESGO: detectar la
   degradación es avisar antes de que la máquina falle.
2. Detector de anomalías (Isolation Forest, no supervisado): solo ve ventanas
   normales y aprende cómo es "lo normal". Marca como anomalía lo que no se
   parece. No necesita etiquetas, algo muy útil en una fábrica real.

Guarda los modelos en modelos/modelo.joblib y las métricas e imágenes en
resultados/.
"""

import io
import json
import os
from datetime import datetime, timezone

import joblib
import matplotlib

matplotlib.use("Agg")  # genera imágenes sin pantalla
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from hdfs import InsecureClient
from sklearn.ensemble import IsolationForest, RandomForestClassifier
from sklearn.metrics import classification_report, confusion_matrix

from caracteristicas import COLUMNAS, MIN_LECTURAS

HDFS_URL = os.getenv("HDFS_URL", "http://localhost:9870")
RUTA_VENTANAS = os.getenv("RUTA_VENTANAS", "/datalake/procesado/ventanas")
CARPETA = os.path.dirname(os.path.abspath(__file__))
ARCHIVO_MODELO = os.path.join(CARPETA, "modelos", "modelo.joblib")
CARPETA_RESULTADOS = os.path.join(CARPETA, "resultados")

ESTADOS = ["normal", "degradacion", "falla"]
UMBRAL_RIESGO = 0.5  # riesgo >= 50 % -> alerta
PROPORCION_TEST = 0.25  # el último 25 % del tiempo se usa para evaluar

# Colores de estado (bien / advertencia / crítico) y tinta para textos.
COLOR_ESTADO = {"normal": "#0ca30c", "degradacion": "#fab219", "falla": "#d03b3b"}
AZUL, TINTA, TINTA_SUAVE, GRILLA = "#2a78d6", "#0b0b0b", "#52514e", "#e4e3df"


def cargar_ventanas():
    """Descarga los Parquet de HDFS (por WebHDFS) y los une en un DataFrame."""
    hdfs = InsecureClient(HDFS_URL, user="hadoop")
    partes = []
    for carpeta, _, archivos in hdfs.walk(RUTA_VENTANAS):
        for nombre in archivos:
            if nombre.endswith(".parquet"):
                with hdfs.read(f"{carpeta}/{nombre}") as lector:
                    partes.append(pd.read_parquet(io.BytesIO(lector.read())))
    if not partes:
        raise SystemExit(f"No hay ventanas en {RUTA_VENTANAS}: ejecutar antes Spark (PR 5).")
    return pd.concat(partes, ignore_index=True)


def anticipacion(test, alerta):
    """Segundos entre la primera alerta y el inicio de cada falla en el test.

    Solo cuenta la alerta dentro del tramo de degradación inmediatamente
    anterior a la falla. Así, si una máquina falla dos veces seguidas, la
    alerta de la primera no se atribuye a la segunda. Devuelve un valor por
    falla: None si no hubo alerta, 0 si la alerta llegó recién en la ventana
    en que empieza la falla. Como se mide por ventanas, los valores son
    múltiplos de 30 s.
    """
    resultados = []
    datos = test.assign(alerta=alerta).sort_values(["maquina_id", "inicio"])
    for _, maquina in datos.groupby("maquina_id"):
        primera_alerta, anterior = None, "normal"
        for fila in maquina.itertuples():
            if fila.estado_mayoritario == "falla" and anterior != "falla":
                if primera_alerta is not None:
                    resultados.append((fila.inicio - primera_alerta).total_seconds())
                else:
                    resultados.append(0.0 if fila.alerta else None)
            if fila.estado_mayoritario == "degradacion":
                if fila.alerta and primera_alerta is None:
                    primera_alerta = fila.inicio
            else:
                primera_alerta = None  # termina el tramo de degradación
            anterior = fila.estado_mayoritario
    return resultados


def estilo(ax):
    ax.spines[["top", "right"]].set_visible(False)
    ax.spines[["left", "bottom"]].set_color(GRILLA)
    ax.tick_params(colors=TINTA_SUAVE, labelsize=9)
    ax.grid(axis="y", color=GRILLA, linewidth=0.8)
    ax.set_axisbelow(True)


def grafico_matriz(matriz, ruta):
    fig, ax = plt.subplots(figsize=(5.2, 4.4), dpi=150)
    ax.imshow(matriz, cmap="Blues")
    etiquetas = ["normal", "degradación", "falla"]
    ax.set_xticks(range(3), etiquetas)
    ax.set_yticks(range(3), etiquetas)
    ax.set_xlabel("Predicción del modelo", color=TINTA_SUAVE)
    ax.set_ylabel("Estado real", color=TINTA_SUAVE)
    limite = matriz.max() / 2
    for i in range(3):
        for j in range(3):
            ax.text(j, i, matriz[i, j], ha="center", va="center", fontsize=12,
                    color="white" if matriz[i, j] > limite else TINTA)
    ax.set_title("Matriz de confusión (ventanas de prueba)", color=TINTA, fontsize=11, loc="left")
    ax.tick_params(colors=TINTA_SUAVE, length=0)
    for borde in ax.spines.values():
        borde.set_visible(False)
    fig.tight_layout()
    fig.savefig(ruta)
    plt.close(fig)


def grafico_importancia(importancias, ruta, top=10):
    serie = pd.Series(importancias, index=COLUMNAS).sort_values().tail(top)
    fig, ax = plt.subplots(figsize=(6.4, 4.2), dpi=150)
    ax.barh(serie.index, serie.values, color=AZUL, height=0.6)
    estilo(ax)
    ax.grid(axis="y", visible=False)
    ax.grid(axis="x", color=GRILLA, linewidth=0.8)
    ax.set_xlabel("Importancia en el Random Forest", color=TINTA_SUAVE)
    ax.set_title(f"Las {top} características más útiles", color=TINTA, fontsize=11, loc="left")
    fig.tight_layout()
    fig.savefig(ruta)
    plt.close(fig)


def grafico_linea_de_tiempo(test, riesgo, ruta):
    """Temperatura y riesgo de una máquina, con el estado real de fondo."""
    # La máquina del test con más ventanas de falla: el ejemplo más ilustrativo.
    maquina = test[test.estado_mayoritario == "falla"].maquina_id.value_counts().idxmax()
    datos = test.assign(riesgo=riesgo)
    datos = datos[datos.maquina_id == maquina].sort_values("inicio").tail(120)

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(9, 5), dpi=150, sharex=True,
                                   gridspec_kw={"height_ratios": [3, 2]})
    for ax in (ax1, ax2):
        for fila in datos.itertuples():
            if fila.estado_mayoritario != "normal":
                ax.axvspan(fila.inicio, fila.fin, color=COLOR_ESTADO[fila.estado_mayoritario],
                           alpha=0.18, linewidth=0)
        estilo(ax)
    ax1.plot(datos.inicio, datos.temperatura_media, color=AZUL, linewidth=2)
    ax1.set_ylabel("Temperatura media (°C)", color=TINTA_SUAVE)
    ax2.plot(datos.inicio, datos.riesgo * 100, color=TINTA, linewidth=2)
    ax2.axhline(UMBRAL_RIESGO * 100, color=COLOR_ESTADO["falla"], linewidth=1, linestyle="--")
    # Etiqueta del umbral fuera del área de datos, para no tapar la curva.
    ax2.text(1.01, UMBRAL_RIESGO * 100, "umbral\nde alerta", transform=ax2.get_yaxis_transform(),
             color=TINTA_SUAVE, fontsize=8, va="center")
    ax2.set_ylabel("Riesgo (%)", color=TINTA_SUAVE)
    ax2.set_ylim(0, 105)
    ax1.set_title(f"{maquina}: el riesgo sube durante la degradación, antes de la falla",
                  color=TINTA, fontsize=11, loc="left")
    leyenda = [plt.Rectangle((0, 0), 1, 1, color=COLOR_ESTADO[e], alpha=0.35)
               for e in ("degradacion", "falla")]
    fig.legend(leyenda, ["Estado real: degradación", "Estado real: falla"], frameon=False,
               fontsize=8, loc="lower center", ncol=2, labelcolor=TINTA_SUAVE)
    ax2.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
    ax2.set_xlabel("Hora (UTC)", color=TINTA_SUAVE)
    fig.tight_layout(rect=(0, 0.05, 1, 1))
    fig.savefig(ruta)
    plt.close(fig)


def main():
    ventanas = cargar_ventanas()
    ventanas = ventanas[ventanas.lecturas >= MIN_LECTURAS].dropna(subset=COLUMNAS)
    ventanas = ventanas.sort_values("inicio").reset_index(drop=True)
    print(f"Ventanas disponibles: {len(ventanas)}")
    print(ventanas.estado_mayoritario.value_counts().reindex(ESTADOS, fill_value=0).to_string())

    # División por tiempo: se entrena con el pasado y se evalúa con el futuro,
    # como pasaría en la fábrica. Mezclar al azar haría trampa, porque ventanas
    # vecinas se parecen mucho.
    corte = ventanas.inicio.quantile(1 - PROPORCION_TEST)
    entreno, test = ventanas[ventanas.inicio < corte], ventanas[ventanas.inicio >= corte]
    print(f"Entrenamiento: {len(entreno)} ventanas | Prueba: {len(test)} ventanas (desde {corte:%H:%M})")

    # 1. Clasificador supervisado
    clasificador = RandomForestClassifier(
        n_estimators=200, class_weight="balanced", random_state=42, n_jobs=-1,
    )
    clasificador.fit(entreno[COLUMNAS], entreno.estado_mayoritario)
    prediccion = clasificador.predict(test[COLUMNAS])
    probabilidades = pd.DataFrame(clasificador.predict_proba(test[COLUMNAS]), columns=clasificador.classes_)
    riesgo = (probabilidades["degradacion"] + probabilidades["falla"]).to_numpy()
    alerta = riesgo >= UMBRAL_RIESGO

    reporte = classification_report(test.estado_mayoritario, prediccion, labels=ESTADOS,
                                    output_dict=True, zero_division=0)
    print("\nClasificador (Random Forest):")
    print(classification_report(test.estado_mayoritario, prediccion, labels=ESTADOS, zero_division=0))
    matriz = confusion_matrix(test.estado_mayoritario, prediccion, labels=ESTADOS)

    hay_problema = (test.estado_mayoritario != "normal").to_numpy()
    avisos = anticipacion(test, alerta)
    # Solo cuenta como aviso anticipado si la alerta llegó en una ventana
    # anterior a la falla (más de 0 s antes).
    detectadas = [a for a in avisos if a is not None and a > 0]
    distribucion = {("sin alerta" if a is None else f"{a:.0f} s"): avisos.count(a) for a in sorted(
        set(avisos), key=lambda v: -1 if v is None else v)}
    print(f"Alertas (riesgo >= {UMBRAL_RIESGO:.0%}): detectan {alerta[hay_problema].mean():.1%} de las "
          f"ventanas con problema; falsas alarmas en {alerta[~hay_problema].mean():.1%} de las normales")
    if avisos:
        print(f"Fallas en la prueba: {len(avisos)}; avisadas antes de empezar: {len(detectadas)}; "
              f"anticipación promedio: {np.mean(detectadas) if detectadas else 0:.0f} s")
        print(f"Anticipación por falla (múltiplos de la ventana de 30 s): {distribucion}")

    # 2. Detector de anomalías: solo ve ventanas normales
    detector = IsolationForest(n_estimators=200, contamination=0.02, random_state=42)
    detector.fit(entreno.loc[entreno.estado_mayoritario == "normal", COLUMNAS])
    es_anomalia = detector.predict(test[COLUMNAS]) == -1
    tasa_anomalias = {e: float(es_anomalia[(test.estado_mayoritario == e).to_numpy()].mean()) for e in ESTADOS}
    print("\nDetector de anomalías (Isolation Forest), % de ventanas marcadas como anomalía:")
    for estado, tasa in tasa_anomalias.items():
        print(f"  {estado:12} {tasa:.1%}")

    # Guardar modelos
    os.makedirs(os.path.dirname(ARCHIVO_MODELO), exist_ok=True)
    joblib.dump({
        "clasificador": clasificador,
        "detector": detector,
        "columnas": COLUMNAS,
        "umbral_riesgo": UMBRAL_RIESGO,
        "entrenado": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }, ARCHIVO_MODELO)
    print(f"\nModelo guardado en {ARCHIVO_MODELO}")

    # Guardar métricas e imágenes
    os.makedirs(CARPETA_RESULTADOS, exist_ok=True)
    metricas = {
        "ventanas": {"total": len(ventanas), "entrenamiento": len(entreno), "prueba": len(test)},
        "clasificador": {
            "exactitud": reporte["accuracy"],
            **{e: {k: round(v, 3) for k, v in reporte[e].items()} for e in ESTADOS},
            "matriz_confusion": {"etiquetas": ESTADOS, "valores": matriz.tolist()},
        },
        "alertas": {
            "umbral_riesgo": UMBRAL_RIESGO,
            "deteccion_ventanas_con_problema": round(float(alerta[hay_problema].mean()), 3),
            "falsas_alarmas_en_normales": round(float(alerta[~hay_problema].mean()), 3),
            "fallas_en_prueba": len(avisos),
            "fallas_avisadas_antes": len(detectadas),
            "anticipacion_promedio_segundos": round(float(np.mean(detectadas)), 1) if detectadas else None,
            "anticipacion_por_falla": distribucion,
        },
        "detector_anomalias": {"proporcion_marcada_por_estado": {k: round(v, 3) for k, v in tasa_anomalias.items()}},
        "importancia_caracteristicas": dict(sorted(
            zip(COLUMNAS, np.round(clasificador.feature_importances_, 4).tolist()),
            key=lambda par: -par[1])),
    }
    with open(os.path.join(CARPETA_RESULTADOS, "metricas.json"), "w", encoding="utf-8") as f:
        json.dump(metricas, f, indent=2, ensure_ascii=False)
    grafico_matriz(matriz, os.path.join(CARPETA_RESULTADOS, "matriz_confusion.png"))
    grafico_importancia(clasificador.feature_importances_, os.path.join(CARPETA_RESULTADOS, "importancia.png"))
    grafico_linea_de_tiempo(test.reset_index(drop=True), riesgo, os.path.join(CARPETA_RESULTADOS, "linea_de_tiempo.png"))
    print(f"Métricas e imágenes en {CARPETA_RESULTADOS}/")


if __name__ == "__main__":
    main()
