"""Predicción de fallas en vivo.

Lee las lecturas de Kafka a medida que llegan, guarda los últimos 30 s de
cada máquina y cada 5 s calcula sus características (las mismas que Spark)
para pasárselas a los modelos. Escribe el resultado en InfluxDB, en el
measurement "predicciones", para que Grafana (PR 7) muestre el riesgo y las
alertas.
"""

import json
import os
import signal
import sys
import time
from collections import Counter, defaultdict, deque
from datetime import datetime

import joblib
import pandas as pd
from confluent_kafka import Consumer
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb_client.rest import ApiException
from urllib3.exceptions import HTTPError

from caracteristicas import COLUMNAS, MIN_LECTURAS, SENSORES, calcular

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9094")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "sensores")
INFLUX_URL = os.getenv("INFLUX_URL", "http://localhost:8086")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN", "token-demo-grupo5")
INFLUX_ORG = os.getenv("INFLUX_ORG", "grupo5")
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET", "sensores")
ARCHIVO_MODELO = os.getenv(
    "ARCHIVO_MODELO", os.path.join(os.path.dirname(os.path.abspath(__file__)), "modelos", "modelo.joblib"))
VENTANA_SEGUNDOS = 30  # igual que en Spark
CADA_SEGUNDOS = 5      # cada cuánto se predice por máquina
# Los mismos rangos que usa Spark para descartar lecturas imposibles.
RANGOS = {"temperatura": (-40, 200), "vibracion": (0, 100), "presion": (0, 20), "rpm": (0, 5000)}

corriendo = True


def leer_mensaje(valor):
    """Devuelve (maquina_id, segundos, valores, estado) o None si el mensaje es inválido."""
    try:
        lectura = json.loads(valor)
        maquina = str(lectura["maquina_id"])
        maquina.encode("utf-8")  # texto con caracteres inválidos -> ValueError
        momento = datetime.fromisoformat(lectura["timestamp"].replace("Z", "+00:00"))
        valores = {s: float(lectura[s]) for s in SENSORES}
        if not all(RANGOS[s][0] <= v <= RANGOS[s][1] for s, v in valores.items()):
            raise ValueError("valor fuera de rango")
        return maquina, momento.timestamp(), valores, str(lectura.get("estado", ""))
    except (ValueError, KeyError, TypeError, AttributeError):
        print(f"Mensaje inválido, se descarta: {valor[:100]!r}")
        return None


def esperar_modelo():
    """Espera a que exista el modelo (se crea con entrenar.py)."""
    avisado = False
    while corriendo and not os.path.exists(ARCHIVO_MODELO):
        if not avisado:
            print(f"Esperando el modelo en {ARCHIVO_MODELO} (ejecutar entrenar.py)...")
            avisado = True
        time.sleep(10)
    return joblib.load(ARCHIVO_MODELO) if corriendo else None


def predecir(modelo, lecturas):
    """Aplica los dos modelos a las lecturas de una ventana."""
    segundos = [l[0] for l in lecturas]
    valores = {s: [l[1][s] for l in lecturas] for s in SENSORES}
    x = pd.DataFrame([calcular(segundos, valores)], columns=COLUMNAS)
    probabilidades = dict(zip(modelo["clasificador"].classes_, modelo["clasificador"].predict_proba(x)[0]))
    riesgo = probabilidades.get("degradacion", 0) + probabilidades.get("falla", 0)
    return {
        "prob_normal": float(probabilidades.get("normal", 0)),
        "prob_degradacion": float(probabilidades.get("degradacion", 0)),
        "prob_falla": float(probabilidades.get("falla", 0)),
        "riesgo": float(riesgo),
        "estado_predicho": max(probabilidades, key=probabilidades.get),
        "alerta": bool(riesgo >= modelo["umbral_riesgo"]),
        # score_samples: cuanto más bajo, más rara es la ventana.
        "anomalia_score": float(-modelo["detector"].score_samples(x)[0]),
        "es_anomalia": bool(modelo["detector"].predict(x)[0] == -1),
    }


def escribir(escritor, puntos):
    """Escribe en InfluxDB. Solo reintenta ante fallos de red o del servidor."""
    while corriendo:
        try:
            escritor.write(bucket=INFLUX_BUCKET, record=puntos)
            return
        except ApiException as error:
            if error.status in (400, 422):
                print(f"InfluxDB rechazó parte del lote: {error.body}")
                return
            motivo = f"HTTP {error.status} {error.reason}"
        except (HTTPError, OSError) as error:
            motivo = error
        print(f"No se pudo escribir en InfluxDB ({motivo}). Reintentando en 5 s...")
        time.sleep(5)


def main():
    sys.stdout.reconfigure(line_buffering=True)

    def detener(*_):
        global corriendo
        print("Deteniendo...")
        corriendo = False

    signal.signal(signal.SIGTERM, detener)
    signal.signal(signal.SIGINT, detener)

    modelo = esperar_modelo()
    if modelo is None:
        return
    print(f"Modelo cargado (entrenado el {modelo['entrenado']})")

    consumidor = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": "predictor",
        # Solo interesa el presente: arranca desde el último mensaje y no
        # guarda su posición (al reiniciar no reprocesa lo viejo).
        "auto.offset.reset": "latest",
        "enable.auto.commit": False,
    })
    consumidor.subscribe([KAFKA_TOPIC])
    influx = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
    escritor = influx.write_api(write_options=SYNCHRONOUS)
    print(f"Prediciendo cada {CADA_SEGUNDOS} s con ventanas de {VENTANA_SEGUNDOS} s -> InfluxDB 'predicciones'")

    recientes = defaultdict(deque)          # maquina -> (segundos, valores, estado)
    ultima_prediccion = defaultdict(float)  # maquina -> segundos
    alerta_activa = defaultdict(bool)

    while corriendo:
        puntos = []
        for msg in consumidor.consume(num_messages=500, timeout=1.0):
            if msg.error():
                print(f"Error de Kafka: {msg.error()}")
                continue
            lectura = leer_mensaje(msg.value())
            if lectura is None:
                continue
            maquina, segundos, valores, estado = lectura
            ventana = recientes[maquina]
            if ventana and segundos <= ventana[-1][0]:
                continue  # lectura atrasada (por ejemplo de generar_historico.py): no es el presente
            ventana.append((segundos, valores, estado))
            while ventana and ventana[0][0] <= segundos - VENTANA_SEGUNDOS:
                ventana.popleft()  # solo los últimos 30 s

            if segundos - ultima_prediccion[maquina] < CADA_SEGUNDOS or len(ventana) < MIN_LECTURAS:
                continue
            ultima_prediccion[maquina] = segundos
            resultado = predecir(modelo, list(ventana))

            punto = Point("predicciones").tag("maquina_id", maquina).time(int(segundos * 1000), WritePrecision.MS)
            for campo, valor in resultado.items():
                punto = punto.field(campo, valor)
            # Estado real = el mayoritario de la ventana, la misma etiqueta con la
            # que se entrenó el modelo (solo existe porque es una simulación).
            estado_real = Counter(l[2] for l in ventana).most_common(1)[0][0]
            puntos.append(punto.field("estado_real", estado_real))

            if resultado["alerta"] != alerta_activa[maquina]:
                alerta_activa[maquina] = resultado["alerta"]
                texto = "ALERTA" if resultado["alerta"] else "sin alerta"
                print(f"{maquina}: {texto} (riesgo {resultado['riesgo']:.0%}, "
                      f"predicho {resultado['estado_predicho']}, real {estado_real})")
        if puntos:
            escribir(escritor, puntos)

    consumidor.close()
    influx.close()
    print("Listo.")


if __name__ == "__main__":
    main()
