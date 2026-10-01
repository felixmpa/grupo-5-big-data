"""Consumidor Kafka -> InfluxDB.

Lee las lecturas del topic ``sensores`` y las guarda en InfluxDB como
series de tiempo, listas para consultarlas y graficarlas en Grafana.

Primero escribe en InfluxDB y recién después le confirma a Kafka que el lote
está procesado. Si algo falla en el medio, al reiniciar vuelve a leer ese
lote. Repetir una lectura no la duplica en InfluxDB, porque un punto con la
misma máquina y el mismo timestamp se sobrescribe.
"""

import json
import os
import signal
import sys
import time
from datetime import datetime

from confluent_kafka import Consumer, KafkaException
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb_client.rest import ApiException
from urllib3.exceptions import HTTPError

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9094")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "sensores")
KAFKA_GRUPO = os.getenv("KAFKA_GRUPO", "consumidor-influxdb")
INFLUX_URL = os.getenv("INFLUX_URL", "http://localhost:8086")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN", "token-demo-grupo5")
INFLUX_ORG = os.getenv("INFLUX_ORG", "grupo5")
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET", "sensores")

TAMANO_LOTE = 500
CAMPOS = ("temperatura", "vibracion", "presion", "rpm")

corriendo = True


def a_punto(valor):
    """Convierte un mensaje de Kafka en un punto de InfluxDB (o None si es inválido)."""
    try:
        lectura = json.loads(valor)
        # Validamos la fecha aquí: Point.time() no la revisa y el error saldría
        # recién al escribir en InfluxDB, bloqueando el lote para siempre.
        momento = datetime.fromisoformat(lectura["timestamp"].replace("Z", "+00:00"))
        punto = Point("lecturas").tag("maquina_id", lectura["maquina_id"])
        for campo in CAMPOS:
            punto = punto.field(campo, float(lectura[campo]))
        punto = punto.field("estado", str(lectura["estado"])).time(momento, WritePrecision.MS)
        # Lo convertimos ya al formato que se envía a InfluxDB. Así cualquier dato
        # que no se pueda enviar (por ejemplo texto con caracteres inválidos como
        # "\ud800") se detecta aquí y no al escribir.
        punto.to_line_protocol().encode("utf-8")
        return punto
    except (ValueError, KeyError, TypeError, AttributeError):
        print(f"Mensaje inválido, se descarta: {valor[:100]!r}")
        return None


def escribir_con_reintentos(escritor, puntos):
    """Escribe el lote; si InfluxDB no responde, espera y reintenta.

    Solo se reintenta ante fallos de red o del servidor. Un error en los datos
    no se arregla reintentando: esos mensajes ya se descartaron en a_punto().
    """
    while corriendo:
        try:
            escritor.write(bucket=INFLUX_BUCKET, record=puntos)
            return True
        except ApiException as error:
            if error.status in (400, 422):
                # InfluxDB rechazó los datos (los válidos del lote sí se guardan).
                print(f"InfluxDB rechazó parte del lote: {error.body}")
                return True
            motivo = f"HTTP {error.status} {error.reason}"
        except (HTTPError, OSError) as error:
            motivo = error
        print(f"No se pudo escribir en InfluxDB ({motivo}). Reintentando en 5 s...")
        time.sleep(5)
    return False


def main():
    sys.stdout.reconfigure(line_buffering=True)

    def detener(*_):
        global corriendo
        print("Deteniendo...")
        corriendo = False

    signal.signal(signal.SIGTERM, detener)
    signal.signal(signal.SIGINT, detener)

    consumidor = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": KAFKA_GRUPO,
        # La primera vez empieza desde el mensaje más antiguo del topic.
        "auto.offset.reset": "earliest",
        # Confirmamos nosotros, solo después de escribir en InfluxDB.
        "enable.auto.commit": False,
    })
    consumidor.subscribe([KAFKA_TOPIC])

    influx = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
    escritor = influx.write_api(write_options=SYNCHRONOUS)
    print(f"Leyendo '{KAFKA_TOPIC}' de {KAFKA_BOOTSTRAP} -> InfluxDB {INFLUX_URL}, bucket '{INFLUX_BUCKET}'")

    guardadas = 0
    while corriendo:
        mensajes = consumidor.consume(num_messages=TAMANO_LOTE, timeout=1.0)
        if not mensajes:
            continue

        puntos = []
        for msg in mensajes:
            if msg.error():
                print(f"Error de Kafka: {msg.error()}")
                continue
            punto = a_punto(msg.value())
            if punto is not None:
                puntos.append(punto)

        if puntos and not escribir_con_reintentos(escritor, puntos):
            break  # nos pidieron detenernos sin haber escrito: no confirmar
        try:
            consumidor.commit(asynchronous=False)
        except KafkaException as error:
            # No es grave: en el peor caso se relee el lote y se sobrescribe igual.
            print(f"No se pudo confirmar el lote en Kafka: {error}")

        anterior, guardadas = guardadas, guardadas + len(puntos)
        if guardadas // 1000 > anterior // 1000:
            print(f"{guardadas} lecturas guardadas en InfluxDB")

    consumidor.close()
    influx.close()
    print(f"Listo. Total guardadas: {guardadas}")


if __name__ == "__main__":
    main()
