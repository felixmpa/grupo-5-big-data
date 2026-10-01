"""Consumidor Kafka -> HDFS (Data Lake).

Lee el topic ``sensores`` y guarda el histórico en crudo en HDFS, en
archivos JSON Lines (una lectura por línea) separados por día:

    /datalake/crudo/sensores/fecha=2026-10-01/sensores-<hora>-<id>.jsonl

Junta las lecturas en memoria y las escribe cada cierto tiempo, para no crear
miles de archivos chiquitos. A cada lectura le agrega la partición y el
offset de Kafka; así Spark (PR 5) puede eliminar duplicados si algún lote se
escribió dos veces.
"""

import json
import os
import signal
import sys
import time
import uuid
from collections import defaultdict
from datetime import datetime, timezone

from confluent_kafka import Consumer, KafkaException
from hdfs import InsecureClient

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9094")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "sensores")
KAFKA_GRUPO = os.getenv("KAFKA_GRUPO", "consumidor-hdfs")
HDFS_URL = os.getenv("HDFS_URL", "http://localhost:9870")
HDFS_USUARIO = os.getenv("HDFS_USUARIO", "hadoop")
HDFS_RUTA = os.getenv("HDFS_RUTA", "/datalake/crudo/sensores")
# Escribe un archivo cada N segundos o cada N lecturas, lo que pase primero.
SEGUNDOS_POR_ARCHIVO = float(os.getenv("SEGUNDOS_POR_ARCHIVO", "60"))
LECTURAS_POR_ARCHIVO = int(os.getenv("LECTURAS_POR_ARCHIVO", "5000"))

corriendo = True


def a_linea(msg):
    """Devuelve (fecha, línea JSON) para el mensaje, o None si no es un objeto JSON."""
    try:
        lectura = json.loads(msg.value())
        if not isinstance(lectura, dict):
            raise ValueError("no es un objeto JSON")
    except ValueError:
        print(f"Mensaje inválido, se descarta: {msg.value()[:100]!r}")
        return None

    # El día sale del timestamp de Kafka (no del mensaje), así un mensaje
    # con fecha rara no puede romper la escritura.
    _, ms = msg.timestamp()
    fecha = datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
    lectura["_kafka_particion"] = msg.partition()
    lectura["_kafka_offset"] = msg.offset()
    return fecha, json.dumps(lectura, ensure_ascii=False)


def escribir(hdfs, lineas_por_fecha):
    """Escribe un archivo por fecha. Reintenta mientras HDFS no responda."""
    nombre = f"sensores-{datetime.now(timezone.utc):%H%M%S}-{uuid.uuid4().hex[:8]}.jsonl"
    for fecha, lineas in lineas_por_fecha.items():
        carpeta = f"{HDFS_RUTA}/fecha={fecha}"
        while True:
            try:
                # Primero se escribe con un nombre oculto (empieza con ".") y
                # recién al final se renombra: Spark nunca ve archivos a medias.
                temporal = f"{carpeta}/.{nombre}.tmp"
                hdfs.write(temporal, data="\n".join(lineas) + "\n", encoding="utf-8", overwrite=True)
                hdfs.rename(temporal, f"{carpeta}/{nombre}")
                print(f"Guardado {carpeta}/{nombre} ({len(lineas)} lecturas)")
                break
            except Exception as error:  # noqa: BLE001 - cualquier fallo de red o de HDFS
                motivo = str(error).splitlines()[0]  # sin la traza completa de Java
                if not corriendo:
                    print(f"No se pudo escribir en HDFS al detenerse ({motivo}).")
                    return False  # sin confirmar: se relee al volver
                print(f"No se pudo escribir en HDFS ({motivo}). Reintentando en 5 s...")
                time.sleep(5)
    return True


def main():
    sys.stdout.reconfigure(line_buffering=True)

    def detener(*_):
        global corriendo
        print("Deteniendo: guardando lo pendiente...")
        corriendo = False

    signal.signal(signal.SIGTERM, detener)
    signal.signal(signal.SIGINT, detener)

    consumidor = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": KAFKA_GRUPO,
        "auto.offset.reset": "earliest",
        # Confirmamos nosotros, solo después de guardar en HDFS.
        "enable.auto.commit": False,
    })
    consumidor.subscribe([KAFKA_TOPIC])
    hdfs = InsecureClient(HDFS_URL, user=HDFS_USUARIO)
    print(f"Leyendo '{KAFKA_TOPIC}' de {KAFKA_BOOTSTRAP} -> HDFS {HDFS_URL}{HDFS_RUTA}")

    pendientes = defaultdict(list)  # fecha -> líneas
    cantidad = 0
    hay_offsets = False  # leímos algo (aunque fuera inválido) que falta confirmar
    ultimo_guardado = time.monotonic()
    guardadas = 0

    while True:
        if corriendo:
            for msg in consumidor.consume(num_messages=500, timeout=1.0):
                if msg.error():
                    print(f"Error de Kafka: {msg.error()}")
                    continue
                hay_offsets = True
                resultado = a_linea(msg)
                if resultado:
                    fecha, linea = resultado
                    pendientes[fecha].append(linea)
                    cantidad += 1

        toca_guardar = (
            cantidad >= LECTURAS_POR_ARCHIVO
            or time.monotonic() - ultimo_guardado >= SEGUNDOS_POR_ARCHIVO
            or not corriendo
        )
        if toca_guardar and hay_offsets:
            if cantidad and not escribir(hdfs, pendientes):
                break  # no se pudo guardar: no confirmar, se relee al volver
            try:
                consumidor.commit(asynchronous=False)
            except KafkaException as error:
                # En el peor caso el lote se escribe dos veces; Spark lo
                # deduplica con (_kafka_particion, _kafka_offset).
                print(f"No se pudo confirmar el lote en Kafka: {error}")
            guardadas += cantidad
            pendientes.clear()
            cantidad = 0
            hay_offsets = False
        if toca_guardar:
            ultimo_guardado = time.monotonic()
        if not corriendo:
            break

    consumidor.close()
    print(f"Listo. Total guardadas: {guardadas}")


if __name__ == "__main__":
    main()
