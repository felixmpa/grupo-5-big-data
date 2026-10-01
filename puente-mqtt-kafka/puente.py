"""Puente MQTT -> Kafka.

Se suscribe a las lecturas de todas las máquinas en MQTT y reenvía cada
mensaje al topic ``sensores`` de Kafka. Usa el id de la máquina como clave,
así todas las lecturas de una máquina caen en la misma partición y Kafka
mantiene su orden.
"""

import json
import os
import signal
import sys
import time

import paho.mqtt.client as mqtt
from confluent_kafka import Producer

MQTT_HOST = os.getenv("MQTT_HOST", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_TOPIC = os.getenv("MQTT_TOPIC", "fabrica/+/lecturas")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9094")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "sensores")

productor = Producer({
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    "client.id": "puente-mqtt-kafka",
    # Espera a que Kafka confirme cada mensaje y evita duplicados al reintentar.
    "acks": "all",
    "enable.idempotence": True,
    # Agrupa mensajes hasta 50 ms para enviarlos en lotes (más eficiente).
    "linger.ms": 50,
})

enviados = 0
errores = 0


def al_entregar(error, mensaje):
    """Kafka llama a esta función cuando confirma (o rechaza) un mensaje."""
    global enviados, errores
    if error:
        errores += 1
        print(f"Error al enviar a Kafka: {error}")
        return
    enviados += 1
    if enviados % 500 == 0:
        print(f"{enviados} mensajes enviados a Kafka (errores: {errores})")


def al_conectar(cliente, userdata, flags, codigo, propiedades):
    if codigo.is_failure:
        print(f"MQTT rechazó la conexión: {codigo}")
        return
    # Suscribirse aquí hace que se vuelva a suscribir si la conexión se corta.
    cliente.subscribe(MQTT_TOPIC, qos=1)
    print(f"Conectado a MQTT en {MQTT_HOST}:{MQTT_PORT}, escuchando '{MQTT_TOPIC}'")


def al_recibir(cliente, userdata, msg):
    try:
        lectura = json.loads(msg.payload)
        maquina_id = lectura["maquina_id"]
    except (ValueError, KeyError, TypeError):
        print(f"Mensaje inválido en '{msg.topic}', se descarta: {msg.payload[:100]!r}")
        return

    while True:
        try:
            productor.produce(
                KAFKA_TOPIC,
                key=maquina_id,
                value=msg.payload,
                on_delivery=al_entregar,
            )
            break
        except BufferError:
            # La cola interna está llena (por ejemplo, Kafka caído): esperar.
            productor.poll(1)
    productor.poll(0)  # procesa las confirmaciones pendientes


def conectar_mqtt():
    cliente = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    cliente.on_connect = al_conectar
    cliente.on_message = al_recibir
    while True:
        try:
            cliente.connect(MQTT_HOST, MQTT_PORT)
            return cliente
        except OSError as error:
            print(f"No se pudo conectar a {MQTT_HOST}:{MQTT_PORT} ({error}). Reintentando...")
            time.sleep(2)


def main():
    sys.stdout.reconfigure(line_buffering=True)
    print(f"Enviando a Kafka en {KAFKA_BOOTSTRAP}, topic '{KAFKA_TOPIC}'")
    cliente = conectar_mqtt()

    def detener(*_):
        print("Deteniendo: enviando los mensajes pendientes a Kafka...")
        cliente.disconnect()

    signal.signal(signal.SIGTERM, detener)
    signal.signal(signal.SIGINT, detener)

    cliente.loop_forever()
    productor.flush(10)
    print(f"Listo. Total enviados: {enviados}, errores: {errores}")


if __name__ == "__main__":
    main()
