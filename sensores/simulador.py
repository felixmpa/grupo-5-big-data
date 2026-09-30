"""Simulador de sensores IoT de una fábrica.

Cada máquina publica una lectura por segundo al broker MQTT en el topic
``fabrica/<maquina_id>/lecturas``. De vez en cuando una máquina empieza a
degradarse (sube la temperatura y la vibración) hasta llegar a una falla;
luego se "repara" y vuelve a la normalidad.
"""

import json
import os
import random
import time
from datetime import datetime, timezone

import paho.mqtt.client as mqtt

MQTT_HOST = os.getenv("MQTT_HOST", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
NUM_MAQUINAS = int(os.getenv("NUM_MAQUINAS", "5"))
INTERVALO_SEGUNDOS = float(os.getenv("INTERVALO_SEGUNDOS", "1"))
# Probabilidad (por lectura) de que una máquina sana empiece a degradarse.
PROB_DEGRADACION = float(os.getenv("PROB_DEGRADACION", "0.005"))

# Cuántas lecturas dura cada etapa antes de pasar a la siguiente.
LECTURAS_DEGRADACION = 60
LECTURAS_FALLA = 20


class Maquina:
    """Una máquina con sus valores normales y su estado actual.

    Estados: ``normal`` -> ``degradacion`` -> ``falla`` -> ``normal``.
    """

    def __init__(self, maquina_id):
        self.maquina_id = maquina_id
        # Cada máquina tiene valores base un poco distintos.
        self.temp_base = random.uniform(55, 65)  # °C
        self.vib_base = random.uniform(1.5, 2.5)  # mm/s
        self.presion_base = random.uniform(4.5, 5.5)  # bar
        self.rpm_base = random.uniform(1450, 1550)
        self.estado = "normal"
        self.contador = 0  # lecturas en el estado actual

    def _avanzar_estado(self):
        self.contador += 1
        if self.estado == "normal" and random.random() < PROB_DEGRADACION:
            self.estado, self.contador = "degradacion", 0
        elif self.estado == "degradacion" and self.contador >= LECTURAS_DEGRADACION:
            self.estado, self.contador = "falla", 0
        elif self.estado == "falla" and self.contador >= LECTURAS_FALLA:
            self.estado, self.contador = "normal", 0  # mantenimiento

    def _severidad(self):
        """0 = sana, 1 = falla. Sube de forma gradual durante la degradación."""
        if self.estado == "degradacion":
            return self.contador / LECTURAS_DEGRADACION
        if self.estado == "falla":
            return 1.0
        return 0.0

    def leer(self):
        self._avanzar_estado()
        s = self._severidad()
        ahora = datetime.now(timezone.utc).isoformat(timespec="milliseconds")
        return {
            "maquina_id": self.maquina_id,
            "timestamp": ahora.replace("+00:00", "Z"),
            "temperatura": round(self.temp_base + 30 * s + random.gauss(0, 0.8), 2),
            "vibracion": round(self.vib_base + 6 * s**2 + random.gauss(0, 0.15), 3),
            "presion": round(self.presion_base - 1.5 * s + random.gauss(0, 0.1), 3),
            "rpm": round(self.rpm_base - 200 * s + random.gauss(0, 10), 1),
            # Etiqueta real del estado: solo existe porque es una simulación.
            # Sirve para entrenar y evaluar el modelo en el PR 6.
            "estado": self.estado,
        }


def conectar():
    # Sin client_id fijo: paho genera uno aleatorio y se pueden correr
    # varios simuladores a la vez (por ejemplo, uno local y otro en Docker).
    cliente = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    while True:
        try:
            cliente.connect(MQTT_HOST, MQTT_PORT)
            break
        except OSError as error:
            print(f"No se pudo conectar a {MQTT_HOST}:{MQTT_PORT} ({error}). Reintentando...")
            time.sleep(2)
    cliente.loop_start()
    print(f"Conectado a MQTT en {MQTT_HOST}:{MQTT_PORT}")
    return cliente


def main():
    maquinas = [Maquina(f"maquina-{i:02d}") for i in range(1, NUM_MAQUINAS + 1)]
    cliente = conectar()

    ciclo = 0
    while True:
        ciclo += 1
        for maquina in maquinas:
            estado_anterior = maquina.estado
            lectura = maquina.leer()
            topic = f"fabrica/{maquina.maquina_id}/lecturas"
            cliente.publish(topic, json.dumps(lectura), qos=1)
            if maquina.estado != estado_anterior:
                print(f"{maquina.maquina_id}: {estado_anterior} -> {maquina.estado}")
        if ciclo % 60 == 0:
            print(f"{ciclo * len(maquinas)} lecturas publicadas")
        time.sleep(INTERVALO_SEGUNDOS)


if __name__ == "__main__":
    main()
