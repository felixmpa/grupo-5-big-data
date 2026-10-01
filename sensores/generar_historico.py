"""Genera historia pasada para entrenar el modelo (PR 6).

Simula las mismas máquinas de simulador.py, pero con lecturas de las últimas
N horas (una por segundo) y las publica por MQTT lo más rápido posible. Pasan
por todo el pipeline (Kafka, InfluxDB, HDFS) igual que las lecturas en vivo:
así no hay que esperar horas para tener datos de entrenamiento.

    uv run generar_historico.py --horas 2
"""

import argparse
import json
import random
import sys
from datetime import datetime, timedelta, timezone

from simulador import NUM_MAQUINAS, Maquina, conectar


def main():
    sys.stdout.reconfigure(line_buffering=True)
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--horas", type=float, default=2, help="horas de historia (por defecto 2)")
    parser.add_argument("--semilla", type=int, default=None, help="para repetir exactamente la misma historia")
    args = parser.parse_args()

    if args.semilla is not None:
        random.seed(args.semilla)
    maquinas = [Maquina(f"maquina-{i:02d}") for i in range(1, NUM_MAQUINAS + 1)]
    cliente = conectar()

    segundos = int(args.horas * 3600)
    inicio = datetime.now(timezone.utc) - timedelta(seconds=segundos)
    print(f"Generando {segundos * len(maquinas)} lecturas desde {inicio:%Y-%m-%d %H:%M} UTC...")

    for segundo in range(segundos):
        momento = inicio + timedelta(seconds=segundo)
        for maquina in maquinas:
            lectura = maquina.leer(momento)
            info = cliente.publish(f"fabrica/{maquina.maquina_id}/lecturas", json.dumps(lectura), qos=1)
        if segundo % 600 == 599:
            info.wait_for_publish()  # no acumular demasiados mensajes en memoria
            print(f"  {(segundo + 1) * len(maquinas)} lecturas publicadas")

    info.wait_for_publish()
    cliente.disconnect()
    print("Listo.")


if __name__ == "__main__":
    main()
