# Mosquitto (broker MQTT)

**MQTT** es un protocolo de mensajería liviano, muy usado en IoT porque consume poca red y batería. Funciona con el modelo *publicar / suscribirse*:

- Los **sensores publican** mensajes en un *topic* (por ejemplo `fabrica/maquina-01/lecturas`).
- El **broker** (Mosquitto) recibe esos mensajes y se los entrega a quien esté **suscrito** a ese topic.

Así los sensores no necesitan saber quién usa sus datos. En el PR 2 el puente hacia Kafka será un suscriptor más.

## Topics que usamos

| Topic | Quién publica | Contenido |
|---|---|---|
| `fabrica/<maquina_id>/lecturas` | Sensores | Una lectura en JSON (ver `sensores/README.md`) |

Para suscribirse a todas las máquinas se usa el comodín `fabrica/#` (o `fabrica/+/lecturas`).

## Configuración

`mosquitto.conf` es mínima:

- Escucha en el puerto **1883**.
- Permite conexiones sin usuario ni contraseña. Alcanza para una demo local; en producción se activaría autenticación y TLS (puerto 8883).
- No guarda mensajes en disco. El histórico lo guardan Kafka y HDFS.

## Cómo probarlo

```bash
docker compose up -d mosquitto

# Terminal 1: suscribirse
docker compose exec mosquitto mosquitto_sub -t 'fabrica/#' -v

# Terminal 2: publicar un mensaje de prueba
docker compose exec mosquitto mosquitto_pub -t 'fabrica/prueba/lecturas' -m '{"hola": "mundo"}'
```
