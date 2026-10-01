# Grafana (dashboard)

Grafana muestra en tiempo real lo que pasa en la fábrica: lecturas de los sensores, **riesgo de falla** calculado por el modelo (PR 6), **alertas** y **anomalías**. Todo se configura solo al arrancar (*provisioning*), sin pasos manuales.

![Dashboard de la fábrica](../docs/img/grafana-dashboard.png)

## Cómo abrirlo

```bash
docker compose up -d --build
```

Abrir **http://localhost:3000**. El dashboard *Fábrica – Mantenimiento predictivo* aparece directamente, sin iniciar sesión (acceso de solo lectura). Para editarlo, iniciar sesión con usuario `admin` y contraseña `admin12345`.

> Para que haya riesgo y alertas, el modelo tiene que estar entrenado (ver `ml/README.md`). Sin modelo, los paneles de sensores funcionan igual y los de predicción quedan vacíos.

## Qué muestra

| Panel | Qué es | De dónde sale |
|---|---|---|
| **Riesgo de falla ahora** | Último riesgo de cada máquina. Verde < 50 %, rojo ≥ 50 % (alerta) | `predicciones.riesgo` |
| **Máquinas en alerta** | Cuántas máquinas superan el 50 % ahora | `predicciones.alerta` |
| **Lecturas por minuto** | Volumen de datos que entra al sistema (5 máquinas × 60 = ~300) | `lecturas` |
| **Estado según el modelo** / **Estado real** | Línea de tiempo por máquina: normal, degradación y falla. Ponerlas lado a lado muestra si el modelo acierta | `predicciones.estado_predicho` / `estado_real` |
| **Riesgo de falla (%)** | Evolución del riesgo. La línea roja punteada es el umbral de alerta | `predicciones.riesgo` |
| **Alertas de Grafana** | Alertas disparadas por la regla de Grafana | Regla `Riesgo de falla alto` |
| **Temperatura, Vibración, Presión, RPM** | Lecturas de los sensores, una línea por máquina | `lecturas` |
| **Puntaje de anomalía** | Isolation Forest: más alto = menos parecido a lo normal | `predicciones.anomalia_score` |
| **Historial de alertas** | Cada vez que una máquina entró en alerta, con su riesgo y los estados predicho y real | `predicciones` |

- Arriba a la izquierda se puede **filtrar por máquina**.
- El dashboard se actualiza solo cada 5 s.
- Si el sistema estuvo apagado, los gráficos muestran el hueco en lugar de unir los puntos con una recta.

**Colores:**
- Cada máquina tiene siempre el mismo color en todos los paneles.
- Los estados usan verde, amarillo y rojo, siempre acompañados de su nombre ("Normal", "Degradación", "Falla"). Así no dependen solo del color.

## Alertas

`provisioning/alerting/reglas.yaml` crea la regla **"Riesgo de falla alto"**:
- Cada 10 s revisa el último riesgo de cada máquina y se dispara **por máquina** cuando supera el 50 %.
- Se ve en el panel *Alertas de Grafana* y en el menú **Alerting → Alert rules**.
- Para recibir avisos por correo, Telegram, Slack, etc., hay que agregar un *contact point* en **Alerting → Contact points**. Para la demo no hace falta.

## Archivos

```
grafana/
├── dashboards/
│   └── fabrica.json                  # el dashboard (13 paneles)
└── provisioning/
    ├── datasources/influxdb.yaml     # conexión a InfluxDB (Flux, token de demo)
    ├── dashboards/fabrica.yaml       # carga los JSON de dashboards/
    ├── alerting/reglas.yaml          # regla de alerta por riesgo
    └── plugins/                      # vacío (Grafana lo busca al arrancar)
```

Las consultas están escritas en **Flux**, el lenguaje de InfluxDB 2. Por ejemplo, el panel de temperatura:

```flux
from(bucket: "sensores")
  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
  |> filter(fn: (r) => r._measurement == "lecturas" and r._field == "temperatura")
  |> filter(fn: (r) => r.maquina_id =~ /^${maquina:regex}$/)
  |> aggregateWindow(every: v.windowPeriod, fn: mean, createEmpty: false)
```

Para ver o cambiar la consulta de cualquier panel: abrir el menú del panel (⋮) → **Edit**.

**Si se edita el dashboard desde la web**, los cambios quedan en Grafana pero no en el repositorio. Para guardarlos en el repo: menú del dashboard → **Export → Export as JSON** y reemplazar `dashboards/fabrica.json`.
