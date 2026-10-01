# Documentación

- **[Informe técnico](informe-tecnico.md)**: el entregable final. Incluye arquitectura, implementación por capas con capturas reales, resultados del modelo, pruebas de resiliencia, escalabilidad hacia la nube, conclusiones y guía de la demo.
- `img/`: capturas reales del sistema funcionando y diagramas en PNG.
- `diagramas/`: fuente de cada diagrama en [Mermaid](https://mermaid.js.org/). GitHub los muestra directamente; para editarlos se puede usar https://mermaid.live y exportar el PNG a `img/` con el mismo nombre.

Las imágenes de resultados del modelo están en `../ml/resultados/` y se regeneran con `docker compose run --rm entrenar`.
