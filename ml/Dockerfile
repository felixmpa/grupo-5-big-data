FROM python:3.12-slim
RUN pip install --no-cache-dir uv==0.8.*

WORKDIR /app

# Instala las versiones exactas de uv.lock (igual que en local).
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev
ENV PATH="/app/.venv/bin:$PATH"

COPY caracteristicas.py entrenar.py predictor.py ./

# Por defecto, la predicción en vivo. El entrenamiento se corre aparte:
#   docker compose run --rm entrenar
CMD ["python", "predictor.py"]
