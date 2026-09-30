FROM python:3.12-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY simulador.py .

# -u: muestra los prints en los logs sin esperar al buffer
CMD ["python", "-u", "simulador.py"]
