FROM python:3.11-slim-bookworm

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY payment_consumer.py .
COPY delivery_consumer.py .
COPY rabbit_consumer.py .

CMD ["python", "payment_consumer.py"]