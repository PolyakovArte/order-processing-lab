FROM python:3.11-slim-bookworm

WORKDIR /app

COPY requirements.txt .
COPY packages/mysql_connector_python-9.4.0-cp311-cp311-manylinux_2_28_x86_64.whl /tmp/

RUN pip install --no-cache-dir /tmp/mysql_connector_python-9.4.0-cp311-cp311-manylinux_2_28_x86_64.whl \
    && pip install --no-cache-dir Flask==3.1.2 kafka-python==2.2.15 pika==1.3.2

COPY app.py .

EXPOSE 5000

CMD ["python", "app.py"]