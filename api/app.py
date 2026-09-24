import json
import time

import mysql.connector
from flask import Flask, jsonify, request
from kafka import KafkaProducer
import pika


app = Flask(__name__)


MYSQL_CONFIG = {
    "host": "mysql",
    "user": "orders_user",
    "password": "orders_password",
    "database": "orders",
}


def get_mysql_connection():
    return mysql.connector.connect(**MYSQL_CONFIG)


def init_database():
    for attempt in range(20):
        try:
            connection = get_mysql_connection()
            cursor = connection.cursor()

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS orders (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    customer VARCHAR(255) NOT NULL,
                    product VARCHAR(255) NOT NULL,
                    amount DECIMAL(10, 2) NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            connection.commit()
            cursor.close()
            connection.close()

            print("MySQL database initialized")
            return

        except Exception as e:
            print(f"MySQL is not ready: {e}")
            time.sleep(3)

    raise RuntimeError("Could not connect to MySQL")


def get_kafka_producer():
    return KafkaProducer(
        bootstrap_servers="kafka:9092",
        key_serializer=lambda key: str(key).encode("utf-8"),
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )


def send_rabbitmq_notification(message):
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(
            host="rabbitmq",
            port=5672,
            credentials=pika.PlainCredentials("admin", "admin"),
        )
    )

    channel = connection.channel()

    channel.queue_declare(
        queue="order-notifications",
        durable=True,
    )

    channel.basic_publish(
        exchange="",
        routing_key="order-notifications",
        body=message,
    )

    connection.close()


@app.route("/api/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"})


@app.route("/api/orders", methods=["POST"])
def create_order():
    data = request.get_json()

    if not data:
        return jsonify({"error": "JSON body is required"}), 400

    customer = data.get("customer")
    product = data.get("product")
    amount = data.get("amount")

    if not customer or not product or amount is None:
        return jsonify({
            "error": "customer, product and amount are required"
        }), 400

    try:
        connection = get_mysql_connection()
        cursor = connection.cursor()

        cursor.execute(
            """
            INSERT INTO orders (customer, product, amount)
            VALUES (%s, %s, %s)
            """,
            (customer, product, amount),
        )

        connection.commit()

        order_id = cursor.lastrowid

        cursor.close()
        connection.close()

        order_event = {
            "order_id": order_id,
            "customer": customer,
            "product": product,
            "amount": float(amount),
        }

        producer = get_kafka_producer()

        producer.send(
            "order-events",
            key=order_id,
            value=order_event,
        )

        producer.flush()
        producer.close()

        notification = (
            f"New order #{order_id}: "
            f"{customer} ordered {product} "
            f"for {amount}"
        )

        send_rabbitmq_notification(notification)

        return jsonify({
            "status": "success",
            "order_id": order_id,
        }), 201

    except Exception as e:
        print(f"Error creating order: {e}")

        return jsonify({
            "status": "error",
            "error": str(e),
        }), 500


if __name__ == "__main__":
    init_database()

    app.run(
        host="0.0.0.0",
        port=5000,
    )