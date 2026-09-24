import pika

connection = pika.BlockingConnection(
    pika.ConnectionParameters(
        host="rabbitmq",
        port=5672,
        credentials=pika.PlainCredentials("admin", "admin")
    )
)

channel = connection.channel()

channel.queue_declare(
    queue="order-notifications",
    durable=True
)

print("RabbitMQ consumer started")


def callback(ch, method, properties, body):
    message = body.decode("utf-8")

    print(f"[RABBITMQ] Notification: {message}")

    ch.basic_ack(delivery_tag=method.delivery_tag)


channel.basic_qos(prefetch_count=1)

channel.basic_consume(
    queue="order-notifications",
    on_message_callback=callback
)

channel.start_consuming()