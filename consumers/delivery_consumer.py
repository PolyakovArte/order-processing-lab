import json

from kafka import KafkaConsumer


consumer = KafkaConsumer(
    "order-events",
    bootstrap_servers="kafka:9092",
    group_id="delivery-preparation",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    key_deserializer=lambda key: key.decode("utf-8") if key else None,
    value_deserializer=lambda value: json.loads(value.decode("utf-8")),
)


print("Delivery consumer started")

for message in consumer:
    order = message.value

    print(
        f"[DELIVERY] Preparing delivery "
        f"for order #{order['order_id']}: "
        f"{order['product']}"
    )