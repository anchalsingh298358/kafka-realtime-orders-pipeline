import json
import time
import uuid
import io
import os
import random
from datetime import datetime
from fastavro import parse_schema, schemaless_writer
from confluent_kafka import Producer


# -------------------------------
# Load Avro Schema
# -------------------------------

SCHEMA_PATH = os.path.join(
    os.path.dirname(__file__),
    "..",
    "schemas",
    "order_schema.json"
)

with open(SCHEMA_PATH) as f:
    schema = json.load(f)

parsed_schema = parse_schema(schema)

# -------------------------------
# Kafka configuration
# -------------------------------

conf = {
    "bootstrap.servers": "localhost:9092",
    "client.id": "order-producer"
}

producer = Producer(conf)

TOPIC = "orders"


# -------------------------------
# generate order 
# -------------------------------
def generate_order():
    """
    Simulates a retail order event.
    """
    order = {
        "order_id": str(uuid.uuid4()),
        "customer_id": f"CUST-{random.randint(1000, 9999)}",
        "product_id": f"PROD-{random.randint(100, 999)}",
        "quantity": random.randint(1, 5),
        "price": round(random.uniform(10, 500), 2),
        "timestamp": datetime.utcnow().isoformat()
    }

    return order

# -------------------------------
# Message deliver
# -------------------------------
def delivery_report(err, msg):
    """
    Callback executed once Kafka acknowledges the message.
    """
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(
            f"Message delivered to {msg.topic()} "
            f"[partition {msg.partition()}] at offset {msg.offset()}"
        )
# -------------------------------
# Serialize schema
# -------------------------------
def serialize_avro(order):

    bytes_writer = io.BytesIO()

    schemaless_writer(bytes_writer, parsed_schema, order)

    return bytes_writer.getvalue()

# -------------------------------
# Produce order
# -------------------------------
def produce_orders():

    while True:

        order = generate_order()

        producer.produce(
            topic=TOPIC,
            key=order["customer_id"],
            value=serialize_avro(order),
            callback=delivery_report
        )
        producer.poll(0)

        print("Produced:", order)

        time.sleep(1)


if __name__ == "__main__":
    produce_orders()