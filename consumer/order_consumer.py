import io
import json
import os
from storage.parquet_writer import ParquetWriter
from confluent_kafka import Consumer, Producer
from fastavro import parse_schema, schemaless_reader



# -------------------------------
# Initialize the parquet writer 
# -------------------------------
writer = ParquetWriter(batch_size=10)

# -------------------------------
# Load Avro Schema
# -------------------------------

SCHEMA_PATH = os.path.join(
    os.path.dirname(__file__),
    "..",
    "schemas",
    "order_schema.json"
)

with open(SCHEMA_PATH, "r") as f:
    schema = json.load(f)

parsed_schema = parse_schema(schema)


# -------------------------------
# Kafka Configurations
# -------------------------------

consumer_conf = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "order-processing-group",
    "auto.offset.reset": "earliest"
}

producer_conf = {
    "bootstrap.servers": "localhost:9092"
}

consumer = Consumer(consumer_conf)
producer = Producer(producer_conf)

consumer.subscribe(["orders"])

DLQ_TOPIC = "orders_dlq"


# -------------------------------
# Avro Deserializer
# -------------------------------

def deserialize_avro(message_bytes):

    bytes_reader = io.BytesIO(message_bytes)

    return schemaless_reader(bytes_reader, parsed_schema)


# -------------------------------
# DLQ Producer
# -------------------------------

def send_to_dlq(event, reason):

    payload = {
        "failed_event": event,
        "reason": reason
    }

    producer.produce(
        DLQ_TOPIC,
        value=json.dumps(payload)
    )

    producer.poll(0)


# -------------------------------
# Processing Logic
# -------------------------------

def process_order(order):

    print(f"Processed order {order['order_id']} successfully")


# -------------------------------
# Consumer Loop
# -------------------------------

try:

    while True:

        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            print("Consumer error:", msg.error())
            continue

        try:

            order = deserialize_avro(msg.value())
            # process_order(order)  -- use if not writting to parquet
            writer.add_event(order)

        except Exception as e:

            print("Processing failed:", str(e))

            send_to_dlq(msg.value().decode("utf-8", errors="ignore"), str(e))


except KeyboardInterrupt:

    print("Stopping consumer")

finally:

    consumer.close()