import io
import json
import os
from confluent_kafka import Consumer, Producer
from fastavro import parse_schema, schemaless_reader

from storage.parquet_writer import ParquetWriter
from .schema_validator import validate_order

from .metrics import (
    start_metrics_server,
    messages_processed_total,
    messages_failed_total,
    message_processing_seconds
)

# -------------------------------
# Start Prometheus metrics server
# -------------------------------
start_metrics_server()

# -------------------------------
# Initialize parquet writer
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

    writer.add_event(order)

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

            with message_processing_seconds.time():

                # Deserialize Avro
                order = deserialize_avro(msg.value())

                # Validate schema
                validate_order(order)

                # Process order
                process_order(order)

                # Update metrics
                messages_processed_total.inc()

        except Exception as e:

            messages_failed_total.inc()

            print("Processing failed:", str(e))

            try:
                send_to_dlq(order if 'order' in locals() else None, str(e))
            except Exception as dlq_error:
                print("DLQ publish failed:", dlq_error)


except KeyboardInterrupt:

    print("Stopping consumer")

finally:

    consumer.close()