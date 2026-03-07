from prometheus_client import Counter, Histogram, start_http_server

# total processed messages
messages_processed_total = Counter(
    "kafka_messages_processed_total",
    "Total number of Kafka messages successfully processed"
)

# total failed messages
messages_failed_total = Counter(
    "kafka_messages_failed_total",
    "Total number of Kafka messages that failed processing"
)

# processing time for each message
message_processing_seconds = Histogram(
    "kafka_message_processing_seconds",
    "Time taken to process a single Kafka message"
)


def start_metrics_server():
    """
    Starts Prometheus metrics server
    """
    start_http_server(8000)