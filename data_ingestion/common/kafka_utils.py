# common/kafka_utils.py
import os, json
from confluent_kafka import Producer

BROKERS = os.getenv("KAFKA_BROKERS", "localhost:9092")

_conf = {"bootstrap.servers": BROKERS}
_producer = Producer(_conf)

def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Delivery failed: {err}")
    else:
        print(f"✅ Delivered to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}")

def publish_event(event: dict, topic: str, key: str | None = None):
    k = key.encode("utf-8") if key else None
    _producer.produce(
        topic,
        key=k,
        value=json.dumps(event).encode("utf-8"),
        callback=delivery_report,
    )
    _producer.flush()
