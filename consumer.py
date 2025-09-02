from confluent_kafka import Consumer, KafkaException
import json
from datetime import datetime
import pandas as pd
from zscore import calculate_zscore
from database_inserter import insert_message
from preprocessor import main

# Kafka configuration
KAFKA_BROKER = 'localhost:9092'
TOPIC_NAME = 'oil_rig_sensor_data'
CONSUMER_GROUP = 'oil_rig_analytics'

# Alert thresholds
ALERT_THRESHOLDS = {
    'vibration_level': 1.5,
    'bit_temperature': 110,
    'motor_temperature': 90,
    'mud_pressure': 3500
}


def create_consumer():
    """Create and return a Confluent Kafka consumer with JSON deserializer"""
    consumer_conf = {
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': CONSUMER_GROUP,
        'auto.offset.reset': 'latest',   # Start from latest messages
        'enable.auto.commit': True
    }
    consumer = Consumer(consumer_conf)
    consumer.subscribe(["oil_rig_sensor_data"])
    return consumer


def process_message(msg):
    """Process a single Kafka message and check for alerts"""
    try:
        data = json.loads(msg.value().decode('utf-8'))
    except Exception as e:
        print(f"⚠️ Error decoding message: {e}")
        return None

    rig_id = data['rig_id']
    timestamp = datetime.fromisoformat(data['timestamp'])

    print(f"\n📥 Processing data from {rig_id} at {timestamp}")

    # Maintenance flag alerts
    if data['maintenance_flag'] == 1:
        print(f"🚨 MAINTENANCE ALERT: {rig_id} has {data['failure_type']}")

    # Threshold alerts
    alerts = []
    for metric, threshold in ALERT_THRESHOLDS.items():
        value = data.get(metric)
        if value is not None and value > threshold:
            alerts.append(f"{metric} ({value:.2f} > {threshold:.2f})")

    if alerts:
        print(f"⚠️ THRESHOLD ALERT for {rig_id}: {' | '.join(alerts)}")

    data = main(data)

    is_anom = calculate_zscore(data)
    data["zscore_anomaly"] = bool(is_anom)

    insert_message(data)
    print(f"✅ Inserted message for {data['rig_id']} at {data['timestamp']}")

    return data


def aggregate_data(consumer, duration_seconds=60, max_idle_seconds=10):
    """
    Listen for messages for `duration_seconds` or stop if no messages arrive for `max_idle_seconds`.
    """
    print(f"\n🚀 Starting consumer. Listening...")

    import time
    start_time = time.time()
    last_msg_time = time.time()

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            # No message received
            if msg is None:
                # Check if idle time exceeded
                if time.time() - last_msg_time > max_idle_seconds:
                    print(f"⏹ No new messages for {max_idle_seconds}s. Stopping consumer.")
                    break
                continue

            if msg.error():
                raise KafkaException(msg.error())

            process_message(msg)
            last_msg_time = time.time()

    finally:
        consumer.close()


def analyze_window(window_data):
    """Analyze aggregated data for a time window"""
    print("\n" + "=" * 50)
    print("📊 WINDOW SUMMARY ANALYSIS")
    print("=" * 50)

    for rig_id, records in window_data.items():
        if not records:
            continue

        df = pd.DataFrame(records)
        numeric_cols = [col for col in df.columns if col not in ['timestamp', 'rig_id', 'failure_type']]
        df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors='coerce')

        print(f"\n{rig_id} - {len(records)} records")
        print("Average values:")
        print(df[numeric_cols].mean().to_string())

        # Maintenance events in this window
        maintenance_count = df['maintenance_flag'].sum()
        if maintenance_count > 0:
            print(f"\n🔴 {maintenance_count} maintenance events detected:")
            print(df[df['maintenance_flag'] == 1][['timestamp', 'failure_type']].to_string(index=False))


if __name__ == "__main__":
    consumer = create_consumer()
    aggregate_data(consumer, duration_seconds=60)
