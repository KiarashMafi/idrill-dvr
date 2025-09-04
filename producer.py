import numpy as np
import random
import json
from datetime import datetime, timedelta
from confluent_kafka import Producer
from time import sleep

# Kafka configuration
KAFKA_BROKER = 'localhost:9092'
TOPIC_NAME = 'oil_rig_sensor_data'
PRODUCE_FREQUENCY = 1  # seconds between messages

num_devices = 10
total_rows = 10000
start_time = datetime(2025, 1, 1, 0, 0, 0)

failure_types = [
    'Motor_Failure', 'Pump_Leak', 'Hydraulic_Failure',
    'Bit_Wear', 'High_Vibration', 'Sensor_Fault', 'Compressor_Failure'
]

rows_per_device = total_rows // num_devices


def json_serializer(obj):
    if isinstance(obj, (np.integer,)):
        return int(obj)
    if isinstance(obj, (np.floating,)):
        return float(obj)
    if isinstance(obj, (np.ndarray,)):
        return obj.tolist()
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")


def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Delivery failed for record {msg.key()}: {err}")
    else:
        print(f"✅ Delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")


def create_producer():
    return Producer({'bootstrap.servers': KAFKA_BROKER})


def generate_device_sample(device_id, timestamp):
    record = {
        'Timestamp': timestamp.isoformat(),
        'Rig_ID': f'RIG_{device_id:02d}',
        'Depth': float(np.random.normal(1000, 50)),  # absolute depth, not cumulative
        'WOB': float(np.random.normal(1500, 100)),
        'RPM': float(np.random.normal(80, 5)),
        'Torque': float(np.random.normal(400, 30)),
        'ROP': float(np.random.normal(12, 2)),
        'Mud_Flow_Rate': float(np.random.normal(1200, 100)),
        'Mud_Pressure': float(np.random.normal(3000, 200)),
        'Mud_Temperature': float(np.random.normal(60, 3)),
        'Mud_Density': float(np.random.normal(1200, 50)),
        'Mud_Viscosity': float(np.random.normal(35, 5)),
        'Mud_PH': float(np.random.normal(8.5, 0.2)),
        'Gamma_Ray': float(np.random.normal(85, 15)),
        'Resistivity': float(np.random.normal(20, 5)),
        'Pump_Status': int(np.random.choice([0, 1], p=[0.01, 0.99])),
        'Compressor_Status': int(np.random.choice([0, 1], p=[0.02, 0.98])),
        'Power_Consumption': float(np.random.normal(200, 20)),
        'Vibration_Level': float(np.random.normal(0.8, 0.3)),
        'Bit_Temperature': float(np.random.normal(90, 5)),
        'Motor_Temperature': float(np.random.normal(75, 4)),
        'Maintenance_Flag': 0,
        'Failure_Type': 'None',
        "Anomaly": False
    }

    # Add 5% chance of failure
    if random.random() < 0.05:
        record['Maintenance_Flag'] = 1
        record['Failure_Type'] = random.choice(failure_types)

    print(record)

    return record


def produce_device_data(producer, max_messages=10000):
    counter = 0
    current_time = start_time
    print(f"🚀 Starting Kafka producer. Sending messages to topic: {TOPIC_NAME}")

    while counter < max_messages:
        for device_id in range(1, num_devices + 1):
            record = generate_device_sample(device_id, current_time)
            producer.produce(
                TOPIC_NAME,
                value=json.dumps(record, default=json_serializer).encode('utf-8'),
                callback=delivery_report
            )
            producer.poll(0)
            counter += 1
            print(f"📤 Sent record {counter} for {record['Rig_ID']} at {record['Timestamp']}")
            if counter >= max_messages:
                producer.flush()
                print("🛑 Stopping producer after reaching max_messages")
                return
            sleep(PRODUCE_FREQUENCY)

        current_time += timedelta(seconds=1)


if __name__ == "__main__":
    producer = create_producer()
    produce_device_data(producer)
