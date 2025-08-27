import pandas as pd
import psycopg2
import time
from datetime import datetime
from preprocessor import main

# --- 1️⃣ Load CSV ---
df = pd.read_csv("output_data/rig_data_10000.csv")

# --- 2️⃣ Connect to PostgreSQL ---
conn = psycopg2.connect(
    dbname="oilrig",  # your DB name
    user="postgres",  # your username
    password="1234",
    host="localhost",
    port=5432
)
cursor = conn.cursor()

# --- 3️⃣ Prepare insert query ---
insert_query = """
INSERT INTO sensor_data (
    timestamp, rig_id, depth, wob, rpm, torque, mud_flow_rate, mud_pressure,
    mud_temperature, mud_density, mud_viscosity, mud_ph, gamma_ray, resistivity,
    pump_status, compressor_status, power_consumption, vibration_level,
    bit_temperature, motor_temperature, maintenance_flag, failure_type, anomaly_flag
) VALUES (
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
)
ON CONFLICT (timestamp) DO NOTHING;  -- avoid duplicates
"""

# --- 4️⃣ Insert each row (simulate streaming) ---
for _, row in df.iterrows():

    row = main(row.to_dict())
    timestamp_val = row.get("Timestamp")

    data = (
        datetime.fromisoformat(timestamp_val) if timestamp_val else datetime.now(),
        row.get('Rig_ID', 'Unknown'),
        row.get('Depth'),
        row.get('WOB'),
        row.get('RPM'),
        row.get('Torque'),
        row.get('Mud_Flow_Rate'),
        row.get('Mud_Pressure'),
        row.get('Mud_Temperature'),
        row.get('Mud_Density'),
        row.get('Mud_Viscosity'),
        row.get('Mud_PH'),
        row.get('Gamma_Ray'),
        row.get('Resistivity'),
        int(round(row.get('Pump_Status', 1))),
        int(round(row.get('Compressor_Status', 1))),
        row.get('Power_Consumption'),
        row.get('Vibration_Level'),
        row.get('Bit_Temperature'),
        row.get('Motor_Temperature'),
        int(row.get('Maintenance_Flag', 0)),
        row.get('Failure_Type'),
        row.get('Anomaly_Flag', False)
    )
    cursor.execute(insert_query, data)

    # simulate delay like Kafka stream
    # time.sleep(0.1)

# --- 5️⃣ Commit and close ---
conn.commit()
cursor.close()
conn.close()

print("✅ All CSV data inserted into sensor_data table!")
