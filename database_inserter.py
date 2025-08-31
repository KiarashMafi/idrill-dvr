import pandas as pd
import duckdb
from preprocessor import main

df = pd.read_csv("output_data/rig_data_10000.csv")

conn = duckdb.connect("oilrig.db")
cursor = conn.cursor()

def insert_value(message: dict):

    insert_query = """
    INSERT OR REPLACE INTO sensor_data (
        timestamp, rig_id, depth, wob, rpm, torque, rop, mud_flow_rate, mud_pressure,
        mud_temperature, mud_density, mud_viscosity, mud_ph, gamma_ray, resistivity,
        pump_status, compressor_status, power_consumption, vibration_level,
        bit_temperature, motor_temperature, maintenance_flag, failure_type
    ) VALUES (
        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
    )
    """
    cursor.execute(insert_query, message.values())


for _, row in df.iterrows():
    row = main(row)
    insert_value(row.to_dict())

cursor.close()
conn.close()


print("✅ All CSV data inserted into sensor_data table!")
