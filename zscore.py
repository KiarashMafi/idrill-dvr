import pandas as pd
import psycopg2
from preprocessor import main

conn = psycopg2.connect(
    dbname="oilrig",
    user="postgres",
    password="1234",
    host="localhost",
    port=5432
)
cursor = conn.cursor()

df = pd.read_csv("output_data/rig_data_10000.csv")

selected_features = [
    "Depth", "WOB", "RPM", "Torque", "ROP",
    "Mud_Flow_Rate", "Mud_Pressure", "Mud_Temperature",
    "Mud_Density", "Mud_Viscosity", "Mud_PH", "Gamma_Ray",
    "Resistivity", "Power_Consumption", "Vibration_Level",
    "Bit_Temperature", "Motor_Temperature"
]

parameters = {i: (df[i].mean(), df[i].std()) for i in selected_features}
z_score_threshold = 3

def calculate_zscore(message):
    for key in selected_features:
        x = message[key]
        mean, std = parameters[key]
        if std == 0:  # avoid div/0
            continue
        z_score = (x - mean) / std
        if abs(z_score) > z_score_threshold:
            return (
                "UPDATE sensor_data SET zscore_anomaly = TRUE WHERE timestamp = %s",
                (message["Timestamp"],)
            )
    return None


# Iterate over messages
for _, row in df.iterrows():
    row = main(row.to_dict())
    query = calculate_zscore(row)
    if query:
        cursor.execute(query[0], query[1])

conn.commit()
cursor.close()
conn.close()



