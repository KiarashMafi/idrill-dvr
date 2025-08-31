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
    "Depth",
    "WOB",
    "RPM",
    "Torque",
    "ROP",
    "Mud_Flow_Rate",
    "Mud_Pressure",
    "Mud_Temperature",
    "Mud_Density",
    "Mud_Viscosity",
    "Mud_PH",
    "Gamma_Ray",
    "Resistivity",
    "Power_Consumption",
    "Vibration_Level",
    "Bit_Temperature",
    "Motor_Temperature"
]

z_scores = {}
parameters = {}
z_score_threshold = 3
test = set()

for i in selected_features:
    mean, std = df[i].mean(), df[i].std()
    parameters[i] = (float(mean), float(std))

for _, row in df.iterrows():
    row = main(row.to_dict())
    for key in selected_features:
        x = row[key]
        z_score = (x-parameters[key][0])/parameters[key][1]
        # print(f"Parameters:‌{parameters}")
        # print(f"{key} value: {row[key]}, z score:‌{z_score}")
        if abs(z_score) > z_score_threshold:
            cursor.execute(
                "UPDATE sensor_data SET zscore_anomaly = TRUE WHERE timestamp = %s", (row["Timestamp"],)
            )

conn.commit()
cursor.close()
conn.close()
# Token: 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