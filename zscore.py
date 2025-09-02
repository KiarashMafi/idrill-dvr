import pandas as pd
import duckdb

conn = duckdb.connect("oilrig.db")
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
        x = message[key.lower()]
        mean, std = parameters[key]
        if std == 0:  # avoid div/0
            continue
        z_score = (x - mean) / std
        if abs(z_score) > z_score_threshold:
            return True
    return False




