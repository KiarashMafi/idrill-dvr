import pandas as pd
from preprocessor import main

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

for i in selected_features:
    mean, std = df[i].mean(), df[i].std()
    parameters[i] = (float(mean), float(std))

for _, row in df.iterrows():
    row = main(row.to_dict())
    for key in selected_features:
        x = row[key]
        z_score = (x-parameters[key][0])/parameters[key][1]
        print(f"Parameters:‌{parameters}")
        print(f"{key} value: {row[key]}, z score:‌{z_score}")
        # if abs(z_score) > 3:
            # print(f"{key}: {row[key]} is anomaly data")

# Token: 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