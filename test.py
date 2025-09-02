import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

num_devices = 10
total_rows = 10000  # total rows across all devices
start_time = datetime(2025, 1, 1, 0, 0, 0)

failure_types = [
    'Motor_Failure', 'Pump_Leak', 'Hydraulic_Failure',
    'Bit_Wear', 'High_Vibration', 'Sensor_Fault', 'Compressor_Failure'
]

rows_per_device = total_rows // num_devices


def generate_device_sample(device_id, rows):
    timestamps = [start_time + timedelta(seconds=i) for i in range(rows)]

    data = {
        'Timestamp': timestamps,
        'Rig_ID': [f'RIG_{device_id:02d}'] * rows,
        'Depth': np.cumsum(np.random.normal(0.002, 0.001, rows)),
        'WOB': np.random.normal(1500, 100, rows),
        'RPM': np.random.normal(80, 5, rows),
        'Torque': np.random.normal(400, 30, rows),
        'ROP': np.random.normal(12, 2, rows),
        'Mud_Flow_Rate': np.random.normal(1200, 100, rows),
        'Mud_Pressure': np.random.normal(3000, 200, rows),
        'Mud_Temperature': np.random.normal(60, 3, rows),
        'Mud_Density': np.random.normal(1200, 50, rows),
        'Mud_Viscosity': np.random.normal(35, 5, rows),
        'Mud_PH': np.random.normal(8.5, 0.2, rows),
        'Gamma_Ray': np.random.normal(85, 15, rows),
        'Resistivity': np.random.normal(20, 5, rows),
        'Pump_Status': np.random.choice([0, 1], size=rows, p=[0.01, 0.99]),
        'Compressor_Status': np.random.choice([0, 1], size=rows, p=[0.02, 0.98]),
        'Power_Consumption': np.random.normal(200, 20, rows),
        'Vibration_Level': np.random.normal(0.8, 0.3, rows),
        'Bit_Temperature': np.random.normal(90, 5, rows),
        'Motor_Temperature': np.random.normal(75, 4, rows),
    }

    failure_flags = np.zeros(rows, dtype=int)
    failure_count = int(0.05 * rows)
    failure_indices = random.sample(range(rows), failure_count)
    for idx in failure_indices:
        failure_flags[idx] = 1
    data['Maintenance_Flag'] = failure_flags

    failure_type_col = ['None'] * rows
    for idx in failure_indices:
        failure_type_col[idx] = random.choice(failure_types)
    data['Failure_Type'] = failure_type_col

    return pd.DataFrame(data)


# Generate all device data and concatenate
all_data = pd.concat([generate_device_sample(device_id, rows_per_device) for device_id in range(1, num_devices + 1)],
                     ignore_index=True)

# Save to CSV
all_data.to_csv("rig_sample_10000.csv", index=False)
print("CSV file 'rig_sample_10000.csv' generated with 10000 rows.")
