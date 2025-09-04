import pandas as pd
import json
import time


def handle_missing_values(message):

    default_values = {
        "Depth": 9.931658328379326,
        "WOB": 1499.1095597598314,
        "RPM": 80.07053557787066,
        "Torque": 400.37012959454245,
        "ROP": 12.015219277468452,
        "Mud_Flow_Rate": 1198.5587561221755,
        "Mud_Pressure": 2997.1692880682226,
        "Mud_Temperature": 60.01337852862287,
        "Mud_Density": 1199.6384710452498,
        "Mud_Viscosity": 34.916067258875955,
        "Mud_PH": 8.498536369838213,
        "Gamma_Ray": 84.78797495503724,
        "Resistivity": 19.971936118192943,
        "Pump_Status": 0.9905850209589226,
        "Compressor_Status": 0.980301546355133,
        "Power_Consumption": 200.38560122394142,
        "Vibration_Level": 0.7988757855927894,
        "Bit_Temperature": 90.02336580400343,
        "Motor_Temperature": 74.97714530662503
    }

    max_buffer = 4
    deselected_columns = ["Timestamp", "Rig_ID", "Failure_Type", "Maintenance_Flag"]
    for key, value in message.items():
        # print(key)
        if pd.isnull(value) and key not in deselected_columns:
            key_values = [
                past_msg[key]
                for past_msg in buffer
                if key in past_msg and not pd.isnull(past_msg[key])]

            if key_values:
                message[key] = sum(key_values) / len(key_values)

            elif buffer and key in buffer[-1] and not pd.isnull(buffer[-1][key]):
                message[key] = buffer[-1][key]
            else:
                message[key] = default_values[key]

    buffer.append(message)
    if len(buffer) > max_buffer:
        buffer.pop(0)

    return message

def handle_binary_values(message):
    binary_columns = ["Pump_Status", "Compressor_Status"]

    for key, value in message.items():

        if key in binary_columns and value is not None:
            message[key] = round(value)
        elif key in binary_columns and value is None:
            message[key] = 1

        if key == "Maintenance_Flag" and value is None:
            message["Maintenance_Flag"] = 1

    return message

def handle_failure_type(message):
    maintenance_flag = message["Maintenance_Flag"]
    failure_type = 1 if not pd.isnull(message["Failure_Type"]) else 0

    if maintenance_flag != failure_type:
        message["Anomaly"] = True

    return message

def main(message):
    output = handle_missing_values(message)
    output = handle_binary_values(output)
    output = handle_failure_type(output)
    return output

df = pd.read_csv("output_data/rig_data_10000.csv")
buffer = []

if __name__=="__main__":
    for _, row in df.iterrows():
        msg_json = row.to_dict()
        print(main(msg_json))



