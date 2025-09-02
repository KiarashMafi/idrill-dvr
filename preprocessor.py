import pandas as pd
import json
import time


def handle_missing_values(message):

    default_values = {
        "depth": 9.931658328379326,
        "wob": 1499.1095597598314,
        "rpm": 80.07053557787066,
        "torque": 400.37012959454245,
        "rop": 12.015219277468452,
        "mud_flow_rate": 1198.5587561221755,
        "mud_pressure": 2997.1692880682226,
        "mud_temperature": 60.01337852862287,
        "mud_density": 1199.6384710452498,
        "mud_viscosity": 34.916067258875955,
        "mud_ph": 8.498536369838213,
        "gamma_ray": 84.78797495503724,
        "resistivity": 19.971936118192943,
        "pump_status": 0.9905850209589226,
        "compressor_status": 0.980301546355133,
        "power_consumption": 200.38560122394142,
        "vibration_level": 0.7988757855927894,
        "bit_temperature": 90.02336580400343,
        "motor_temperature": 74.97714530662503,
        "maintenance_flag": 0
    }

    max_buffer = 4
    deselected_columns = ["timestamp", "rig_id", "failure_type", "maintenance_flag"]

    for key, value in message.items():
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
        # elif key == "Maintenance_Flag" and

    return message

def handle_failure_type(message):
    maintenance_flag = message["maintenance_flag"]
    failure_type = 1 if not pd.isnull(message["failure_type"]) else 0

    if maintenance_flag != failure_type:
        message["anomaly_flag"] = True

    # if not maintenance_flag and failure_type == 0:
    #     message["Maintenance_Flag"] = 1
    #     message["Failure_Type"] = "Unknown_Error"

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



