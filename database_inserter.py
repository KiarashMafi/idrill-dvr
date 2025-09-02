import duckdb

# Keep connection open
conn = duckdb.connect("oilrig.db")
cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS sensor_data (
    timestamp TIMESTAMP PRIMARY KEY,
    rig_id VARCHAR,
    depth DOUBLE,
    wob DOUBLE,
    rpm DOUBLE,
    torque DOUBLE,
    rop DOUBLE,
    mud_flow_rate DOUBLE,
    mud_pressure DOUBLE,
    mud_temperature DOUBLE,
    mud_density DOUBLE,
    mud_viscosity DOUBLE,
    mud_ph DOUBLE,
    gamma_ray DOUBLE,
    resistivity DOUBLE,
    pump_status INTEGER,
    compressor_status INTEGER,
    power_consumption DOUBLE,
    vibration_level DOUBLE,
    bit_temperature DOUBLE,
    motor_temperature DOUBLE,
    maintenance_flag INTEGER,
    failure_type VARCHAR
)
""")

COLUMNS = [
    "timestamp", "rig_id", "depth", "wob", "rpm", "torque", "rop", "mud_flow_rate",
    "mud_pressure", "mud_temperature", "mud_density", "mud_viscosity", "mud_ph",
    "gamma_ray", "resistivity", "pump_status", "compressor_status", "power_consumption",
    "vibration_level", "bit_temperature", "motor_temperature", "maintenance_flag",
    "failure_type"
]

INSERT_QUERY = f"""
INSERT OR REPLACE INTO sensor_data (
    {', '.join(COLUMNS)}
) VALUES ({', '.join(['?' for _ in COLUMNS])})
"""

def insert_message(message: dict):
    """Insert a single message (preprocessed) into DuckDB."""

    import os
    print("DB path:", os.path.abspath("oilrig.db"))

    data = tuple(message.get(col) for col in COLUMNS)
    print("💾 Inserting into DB:", data)
    cursor.execute(INSERT_QUERY, data)
    conn.commit()   # 🔑 ensure changes are written
