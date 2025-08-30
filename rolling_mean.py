import pandas as pd
import psycopg2

window_size=20
k = 5

df = pd.read_csv("output_data/rig_data_10000.csv")

conn = psycopg2.connect(
    dbname="oilrig",
    user="postgres",
    password="1234",
    host="localhost",
    port=5432
)
cursor = conn.cursor()

query = f"""SELECT *
    FROM sensor_data
    ORDER BY timestamp DESC
    LIMIT  {window_size};"""

cursor.execute(query)
rows = cursor.fetchall()

columns = [desc[0] for desc in cursor.description]
result = pd.DataFrame(dict(zip(columns, row)) for row in rows)

print(result)

for _, row in df.iterrows():



# for i in

conn.commit()
cursor.close()
conn.close()