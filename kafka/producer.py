from kafka import KafkaProducer
import json
import psycopg2
import os
from dotenv import load_dotenv
import time
load_dotenv()

pg_user = os.getenv("POSTGRES_USER")
pg_pass = os.getenv("POSTGRES_PASSWORD")

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

conn = psycopg2.connect(
    dbname="users_db", user=pg_user, password=pg_pass, host="localhost", port=5432)
cursor = conn.cursor()

cursor.execute("""
UPDATE user_logins
    SET sent_to_kafka = TRUE
    WHERE sent_to_kafka = FALSE
    RETURNING id, username, event_type, extract(epoch FROM event_time);
""")

#Server-side cursor iteration --> README
for row in cursor:
    data = {
        "id" : row[0],
        "user" : row[1],
        "event" : row[2],
        "timestamp" : float(row[3])
    }
    producer.send("user_events", value=data)
    print(f"Sent to Kafka - {data}")
    time.sleep(0.5)

producer.flush()
producer.close()
print("Kafka send complete")

conn.commit() #WHY HERE? --> README
print("New logins --> sent_to_kafka = TRUE")

cursor.close()
conn.close()
print("PG connection closed")
