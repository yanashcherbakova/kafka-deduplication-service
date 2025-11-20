from faker import Faker
import random
import time
import psycopg2
import os
from dotenv import load_dotenv
load_dotenv()

pg_user = os.environ["POSTGRES_USER"]
pg_pass = os.environ["POSTGRES_PASSWORD"]

conn = psycopg2.connect(
    dbname="users_db", user=pg_user, password=pg_pass, host="localhost", port=5432)
cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS user_logins (
    id SERIAL PRIMARY KEY,
    username TEXT,
    event_type TEXT DEFAULT 'login',
    event_time TIMESTAMP,
    sent_to_kafka BOOLEAN DEFAULT FALSE
    );
""")

fake = Faker()
users = [fake.user_name() for _ in range(50)]

for _ in range(100):
    data = {
        "user": random.choice(users),
        "timestamp": time.time() - random.randint(0, 15 * 60)
    }
    cursor.execute(
        "INSERT INTO user_logins (username, event_time) VALUES (%s, to_timestamp(%s))",
        (data["user"], data["timestamp"])
    )
    print(f'User - {data["user"]} logged at {data["timestamp"]}')
conn.commit()

cursor.close()
conn.close()
print("Synth logins loaded to PG")

