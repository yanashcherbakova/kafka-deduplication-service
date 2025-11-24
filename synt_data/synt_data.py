from faker import Faker
import random
import time
import psycopg2
import os
from dotenv import load_dotenv
load_dotenv()

pg_user = os.getenv("POSTGRES_USER")
pg_pass = os.getenv("POSTGRES_PASSWORD")

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

timestamps = [time.time() - random.randint(0, 15 * 60)for _ in range(100)]
timestamps.sort()

for ts in timestamps:
    user = random.choice(users)
    cursor.execute(
        "INSERT INTO user_logins (username, event_time) VALUES (%s, to_timestamp(%s))",
        (user, ts)
    )
    print(f'User - {user} logged at {ts}')
conn.commit()

cursor.close()
conn.close()
print("Synth logins loaded to PG")

