from kafka import KafkaConsumer
import json
import clickhouse_connect
import os
from dotenv import load_dotenv
load_dotenv()

click_user = os.getenv("CLICKHOUSE_USER")
click_pass = os.getenv("CLICKHOUSE_PASSWORD")

consumer = KafkaConsumer(
    "user_events",
    group_id="clickhouse_writer",
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

client = clickhouse_connect.get_client(host='localhost', port=8123, username=click_user, password=click_pass)

client.command("""
CREATE TABLE IF NOT EXISTS user_logins (
    id UInt64,
    username String,
    event_type String,
    event_time DateTime
) ENGINE = ReplacingMergeTree()
ORDER BY id
""")

for message in consumer:
    data = message.value
    print(f"[Kafka] Received - {data}")
    client.insert(
        "user_logins",
        [[data["id"], data["user"], data["event"], int(data["timestamp"])]],
        column_names=["id", "username", "event_type", "event_time"]
        )
    print(f"Migrated to [ClickHouse] - id={data['id']}")

consumer.close()
client.close()
