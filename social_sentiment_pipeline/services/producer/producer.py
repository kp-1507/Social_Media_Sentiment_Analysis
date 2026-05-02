import json
import os
import time
from azure.eventhub import EventHubProducerClient, EventData

CONN_STR = os.getenv("EVENT_HUB_CONN_STR")
EVENT_HUB_NAME = os.getenv("EVENT_HUB_NAME")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", 50))
DELAY = int(os.getenv("DELAY_SECONDS", 2))

producer = EventHubProducerClient.from_connection_string(
    conn_str=CONN_STR,
    eventhub_name=EVENT_HUB_NAME
)

with open("data/posts.json", "r") as f:
    posts = json.load(f)

print(f"Loaded {len(posts)} posts")

for i in range(0, len(posts), BATCH_SIZE):

    batch = producer.create_batch()
    chunk = posts[i:i+BATCH_SIZE]

    for post in chunk:
        event = EventData(json.dumps(post))
        producer.send_batch(
            [event],
            partition_key=post["user_id"]
    )

    producer.send_batch(batch)

    print(f"Sent batch {i // BATCH_SIZE + 1}")

    time.sleep(DELAY)

print("All events sent!")