import json
import os
from azure.eventhub import EventHubConsumerClient, EventHubProducerClient, EventData

# Load environment variables
CONN_STR = os.getenv("EVENT_HUB_CONN_STR")
RAW_HUB = os.getenv("EVENT_HUB_NAME")
ENRICHED_HUB = os.getenv("ENRICHED_EVENT_HUB_NAME")
ENRICHED_HUB2 = os.getenv("ENRICHED_EVENT_HUB_NAME2")

# Validate environment variables
if not all([CONN_STR, RAW_HUB, ENRICHED_HUB, ENRICHED_HUB2]):
    raise ValueError("One or more required environment variables are missing!")

# Load reference data
with open("data/users.json") as f:
    users = {u["user_id"]: u for u in json.load(f)}

with open("data/hashtags.json") as f:
    hashtags = {h["hashtag"]: h for h in json.load(f)}

print("Reference data loaded")

# Producers
producer1 = EventHubProducerClient.from_connection_string(
    conn_str=CONN_STR,
    eventhub_name=ENRICHED_HUB
)

producer2 = EventHubProducerClient.from_connection_string(
    conn_str=CONN_STR,
    eventhub_name=ENRICHED_HUB2
)


def send_event(producer, post):
    batch = producer.create_batch(partition_key=post["user_id"])
    batch.add(EventData(json.dumps(post)))
    producer.send_batch(batch)


def on_event(partition_context, event):
    post = json.loads(event.body_as_str())

    user_id = post["user_id"]
    user_data = users.get(user_id, {})

    # Enrich post
    post["followers"] = user_data.get("followers")
    post["country"] = user_data.get("country")
    post["is_verified"] = user_data.get("is_verified")

    # Send to both hubs
    send_event(producer1, post)
    send_event(producer2, post)

    print(f"Enriched post {post['post_id']}")

    partition_context.update_checkpoint(event)


# Consumer
client = EventHubConsumerClient.from_connection_string(
    conn_str=CONN_STR,
    consumer_group="$Default",
    eventhub_name=RAW_HUB
)

print("Enricher started...")

with client:
    client.receive(
        on_event=on_event,
        starting_position="-1"
    )