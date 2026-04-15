import json
import logging
import os
from dotenv import load_dotenv   # ✅ NEW

from azure.eventhub import EventHubConsumerClient
from google.cloud import pubsub_v1

# ---------------- LOAD ENV ----------------
load_dotenv()   # ✅ NEW

# ---------------- CONFIG ----------------
EVENT_HUB_CONN_STR = os.getenv("EVENT_HUB_CONN_STR")   # ✅ CHANGED
EVENT_HUB_NAME = os.getenv("EVENT_HUB_NAME")           # ✅ CHANGED
CONSUMER_GROUP = os.getenv("CONSUMER_GROUP")           # ✅ CHANGED

GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")           # ✅ CHANGED
PUBSUB_TOPIC = os.getenv("PUBSUB_TOPIC")               # ✅ CHANGED

# ---------------- LOGGING ----------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("router")

# ---------------- PUBSUB ----------------
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(GCP_PROJECT_ID, PUBSUB_TOPIC)

# ---------------- EVENT HANDLER ----------------
def on_event(partition_context, event):
    try:
        body = event.body_as_str(encoding="UTF-8")
        data = json.loads(body)

        logger.info(f"Received from Event Hub: {data}")

        # 🔥 publish to Pub/Sub
        publisher.publish(
            topic_path,
            json.dumps(data).encode("utf-8"),
            platform=str(data.get("platform", "unknown")),
            sentiment=str(data.get("sentiment", "unknown"))
        )

        logger.info("Published to Pub/Sub")

        # checkpoint
        partition_context.update_checkpoint(event)

    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)

# ---------------- MAIN ----------------
def main():
    client = EventHubConsumerClient.from_connection_string(
        conn_str=EVENT_HUB_CONN_STR,
        consumer_group=CONSUMER_GROUP,
        eventhub_name=EVENT_HUB_NAME
    )

    logger.info("Listening to Event Hub...")

    with client:
        client.receive(
            on_event=on_event,
            starting_position="-1"
        )

if __name__ == "__main__":
    main()