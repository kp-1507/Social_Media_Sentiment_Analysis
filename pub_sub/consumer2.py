from google.cloud import pubsub_v1
import json
import snowflake.connector
from datetime import datetime
import os
from dotenv import load_dotenv   # ✅ NEW

# ---------------- LOAD ENV ----------------
load_dotenv()   # ✅ NEW

# ---------------- CONFIG ----------------
PROJECT_ID = os.getenv("PROJECT_ID")   # ✅ CHANGED
SUBSCRIPTION = os.getenv("SUBSCRIPTION")   # ✅ CHANGED

# ---------------- PUBSUB ----------------
subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION)

# ---------------- SNOWFLAKE CONNECTION ----------------
conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),              # ✅ CHANGED
    password=os.getenv("SNOWFLAKE_PASSWORD"),      # ✅ CHANGED
    account=os.getenv("SNOWFLAKE_ACCOUNT"),        # ✅ CHANGED
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),    # ✅ CHANGED
    database=os.getenv("SNOWFLAKE_DATABASE"),      # ✅ CHANGED
    schema=os.getenv("SNOWFLAKE_SCHEMA")           # ✅ CHANGED
)

cursor = conn.cursor()

# ---------------- CALLBACK ----------------
def callback(message):
    try:
        data = json.loads(message.data.decode("utf-8"))
        print("Received:", data)

        post_id = data.get("post_id")
        user_id = data.get("user_id")
        platform = data.get("platform")

        timestamp_str = data.get("timestamp")
        event_timestamp = None
        if timestamp_str:
            event_timestamp = datetime.fromisoformat(
                timestamp_str.replace("Z", "+00:00")
            ).replace(tzinfo=None)

        content = data.get("content")
        language = data.get("language")
        sentiment = data.get("sentiment")
        sentiment_score = data.get("sentiment_score")
        likes = data.get("likes")
        shares = data.get("shares")
        comments = data.get("comments")

        hashtags = data.get("hashtags") or []
        hashtags_json = json.dumps(hashtags)

        is_reply = data.get("is_reply")
        is_repost = data.get("is_repost")
        word_count = data.get("word_count")
        contains_media = data.get("contains_media")
        followers = data.get("followers")
        country = data.get("country")
        is_verified = data.get("is_verified")

        cursor.execute(
            """
            INSERT INTO SOCIALMEDIA.ANALYTICS.POSTS (
                post_id, user_id, platform, event_timestamp, content, language,
                sentiment, sentiment_score, likes, shares, comments,
                hashtags, is_reply, is_repost, word_count,
                contains_media, followers, country, is_verified
            )
            SELECT
                %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                PARSE_JSON(%s),
                %s, %s, %s, %s, %s, %s, %s
            """,
            (
                post_id, user_id, platform, event_timestamp, content, language,
                sentiment, sentiment_score, likes, shares, comments,
                hashtags_json,
                is_reply, is_repost, word_count,
                contains_media, followers, country, is_verified
            )
        )

        conn.commit()
        message.ack()

    except Exception as e:
        print("Error:", e)
        message.nack()

# ---------------- START ----------------
subscriber.subscribe(subscription_path, callback=callback)

print("Listening for messages...")

import time
while True:
    time.sleep(10)