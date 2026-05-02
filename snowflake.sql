CREATE DATABASE IF NOT EXISTS SOCIALMEDIA;
CREATE SCHEMA IF NOT EXISTS SOCIALMEDIA.ANALYTICS;

USE DATABASE SOCIALMEDIA;
USE SCHEMA ANALYTICS;

CREATE OR REPLACE TABLE "USERS" (
    user_id STRING,
    username STRING,
    platform STRING,
    followers INTEGER,
    "following" INTEGER,
    account_created DATE,
    is_verified BOOLEAN,
    country STRING,
    "language" STRING
);

CREATE OR REPLACE TABLE HASHTAGS (
    hashtag_id STRING,
    hashtag STRING,
    category STRING,
    avg_daily_mentions INTEGER,
    is_trending BOOLEAN
);

CREATE OR REPLACE TABLE POSTS (
    post_id STRING,
    user_id STRING,
    platform STRING,
    event_timestamp TIMESTAMP_NTZ,
    content STRING,
    language STRING,
    sentiment STRING,
    sentiment_score FLOAT,
    likes NUMBER,
    shares NUMBER,
    comments NUMBER,
    hashtags ARRAY,
    is_reply BOOLEAN,
    is_repost BOOLEAN,
    word_count NUMBER,
    contains_media BOOLEAN,
    followers NUMBER,
    country STRING,
    is_verified BOOLEAN
);

CREATE OR REPLACE STAGE my_stage;

-- Upload data to stage and load them to tables -- done



-- Check flattening the hastags in posts
SELECT
    post_id,
    value::string AS hashtag
FROM POSTS,
LATERAL FLATTEN(input => hashtags);


CREATE OR REPLACE TABLE SENTIMENT_ALERTS (
    post_id STRING,
    sentiment_score FLOAT,
    alert_time TIMESTAMP
);

CREATE OR REPLACE STREAM POSTS_STREAM
ON TABLE POSTS;

CREATE OR REPLACE TASK NEGATIVE_SENTIMENT_TASK
WAREHOUSE = COMPUTE_WH
SCHEDULE = '15 MINUTE'
AS
INSERT INTO SENTIMENT_ALERTS
SELECT
    post_id,
    sentiment_score,
    CURRENT_TIMESTAMP()
FROM POSTS_STREAM
WHERE sentiment_score < -0.7;

-- Start the task
ALTER TASK NEGATIVE_SENTIMENT_TASK RESUME;


-- (a) Hourly Sentiment (Last 7 Days)
SELECT
    platform,
    DATE_TRUNC('HOUR', event_timestamp) AS hour,
    AVG(sentiment_score) AS avg_sentiment
FROM POSTS
WHERE event_timestamp >= DATEADD(DAY, -7, CURRENT_TIMESTAMP())
GROUP BY platform, hour
ORDER BY hour;

-- (b) Top 10 Posts per Platform
SELECT * FROM (
    SELECT post_id, platform,
        (likes + shares + comments) AS engagement,
        RANK() OVER (
            PARTITION BY platform
            ORDER BY (likes + shares + comments) DESC
        ) AS rnk FROM POSTS
)
WHERE rnk <= 10;


-- (c) 7-Day Rolling Average
WITH daily AS (
    SELECT
        platform,
        DATE(event_timestamp) AS day,
        AVG(sentiment_score) AS avg_sentiment
    FROM POSTS
    GROUP BY platform, day
)
SELECT
    platform,
    day,
    AVG(avg_sentiment) OVER (
        PARTITION BY platform
        ORDER BY day
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS rolling_avg
FROM daily;


-- (d) Correlation
SELECT
    CORR(followers, (likes + shares + comments)) AS correlation
FROM POSTS
WHERE is_verified = TRUE;


select * from POSTS_STREAM limit 100;

select distinct sentiment from POSTS;

select count(*) from sentiment_alerts;