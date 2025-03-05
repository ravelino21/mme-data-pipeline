{{ config(
    materialized='table',
    tags=['daily', 'bronze', 'youtube', 'l1']
)}}

SELECT 
    code,
    song_title,
    original_artist,
    channel_id,
    title,
    channel_title,
    CAST(publish_time AS TIMESTAMP) AS publish_time,
    video_id
FROM {{ source('dwh', 'youtube') }}

