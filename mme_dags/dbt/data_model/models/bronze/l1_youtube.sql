{{ config(
    materialized='table',
    tags=['daily', 'bronze', 'youtube', 'l1']
)}}

SELECT 
    song_title,
    channel_id,
    isrc,
    title,
    channel_title,
    CAST(publish_time AS TIMESTAMP) AS publish_time,
    video_id, 
    `description`
FROM {{ source('dwh', 'youtube') }}

