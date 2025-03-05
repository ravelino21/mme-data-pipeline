{{ config(
    materialized='table',
    tags=['daily', 'bronze', 'spotify', 'l1']
)}}

SELECT 
    album_name,
    album_release,
    CAST(release_date AS DATE) AS release_date,
    artist_name,
    CAST(is_playable AS BOOLEAN) AS is_playable,
    isrc,
    recodings_title,
    CAST(popularity AS INT64) AS popularity,
    song_title
FROM {{ source('dwh', 'spotify') }}
QUALIFY ROW_NUMBER() OVER(PARTITION BY isrc ORDER BY album) = 1
