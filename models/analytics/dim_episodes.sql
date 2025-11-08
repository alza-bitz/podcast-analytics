{{ config(materialized='table') }}

select
  episode_id::varchar as episode_id,
  podcast_id::varchar as podcast_id,
  title::varchar as title,
  release_date::date as release_date,
  duration_seconds::integer as duration_seconds
from {{ ref('cleansed_episodes') }}
