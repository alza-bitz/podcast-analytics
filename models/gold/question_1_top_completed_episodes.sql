{{ config(
    materialized='view'
) }}

-- Analysis Question 1: The top 10 most completed episodes in the past 7 days
-- This answers the first slice question from the requirements

with completed_episodes as (
  select 
    f.episode_id,
    e.title,
    e.podcast_id,
    e.release_date,
    e.duration_seconds,
    count(*) as completion_count
  from {{ ref('fact_user_interactions') }} f
  join {{ ref('dim_episodes') }} e on f.episode_id = e.episode_id
  where f.event_type = 'complete'
    and f.timestamp >= date('{{ var("analysis_end_date", "2024-01-07") }}') - interval 7 days
    and f.timestamp < date('{{ var("analysis_end_date", "2024-01-07") }}')
  group by f.episode_id, e.title, e.podcast_id, e.release_date, e.duration_seconds
)

select 
  episode_id,
  title,
  podcast_id,
  completion_count,
  release_date,
  duration_seconds
from completed_episodes
order by completion_count desc, episode_id
limit 10
