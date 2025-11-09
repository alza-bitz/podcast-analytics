{{ config(
    materialized='view'
) }}

-- Analysis Question 2: Average listen-through rate (completion duration/episode duration) by country
-- This answers the second slice question from the requirements

with completed_events_with_rate as (
  select 
    u.country,
    f.duration::float / e.duration_seconds::float as listen_through_rate
  from {{ ref('fact_user_interactions') }} f
  join {{ ref('dim_users') }} u on f.user_id = u.user_id
  join {{ ref('dim_episodes') }} e on f.episode_id = e.episode_id
  where f.event_type = 'complete'
    and f.duration is not null
    and e.duration_seconds > 0
    and f.duration > 0
)

select 
  country,
  avg(listen_through_rate) as avg_listen_through_rate,
  count(*) as total_completions
from completed_events_with_rate
group by country
order by avg_listen_through_rate desc, country
