{{ config(
    unique_key='interaction_id',
    on_schema_change='fail'
) }}

with cleansed_events as (
  select *
  from {{ ref('cleansed_events') }}
  {% if is_incremental() %}
    -- Only process new events based on load_at timestamp
    -- This ensures we don't miss events regardless of their occurrence time
    where load_at > (select max(load_at) from {{ this }})
  {% endif %}
),

numbered_events as (
  select
    -- Generate deterministic interaction_id based on business key
    md5(user_id || episode_id || event_type || timestamp) as interaction_id,
    user_id,
    episode_id,
    event_type,
    timestamp,
    duration,
    load_at,
    filename
  from cleansed_events
)

select
  interaction_id,
  user_id,
  episode_id,
  event_type,
  timestamp,
  duration,
  load_at,
  filename
from numbered_events
