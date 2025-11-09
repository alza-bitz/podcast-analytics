
with valid_events as (
  select *
  from {{ ref('validated_events') }}
  where array_length(validation_errors) = 0
  {% if is_incremental() %}
  and load_at > (select max(load_at) from {{ this }})
  {% endif %}
)

select
  -- Convert event_type to the expected enum values (already validated in previous step)
  event_type::varchar as event_type,
  
  -- Ensure user_id and episode_id are not null (already validated)
  user_id::varchar as user_id,
  episode_id::varchar as episode_id,
  
  -- Convert timestamp from string to timestamp type
  timestamp::timestamp as timestamp,
  
  -- Convert duration to integer, set to null for non-play/complete events
  case 
    when event_type in ('play', 'complete') then duration::integer
    else null
  end as duration,
  
  -- Include load_at and filename for auditing
  load_at,
  filename

from valid_events
