-- Raw events model: Load event data from JSON files
-- This model loads event data as strings initially
-- Append rows for new events since the last run, determined by filename

select 
    event_type::varchar as event_type,
    user_id::varchar as user_id,
    episode_id::varchar as episode_id,
    timestamp::varchar as timestamp,
    duration::varchar as duration,
    filename::varchar as filename,
    current_timestamp::timestamp as load_at
from {{ source('external', 'events') }}

{% if is_incremental() %}
where filename not in (select filename from {{ this }})
{% endif %}
