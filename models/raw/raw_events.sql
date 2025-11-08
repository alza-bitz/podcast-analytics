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
from read_ndjson(
    '{{ var("data_load_path") }}/event_logs_*.json',
    filename=true,
    auto_detect=false,
    columns={
        'event_type': 'VARCHAR',
        'user_id': 'VARCHAR', 
        'episode_id': 'VARCHAR',
        'timestamp': 'VARCHAR',
        'duration': 'VARCHAR'
    }
)

{% if is_incremental() %}
where filename not in (select filename from {{ this }})
{% endif %}
