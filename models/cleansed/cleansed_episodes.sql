{{ config(
    materialized='incremental',
    unique_key='episode_id')
}}

select
    episode_id::varchar as episode_id,
    podcast_id::varchar as podcast_id,
    title::varchar as title,
    release_date::date as release_date,
    duration_seconds::int as duration_seconds,
    filename::varchar as filename,
    current_timestamp::timestamp as load_at
from read_csv('{{ var("data_load_base_path") }}/episodes_*.csv',
    header=true,
    auto_detect=false,
    columns={
        'episode_id': 'VARCHAR',
        'podcast_id': 'VARCHAR',
        'title': 'VARCHAR',
        'release_date': 'DATE',
        'duration_seconds': 'INT'
    })
{% if is_incremental() %}
where filename not in (select filename from {{ this }})
{% endif %}