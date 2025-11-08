{{ config(unique_key='episode_id') }}

with deduplicated_episodes as (
    select
        episode_id::varchar as episode_id,
        podcast_id::varchar as podcast_id,
        title::varchar as title,
        release_date::date as release_date,
        duration_seconds::int as duration_seconds,
        filename::varchar as filename,
        current_timestamp::timestamp as load_at,
        row_number() over (partition by episode_id order by filename desc) as row_num -- only computed over all files when doing full refresh
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
)

select
    episode_id,
    podcast_id,
    title,
    release_date,
    duration_seconds,
    filename,
    load_at
from deduplicated_episodes
where row_num = 1