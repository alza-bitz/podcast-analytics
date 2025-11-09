{{ config(unique_key='episode_id') }} -- merge strategy

with cleansed_episodes as (
    select * from {{ ref('cleansed_episodes') }}
)
select * from cleansed_episodes
{% if is_incremental() %}
where load_at > (select max(load_at) from {{ this }})
{% endif %}
