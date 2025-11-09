-- Test that target row count is less than or equal to source row count

with source as (
  select count(*) as row_count
  from read_csv_auto('{{ var("data_load_path") }}/episodes_*.csv')
),
target as (
  select count(*) as row_count
  from {{ ref('cleansed_episodes') }}
)

select 1 as invalid_count
where (select row_count from target) > (select row_count from source)

