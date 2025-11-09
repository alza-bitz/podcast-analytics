-- Test that raw_events table has same row count as source JSON file
-- This verifies data loading integrity by comparing source vs target counts

with source_count as (
  select count(*) as source_rows
  from read_json_auto('{{ var("data_load_path") }}/event_logs_*.json')
),

target_count as (
  select count(*) as target_rows  
  from {{ ref('raw_events') }}
)

select 'Row count mismatch between source and target' as test_failure
from source_count, target_count
where source_rows != target_rows
