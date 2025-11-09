-- Test that validated_events table has same row count as raw_events table
-- This verifies that the validation step preserves all records (both valid and invalid)
-- ensuring no data is lost during the validation transformation

with raw_count as (
  select count(*) as raw_rows
  from {{ ref('raw_events') }}
),

validated_count as (
  select count(*) as validated_rows  
  from {{ ref('validated_events') }}
)

select 'Row count mismatch between raw_events and validated_events' as test_failure
from raw_count, validated_count
where raw_rows != validated_rows
