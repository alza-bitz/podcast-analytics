-- Test that cleansed_events row count is less than or equal to validated_events where validation_errors is empty (accounting for deduplication)
with cleansed_count as (
  select count(*) as cnt from {{ ref('cleansed_events') }}
),
validated_count as (
  select count(*) as cnt from {{ ref('validated_events') }} where array_length(validation_errors) = 0
)
select 1 as invalid_count
where (select cnt from cleansed_count) > (select cnt from validated_count)
