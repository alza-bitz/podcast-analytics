-- Test that duration is null or non-negative in cleansed_events
select *
from {{ ref('cleansed_events') }}
where duration is not null and duration < 0
