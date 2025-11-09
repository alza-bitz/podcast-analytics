-- Test that episode durations are positive
-- This validates the dimension data quality

select episode_id, duration_seconds
from {{ ref('dim_episodes') }}
where duration_seconds <= 0
