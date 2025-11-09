-- Test that all completion counts are positive in the first slice analysis
-- This ensures the analysis logic is working correctly

select episode_id, completion_count
from {{ ref('question_1_top_completed_episodes') }}
where completion_count <= 0
