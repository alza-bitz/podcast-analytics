-- Test that all listen-through rates are reasonable values (between 0 and some upper bound)
-- This ensures the analysis logic is working correctly

select country, avg_listen_through_rate
from {{ ref('question_2_avg_listen_through_rate_by_country') }}
where avg_listen_through_rate < 0 
   or avg_listen_through_rate is null
   or avg_listen_through_rate > 10  -- Allow some flexibility for edge cases but flag unreasonable values
