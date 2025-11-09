-- Test that fact table row count matches cleansed events
-- This ensures no data is lost in the analytics transformation

select count(*) as fact_count
from {{ ref('fact_user_interactions') }}

except

select count(*) as cleansed_count
from {{ ref('cleansed_events') }}
