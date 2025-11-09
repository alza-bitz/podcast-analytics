{{ config(materialized='table') }}

select
  user_id::varchar as user_id,
  signup_date::date as signup_date,
  country::varchar as country
from {{ ref('cleansed_users') }}
