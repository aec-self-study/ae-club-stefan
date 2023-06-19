{{ config(
  materialized = 'table'
)}}

with session_info as (
  select
      *,
    DATE_DIFF(session_start_time, session_finish_time, SECOND) as session_length,
    COUNT(page) OVER(
        PARTITION BY customer_id, session_id
      ) as page_count

  from {{ ref('session_id') }}
)

-- distinct_sessions as (
--   select
--     distinct
--       customer_id,
--       visitor_id,
--       session_id,
--       session_start_time,
--       session_finish_time,
--       session_length,
--       page_count
--   from session_info
-- )

select * from session_info
