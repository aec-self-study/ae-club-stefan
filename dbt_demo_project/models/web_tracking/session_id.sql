{{ config(
    materialized = 'table'
)}}

with page_views as (
  select 
    *,
    LAG(timestamp)
        OVER(
          PARTITION BY visitor_id
          ORDER BY timestamp
        ) as prev_time
  from {{ ref('site_visits') }}
  order by visitor_id, timestamp
),

time_page_diff as (
  select 
    *,
    date_diff(timestamp, prev_time, MINUTE) as time_diff
  from page_views
),

session_marker as (
  select
    *,
    {% for session_length in [30, 240, 1440] %}
    CAST(coalesce(time_diff > {{session_length}}, true) as integer) as is_new_session_length_{{session_length}}
        {% if not loop.last %}
            ,
        {% endif %}
    {% endfor %}
    from time_page_diff
),

assign_session_id as (
  select
    id,
    visitor_id,
    device_type,
    timestamp,
    page, customer_id,
    is_new_session_length_30,
    -- is_new_session_length_240,
    -- is_new_session_length_1440,
    sum(is_new_session_length_30) OVER(
      PARTITION BY visitor_id
      ORDER BY timestamp
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) as session_id
  from session_marker
),

session_duration as (
    select 
        *,
        FIRST_VALUE(timestamp) OVER(
            PARTITION BY customer_id, session_id
            ORDER BY timestamp
        ) as session_start_time,
        FIRST_VALUE(timestamp) OVER(
            PARTITION BY customer_id, session_id
            ORDER BY timestamp DESC
        ) as session_finish_time
    from assign_session_id
)

select * from session_duration
