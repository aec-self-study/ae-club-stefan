{{ config(
    materialized = 'table'
)}}

select 
  id,
  FIRST_VALUE(visitor_id) OVER (
      PARTITION BY customer_id
      ORDER BY timestamp ASC
  ) as visitor_id,
  device_type,
  timestamp,
  page,
  customer_id,
  from {{ source('web_tracking', 'pageviews')}}
where customer_id is not null
