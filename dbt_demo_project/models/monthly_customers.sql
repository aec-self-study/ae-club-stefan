SELECT
  date_trunc(first_order_at, month) as date_month,
  count(*) as new_customers
FROM {{ ref('customers') }}
group by 1