with orders as (

  select 
    customer_id, 
    min(created_at) as first_order_at,
    Count(id) as total_orders
  from `analytics-engineers-club.coffee_shop.orders`
  group by 
    customer_id
),

customers as (
  select 
    id as customer_id,
    name,
    email
  from `analytics-engineers-club.coffee_shop.customers`
)


select 
  orders.customer_id,
  customers.name,
  email,
  first_order_at,
  total_orders
from orders
left join customers on customers.customer_id=orders.customer_id
limit 5