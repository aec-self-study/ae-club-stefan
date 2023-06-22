with customers as (
  select
    id as customer_id,
    name
  from `analytics-engineers-club.coffee_shop.customers`
  limit 1
),

orders as (
  select
    o.id as order_id,
    o.customer_id,
    items.product_id,
    price.price
  from `analytics-engineers-club.coffee_shop.orders` o
  left join `analytics-engineers-club.coffee_shop.order_items` items on items.order_id = o.order_id
  left join `analytics-engineers-club.coffee_shop.product_prices` price on price.product_id = items.product_id
),

customer_orders as (
  select *
  from customers
  left join orders on orders.customer_id = customers.customer_id
)
select * from customer_orders