{{ config(
    materialized = 'table'
)}}

with orders as (
    select
        id as order_id,
        date_trunc(created_at, month) as month,
    from {{ source('coffee_shop', 'orders') }}
),

products as (
  select
    order_id,
    product_info.name
  from {{ source('coffee_shop', 'order_items')}} as items
  left join {{ source('coffee_shop', 'products') }} as product_info
    on items.product_id = product_info.id
)


select 
    month,
    count(products.name) as product_count
from orders
left join products
    on orders.order_id = products.order_id
group by month
order by month ASC



