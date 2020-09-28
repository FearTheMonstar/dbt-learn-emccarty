with payments as (

    select * from {{ref('stg_payments')}}

),

orders as (

    select * from {{ref('stg_orders')}}

),

final as (
select
    orders.order_id,
    orders.customer_id,
    orders.order_date,
    sum(payments.amount) as total_order_amount
from orders
left join payments using (order_id)
where payments.status = 'success'
group by order_id, customer_id, order_date
)

select * from final