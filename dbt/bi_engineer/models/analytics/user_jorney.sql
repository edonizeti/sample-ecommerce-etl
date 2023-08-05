{{
    config(
        materialized="table",
        schema="analytics"
    )
}}

with users as (
    select
        user_id,
        username,
        email,
        first_name,
        last_name,
        address1,
        country,
        city,
        state,
        zipcode
    from {{ ref('dim_users') }}
),
total_orders as(
    select
        user_id as user_id,
        count(USER_ID) as total_orders
    from {{ ref('interactions') }}
    where EVENT_TYPE = 'OrderCompleted'
    group by USER_ID, EVENT_TYPE
),
user_jorney as (
    select
        users.*,
        coalesce(total_orders.total_orders,0) as total_orders
    from users
    left join total_orders
    on users.user_id = total_orders.user_id
    order by users.user_id
)
select * from user_jorney