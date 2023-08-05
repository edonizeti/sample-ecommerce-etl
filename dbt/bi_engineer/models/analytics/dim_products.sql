{{
    config(
        materialized="table",
        schema="analytics"
    )
}}

select
    id as product_id,
    name,
    category,
    style,
    iff (price < 0, 0, price) as price,
    gender_affinity,
    iff (current_stock < 0, 0, current_stock) as current_stock
from {{ ref('products') }}