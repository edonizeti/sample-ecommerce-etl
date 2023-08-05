{{
    config(
        materialized="table",
        schema="analytics"
    )
}}

--The JSON string are between [ ], which means that it represents a JSON matrix.
--TO extract the information of the JSON matrix, I needed to use the FLATTEN()
--function to convert the matrix in lines and then access the desire values.
--Also I used the REPLACE() function to remove the ""

select
    id as user_id,
    username,
    email,
    first_name,
    last_name,
    replace(value:address1,'"','') as address1,
    replace(value:address2,'"','') as address2,
    replace(value:country,'"','') as country,
    replace(value:city,'"','') as city,
    replace(value:state,'"','') as state,
    replace(value:zipcode,'"','') as zipcode,
    age,
    gender,
    persona,
    discount_persona
from {{ ref('users') }}, lateral flatten(input => parse_json(ADDRESSES))
