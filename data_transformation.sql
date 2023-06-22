--Create a User Journey table

--The JSON string are between [ ], which means that it represents a JSON matrix.
--TO extract the information of the JSON matrix, I needed to use the FLATTEN()
--function to convert the matrix in lines and then access the desire values.
--Also I used the REPLACE() function to remove the ""
use datatalks.analytics;

create or replace table user_jorney as (
    with users as (
        select
            id as user_id,
            username,
            email,
            first_name,
            last_name,
            replace(value:address1,'"','') as address1,
            replace(value:country,'"','') as country,
            replace(value:city,'"','') as city,
            replace(value:state,'"','') as state,
            replace(value:zipcode,'"','') as zipcode
        from RAW.USERS, lateral flatten(input => parse_json(ADDRESSES))
    ),
    total_orders as(
        select
            user_id as user_id,
            count(USER_ID) as total_orders
        from RAW.INTERACTIONS
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
);

----------------------------------------------------------------------------------------------------
