--Create User_Journey table

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
-- Create Transaction_Journey table

WITH
    UNICOS AS (
        SELECT DISTINCT *
        FROM RAW.INTERACTIONS
        WHERE 1=1
 --       AND ITEM_ID = '00096972-5f6b-44df-917b-f7d21ae5644c'
        AND USER_ID = '1752'
    ),
    EVENTORDER AS (
        SELECT
            USER_ID
            ,ITEM_ID
            ,CASE
                WHEN EVENT_TYPE = 'ProductViewed' THEN 1
                WHEN EVENT_TYPE = 'ProductAdded' THEN 2
                WHEN EVENT_TYPE = 'CartViewed' THEN 3
                WHEN EVENT_TYPE = 'CheckoutStarted' THEN 4
                WHEN EVENT_TYPE = 'OrderCompleted' THEN 5
                END AS EVENT_TYPE
            ,TIMESTAMP
            ,DISCOUNT
        FROM UNICOS
    ),
    ROWNUMBER AS (
        SELECT DISTINCT
            USER_ID
            ,ITEM_ID
            ,EVENT_TYPE
            ,TIMESTAMP
            ,IFF(TIMESTAMP = LEAD(TIMESTAMP) OVER (PARTITION BY USER_ID,ITEM_ID ORDER BY TIMESTAMP,EVENT_TYPE)
                     AND EVENT_TYPE < LEAD(EVENT_TYPE)OVER (PARTITION BY USER_ID,ITEM_ID ORDER BY TIMESTAMP,EVENT_TYPE), TIMESTAMP + 1,TIMESTAMP
                ) AS TIMESTAMP2
            ,DISCOUNT
            ,IFF(EVENT_TYPE = 1,ROW_NUMBER() OVER (PARTITION BY USER_ID,ITEM_ID,EVENT_TYPE ORDER BY TIMESTAMP),NULL) AS ROWNUMBER
        FROM EVENTORDER
    ),
    ADJUSTMENT AS (
        SELECT
            USER_ID
            ,ITEM_ID
            ,EVENT_TYPE
            ,TIMESTAMP2
            ,DISCOUNT
            ,IFF(ROWNUMBER IS NULL,LAG(ROWNUMBER) IGNORE NULLS OVER (ORDER BY TIMESTAMP2,EVENT_TYPE),ROWNUMBER) AS ROW1
        FROM ROWNUMBER
    ),
    PIVO AS (
        SELECT
            USER_ID
            ,ITEM_ID
            ,"1" AS EVENT_1
            ,"2" AS EVENT_2
            ,"3" AS EVENT_3
            ,"4" AS EVENT_4
            ,"5" AS EVENT_5
            ,DISCOUNT
        FROM ADJUSTMENT
            PIVOT (MIN(TIMESTAMP2) FOR EVENT_TYPE IN (1,2,3,4,5) )
        ORDER BY USER_ID,ITEM_ID,ROW1
    ),
    FINAL AS (
        SELECT
            PIVO.ITEM_ID
            ,PIVO.USER_ID
            ,PRODUCT.NAME
            ,PRODUCT.CATEGORY
            ,TO_TIMESTAMP(PIVO.EVENT_1) AS ProductViewed
            ,TO_TIMESTAMP(PIVO.EVENT_2) AS ProductAdded
            ,TO_TIMESTAMP(PIVO.EVENT_3) AS CartViewed
            ,TO_TIMESTAMP(PIVO.EVENT_4) AS CheckoutStarted
            ,TO_TIMESTAMP(PIVO.EVENT_5) AS OrderCompleted
            ,PIVO.DISCOUNT
            ,PRODUCT.PRICE
        FROM PIVO
        LEFT JOIN RAW.PRODUCTS AS PRODUCT
        ON PIVO.ITEM_ID = PRODUCT.ID
    )
SELECT * FROM FINAL;
