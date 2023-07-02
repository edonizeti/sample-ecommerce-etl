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
CREATE OR REPLACE TABLE TRANSACTION_JORNEY as (
    WITH
        DUPLICATES AS (
            SELECT *
                ,ROW_NUMBER() OVER(PARTITION BY ITEM_ID,USER_ID,EVENT_TYPE,TIMESTAMP,DISCOUNT ORDER BY TIMESTAMP) AS REGNUMBER
            FROM RAW.INTERACTIONS
        ),
        UNICOS AS (
            SELECT *
            FROM DUPLICATES
            WHERE REGNUMBER = 1
        ),
        EVENTORDER AS (
            SELECT
                USER_ID
                ,ITEM_ID
                ,EVENT_TYPE
                ,CASE
                    WHEN EVENT_TYPE = 'ProductViewed' THEN 1
                    WHEN EVENT_TYPE = 'ProductAdded' THEN 2
                    WHEN EVENT_TYPE = 'CartViewed' THEN 3
                    WHEN EVENT_TYPE = 'CheckoutStarted' THEN 4
                    WHEN EVENT_TYPE = 'OrderCompleted' THEN 5
                    END AS NEW_EVENT_TYPE
                ,TO_TIMESTAMP(TIMESTAMP) AS TIMESTAMP
                ,DISCOUNT
            FROM UNICOS
        ),
        ROWNUMBER AS (
            SELECT DISTINCT
                USER_ID
                ,ITEM_ID
                ,NEW_EVENT_TYPE
                ,TIMESTAMP
                ,DISCOUNT
                ,IFF(NEW_EVENT_TYPE = 1,ROW_NUMBER() OVER (PARTITION BY USER_ID,ITEM_ID,EVENT_TYPE ORDER BY TIMESTAMP),NULL) AS ROWNUMBER
            FROM EVENTORDER
        ),
        ADJUSTMENT AS (
            SELECT
                USER_ID
                ,ITEM_ID
                ,NEW_EVENT_TYPE
                ,TIMESTAMP
                ,DISCOUNT
                ,IFF(ROWNUMBER IS NULL,LAG(ROWNUMBER) IGNORE NULLS OVER (ORDER BY TIMESTAMP,NEW_EVENT_TYPE),ROWNUMBER) AS ROW1
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
                PIVOT (MIN(TIMESTAMP) FOR NEW_EVENT_TYPE IN (1,2,3,4,5) )
            ORDER BY USER_ID,ITEM_ID,ROW1
        ),
        FINAL AS (
            SELECT
                PIVO.ITEM_ID
                ,PIVO.USER_ID
                ,PRODUCT.NAME
                ,PRODUCT.CATEGORY
                ,TO_DATE(PIVO.EVENT_1) AS Product_Viewed
                ,TO_DATE(PIVO.EVENT_2) AS Product_Added
                ,TO_DATE(PIVO.EVENT_3) AS Cart_Viewed
                ,TO_DATE(PIVO.EVENT_4) AS Checkout_Started
                ,TO_DATE(PIVO.EVENT_5) AS Order_Completed
                ,PIVO.DISCOUNT
                ,PRODUCT.PRICE
            FROM PIVO
            LEFT JOIN RAW.PRODUCTS AS PRODUCT
            ON PIVO.ITEM_ID = PRODUCT.ID
        )
    SELECT * FROM FINAL
);

----------------------------------------------------------------------------------------------------
-- Create Transaction_Journey table
