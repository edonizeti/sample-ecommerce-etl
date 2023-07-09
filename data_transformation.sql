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
                ,PIVO.EVENT_1 AS Product_Viewed
                ,PIVO.EVENT_2 AS Product_Added
                ,PIVO.EVENT_3 AS Cart_Viewed
                ,PIVO.EVENT_4 AS Checkout_Started
                ,PIVO.EVENT_5 AS Order_Completed
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
CREATE OR REPLACE TABLE SUMMARY as (
    WITH
    TR_DUPLICATES AS (
        SELECT *
            ,ROW_NUMBER() OVER(PARTITION BY ITEM_ID,USER_ID,EVENT_TYPE,TIMESTAMP,DISCOUNT ORDER BY TIMESTAMP) AS REGNUMBER
        FROM RAW.INTERACTIONS
    ),
    TR_UNIQUES AS (
        SELECT
            TR.ITEM_ID
            ,PD.NAME
            ,PD.CATEGORY
            ,PD.PRICE
            ,TR.USER_ID
            ,TR.EVENT_TYPE
            ,TO_TIMESTAMP(TR.TIMESTAMP) AS TIMESTAMP
        FROM TR_DUPLICATES AS TR
        LEFT JOIN RAW.PRODUCTS AS PD
        ON TR.ITEM_ID = PD.ID
        WHERE TR.REGNUMBER = 1

    ),
    COUNT_CATEGORY AS (
        SELECT
            CATEGORY
            ,COUNT_IF(EVENT_TYPE = 'ProductViewed') AS total_products_viewed
            ,COUNT_IF(EVENT_TYPE = 'ProductAdded') AS total_products_added
            ,COUNT_IF(EVENT_TYPE = 'CartViewed') AS total_cart_viewed
            ,COUNT_IF(EVENT_TYPE = 'CheckoutStarted') AS total_checkout_started
            ,COUNT_IF(EVENT_TYPE = 'OrderCompleted') AS total_orders_completed
            ,COUNT(EVENT_TYPE) AS total_interactions
        FROM TR_UNIQUES
        GROUP BY CATEGORY
    ),
    PRODUCT_ADDED AS (
        SELECT
            CATEGORY,
            ITEM_ID,
            USER_ID,
            EVENT_TYPE,
            TO_DATE(TO_TIMESTAMP(TIMESTAMP)) AS DATE
        FROM TR_UNIQUES
        WHERE EVENT_TYPE = 'ProductAdded'
    ),
-- NUMERO DE VEZES QUE O ITEM FOI ADICIONADO AO CARRINHO NO MESMO DIA
    NUMBER_ADD AS (
        SELECT DISTINCT *,
            COUNT(*) OVER (PARTITION BY USER_ID,ITEM_ID,DATE ORDER BY DATE) AS NUMBER_ADD
        FROM PRODUCT_ADDED
    ),
    TOTAL_ORDERS AS (
        SELECT
            CATEGORY,
            COUNT(NUMBER_ADD) AS total_orders
        FROM NUMBER_ADD
        GROUP BY CATEGORY
    ),
    CUST_ORDERED AS (
        SELECT *,
            ROW_NUMBER() OVER(PARTITION BY USER_ID,CATEGORY ORDER BY TIMESTAMP) AS CUST_ORDERED
        FROM TR_UNIQUES
        WHERE EVENT_TYPE = 'OrderCompleted'
    ),
    REVENUE AS (
        SELECT CATEGORY,
            SUM(PRICE) AS total_revenue
        FROM TR_UNIQUES
        WHERE EVENT_TYPE = 'OrderCompleted'
        GROUP BY CATEGORY
    ),
    FINAL AS (
        SELECT
            CTC.CATEGORY AS product_category,
            CTC.total_products_viewed,
            CTC.total_products_added,
            CTC.total_cart_viewed,
            CTC.total_checkout_started,
            CTC.total_orders_completed,
            CTC.total_interactions,
            TTO.total_orders,
            COUNT_IF(CTO.CUST_ORDERED = 1) AS total_customers_ordered,
            ZEROIFNULL(ROUND(REV.total_revenue,2)) AS total_revenue
        FROM COUNT_CATEGORY AS CTC
        LEFT JOIN TOTAL_ORDERS AS TTO
        ON CTC.CATEGORY = TTO.CATEGORY
            LEFT JOIN CUST_ORDERED AS CTO
            ON CTC.CATEGORY = CTO.CATEGORY
                LEFT JOIN REVENUE AS REV
                ON CTC.CATEGORY = REV.CATEGORY
        GROUP BY 1,2,3,4,5,6,7,8,10
    )
    SELECT * FROM FINAL
);