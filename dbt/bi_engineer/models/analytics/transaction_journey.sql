{{
    config(
        materialized="table",
        schema="analytics"
    )
}}

WITH
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
        FROM {{ ref('interactions') }}
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
        LEFT JOIN {{ ref('dim_products') }} AS PRODUCT
        ON PIVO.ITEM_ID = PRODUCT.PRODUCT_ID
    )
SELECT * FROM FINAL