{{
config(
    materialized="table",
    schema="analytics"
)
}}

WITH interactions AS (
        SELECT
            TR.ITEM_ID
            ,PD.NAME
            ,PD.CATEGORY
            ,PD.PRICE
            ,TR.USER_ID
            ,TR.EVENT_TYPE
            ,TO_TIMESTAMP(TR.TIMESTAMP) AS TIMESTAMP
        FROM {{ ref('interactions') }} AS TR
        LEFT JOIN {{ ref('dim_products') }} AS PD
        ON TR.ITEM_ID = PD.PRODUCT_ID
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
    FROM interactions
    GROUP BY CATEGORY
),
PRODUCT_ADDED AS (
    SELECT
        CATEGORY,
        ITEM_ID,
        USER_ID,
        EVENT_TYPE,
        TO_DATE(TO_TIMESTAMP(TIMESTAMP)) AS DATE
    FROM interactions
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
    FROM interactions
    WHERE EVENT_TYPE = 'OrderCompleted'
),
REVENUE AS (
    SELECT CATEGORY,
        SUM(PRICE) AS total_revenue
    FROM interactions
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
SELECT * FROM FINAL limit 10