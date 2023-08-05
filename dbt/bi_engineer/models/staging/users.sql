{{
    config(
        materialized="table",
        schema="staging"
    )
}}

SELECT DISTINCT * FROM RAW.USERS