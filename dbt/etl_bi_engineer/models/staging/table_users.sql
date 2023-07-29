{{config(materialized='table')}}

with duplicate as (
  select *
  from raw.users
)
select * from duplicate