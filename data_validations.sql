-- Table validation according to the data dictionary

-- User table validation
-- there are no id duplicated
with duplicated_id as (
    select USERS.*,
           row_number() over(partition by ID order by ID) review_id
    from USERS
)
select * from duplicated_id where review_id > 1 or ID is null;

-- there are users who signed up twice or more but in different periods according to their address and age.
with duplicates_email as (
    select USERS.*,
           row_number() over(partition by EMAIL order by EMAIL) review_email
    from USERS
)
select * from duplicates_email where review_email > 1;

select * from USERS where EMAIL = 'john.molina@example.com';

-- user email validation - email addresses are ok
select * from users where not regexp_like(email, '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$');

-- age validation - OK
select * from USERS where age < 1;

-- gender validation - Ok
select * from USERS where not (GENDER like any ('F','M'));

---------------------------------------------------------------------------------------------------------

-- Products table validation
-- Duplicated ID validation - ok
with duplicate_id as (
    select PRODUCTS.*,
           row_number() over(partition by ID order by ID) review_id
    from PRODUCTS
)
select * from duplicate_id where review_id > 1;

select * from PRODUCTS where id is null;

-- Duplicate name validation
-- There are some products with the same Name, Style, description and stock but with different id and prices
with duplicate_name as (
    select PRODUCTS.*,
           row_number() over(partition by NAME,STYLE,DESCRIPTION,CURRENT_STOCK order by NAME) review_name
    from PRODUCTS
)
select * from duplicate_name where review_name > 1;

select * from PRODUCTS where NAME = 'Alarm Clock' order by DESCRIPTION, CURRENT_STOCK;

-- there are products with negative price and stock
select * from PRODUCTS where PRICE < 1 or CURRENT_STOCK < 1;


-- Interactions table validation
-- there are interactions whit the USER_ID null
select * from INTERACTIONS where ITEM_ID is null or USER_ID is null;

select * from INTERACTIONS where USER_ID is null;

select distinct DISCOUNT from INTERACTIONS;

-- there are duplicated entries
with valid_int as (
    select INTERACTIONS.*,
           row_number() over (partition by ITEM_ID,USER_ID,EVENT_TYPE,TIMESTAMP,DISCOUNT order by TIMESTAMP,ITEM_ID,USER_ID,EVENT_TYPE,DISCOUNT) review
    from INTERACTIONS
)
select * from valid_int where review > 1;

select * from INTERACTIONS where ITEM_ID = '4ee252f6-4670-456d-896b-8c9879161694' and USER_ID = '3077';

