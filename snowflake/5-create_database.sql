use role sysadmin;

-- Create a Database
create or replace database bi_engineer_challenger;

-- Create schemas
create or replace schema raw;
create or replace schema staging;
create or replace schema analytics;

-- Create tables that has the same structure as the CSV file
create or replace table users(
    id integer,
    username varchar,
    email varchar,
    first_name varchar,
    last_name varchar,
    addresses variant,
    age integer,
    gender varchar,
    persona	varchar,
    discount_persona string
);

create or replace table products(
    id varchar,
    url varchar,
    name varchar,
    category varchar,
    style varchar,
    description varchar,
    price float,
    image varchar,
    gender_affinity varchar,
    current_stock integer
);

create or replace table interactions(
    Item_ID varchar,
    User_ID integer,
    Event_Type varchar,
    Timestamp integer,
    Discount boolean
);