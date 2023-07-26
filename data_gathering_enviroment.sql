-- Set up roles and warehouse permissions
use role securityadmin;
create role loader;
grant all on warehouse loading to role loader; 
create role transformer;
grant all on warehouse transforming to role transformer;
create role reporter;
grant all on warehouse reporting to role reporter;

-- Create users and assigning them to their roles
create user eduardo
    password = '_generate_this_'
    default_warehouse = transforming
    default_role = transformer
    must_change_password = true;

create user dbt_user
    password = '_generate_this_'
    default_warehouse = transforming
    default_role = transformer;

create user tableau_user
    password = '_generate_this_'
    default_warehouse = reporting
    default_role = reporter;

-- Grant these roles to each user
grant role transformer to user '<user_name>';
grant role transformer to user dbt_user; 
grant role reporter to user tableau_user;

-- The role LOADER can access just the RAW schema.
grant usage on database bi_engineer_challenger to role loader;
grant all privileges on schema bi_engineer_challenger.raw to role loader;
revoke all privileges on schema bi_engineer_challenger.staging from role loader;
revoke all privileges on schema bi_engineer_challenger.analytics from role loader;

-- The role TRANSFORMER can only read the RAW schema but must have read and write access to the STAGING and ANALYTICS schemas.
grant usage on database bi_engineer_challenger to role transformer;
grant select on all tables in schema bi_engineer_challenger.raw to role transformer;
grant all privileges on schema bi_engineer_challenger.staging to role transformer;
grant all privileges on schema bi_engineer_challenger.analytics to role transformer;

-- The role REPORTER can acces just read the ANALYTICS schema
grant usage on database bi_engineer_challenger to role reporter;
revoke all privileges on schema bi_engineer_challenger.raw from role reporter;
revoke all privileges on schema bi_engineer_challenger.staging from role reporter;
grant select on all tables in schema bi_engineer_challenger.analytics to role reporter;


-- Integrate IAM role with Snowflake storage.
use role sysadmin;

-- CREATE STORAGE INTEGRATION
create or replace storage integration S3_role_integration
    type = external_stage
    storage_provider = S3
    enabled = true
    storage_aws_role_arn = 'arn:aws:iam::<role_account_id>:role/snowflake_role'
    storage_allowed_locations = ('s3://bi-engineer-challenger/');

-- Run storage integration description command
desc integration S3_role_integration;

-- Create a Database
create or replace database bi_engineer_challenger;

-- Create schemas
create or replace schema raw;
create or replace schema staging;
create or replace schema analytics;

-- Set up warehouses
create warehouse loading
    warehouse_size = xsmall
    auto_suspend = 3600
    auto_resume = false
    initially_suspended = true;
create warehouse transforming
    warehouse_size = xsmall
    auto_suspend = 60
    auto_resume = true
    initially_suspended = true;
create warehouse reporting
    warehouse_size = xsmall
    auto_suspend = 60
    auto_resume = true
    initially_suspended = true;


-- Create a Snowflake stage (stage can be defined as an intermediary space for uploading/unloading source files)
use schema bi_engineer_challenger.raw;

create or replace stage s3_products_stage
  url = 's3://bi-engineer-challenger/products/'
  storage_integration = S3_role_integration;

create or replace stage s3_interactions_stage
  url = 's3://bi-engineer-challenger/interactions/'
  storage_integration = S3_role_integration;

create or replace stage s3_users_stage
  url = 's3://bi-engineer-challenger/users/'
  storage_integration = S3_role_integration;

show stages;

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

-- Create Pipes to ingest data
create or replace pipe S3_pipe_interactions auto_ingest=true as
    copy into interactions
    from @s3_interactions_stage
    file_format = ( type = CSV
                    field_delimiter = ','
                    skip_header = 1);

create or replace pipe S3_pipe_products auto_ingest=true as
    copy into products
    from @s3_products_stage
    file_format = ( type = CSV
                    field_optionally_enclosed_by = '"'
                    record_delimiter = '\n'
                    field_delimiter = ','
                    skip_header = 1
                    null_if = ('NULL', 'null')
                    empty_field_as_null = true);

create or replace pipe S3_pipe_users auto_ingest=true as
    copy into users
    from @s3_users_stage
    file_format = ( type = CSV
                    field_optionally_enclosed_by = '"'
                    record_delimiter = '\n'
                    field_delimiter = ','
                    skip_header = 1
                    null_if = ('NULL', 'null')
                    empty_field_as_null = true);

show pipes;

-- Check Pipe status (copy the "notificationChannelName" for the AWS SQS)
select SYSTEM$PIPE_STATUS('S3_pipe_users');
select SYSTEM$PIPE_STATUS('S3_pipe_products');
select SYSTEM$PIPE_STATUS('S3_pipe_interactions');


-- Check the ingestion status
select * from table (information_schema.copy_history(table_name=>'bi_engineer_challenger.raw.users',start_time=> dateadd(hours, -1,current_timestamp())));

