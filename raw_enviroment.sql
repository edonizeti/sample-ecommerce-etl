-- Integrate IAM role with Snowflake storage.
-- CREATE STORAGE INTEGRATION
create or replace storage integration S3_role_integration
    type = external_stage
    storage_provider = S3
    enabled = true
    storage_aws_role_arn = 'arn:aws:iam::<role_account_id>:role/snowflake_role'
    storage_allowed_locations = ('s3://datatalks-bi-engineer/');

-- Run storage integration description command
desc integration S3_role_integration;

-- Create a Database
create or replace database datatalks;

-- Create schema RAW
create or replace schema raw;

-- Create a Snowflake stage (stage can be defined as an intermediary space for uploading/unloading source files)
use schema datatalks.raw;
create or replace stage datatalks_s3_products_stage
  url = 's3://datatalks-bi-engineer/products/'
  storage_integration = S3_role_integration;

create or replace stage datatalks_s3_interactions_stage
  url = 's3://datatalks-bi-engineer/interactions/'
  storage_integration = S3_role_integration;

create or replace stage datatalks_s3_users_stage
  url = 's3://datatalks-bi-engineer/users/'
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
create or replace pipe datatalks.raw.S3_pipe_interactions auto_ingest=true as
    copy into datatalks.raw.interactions
    from @datatalks.raw.datatalks_s3_interactions_stage
    file_format = ( type = CSV
                    field_delimiter = ','
                    skip_header = 1);

create or replace pipe datatalks.raw.S3_pipe_products auto_ingest=true as
    copy into datatalks.raw.products
    from @datatalks.raw.datatalks_s3_products_stage
    file_format = ( type = CSV
                    field_optionally_enclosed_by = '"'
                    record_delimiter = '\n'
                    field_delimiter = ','
                    skip_header = 1
                    null_if = ('NULL', 'null')
                    empty_field_as_null = true);

create or replace pipe datatalks.raw.S3_pipe_users auto_ingest=true as
    copy into datatalks.raw.users
    from @datatalks.raw.datatalks_s3_users_stage
    file_format = ( type = CSV
                    field_optionally_enclosed_by = '"'
                    record_delimiter = '\n'
                    field_delimiter = ','
                    skip_header = 1
                    null_if = ('NULL', 'null')
                    empty_field_as_null = true);

show pipes;

-- Check Pipe status (copy the "notificationChannelName" for the AWS SQS)
select SYSTEM$PIPE_STATUS('datatalks.raw.S3_pipe_users');
select SYSTEM$PIPE_STATUS('datatalks.raw.S3_pipe_products');
select SYSTEM$PIPE_STATUS('datatalks.raw.S3_pipe_interactions');


-- Check the ingestion status
select * from table (information_schema.copy_history(table_name=>'datatalks.raw.users',start_time=> dateadd(hours, -1,current_timestamp())));
