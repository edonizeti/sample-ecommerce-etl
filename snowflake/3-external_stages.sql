-- Create a Snowflake stage (stage can be defined as an intermediary space for uploading/unloading source files)
use schema bi_engineer.raw;

create or replace stage s3_products_stage
  url = 's3://etl-bi-engineer/products/'
  storage_integration = S3_role_integration;

create or replace stage s3_interactions_stage
  url = 's3://etl-bi-engineer/interactions/'
  storage_integration = S3_role_integration;

create or replace stage s3_users_stage
  url = 's3://etl-bi-engineer/users/'
  storage_integration = S3_role_integration;

show stages;