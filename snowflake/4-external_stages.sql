use role sysadmin;
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