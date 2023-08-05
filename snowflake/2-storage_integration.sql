-- Integrate AWS IAM role with Snowflake storage.
use role sysadmin;

-- CREATE STORAGE INTEGRATION
create or replace storage integration S3_role_integration
    type = external_stage
    storage_provider = S3
    enabled = true
    storage_aws_role_arn = 'arn:aws:iam::510139739254:role/snowflake_role'
    storage_allowed_locations = ('s3://etl-bi-engineer/');

show integrations;

-- Run storage integration description command
desc integration S3_role_integration;