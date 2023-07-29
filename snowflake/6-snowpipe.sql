use role sysadmin;

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
