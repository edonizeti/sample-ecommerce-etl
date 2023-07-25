# Create a pipe in Snowflake

Now that your AWS and Snowflake accounts have the right security conditions, complete Snowpipe 
setup with S3 event notifications.

### Create a Database, Table, Stage, and Pipe
On a fresh Snowflake web console worksheet, use the commands below to create the objects needed for Snowpipe ingestion.

#### Create Database

```snowflake
create or replace database S3_db;
```
Execute the above command to create a database called **S3_db**.

#### Create Stage

```snowflake
use schema S3_db.public;
create or replace stage S3_stage
  url = ('s3://<bucket>/<path>/')
  storage_integration = S3_role_integration;
```
To make the external stage needed for our S3 bucket, use this command. 
Be mindful to replace the `<bucket>` and `<path>` with your S3 bucket name and file path.

#### Create Table

This command will make a table by the name of ‘S3_table' on the S3_db database.

```snowflake
create or replace table S3_table(files string);
```

#### Create Pipe

The magic of automation is in the create pipe parameter `auto_ingest=true`. With auto_ingest set to true, data stagged 
will automatically integrate into your database.

```snowflake
create or replace pipe S3_db.public.S3_pipe auto_ingest=true as
  copy into S3_db.public.S3_table
  from @S3_db.public.S3_stage;
```

```Snowflake
show pipes;
```

### Check the Pipe status and copy the "notificationChannelName" for the AWS SQS

```snowflake
select SYSTEM$PIPE_STATUS('S3_db.public.S3_pipe');
```

### Creating a New S3 Event Notification to Automate Snowpipe

Sign in to the AWS Management Console and open the Amazon S3 console. In the **Buckets** list, choose the name of the 
bucket that you want to enable events for. Choose **Properties** and navigate to the **Event Notifications** section and 
choose **Create event notification.**

Complete the fields as follows:

- Name: Name of the event notification (e.g. Auto-ingest Snowflake).
- Event Types: Select **All object create events**.
- Destination: Select **SQS Queue**.
- Specify SQS queue: Select **Enter SQS queue ARN**.
- SQS queue ARN: Paste the **"notificationChannelName"** from the SHOW PIPES output.

Choose **Save changes**, and Amazon S3 sends a test message to the event notification destination.

Check the Pipe status to check if the pipe is running.

```snowflake
select SYSTEM$PIPE_STATUS('S3_db.public.S3_pipe');
```

