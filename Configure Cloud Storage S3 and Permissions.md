# Configure Cloud Storage S3 and Permissions

## Create AWS S3 Bucket

1. Sign in to the AWS Management Console and open the Amazon S3 console at https://console.aws.amazon.com/s3/.
2. In the left navigation pane, choose **Buckets** and than choose **Create bucket**.
3. Enter a name for your bucket.
4. For **Region**, choose the AWS Region where you want the bucket to reside.
5. Under **Block Public Access settings for this bucket**, choose the Block Public Access settings that you want to apply to the bucket.
6. Choose Create bucket.

You've created a bucket in Amazon S3.

### Create IAM Policy for Snowflake's S3 Access

Snowflake needs IAM policy permission to access your S3 with `GetObject`, `GetObjectVersion`, and `ListBucket`. Log into
your AWS console and navigate to the IAM service. Within the **Account settings**, confirm the **Security Token Service** 
list records your account's region as **Active**.

Navigate to **Policies** and use the JSON below to create a new IAM policy named *snowflake_access*.

```
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
              "s3:PutObject",
              "s3:GetObject",
              "s3:GetObjectVersion",
              "s3:DeleteObject",
              "s3:DeleteObjectVersion"
            ],
            "Resource": "arn:aws:s3:::<bucket_name>/<prefix>/*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:ListBucket",
                "s3:GetBucketLocation"
            ],
            "Resource": "arn:aws:s3:::<bucket_name>",
            "Condition": {
                "StringLike": {
                    "s3:prefix": [
                        "<prefix>/*"
                    ]
                }
            }
        }
    ]
}
```
>Make sure to replace `bucket` and `prefix` with your actual bucket name and folder path prefix.

### New IAM Role

On the AWS IAM console, add a new IAM role tied to the *snowflake_access* IAM policy. 
Create the role with the following settings.

- Trusted Entity: Another AWS account
- Account ID: <your_account_id>
- Require External ID: [x]
- External ID: 0000
- Role Name: snowflake_role
- Role Description: Snowflake role for access to S3
- Policies: snowflake_access

Create the role, then click to see the role's summary and record the Role ARN.

### Integrate IAM user with Snowflake storage.

Within your Snowflake web console, you'll run a `CREATE STORAGE INTEGRATION` command on a worksheet.

```snowflake
CREATE OR REPLACE STORAGE INTEGRATION S3_role_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = S3
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = "arn:aws:iam::<role_account_id>:role/snowflake_role"
  STORAGE_ALLOWED_LOCATIONS = ("s3://<bucket>/<path>/");
```
>Be sure to change the `<bucket>`, `<path>` and `<role_account_id>` is replaced with your AWS S3 bucket name, folder 
>path prefix, and IAM role account ID.

### Run storage integration description command

Run the command to display your new integration's description.

```snowflake
desc integration S3_role_integration;
```
Record the property values displayed for `STORAGE_AWS_IAM_USER_ARN` and `STORAGE_AWS_EXTERNAL_ID`

### IAM User Permissions

Navigate back to your AWS IAM service console. Within the **Roles**, click the *snowflake_role*. On the 
**Trust relationships** tab, click **Edit trust policy** and edit the file with the `STORAGE_AWS_IAM_USER_ARN` and 
`STORAGE_AWS_EXTERNAL_ID` retrieved in the previous step.

```
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "AWS": "<STORAGE_AWS_IAM_USER_ARN>"
      },
      "Action": "sts:AssumeRole",
      "Condition": {
        "StringEquals": {
          "sts:ExternalId": "<STORAGE_AWS_EXTERNAL_ID>"
        }
      }
    }
  ]
}
```
**Update Trust Policy** after replacing the string values for your `STORAGE_AWS_IAM_USER_ARN` and 
`STORAGE_AWS_EXTERNAL_ID`.