# Importing the necessary modules
import os
import zipfile
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# Default DAG arguments
default_args = {
    "owner": "edonizeti",
    'depends_on_past': False,
    "start_date": days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Local file directories
zip_dir = "/etl-bi-engineer-datatalks-challenge/data"
extract_to = "/etl-bi-engineer-datatalks-challenge/data/csv_files"
move_zip_to = "/etl-bi-engineer-datatalks-challenge/data/extracted"
move_csv_to = "/etl-bi-engineer-datatalks-challenge/data/uploaded"

# S3 Bucket
bucket_name = "bi-engineer-challenger"

# Unzip the .zip files
def unzip_files(zip_dir, extract_to):
    zip_files_list = []
    for file in os.listdir(zip_dir):
        # Checking if the file is a .zip
        if file.endswith(".zip"):
            # Creating the full path of the .zip file
            zip_path = os.path.join(zip_dir, file)
            # Creating a ZipFile object to manipulate the .zip file
            with zipfile.ZipFile(zip_path, "r") as zip_ref:
                # Extracting all .zip files to the same directory
                if not os.path.exists(extract_to):
                    os.makedirs(extract_to)
                zip_ref.extractall(extract_to)
                print(f"File {file} unzipped successfully"),
                # Generates a list of .zip files
                zip_files_list.append(os.path.join(zip_dir, file))
    return zip_files_list

# Move files that have already been unzipped to another folder
def move_zip_file(zip_paths, move_zip_to):
    for zip_path in zip_paths:
        if not os.path.exists(move_zip_to):
            os.makedirs(move_zip_to)
        os.rename(zip_path, os.path.join(move_zip_to, os.path.basename(zip_path)))
        print(f"File {zip_path} successfully moved to {move_zip_to}")

# upload the unzipped files to S3
def upload_to_s3(csv_dir,bucket_name):
    # Use airflow connection to authenticate to S3
    hook = S3Hook(aws_conn_id='my_aws_conn') 
    # List to store .csv file paths
    csv_files_list = []
    for file in os.listdir(csv_dir):
        if file.endswith(".csv"):
            csv_path = os.path.join(csv_dir,file)
            # Returns the file name and extension separately. The file name without the .csv will be the folder name in S3
            file_name, file_ext = os.path.splitext(file)
            folder = file_name 
            # Appending the date and time to the file name
            timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
            csv_file = f"{file_name}_{timestamp}{file_ext}"            
            s3_key = os.path.join(folder,csv_file)
            try:
                hook.load_file(csv_path,s3_key,bucket_name) 
                print(f"File {file} uploaded to S3 successfully")
                # Add file path to list
                csv_files_list.append(csv_path)
            except Exception as e:
                error_message=f"Error sending file {file} to S3: {e}"
                print(error_message)
                raise Exception(error_message)
    return csv_files_list

# Move the .csv files after successful upload to S3
def move_csv_files(csv_files_list, move_csv_to):
    for csv_path in csv_files_list:
        if not os.path.exists(move_csv_to):
            os.makedirs(move_csv_to)
        os.rename(csv_path, os.path.join(move_csv_to, os.path.basename(csv_path)))
        print(f"File {csv_path} successfully moved to {move_csv_to}")

# Create DAG
with DAG(
    dag_id="load_file_to_S3",
    description='DAG to unzip a .zip file and move it to the "extracted" folder',
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
) as dag:

    unzip_task = PythonOperator(
        task_id="unzip_file",
        python_callable=unzip_files,
        op_args=[zip_dir, extract_to],
        # To return the function's return value
        provide_context=True,  
    )

    move_zip_task = PythonOperator(
        task_id="move_zip_file",
        python_callable=move_zip_file,
        # Use the result of the previous task as an argument
        op_args=[unzip_task.output, move_zip_to],  
    )

    upload_to_S3_task = PythonOperator(
        task_id="upload_csv_to_s3",
        python_callable=upload_to_s3,
        op_args=[extract_to, bucket_name],
        provide_context=True,
    )

    move_csv_task = PythonOperator(
        task_id="move_csv_files",
        python_callable=move_csv_files,
        op_args=[upload_to_S3_task.output, move_csv_to],
    )

# Defining the task order in the DAG
unzip_task >> move_zip_task >> upload_to_S3_task >> move_csv_task