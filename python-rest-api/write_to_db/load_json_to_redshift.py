import os
import json
import glob
import boto3
import redshift_connector
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# AWS S3 and Redshift connection details
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
S3_FOLDER = os.getenv("S3_FOLDER")
REDSHIFT_HOST = os.getenv("REDSHIFT_HOST")
REDSHIFT_PORT = int(os.getenv("REDSHIFT_PORT", 5439))
REDSHIFT_DBNAME = os.getenv("REDSHIFT_DBNAME")
REDSHIFT_USER = os.getenv("REDSHIFT_USER")
REDSHIFT_PASSWORD = os.getenv("REDSHIFT_PASSWORD")
REDSHIFT_IAM_ROLE = os.getenv("REDSHIFT_IAM_ROLE")

def connect_to_redshift():
    """Establish a connection to the Redshift database."""
    return redshift_connector.connect(
        host=REDSHIFT_HOST,
        port=REDSHIFT_PORT,
        database=REDSHIFT_DBNAME,
        user=REDSHIFT_USER,
        password=REDSHIFT_PASSWORD
    )

def test_redshift_connection():
    """Test the connection to the Redshift database."""
    try:
        connection = connect_to_redshift()
        print("Successfully connected to the Redshift database!")
        connection.close()
    except Exception as e:
        print(f"Failed to connect to the Redshift database: {e}")

def test_s3_connection():
    """Test the connection to the S3 bucket."""
    s3_client = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY
    )
    try:
        response = s3_client.list_objects_v2(Bucket=S3_BUCKET_NAME)
        if 'Contents' in response:
            print(f"Successfully connected to S3 bucket '{S3_BUCKET_NAME}'.")
            print(f"Bucket contains {len(response['Contents'])} objects.")
        else:
            print(f"Successfully connected to S3 bucket '{S3_BUCKET_NAME}', but it is empty.")
    except Exception as e:
        print(f"Failed to connect to S3 bucket '{S3_BUCKET_NAME}': {e}")

def upload_to_s3(file_path, bucket_name, s3_key):
    """Upload a file to an S3 bucket."""
    s3_client = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY
    )
    try:
        s3_client.upload_file(file_path, bucket_name, s3_key)
        print(f"Uploaded {file_path} to s3://{bucket_name}/{s3_key}")
    except Exception as e:
        print(f"Failed to upload {file_path} to S3: {e}")

def copy_from_s3_to_redshift(s3_path, table_name):
    """Trigger the Redshift COPY command to load data from S3."""
    connection = connect_to_redshift()
    try:
        with connection.cursor() as cursor:
            sql = f"""
            COPY {table_name}
            FROM '{s3_path}'
            IAM_ROLE '{REDSHIFT_IAM_ROLE}'
            FORMAT AS JSON 'auto';
            """
            cursor.execute(sql)
            connection.commit()
            print(f"Data successfully copied from {s3_path} to Redshift table {table_name}.")
    except Exception as e:
        print(f"Failed to copy data from S3 to Redshift: {e}")
    finally:
        connection.close()

def load_json_files_to_s3_and_redshift(json_folder, table_name):
    """Load JSON files into S3 and trigger the Redshift COPY command."""
    # Get all JSON files in the specified folder
    json_files = glob.glob(os.path.join(json_folder, "*.json"))
    if not json_files:
        print("No JSON files found in the folder.")
        return

    for json_file in json_files:
        print(f"Processing file: {json_file}")
        # Upload the JSON file to S3
        s3_key = f"{S3_FOLDER}/{os.path.basename(json_file)}" if S3_FOLDER else os.path.basename(json_file)
        upload_to_s3(json_file, S3_BUCKET_NAME, s3_key)

        # Trigger the Redshift COPY command
        s3_path = f"s3://{S3_BUCKET_NAME}/{s3_key}"
        copy_from_s3_to_redshift(s3_path, table_name)

if __name__ == "__main__":
    # Test the connection to the Redshift database
    test_redshift_connection()

    # Test the connection to the S3 bucket
    test_s3_connection()

    # Folder containing JSON files
    JSON_FOLDER = "/path/to/json/files"
    # Target Redshift table name
    TABLE_NAME = "your_table_name"

    # Load JSON files into S3 and trigger the Redshift COPY command
    # load_json_files_to_s3_and_redshift(JSON_FOLDER, TABLE_NAME)