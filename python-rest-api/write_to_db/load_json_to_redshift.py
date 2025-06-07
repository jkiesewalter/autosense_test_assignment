import os
import json
import glob
import boto3
import redshift_connector
import pandas as pd
from dotenv import load_dotenv
from scipy.stats import zscore
import numpy as np
from datetime import datetime
import pytz

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
            FORMAT AS CSV
            IGNOREHEADER 1;
            """
            cursor.execute(sql)
            connection.commit()
            print(f"Data successfully copied from {s3_path} to Redshift table {table_name}.")
    except Exception as e:
        print(f"Failed to copy data from S3 to Redshift: {e}")
    finally:
        connection.close()

def transform_json_to_csv(json_folder, csv_output_path, table_name):
    """
    Transform JSON files into a CSV file with the required fields and clean the data.

    Args:
        json_folder (str): Path to the folder containing JSON files.
        csv_output_path (str): Path to save the output CSV file.
        table_name (str): Name of the target table (used to determine fields).
    """
    # Define the fields for each table
    table_fields = {
        "transactions": [
            "session_id", "user_id", "charger_id", "start_time", "end_time",
            "kWh_consumed", "status", "payment_method", "amount", "currency"
        ],
        "users": [
            "user_id", "name", "email", "tier", "created_at"
        ],
        "chargers": [
            "charger_id", "city", "location.lat", "location.lon", "installed_at"
        ]
    }

    # Ensure the table name is valid
    if table_name not in table_fields:
        raise ValueError(f"Unknown table name: {table_name}")

    # Get the required fields for the table
    required_fields = table_fields[table_name]

    # Collect data from the relevant JSON files
    data = []
    if table_name == "transactions":
        transactions_file = os.path.join(json_folder, "transactions.json")
        payments_file = os.path.join(json_folder, "payments.json")

        # Load transactions and payments
        with open(transactions_file, "r") as file:
            transactions = json.load(file)
        with open(payments_file, "r") as file:
            payments = json.load(file)

        # Create a mapping of session_id to payment details
        payment_map = {payment["session_id"]: payment for payment in payments}

        # Combine transactions with payment details
        for transaction in transactions:
            combined_record = {field: transaction.get(field, None) for field in required_fields}
            if transaction["session_id"] in payment_map:
                payment = payment_map[transaction["session_id"]]
                combined_record["amount"] = payment.get("amount")
                combined_record["currency"] = payment.get("currency")
            # Set end_time to start_time for failed transactions without an end_time
            if combined_record["status"] == "failed" and not combined_record["end_time"]:
                combined_record["end_time"] = combined_record["start_time"]
            data.append(combined_record)

    elif table_name == "users":
        users_file = os.path.join(json_folder, "users.json")
        with open(users_file, "r") as file:
            users = json.load(file)
        for user in users:
            filtered_record = {field: user.get(field, None) for field in required_fields}
            data.append(filtered_record)

    elif table_name == "chargers":
        chargers_file = os.path.join(json_folder, "chargers.json")
        with open(chargers_file, "r") as file:
            chargers = json.load(file)
        for charger in chargers:
            filtered_record = {}
            for field in required_fields:
                if "." in field:  # Handle nested fields like "location.lat"
                    keys = field.split(".")
                    value = charger
                    for key in keys:
                        value = value.get(key, None) if isinstance(value, dict) else None
                    filtered_record[field.replace(".", "_")] = value
                else:
                    filtered_record[field] = charger.get(field, None)
            data.append(filtered_record)

    # Convert the data to a DataFrame
    df = pd.DataFrame(data)

    # Transform timestamps to the required format and ensure they are in the same timezone
    timestamp_fields = ["start_time", "end_time", "created_at", "installed_at"]
    for field in timestamp_fields:
        if field in df.columns:
            # Convert to datetime and ensure timezone is UTC
            df[field] = pd.to_datetime(df[field], errors="coerce").dt.tz_localize(None).dt.strftime('%Y-%m-%d %H:%M:%S')
            print(f"Transformed timestamps in column '{field}' to format 'yyyy-mm-dd hh:mi:ss' and ensured timezone consistency.")

    # Remove duplicates
    initial_row_count = len(df)
    df.drop_duplicates(inplace=True)
    print(f"Removed {initial_row_count - len(df)} duplicate rows.")

    # Check for unique primary IDs
    if table_name in ["transactions", "users", "chargers"]:
        primary_id = "session_id" if table_name == "transactions" else "user_id" if table_name == "users" else "charger_id"
        if not df[primary_id].is_unique:
            duplicate_count = df[primary_id].duplicated().sum()
            raise ValueError(f"Duplicate primary IDs found in {table_name} table: {duplicate_count} duplicates.")
        print(f"All primary IDs in the {table_name} table are unique.")

    # Clean city names for chargers
    if table_name == "chargers":
        city_corrections = {
            "Zurich": "Zurich",
            "Zuerich": "Zurich",
            "Zürich": "Zurich",
            "St. Gallen": "St. Gallen",
            "Sankt Gallen": "St. Gallen"
        }
        df["city"] = df["city"].replace(city_corrections)
        print("Corrected city names for chargers.")

        # Remove invalid coordinates
        valid_coords = df[(df["location_lat"].between(-90, 90)) & (df["location_lon"].between(-180, 180))]
        print(f"Removed {len(df) - len(valid_coords)} rows with invalid coordinates.")
        df = valid_coords

        # Remove location outliers using z-score
        df["z_lat"] = zscore(df["location_lat"])
        df["z_lon"] = zscore(df["location_lon"])
        outliers = df[(df["z_lat"].abs() > 2) | (df["z_lon"].abs() > 2)]
        print(f"Removed {len(outliers)} location outliers based on z-score.")
        df = df[(df["z_lat"].abs() <= 2) & (df["z_lon"].abs() <= 2)]
        df.drop(columns=["z_lat", "z_lon"], inplace=True)

    # Save the cleaned DataFrame to a CSV file
    df.to_csv(csv_output_path, index=False)
    print(f"Cleaned CSV file saved to: {csv_output_path}")

def load_json_files_to_s3_and_redshift(json_folder, table_name):
    """Transform JSON files to CSV, upload to S3, and trigger the Redshift COPY command."""
    # Transform JSON files into a single CSV file
    csv_output_path = os.path.join(json_folder, f"{table_name}.csv")
    transform_json_to_csv(json_folder, csv_output_path, table_name)

    # Upload the CSV file to S3
    s3_key = f"{S3_FOLDER}/{os.path.basename(csv_output_path)}" if S3_FOLDER else os.path.basename(csv_output_path)
    upload_to_s3(csv_output_path, S3_BUCKET_NAME, s3_key)

    # Trigger the Redshift COPY command
    s3_path = f"s3://{S3_BUCKET_NAME}/{s3_key}"
    copy_from_s3_to_redshift(s3_path, table_name)

if __name__ == "__main__":
    # Test the connection to the Redshift database
    test_redshift_connection()

    # Test the connection to the S3 bucket
    test_s3_connection()

    JSON_FOLDER ="/Users/jkiesewalter/Documents/autosense_test_assignment/resources"
    TABLE_NAME = "users"  # Change to "users" or "chargers" as needed

    # Load JSON files into S3 and trigger the Redshift COPY command
    #load_json_files_to_s3_and_redshift(JSON_FOLDER, "users")
    #load_json_files_to_s3_and_redshift(JSON_FOLDER, "chargers")
    #load_json_files_to_s3_and_redshift(JSON_FOLDER, "transactions")