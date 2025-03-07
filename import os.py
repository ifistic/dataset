import os
import boto3
import pandas as pd
import json
import mysql.connector
from sqlalchemy import create_engine
from io import StringIO
from dotenv import load_dotenv
from botocore.exceptions import ClientError

# Load environment variables from a .env file
load_dotenv()

# Set up the AWS clients
s3_client = boto3.client('s3')
sns_client = boto3.client('sns')
secrets_client = boto3.client('secretsmanager')

# Get AWS Secret from Secrets Manager
def get_db_credentials(secret_name):
    try:
        # Retrieve the secret value from AWS Secrets Manager
        response = secrets_client.get_secret_value(SecretId=secret_name)
        
        if 'SecretString' in response:
            secret = json.loads(response['SecretString'])
        else:
            secret = json.loads(response['SecretBinary'])
        return secret
    except ClientError as e:
        print(f"Error getting secrets: {e}")
        return None

# Read data from S3
def read_s3_file(bucket_name, file_key):
    try:
        # Download the file from S3
        s3_response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        file_content = s3_response['Body'].read().decode('utf-8')
        # If the file is CSV, read it using pandas
        df = pd.read_csv(StringIO(file_content))
        return df
    except Exception as e:
        print(f"Error reading from S3: {e}")
        return None

# Insert data into MySQL RDS using SQLAlchemy
def insert_data_to_rds(df, db_credentials):
    try:
        # Create a connection to MySQL RDS
        db_url = f"mysql+mysqlconnector://{db_credentials['username']}:{db_credentials['password']}@{db_credentials['host']}:{db_credentials['port']}/{db_credentials['dbname']}"
        
        engine = create_engine(db_url)
        # Assuming the DataFrame matches the table schema, you can write to the RDS directly
        df.to_sql('your_table_name', con=engine, if_exists='append', index=False)
        
        print("Data inserted successfully into RDS")
    except Exception as e:
        print(f"Error inserting data into RDS: {e}")

# Send SNS notification
def send_sns_notification(topic_arn, message):
    try:
        sns_client.publish(TopicArn=topic_arn, Message=message)
        print("Notification sent successfully")
    except ClientError as e:
        print(f"Error sending SNS notification: {e}")

def main():
    # Load the environment variables
    secret_name = os.getenv('dev/rds/database-2')  # The secret name in AWS Secrets Manager
    bucket_name = os.getenv('classbucket2024')  # The S3 bucket name
    file_key = os.getenv('demo_sales.csv')       # The S3 file key (path to the file in S3)
    sns_topic_arn = os.getenv('arn:aws:sns:ap-south-1:811575226032:ifiokmessage')  # ARN of the SNS topic

    # Step 1: Get DB credentials from AWS Secrets Manager
    db_credentials = get_db_credentials(secret_name)
    if not db_credentials:
        return
    
    # Step 2: Read data from the S3 bucket
    data_frame = read_s3_file(bucket_name, file_key)
    if data_frame is None:
        return
    
    # Step 3: Insert data into MySQL RDS instance
    insert_data_to_rds(data_frame, db_credentials)
    
    # Step 4: Send notification via SNS
    message = f"Data from S3 bucket {bucket_name} has been successfully copied to RDS."
    send_sns_notification(sns_topic_arn, message)

if __name__ == '__main__':
    main()

