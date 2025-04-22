import pandas as pd
import boto3
import os
from datetime import datetime
import json
from sqlalchemy import create_engine
from botocore.exceptions import NoCredentialsError, PartialCredentialsError


def get_aws_account_id():
    sts_client = boto3.client('sts')
    try:
        response = sts_client.get_caller_identity()
        return response['Account']
    except Exception as e:
        raise Exception(f"Error retrieving AWS account ID: {e}")

def get_ssm_parameter(parameter_name, region_name):
    ssm_client = boto3.client('ssm', region_name=region_name)
    try:
        response = ssm_client.get_parameter(Name=parameter_name, WithDecryption=True)
        return response['Parameter']['Value']
    except Exception as e:
        raise Exception(f"Error retrieving parameter from SSM Parameter Store: {e}")

def get_rds_credentials(secret_name, region_name):
    secrets_manager_client = boto3.client('secretsmanager', region_name=region_name)
    try:
        response = secrets_manager_client.get_secret_value(SecretId=secret_name)
        return json.loads(response['SecretString'])
    except Exception as e:
        raise Exception(f"Error retrieving RDS credentials from Secrets Manager: {e}")

def create_sqlalchemy_engine(credentials, database, hostname):
    try:
        connection_string = f"mysql+pymysql://{credentials['username']}:{credentials['password']}@{hostname}/{database}"
        engine = create_engine(connection_string)
        return engine
    except Exception as e:
        raise Exception(f"Error creating SQLAlchemy engine: {e}")

def export_sales_summary_to_csv(engine, s3_bucket, db_name):
    try:
        # Query sales_summary table
        query = f"SELECT * FROM {db_name}.sales_summary"
        df = pd.read_sql(query, engine)
        
        # Generate file path
        current_date = datetime.now().strftime("%m%d%Y")
        file_name = f"maze_sales_summary_{current_date}.csv"
        local_file_path = f"/tmp/{file_name}"
        
        # Save CSV file locally
        df.to_csv(local_file_path, index=False)
        
        # Upload CSV file to S3
        s3_client = boto3.client('s3')
        s3_key = f"outbound/maze/{current_date}/{file_name}"
        s3_client.upload_file(local_file_path, s3_bucket, s3_key)
        
        print(f"Sales summary data exported to: s3://{s3_bucket}/{s3_key}")
    except Exception as e:
        print(f"Error exporting sales summary data: {e}")

def lambda_handler(event, context):
    db_name = os.environ.get('dbname')
    region_name = boto3.Session().region_name
    secret_name = 'dev/database-1/salesdb'
    rds_hostname_parameter_name = '/dev/rds/hostname'
    s3_bucket = f"dehlive-sales-partner-{get_aws_account_id()}-{region_name}"

    try:
        # Get RDS credentials
        credentials = get_rds_credentials(secret_name, region_name)

        # Get RDS hostname
        rds_hostname = get_ssm_parameter(rds_hostname_parameter_name, region_name)
        
        # Create SQLAlchemy engine
        engine = create_sqlalchemy_engine(credentials, db_name, rds_hostname)

        # Export sales summary data to CSV and store in S3
        export_sales_summary_to_csv(engine, s3_bucket, db_name)

    except Exception as e:
        print(f"Error: {e}")
