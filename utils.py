import boto3
import json
import pandas as pd
import psycopg2
from pyspark.sql.functions import col

def get_secrets(secret_name, region_name):
    """Retrieve secrets from AWS Secrets Manager."""
    client = boto3.client("secretsmanager", region_name=region_name)
    response = client.get_secret_value(SecretId=secret_name)
    secret = json.loads(response["SecretString"])
    return secret

def get_secrets_cross_account(secret_name, region_name, iam_role_arn, glue_job_name):
    """
    Retrieve secrets from another AWS account using an IAM role and Glue job name as the session name.

    Args:
        secret_name (str): The name of the secret to retrieve.
        region_name (str): The AWS region where the secret is stored.
        iam_role_arn (str): The ARN of the IAM role to assume.
        glue_job_name (str): The name of the Glue job to use as the session name.

    Returns:
        dict: The secret retrieved from AWS Secrets Manager.

    Raises:
        botocore.exceptions.ClientError: If there is an error retrieving the secret.
    """
    sts_client = boto3.client("sts", region_name=region_name)
    assumed_role = sts_client.assume_role(
        RoleArn=iam_role_arn,
        RoleSessionName=glue_job_name
    )
    credentials = assumed_role["Credentials"]

    client = boto3.client(
        "secretsmanager",
        region_name=region_name,
        aws_access_key_id=credentials["AccessKeyId"],
        aws_secret_access_key=credentials["SecretAccessKey"],
        aws_session_token=credentials["SessionToken"]
    )
    try:
        response = client.get_secret_value(SecretId=secret_name)
    except client.exceptions.ResourceNotFoundException:
        raise ValueError(f"The requested secret {secret_name} was not found")
    except client.exceptions.DecryptionFailure as e:
        raise ValueError(f"The requested secret can't be decrypted using the provided KMS key: {e}")
    
    response = client.get_secret_value(SecretId=secret_name)
    secret = json.loads(response["SecretString"])
    return secret

def load_csv_to_dataframe(spark, s3_path):
    """Load CSV files from S3 into a Spark DataFrame."""
    return spark.read.option("header", "true").csv(s3_path)

def validate_data(df, columns):
    """Perform data validation: check for null values, identify duplicates, and remove them."""
    # Check for null values in specified columns
    for col_name in columns:
        null_count = df.filter(col(col_name).isNull()).count()
        if null_count > 0:
            raise ValueError(f"Column '{col_name}' contains {null_count} null values.")

    # Identify duplicate rows
    duplicate_rows = df.groupBy(df.columns).count().filter(col("count") > 1)
    duplicate_count = duplicate_rows.count()
    if duplicate_count > 0:
        print(f"Found {duplicate_count} duplicate rows:")
        duplicate_rows.show(truncate=False)

    # Remove duplicate rows
    df = df.dropDuplicates()

    return df

def filter_columns(df, file_columns, table_columns):
    """Filter DataFrame to match table columns."""
    missing_columns = [col for col in table_columns if col not in file_columns]
    if missing_columns:
        raise ValueError(f"Missing columns in source data: {missing_columns}")

    return df.select(table_columns)

def convert_to_pandas(df):
    """Convert Spark DataFrame to pandas DataFrame."""
    return df.toPandas()

def load_to_redshift(pandas_df, table_name,s3_temp_bucket, connection_params):
    """Load pandas DataFrame into a Redshift table using psycopg2."""
    connection = psycopg2.connect(**connection_params)
    cursor = connection.cursor()
    
    # Create a temporary S3 bucket for loading data into Redshift
    temp_file_path = f"s3://{s3_temp_bucket}/temp_redshift_{table_name}_data.csv"

    # Save the pandas DataFrame to S3 as a CSV
    pandas_df.to_csv(temp_file_path, index=False)
    
    # Copy data into Redshift
    copy_query = f"""
        COPY {table_name}
        FROM '{temp_file_path}'
        CREDENTIALS default
        DELIMITER ','
        IGNOREHEADER 1;
    """
    cursor.execute(copy_query)
    connection.commit()

    # Clean up temporary file
    s3 = boto3.client('s3')
    bucket_name, key = temp_file_path.replace("s3://", "").split("/", 1)
    s3.delete_object(Bucket=bucket_name, Key=key)

    cursor.close()
    connection.close()