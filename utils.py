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