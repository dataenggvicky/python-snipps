import sys
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from utils import get_secrets, load_csv_to_dataframe, validate_data, filter_columns, convert_to_pandas, load_to_redshift

# Initialize Spark session
spark = SparkSession.builder.appName("AWS Glue Redshift Loader").getOrCreate()

# Read job parameters
args = getResolvedOptions(sys.argv, [
    's3_bucket', 'input_folder', 'redshift_table_name',
    'secret_name', 'columns_to_validate', 'file_columns',
    'table_columns', 'region_name'
])

s3_bucket = args['s3_bucket']
input_folder = args['input_folder']
redshift_table_name = args['redshift_table_name']
secret_name = args['secret_name']
columns_to_validate = args['columns_to_validate'].split(",")
file_columns = args['file_columns'].split(",")
table_columns = args['table_columns'].split(",")
region_name = args['region_name']

s3_input_path = f"s3://{s3_bucket}/{input_folder}/"

# Main execution
try:
    # Retrieve Redshift connection parameters from Secrets Manager
    secrets = get_secrets(secret_name, region_name)
    redshift_connection_params = {
        "host": secrets["host"],
        "port": secrets["port"],
        "dbname": secrets["dbname"],
        "user": secrets["username"],
        "password": secrets["password"]
    }

    spark_df = load_csv_to_dataframe(spark, s3_input_path)
    validated_df = validate_data(spark_df, columns_to_validate)
    filtered_df = filter_columns(validated_df, file_columns, table_columns)
    pandas_df = convert_to_pandas(filtered_df)
    load_to_redshift(pandas_df, redshift_table_name, redshift_connection_params)
    print("Data successfully loaded into Redshift!")
except Exception as e:
    print(f"An error occurred: {e}")
