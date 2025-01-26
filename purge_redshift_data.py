import boto3
import yaml
import time
import logging
from datetime import datetime

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def read_yaml_from_s3(s3_path):
    s3 = boto3.client('s3')
    bucket, key = s3_path.replace('s3://', '').split('/', 1)
    response = s3.get_object(Bucket=bucket, Key=key)
    return yaml.safe_load(response['Body'])

def execute_redshift_procedure(client, sql, params, cluster_id, database, secret_arn):
    try:
        response = client.execute_statement(
            ClusterIdentifier=cluster_id,
            Database=database,
            SecretArn=secret_arn,
            Sql=sql,
            Parameters=params
        )
        statement_id = response['Id']
        while True:
            desc = client.describe_statement(Id=statement_id)
            if desc['Status'] in ('SUBMITTED', 'STARTED', 'PICKED'):
                time.sleep(5)
            else:
                break
        if desc['Status'] != 'FINISHED':
            error = desc.get('Error', 'Unknown error')
            logger.error(f"Error executing statement: {error}")
            raise Exception(f"Statement failed: {error}")
    except Exception as e:
        logger.error(f"Error executing procedure: {e}")

def main():
    config = read_yaml_from_s3("s3://your-bucket/config.yaml")
    redshift_config = config['redshift_config']
    tables = config['tables']
    test_mode = False  # Set via Glue job parameters

    client = boto3.client('redshift-data')
    
    for table in tables:
        s3_path = f"{redshift_config['s3_export_path']}{table['table_name']}/{datetime.now().isoformat()}/"
        params = [
            {'name': 'table_name', 'value': table['table_name']},
            {'name': 'date_column', 'value': table['date_field']},
            {'name': 'date_format', 'value': table.get('date_format', '')},
            {'name': 's3_path', 'value': s3_path},
            {'name': 'days_to_retain', 'value': str(table.get('days_to_retain', '')) if 'days_to_retain' in table else None},
            {'name': 'months_to_retain', 'value': str(table.get('months_to_retain', '')) if 'months_to_retain' in table else None},
            {'name': 'test_mode', 'value': str(test_mode).lower()}
        ]
        sql = "CALL purge_data(?, ?, ?, ?, ?, ?, ?)"
        logger.info(f"Executing stored procedure for table: {table['table_name']}")
        execute_redshift_procedure(
            client=client,
            sql=sql,
            params=params,
            cluster_id=redshift_config['cluster_id'],
            database=redshift_config['database'],
            secret_arn=redshift_config['secret_arn']
        )

if __name__ == "__main__":
    main()