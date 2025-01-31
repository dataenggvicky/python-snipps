import boto3
import pandas as pd
from pyspark.sql import SparkSession
from typing import Optional, Dict, Any, Union, List
import logging
import json
import time
class RedshiftDataUtils:
    # Initialize logger
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    def __init__(self, region: str, secret_name: str):
        # Initialize with region and secret name
        self.region = region
        self.secret_name = secret_name
        # Get secrets and set up connection parameters
        self._initialize_from_secrets()
        # Initialize AWS clients
        self.redshift_data_client = boto3.client('redshift-data',
            aws_access_key_id=self.aws_access_key,
            aws_secret_access_key=self.aws_secret_key,
            region_name=region
        )
    def _get_secrets(self) -> Dict[str, Any]:
        # Retrieve secrets from AWS Secrets Manager
        try:
            session = boto3.session.Session()
            secrets_client = session.client(
                service_name='secretsmanager',
                region_name=self.region
            )
            secret_value = secrets_client.get_secret_value(SecretId=self.secret_name)
            return json.loads(secret_value['SecretString'])
        except Exception as e:
            logging.error(f"Error retrieving secrets: {str(e)}")
            raise
    def _initialize_from_secrets(self) -> None:
        # Initialize connection parameters from secrets
        secrets = self._get_secrets()
        self.aws_access_key = secrets['aws_access_key']
        self.aws_secret_key = secrets['aws_secret_key']
        self.cluster_identifier = secrets['redshift_cluster_identifier']
        self.database = secrets['database']
        self.db_user = secrets['user']
    def _wait_for_query_completion(self, query_id: str) -> bool:
        # Wait for query execution to complete
        while True:
            response = self.redshift_data_client.describe_statement(Id=query_id)
            status = response['Status']
            if status == 'FINISHED':
                return True
            elif status in ['FAILED', 'ABORTED']:
                error_message = response.get('Error', 'Unknown error')
                raise Exception(f"Query failed with status {status}: {error_message}")
            time.sleep(0.5)
    def _process_query_results(self, query_id: str, mode: str) -> Union[pd.DataFrame, 'pyspark.sql.DataFrame']:
        # Process query results and return as DataFrame
        try:
            response = self.redshift_data_client.get_statement_result(Id=query_id)
            # Extract column names
            columns = [col['name'] for col in response['ColumnMetadata']]
            # Extract data
            data = []
            for record in response['Records']:
                row = []
                for value in record:
                    # Handle different value types
                    row.append(value.get('stringValue') or value.get('longValue') or 
                             value.get('doubleValue') or None)
                data.append(row)
            # Convert to specified format
            if mode.lower() == 'pandas':
                return pd.DataFrame(data, columns=columns)
            elif mode.lower() == 'pyspark':
                spark = SparkSession.builder.appName("RedshiftData").getOrCreate()
                return spark.createDataFrame(data, columns)
            else:
                raise ValueError("Mode must be either 'pandas' or 'pyspark'")
        except Exception as e:
            logging.error(f"Error processing query results: {str(e)}")
            raise
    def execute_query(self, query: str, mode: str = 'pandas') -> Union[pd.DataFrame, 'pyspark.sql.DataFrame']:
        # Execute query using Redshift Data API
        try:
            response = self.redshift_data_client.execute_statement(
                ClusterIdentifier=self.cluster_identifier,
                Database=self.database,
                DbUser=self.db_user,
                Sql=query
            )
            query_id = response['Id']
            # Wait for query completion
            self._wait_for_query_completion(query_id)
            # Process and return results
            return self._process_query_results(query_id, mode)
        except Exception as e:
            logging.error(f"Error executing query: {str(e)}")
            raise
    def read_table(self, schema: str, table_name: str, mode: str = 'pandas',
                   where_clause: Optional[str] = None,
                   columns: Optional[List[str]] = None) -> Union[pd.DataFrame, 'pyspark.sql.DataFrame']:
        # Read data from Redshift table using Data API
        try:
            cols = "*" if not columns else ", ".join(columns)
            query = f"SELECT {cols} FROM {schema}.{table_name}"
            if where_clause:
                query += f" WHERE {where_clause}"
            return self.execute_query(query, mode)
        except Exception as e:
            logging.error(f"Error reading from table {schema}.{table_name}: {str(e)}")
            raise
    def batch_execute_queries(self, queries: List[str]) -> List[str]:
        # Execute multiple queries in batch mode
        try:
            query_ids = []
            for query in queries:
                response = self.redshift_data_client.execute_statement(
                    ClusterIdentifier=self.cluster_identifier,
                    Database=self.database,
                    DbUser=self.db_user,
                    Sql=query
                )
                query_ids.append(response['Id'])
            # Wait for all queries to complete
            for query_id in query_ids:
                self._wait_for_query_completion(query_id)
            return query_ids
        except Exception as e:
            logging.error(f"Error executing batch queries: {str(e)}")
            raise