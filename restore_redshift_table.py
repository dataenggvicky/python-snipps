import boto3
import logging
from botocore.exceptions import ClientError

# Initialize clients outside handler for reuse
redshift = boto3.client('redshift')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    """Restores Redshift table from latest snapshot using AWS SDK"""
    try:
        cluster_id = 'your-cluster-identifier'  # Replace with env var
        source_db = 'your-source-database'       # Replace with env var
        source_table = 'your-source-table'      # Replace with env var
        target_table = 'restored_table_' + context.aws_request_id[-8:]  # Unique name
        
        # Get latest snapshot
        snapshots = redshift.describe_cluster_snapshots(
            ClusterIdentifier=cluster_id,
            SnapshotType='manual',
            MaxRecords=100,
            Sorting='creation_time DESC'
        )['Snapshots']
        
        if not snapshots:
            logger.error("No snapshots found")
            return {'status': 'error', 'message': 'No snapshots available'}
        
        latest_snapshot = snapshots[0]['SnapshotIdentifier']
        
        # Initiate table restore
        response = redshift.restore_table_from_cluster_snapshot(
            ClusterIdentifier=cluster_id,
            SnapshotIdentifier=latest_snapshot,
            SourceDatabaseName=source_db,
            SourceTableName=source_table,
            TargetDatabaseName=source_db,  # Same DB for example
            NewTableName=target_table
        )
        
        # Return restore status
        return {
            'status': 'success',
            'restore_request_id': response['TableRestoreStatus']['TableRestoreRequestId'],
            'target_table': target_table,
            'snapshot_used': latest_snapshot
        }
        
    except ClientError as e:
        logger.error(f"Restore failed: {e.response['Error']['Message']}")
        return {'status': 'error', 'message': str(e)}
