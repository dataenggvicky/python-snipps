import boto3
from botocore.exceptions import ClientError
from typing import Dict, Any, List, Optional
from datetime import datetime
import logging
# Configure logging
logger = logging.getLogger(__name__)
class DynamoDBUtils:
    def __init__(self, table_name: str, region: str = 'us-west-2'):
        # Initialize DynamoDB resource and table
        self.dynamodb = boto3.resource('dynamodb', region_name=region)
        self.table = self.dynamodb.Table(table_name)
        self.table_name = table_name
    def put_item(self, item: Dict[str, Any]) -> bool:
        # Add timestamp for auditing
        item['created_at'] = datetime.utcnow().isoformat()
        item['updated_at'] = item['created_at']
        try:
            self.table.put_item(Item=item)
            logger.info(f"Successfully added item to {self.table_name}")
            return True
        except ClientError as e:
            logger.error(f"Error adding item to {self.table_name}: {str(e)}")
            return False
    def get_item(self, key: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        # Retrieve single item based on primary key
        try:
            response = self.table.get_item(Key=key)
            return response.get('Item')
        except ClientError as e:
            logger.error(f"Error retrieving item from {self.table_name}: {str(e)}")
            return None