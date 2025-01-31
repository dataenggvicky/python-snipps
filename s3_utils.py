import boto3
from botocore.exceptions import ClientError
import logging
from typing import List, Optional, Union
from pathlib import Path

class S3Utils:
    def __init__(self, bucket_name: str, region_name: Optional[str] = None):
        self.bucket_name = bucket_name
        self.s3_client = boto3.client('s3', region_name=region_name)
        self.logger = logging.getLogger(__name__)
    
    def read_object(self, object_key: str) -> bytes:
        """Reads and returns content of an S3 object"""
        try:
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=object_key)
            return response['Body'].read()
        except ClientError as e:
            self.logger.error(f"Error reading object {object_key}: {str(e)}")
            raise
    
    def write_object(self, object_key: str, data: Union[bytes, str, Path], content_type: Optional[str] = None) -> bool:
        """Writes data to an S3 object. Returns True if successful"""
        try:
            if isinstance(data, (str, Path)):
                with open(str(data), 'rb') as file:
                    data = file.read()
            
            upload_args = {'Bucket': self.bucket_name, 'Key': object_key, 'Body': data}
            if content_type:
                upload_args['ContentType'] = content_type
                
            self.s3_client.put_object(**upload_args)
            return True
        except ClientError as e:
            self.logger.error(f"Error writing object {object_key}: {str(e)}")
            return False
    
    def list_files(self, prefix: str = "", max_keys: int = 1000) -> List[dict]:
        """Lists files in bucket with optional prefix filter"""
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=prefix,
                MaxKeys=max_keys
            )
            
            files = []
            if 'Contents' in response:
                for obj in response['Contents']:
                    files.append({
                        'key': obj['Key'],
                        'size': obj['Size'],
                        'last_modified': obj['LastModified']
                    })
            return files
        except ClientError as e:
            self.logger.error(f"Error listing files with prefix {prefix}: {str(e)}")
            return []
    
    def get_file_size(self, object_key: str) -> Optional[int]:
        """Returns size of S3 object in bytes"""
        try:
            response = self.s3_client.head_object(Bucket=self.bucket_name, Key=object_key)
            return response['ContentLength']
        except ClientError as e:
            self.logger.error(f"Error getting size for {object_key}: {str(e)}")
            return None

    def copy_object(self, source_bucket: str, source_key: str, destination_key: str, 
                   destination_bucket: Optional[str] = None, extra_args: Optional[dict] = None) -> bool:
        """Copies an object between S3 locations"""
        try:
            dest_bucket = destination_bucket or self.bucket_name
            copy_source = {'Bucket': source_bucket, 'Key': source_key}
            copy_args = {'CopySource': copy_source, 'Bucket': dest_bucket, 'Key': destination_key}
            
            if extra_args:
                copy_args.update(extra_args)
                
            self.s3_client.copy_object(**copy_args)
            return True
        except ClientError as e:
            self.logger.error(f"Error copying from {source_bucket}/{source_key} to {dest_bucket}/{destination_key}: {str(e)}")
            return False

    def move_object(self, source_bucket: str, source_key: str, destination_key: str,
                   destination_bucket: Optional[str] = None, extra_args: Optional[dict] = None) -> bool:
        """Moves an object between S3 locations (copy + delete)"""
        try:
            if not self.copy_object(source_bucket, source_key, destination_key, destination_bucket, extra_args):
                return False
                
            self.s3_client.delete_object(Bucket=source_bucket, Key=source_key)
            return True
        except ClientError as e:
            self.logger.error(f"Error moving from {source_bucket}/{source_key} to {destination_bucket or self.bucket_name}/{destination_key}: {str(e)}")
            return False