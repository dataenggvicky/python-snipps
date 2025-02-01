import boto3
import os
import zipfile
import tarfile
import gzip
import io
from botocore.exceptions import ClientError
from typing import Union, List, Optional

class S3CompressionUtils:
    def __init__(self, aws_access_key: Optional[str] = None, aws_secret_key: Optional[str] = None, region: str = 'us-east-1'):
        self.s3_client = boto3.client('s3', aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key, region_name=region)

    def compress_file(self, bucket_name: str, s3_key: str, compression_type: str = 'zip', output_key: Optional[str] = None) -> str:
        try:
            response = self.s3_client.get_object(Bucket=bucket_name, Key=s3_key)
            file_content = response['Body'].read()
            
            if compression_type == 'zip':
                compressed_data = io.BytesIO()
                with zipfile.ZipFile(compressed_data, 'w', zipfile.ZIP_DEFLATED) as zip_file:
                    zip_file.writestr(os.path.basename(s3_key), file_content)
                extension = '.zip'
                
            elif compression_type == 'gzip':
                compressed_data = io.BytesIO()
                with gzip.GzipFile(fileobj=compressed_data, mode='wb') as gz_file:
                    gz_file.write(file_content)
                extension = '.gz'
                
            elif compression_type == 'tar':
                compressed_data = io.BytesIO()
                with tarfile.open(fileobj=compressed_data, mode='w:gz') as tar_file:
                    file_obj = io.BytesIO(file_content)
                    tar_info = tarfile.TarInfo(name=os.path.basename(s3_key))
                    tar_info.size = len(file_content)
                    tar_file.addfile(tar_info, file_obj)
                extension = '.tar.gz'
            else:
                raise ValueError(f"Unsupported compression type: {compression_type}")
            
            compressed_data.seek(0)
            output_key = output_key or f"{s3_key}{extension}"
            self.s3_client.upload_fileobj(compressed_data, bucket_name, output_key)
            return output_key
            
        except ClientError as e:
            raise Exception(f"Error accessing S3: {str(e)}")
        except Exception as e:
            raise Exception(f"Compression failed: {str(e)}")

    def decompress_file(self, bucket_name: str, s3_key: str, output_key: Optional[str] = None) -> str:
        try:
            response = self.s3_client.get_object(Bucket=bucket_name, Key=s3_key)
            compressed_data = response['Body'].read()
            
            if s3_key.endswith('.zip'):
                decompressed_data = io.BytesIO()
                with zipfile.ZipFile(io.BytesIO(compressed_data)) as zip_file:
                    file_name = zip_file.namelist()[0]
                    decompressed_data.write(zip_file.read(file_name))
                    
            elif s3_key.endswith('.gz'):
                decompressed_data = io.BytesIO()
                with gzip.GzipFile(fileobj=io.BytesIO(compressed_data), mode='rb') as gz_file:
                    decompressed_data.write(gz_file.read())
                    
            elif s3_key.endswith('.tar.gz'):
                decompressed_data = io.BytesIO()
                with tarfile.open(fileobj=io.BytesIO(compressed_data), mode='r:gz') as tar_file:
                    file_name = tar_file.getnames()[0]
                    decompressed_data.write(tar_file.extractfile(file_name).read())
            else:
                raise ValueError(f"Unsupported file format: {s3_key}")
            
            decompressed_data.seek(0)
            output_key = output_key or s3_key.rsplit('.', 1)[0]
            self.s3_client.upload_fileobj(decompressed_data, bucket_name, output_key)
            return output_key
            
        except ClientError as e:
            raise Exception(f"Error accessing S3: {str(e)}")
        except Exception as e:
            raise Exception(f"Decompression failed: {str(e)}")

    def compress_multiple_files(self, bucket_name: str, s3_keys: List[str], output_key: str, compression_type: str = 'zip') -> str:
        try:
            compressed_data = io.BytesIO()
            
            if compression_type == 'zip':
                with zipfile.ZipFile(compressed_data, 'w', zipfile.ZIP_DEFLATED) as zip_file:
                    for s3_key in s3_keys:
                        response = self.s3_client.get_object(Bucket=bucket_name, Key=s3_key)
                        zip_file.writestr(os.path.basename(s3_key), response['Body'].read())
                        
            elif compression_type == 'tar':
                with tarfile.open(fileobj=compressed_data, mode='w:gz') as tar_file:
                    for s3_key in s3_keys:
                        response = self.s3_client.get_object(Bucket=bucket_name, Key=s3_key)
                        file_content = response['Body'].read()
                        file_obj = io.BytesIO(file_content)
                        tar_info = tarfile.TarInfo(name=os.path.basename(s3_key))
                        tar_info.size = len(file_content)
                        tar_file.addfile(tar_info, file_obj)
            else:
                raise ValueError(f"Unsupported compression type for multiple files: {compression_type}")
            
            compressed_data.seek(0)
            self.s3_client.upload_fileobj(compressed_data, bucket_name, output_key)
            return output_key
            
        except ClientError as e:
            raise Exception(f"Error accessing S3: {str(e)}")
        except Exception as e:
            raise Exception(f"Multiple file compression failed: {str(e)}")


s3_utils = S3CompressionUtils(aws_access_key='your_key', aws_secret_key='your_secret')

# Compress a single file
compressed_key = s3_utils.compress_file('my-bucket', 'path/to/file.txt', compression_type='zip')

# Decompress a file
original_key = s3_utils.decompress_file('my-bucket', 'path/to/file.txt.zip')

# Compress multiple files
files_to_compress = ['file1.txt', 'file2.txt', 'file3.txt']
archive_key = s3_utils.compress_multiple_files('my-bucket', files_to_compress, 'output/archive.zip')