# S3 to SFTP File Transfer Lambda Function

This script facilitates secure file transfer from AWS S3 to an external SFTP server using the Paramiko library. The script is designed to run as an AWS Lambda function and handles authentication, connection management, and error handling.

## Prerequisites

- AWS Lambda execution role with permissions to access S3
- The following environment variables must be set in Lambda:
  - SFTP_HOST: External SFTP server hostname
  - SFTP_PORT: SFTP server port (usually 22)
  - SFTP_USERNAME: SFTP username
  - SFTP_PASSWORD: SFTP password
  - SFTP_REMOTE_PATH: Remote directory path for file upload
  - SSH_KEY_SECRET_NAME (optional): AWS Secrets Manager secret name containing SSH key

## Implementation

```python
import os
import boto3
import paramiko
import io
from botocore.exceptions import ClientError

def get_ssh_key_from_secrets():
    """
    Retrieve SSH private key from AWS Secrets Manager if configured.
    Returns None if using password authentication.
    """
    if 'SSH_KEY_SECRET_NAME' not in os.environ:
        return None
        
    secrets_client = boto3.client('secrets-manager')
    try:
        secret = secrets_client.get_secret_value(
            SecretId=os.environ['SSH_KEY_SECRET_NAME']
        )
        return paramiko.RSAKey.from_private_key(
            io.StringIO(secret['SecretString'])
        )
    except ClientError as e:
        print(f"Error retrieving SSH key: {str(e)}")
        raise

def create_sftp_client():
    """
    Create and configure SFTP client with proper authentication.
    Returns a connected SFTP client object.
    """
    # Initialize SSH client
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    
    # Get authentication credentials
    ssh_key = get_ssh_key_from_secrets()
    
    try:
        if ssh_key:
            # Connect using SSH key
            ssh.connect(
                hostname=os.environ['SFTP_HOST'],
                port=int(os.environ['SFTP_PORT']),
                username=os.environ['SFTP_USERNAME'],
                pkey=ssh_key
            )
        else:
            # Connect using password
            ssh.connect(
                hostname=os.environ['SFTP_HOST'],
                port=int(os.environ['SFTP_PORT']),
                username=os.environ['SFTP_USERNAME'],
                password=os.environ['SFTP_PASSWORD']
            )
        
        return ssh.open_sftp()
        
    except Exception as e:
        print(f"Error connecting to SFTP server: {str(e)}")
        raise

def transfer_file(s3_bucket, s3_key, sftp_client):
    """
    Transfer a single file from S3 to SFTP server.
    
    Args:
        s3_bucket (str): Source S3 bucket name
        s3_key (str): Source S3 object key
        sftp_client (paramiko.SFTPClient): Connected SFTP client
    """
    s3_client = boto3.client('s3')
    
    try:
        # Get file from S3
        response = s3_client.get_object(Bucket=s3_bucket, Key=s3_key)
        file_content = response['Body'].read()
        
        # Construct remote path
        remote_path = os.path.join(
            os.environ['SFTP_REMOTE_PATH'],
            os.path.basename(s3_key)
        )
        
        # Upload to SFTP server
        with sftp_client.file(remote_path, 'wb') as f:
            f.write(file_content)
            
        print(f"Successfully transferred {s3_key} to {remote_path}")
        
    except Exception as e:
        print(f"Error transferring file {s3_key}: {str(e)}")
        raise

def lambda_handler(event, context):
    """
    AWS Lambda handler function.
    
    Expected event structure:
    {
        "s3_bucket": "source-bucket",
        "s3_keys": ["file1.txt", "file2.txt"]
    }
    """
    try:
        # Validate input
        if 's3_bucket' not in event or 's3_keys' not in event:
            raise ValueError("Missing required parameters: s3_bucket and s3_keys")
            
        # Create SFTP client
        sftp_client = create_sftp_client()
        
        # Transfer each file
        for s3_key in event['s3_keys']:
            transfer_file(event['s3_bucket'], s3_key, sftp_client)
            
        return {
            'statusCode': 200,
            'body': f"Successfully transferred {len(event['s3_keys'])} files"
        }
        
    except Exception as e:
        print(f"Error in lambda execution: {str(e)}")
        raise
    
    finally:
        # Clean up SFTP connection
        if 'sftp_client' in locals():
            sftp_client.close()
```

## Required Lambda Layer Dependencies

Create a Lambda layer with the following Python packages:
- paramiko
- cryptography (required by paramiko)

## Usage Example

Deploy the Lambda function with the required environment variables and invoke it with an event like:

```json
{
    "s3_bucket": "my-source-bucket",
    "s3_keys": [
        "path/to/file1.txt",
        "path/to/file2.csv"
    ]
}
```

## Error Handling

The script includes comprehensive error handling for:
- S3 access issues
- SFTP connection failures
- Authentication problems
- File transfer errors
- Invalid input parameters

All errors are logged to CloudWatch Logs for monitoring and debugging.

## Security Considerations

1. Store sensitive credentials (password, SSH key) in AWS Secrets Manager
2. Use VPC endpoints or NAT Gateway for secure outbound connections
3. Implement proper IAM roles with least privilege access
4. Consider using SSH keys instead of passwords for enhanced security
5. Regularly rotate credentials and monitor access logs

## Monitoring

Configure CloudWatch Alarms for:
- Lambda function errors
- Duration thresholds
- Memory usage
- Concurrent executions

Monitor SFTP server logs for successful file transfers and any access issues.