import boto3
import logging
import string
import random
import psycopg2
from botocore.exceptions import ClientError

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def generate_password(length=16):
    """
    Generate a random password containing letters, digits, and punctuation.
    """
    characters = string.ascii_letters + string.digits + string.punctuation
    return ''.join(random.choice(characters) for i in range(length))

def lambda_handler(event, context):
    """
    AWS Lambda function to rotate an Amazon Redshift user's secret.
    """
    # Extract the step from the event
    step = event.get('Step')
    secret_arn = event.get('SecretId')
    client_request_token = event.get('ClientRequestToken')

    # Initialize boto3 client for Secrets Manager
    client = boto3.client('secretsmanager')

    try:
        # Retrieve the secret details
        metadata = client.describe_secret(SecretId=secret_arn)

        # Check if the version is staged correctly
        if not metadata['RotationEnabled']:
            logger.error("Secret rotation is not enabled for this secret.")
            return

        versions = metadata['VersionIdsToStages']
        if client_request_token not in versions or 'AWSCURRENT' in versions[client_request_token]:
            logger.info("Secret version is already set as AWSCURRENT.")
            return

        # Handle the rotation steps
        if step == 'create_secret':
            create_secret(client, secret_arn, client_request_token)
        elif step == 'set_secret':
            set_secret(client, secret_arn, client_request_token)
        elif step == 'test_secret':
            test_secret(client, secret_arn, client_request_token)
        elif step == 'finish_secret':
            finish_secret(client, secret_arn, client_request_token)
        else:
            logger.error(f"Unknown step: {step}")
            raise ValueError(f"Unknown step: {step}")

    except ClientError as e:
        logger.error(f"An error occurred: {e}")
        raise e

def create_secret(client, secret_arn, token):
    """
    Create a new secret version with a new password.
    """
    # Retrieve the current secret
    current_secret = client.get_secret_value(SecretId=secret_arn, VersionStage='AWSCURRENT')
    secret_dict = eval(current_secret['SecretString'])

    # Generate a new password
    new_password = generate_password()
    secret_dict['password'] = new_password

    # Put the new secret version
    client.put_secret_value(
        SecretId=secret_arn,
        ClientRequestToken=token,
        SecretString=str(secret_dict),
        VersionStages=['AWSPENDING']
    )
    logger.info("create_secret: Successfully created new secret version.")

def set_secret(client, secret_arn, token):
    """
    Set the new password in the Amazon Redshift cluster.
    """
    # Retrieve the pending secret
    pending_secret = client.get_secret_value(SecretId=secret_arn, VersionId=token, VersionStage='AWSPENDING')
    secret_dict = eval(pending_secret['SecretString'])

    # Extract connection details
    dbname = secret_dict['dbname']
    host = secret_dict['host']
    port = secret_dict['port']
    username = secret_dict['username']
    new_password = secret_dict['password']

    # Connect to Redshift and update the user password
    try:
        conn = psycopg2.connect(
            dbname=dbname,
            user=username,
            password=new_password,
            host=host,
            port=port
        )
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute(f"ALTER USER {username} WITH PASSWORD '{new_password}';")
        conn.close()
        logger.info("set_secret: Successfully set new password in Redshift.")
    except Exception as e:
        logger.error(f"set_secret: Failed to set new password in Redshift: {e}")
        raise e

def test_secret(client, secret_arn, token):
    """
    Test the new password by connecting to the Amazon Redshift cluster.
    """
    # Retrieve the pending secret
    pending_secret = client.get_secret_value(SecretId=secret_arn, VersionId=token, VersionStage='AWSPENDING')
    secret_dict = eval(pending_secret['SecretString'])

    # Extract connection details
    dbname = secret_dict['dbname']
    host = secret_dict['host']
    port = secret_dict['port']
    username = secret_dict['username']
    new_password = secret_dict['password']

    # Test the new password
    try:
        conn = psycopg2.connect(
            dbname=dbname,
            user=username,
            password=new_password,
            host=host,
            port=port
        )
        conn.close()
        logger.info("test_secret: Successfully tested new password.")
    except Exception as e:
        logger.error(f"test_secret: Failed to authenticate with new password: {e}")
        raise e

def finish_secret(client, secret_arn, token):
    """
    Mark the new secret version as the current version.
    """
    # Retrieve metadata for the secret
    metadata = client.describe_secret(SecretId=secret_arn)

    # Determine the current version
    current_version = None
    for version, stages in metadata['VersionIdsToStages'].items():
        if 'AWSCURRENT' in stages:
            current_version = version
            break

    # Finalize the rotation
    client.update_secret_version_stage(
        SecretId=secret_arn,
        VersionStage='AWSCURRENT',
        MoveToVersionId=token,
        RemoveFromVersionId=current_version
    )
    logger.info("finish_secret: Successfully marked new secret version as AWSCURRENT.")
