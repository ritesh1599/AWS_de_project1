import boto3
from botocore.exceptions import ClientError
from .exceptions import S3OperationError

class S3Helper:
    def __init__(self):
        self.s3 = boto3.client('s3')

    def read_file(self, bucket_name, file_key):
        try:
            response = self.s3.get_object(Bucket=bucket_name, Key=file_key)
            return response['Body'].read().decode('utf-8')
        except ClientError as e:
            raise S3OperationError(f"Failed to read file from S3: {e}")

    def write_file(self, bucket_name, file_key, content):
        try:
            self.s3.put_object(Bucket=bucket_name, Key=file_key, Body=content)
        except ClientError as e:
            raise S3OperationError(f"Failed to write file to S3: {e}")




