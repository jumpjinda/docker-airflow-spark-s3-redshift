import logging
import boto3
from botocore.exceptions import ClientError
import configparser


def upload_to_s3():
    file = 'data/clean_files/DataEngineer.csv'
    bucket = 'tanawat-s3'
    object_name = 'clean-data/DataEngineer.csv'

    logging.info("Reading AWS Credentials")
    config = configparser.ConfigParser()
    config.read_file(open('/opt/airflow/credentials/aws.cfg'))
    
    AWS_ACCESS_KEY_ID = config.get('AWS', 'AWS_ACCESS_KEY_ID')
    AWS_SECRET_ACCESS_KEY = config.get('AWS', 'AWS_SECRET_ACCESS_KEY')

    s3_client = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
        )
    try:
        response = s3_client.upload_file(file, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True