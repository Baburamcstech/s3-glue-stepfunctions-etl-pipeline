import json
import pandas as pd
from cerberus import Validator
from datetime import datetime
import boto3
import os
import logging

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)
            
def lambda_handler(event, context):
    ''' 
    This function validates input raw dataset against schema specified in the env variable
    '''
    logger.info("Received event: %s", event)
    result = {}
    s3_resource = boto3.resource('s3')

    try:
        bucket_name = event['bucket_name']
        key_name = event['key_name']
        source_file_name = event['file_name']
    except KeyError as e:
        logger.error("Missing required key in event: %s", str(e))
        result['Validation'] = "FAILURE"
        result['Reason'] = f"Missing required key: {str(e)}"
        result['Location'] = os.environ.get('source_folder_name', 'source')
        return result

    try:
        schema = json.loads(os.environ['schema'])
    except Exception as e:
        logger.error("Failed to load schema from environment: %s", str(e))
        result['Validation'] = "FAILURE"
        result['Reason'] = "Schema not found or invalid"
        result['Location'] = os.environ.get('source_folder_name', 'source')
        return result

    for keys in schema:
        if 'format' in schema[keys]:
            date_format_provided = schema[keys]['format']
            to_date = lambda s: datetime.strptime(s, date_format_provided)
            schema[keys].pop("format")
            schema[keys]['coerce'] = to_date

    v = Validator(schema)
    v.allow_unknown = False
    v.require_all = True
    source_file_path = f"s3://{bucket_name}/{key_name}"
    try:
        df = pd.read_csv(source_file_path)
        logger.info("Successfully read: %s", source_file_path)
    except Exception as e:
        logger.error("Error while reading csv: %s", str(e))
        result['Validation'] = "FAILURE"
        result['Reason'] = "Error while reading csv"
        result['Location'] = os.environ.get('source_folder_name', 'source')
        return result

    result['Validation'] = "SUCCESS"
    result['Location'] = os.environ.get('source_folder_name', 'source')
    df_dict = df.to_dict(orient='records')
    transformed_file_name = f"s3://{bucket_name}/{os.environ.get('stage_folder_name', 'stage')}/{source_file_name}"

    if len(df_dict) == 0:
        logger.error("No record found in file")
        result['Validation'] = "FAILURE"
        result['Reason'] = "NO RECORD FOUND"
        result['Location'] = os.environ.get('source_folder_name', 'source')
        return result

    for idx, record in enumerate(df_dict):
        if not v.validate(record):
            logger.error("Validation failed for record %d: %s", idx, v.errors)
            result['Validation'] = "FAILURE"
            result['Reason'] = f"{v.errors} in record number {idx}"
            result['Location'] = os.environ.get('source_folder_name', 'source')
            return result

    try:
        df['Month'] = df['Date'].astype(str).str[0:2]
        df['Day'] = df['Date'].astype(str).str[3:5]
        df['Year'] = df['Date'].astype(str).str[6:10]
        df.to_csv(transformed_file_name, index=False)
        s3_resource.Object(bucket_name, key_name).delete()
        logger.info("Successfully moved file to: %s", transformed_file_name)
    except Exception as e:
        logger.error("Error during file transformation or move: %s", str(e))
        result['Validation'] = "FAILURE"
        result['Reason'] = "Error during file transformation or move"
        result['Location'] = os.environ.get('source_folder_name', 'source')
        return result

    return result