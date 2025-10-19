import json
import boto3
import botocore
import os
import logging

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):  
    """
    Moves the source dataset to archive/transform/error folder based on status.
    Expects 'bucket_name', 'file_name', and either 'error-info' or 'taskresult' in the event.
    """
    logger.info("Received event: %s", event)
    s3_resource = boto3.resource('s3')
    result = {}

    # Input validation
    if 'bucket_name' not in event or 'file_name' not in event:
        logger.error("Missing required keys in event: %s", event)
        result['Status'] = "ERROR"
        result['msg'] = "Missing required keys: 'bucket_name' and/or 'file_name'"
        return result

    bucket_name = event['bucket_name']

    if "error-info" in event:
        source_location = "stage"
        status = "FAILURE"
    else:
        if 'taskresult' not in event or 'Location' not in event['taskresult'] or 'Validation' not in event['taskresult']:
            logger.error("Missing 'taskresult' or required keys in event: %s", event)
            result['Status'] = "ERROR"
            result['msg'] = "Missing 'taskresult' or required keys: 'Location' and/or 'Validation'"
            return result
        source_location = event['taskresult']['Location']
        status = event['taskresult']['Validation']

    file_name = event['file_name']
    key_name = f"{source_location}/{file_name}"

    # Determine destination folder based on status
    try:
        if status == "FAILURE":
            logger.info("Status is FAILURE. Moving to error folder.")
            folder = os.environ['error_folder_name']
        elif status == "SUCCESS":
            logger.info("Status is SUCCESS. Moving to archive folder.")
            folder = os.environ['archive_folder_name']
        else:
            logger.error("Unknown status: %s", status)
            result['Status'] = "ERROR"
            result['msg'] = f"Unknown status: {status}"
            return result
    except KeyError as e:
        logger.error("Missing environment variable: %s", str(e))
        result['Status'] = "ERROR"
        result['msg'] = f"Missing environment variable: {str(e)}"
        return result

    source_file_name_to_copy = f"{bucket_name}/{source_location}/{file_name}"
    move_file_name = f"{folder}/{file_name}"
    logger.info("Moving file to %s", move_file_name)

    try:
        s3_resource.Object(bucket_name, move_file_name).copy_from(CopySource=source_file_name_to_copy)
        s3_resource.Object(bucket_name, key_name).delete()
    except botocore.exceptions.BotoCoreError as e:
        logger.error("Error moving file in S3: %s", str(e))
        result['Status'] = "ERROR"
        result['msg'] = f"Error moving file in S3: {str(e)}"
        return result

    result['Status'] = status
    result['msg'] = f"File moved to {move_file_name}"
    return result