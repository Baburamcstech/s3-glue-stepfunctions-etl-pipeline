import json
import boto3
import logging
from botocore.exceptions import BotoCoreError, ClientError

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    '''
    This function starts AWS Glue Crawler.
    Expects 'Crawler_Name' key in the event object.
    '''
    logger.info("Received event: %s", event)
    result = {}

    # Input validation
    if 'Crawler_Name' not in event:
        logger.error("Missing required key: 'Crawler_Name'")
        result['Status'] = "ERROR"
        result['error'] = "Missing required key: 'Crawler_Name'"
        return result

    Crawler_Name = event['Crawler_Name']
    client = boto3.client('glue')
    try:
        logger.info("Starting crawler: %s", Crawler_Name)
        response = client.start_crawler(Name=Crawler_Name)
        logger.info("Crawler start response: %s", response)
        result['crawler_name'] = Crawler_Name
        result['Status'] = "STARTED"
    except (BotoCoreError, ClientError) as e:
        logger.error("Failed to start crawler: %s", str(e))
        result['Status'] = "ERROR"
        result['error'] = str(e)
    return result