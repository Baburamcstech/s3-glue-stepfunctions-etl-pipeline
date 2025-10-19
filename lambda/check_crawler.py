import os
import boto3
import logging
from botocore.exceptions import BotoCoreError, ClientError

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    """
    Checks the status of the AWS Glue Crawler.
    Expects 'crawler_name' and 'cnt' keys in the event object.
    """
    logger.info("Received event: %s", event)
    result = {}

    # Input validation
    if 'crawler_name' not in event or 'cnt' not in event:
        logger.error("Missing required keys in event: %s", event)
        result['Status'] = "ERROR"
        result['Validation'] = "FAILURE"
        result['error'] = "Missing required keys: 'crawler_name' and/or 'cnt'"
        return result

    Crawler_Name = event['crawler_name']
    try:
        cnt = int(event['cnt']) + 1
    except (ValueError, TypeError):
        logger.error("Invalid 'cnt' value: %s", event.get('cnt'))
        result['Status'] = "ERROR"
        result['Validation'] = "FAILURE"
        result['error'] = "Invalid 'cnt' value"
        return result

    logger.info("Crawler name is set to: %s", Crawler_Name)
    try:
        client = boto3.client('glue')
        logger.info("Checking crawler status")
        response = client.get_crawler(Name=Crawler_Name)
        state = response['Crawler']['State']
        last_state = "INITIAL"
        if 'LastCrawl' in response['Crawler']:
            if 'Status' in response['Crawler']['LastCrawl']:
                last_state = response['Crawler']['LastCrawl']['Status']
        logger.info("Crawler state: %s, Last crawl status: %s", state, last_state)
    except (BotoCoreError, ClientError) as e:
        logger.error("Error fetching crawler status: %s", str(e))
        result['Status'] = "ERROR"
        result['Validation'] = "FAILURE"
        result['error'] = str(e)
        return result

    result['Status'] = state
    result['Validation'] = "RUNNING"
    if state == "READY":
        result['Validation'] = "SUCCESS"
        if last_state == "FAILED":
            result['Status'] = "FAILED"
            result['error'] = "Crawler Failed"
            result['Validation'] = "FAILURE"

    # Environment variable validation
    try:
        Retry_Count = int(os.environ['RETRYLIMIT'])
    except (KeyError, ValueError):
        logger.error("Environment variable RETRYLIMIT not set or invalid")
        result['Status'] = "ERROR"
        result['Validation'] = "FAILURE"
        result['error'] = "Environment variable RETRYLIMIT not set or invalid"
        return result

    if cnt > Retry_Count:
        result['Status'] = "RETRYLIMITREACH"
        result['error'] = "Retry limit reached"
        result['Validation'] = "FAILURE"

    result['crawler_name'] = Crawler_Name
    result['running_time'] = response['Crawler'].get('CrawlElapsedTime')
    result['cnt'] = cnt
    result['last_crawl_status'] = last_state
    result['Location'] = "stage"
    return result