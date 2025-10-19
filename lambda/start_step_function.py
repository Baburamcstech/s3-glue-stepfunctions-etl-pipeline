import json
import boto3
import os
import logging
from botocore.exceptions import BotoCoreError, ClientError

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    '''
    This function starts AWS Step Functions execution.
    '''
    try:
        logger.info("Received event: %s", event)
        bucket_name = event["Records"][0]['s3']['bucket']['name'] 
        bucket_arn = event["Records"][0]['s3']['bucket']['arn']
        key_name = event["Records"][0]['s3']['object']['key']
        file_name = key_name.split('/')[-1]
        step_function_input = {
            'bucket_name': bucket_name,
            'bucket_arn': bucket_arn,
            'key_name': key_name,
            'file_name': file_name
        }
        logger.info("Step function input: %s", step_function_input)
        client = boto3.client('stepfunctions')
        try:
            step_func_arn = os.environ['STEP_FUNC_ARN']
        except KeyError:
            logger.error("Environment variable STEP_FUNC_ARN not set")
            return {"Status": "ERROR", "error": "Environment variable STEP_FUNC_ARN not set"}
        response = client.start_execution(
            stateMachineArn=step_func_arn,
            input=json.dumps(step_function_input)
        )
        logger.info("Step function execution started: %s", response)
        return {"Status": "STARTED", "executionArn": response.get("executionArn")}
    except (BotoCoreError, ClientError, KeyError, IndexError, Exception) as e:
        logger.error("Failed to start step function: %s", str(e))
        return {"Status": "ERROR", "error": str(e)}