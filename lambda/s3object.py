import boto3
import cfnresponse
import logging

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def handler(event, context):
    """
    This function creates required directory structure inside S3 bucket.
    Handles Create, Update, and Delete events from CloudFormation.
    """
    the_event = event.get('RequestType')
    logger.info("The event is: %s", str(the_event))
    response_data = {}
    s_3 = boto3.client('s3')
    s3_resource = boto3.resource('s3')

    # Retrieve parameters with validation
    try:
        the_bucket = event['ResourceProperties']['the_bucket']
        dirs_to_create = event['ResourceProperties']['dirs_to_create']
        file_content = event['ResourceProperties']['file_content']
        file_prefix = event['ResourceProperties']['file_prefix']
    except KeyError as e:
        logger.error("Missing required ResourceProperties key: %s", str(e))
        response_data['Data'] = f"Missing required ResourceProperties key: {str(e)}"
        cfnresponse.send(event, context, cfnresponse.FAILED, response_data)
        return

    logger.info("file_content is: %s", file_content)
    logger.info("file_prefix is: %s", file_prefix)

    try:
        if the_event in ('Create', 'Update'):
            logger.info("Requested folders: %s", str(dirs_to_create))
            for dir_name in dirs_to_create.split(","):
                logger.info("Creating: %s", str(dir_name))
                s_3.put_object(Bucket=the_bucket, Key=(dir_name + '/'))
            s3_resource.Object(the_bucket, file_prefix).put(Body=file_content)
            logger.info("File created")
        elif the_event == 'Delete':
            logger.info("Deleting S3 content...")
            bucket = s3_resource.Bucket(the_bucket)
            bucket.objects.all().delete()
            logger.info("All objects deleted")
            bucket.object_versions.delete()
            logger.info("All object versions deleted")
            s_3.delete_bucket(Bucket=str(the_bucket))
            logger.info("Bucket deleted")
        # Everything OK... send the signal back
        logger.info("Execution successful!")
        cfnresponse.send(event, context, cfnresponse.SUCCESS, response_data)
    except Exception as e:
        logger.error("Execution failed: %s", str(e))
        response_data['Data'] = str(e)
        cfnresponse.send(event, context, cfnresponse.FAILED, response_data)