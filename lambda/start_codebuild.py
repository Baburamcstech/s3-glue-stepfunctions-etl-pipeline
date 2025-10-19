import json
import boto3
import time
import cfnresponse
import os
import logging

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    '''
    This function starts AWS codebuild project
    '''
    logger.info("Lambda execution started")
    response_data = {}
    the_event = event.get('RequestType')
    if the_event in ('Create', 'Update'):
        try:
            logger.info("The event is: %s", str(the_event))
            counter = 0
            client = boto3.client(service_name='codebuild')
            buildSucceeded = False
            Update_lambda_layer = event['ResourceProperties'].get('Update_lambda_layer')

            if Update_lambda_layer == "yes":
                try:
                    code_build_project_name = os.environ['PROJECT_NAME']
                except KeyError:
                    logger.error("Environment variable PROJECT_NAME not set")
                    response_data['Data'] = "Environment variable PROJECT_NAME not set"
                    cfnresponse.send(event, context, cfnresponse.FAILED, response_data)
                    return

                new_build = client.start_build(projectName=code_build_project_name)
                buildId = new_build['build']['id']
                while counter < 50:
                    time.sleep(5)
                    logger.info("Waiting for build to complete... Attempt %d", counter + 1)
                    counter += 1
                    theBuild = client.batch_get_builds(ids=[buildId])
                    buildStatus = theBuild['builds'][0]['buildStatus']

                    if buildStatus == 'SUCCEEDED':
                        buildSucceeded = True
                        logger.info("Build Succeeded")
                        time.sleep(15)
                        cfnresponse.send(event, context, cfnresponse.SUCCESS, response_data)
                        break
                    elif buildStatus in ('FAILED', 'FAULT', 'STOPPED', 'TIMED_OUT'):
                        logger.error("Build failed with status: %s", buildStatus)
                        response_data['Data'] = buildStatus
                        cfnresponse.send(event, context, cfnresponse.FAILED, response_data)
                        break
            else:
                response_data['Data'] = "No update needed"
                logger.info("No update needed")
                cfnresponse.send(event, context, cfnresponse.SUCCESS, response_data)

        except Exception as e:
            logger.error("Execution failed: %s", str(e))
            response_data['Data'] = str(e)
            cfnresponse.send(event, context, cfnresponse.FAILED, response_data)
    else:
        logger.info("Event type is not Create or Update, sending SUCCESS")
        cfnresponse.send(event, context, cfnresponse.SUCCESS, response_data)