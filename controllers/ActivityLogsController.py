from flask import jsonify, g, request

import config
# local imports
from controllers.Middleware import validate_token_middleware
from models.ActivityLogs import ActivityLogs
from models.Merchants import Merchants
from utilities.errors import unauthorised, unhandled, invalid
from utilities.helpers import success, validateAdminUser, create_log_data, publish_sns_message, validateLoginToken

import boto3
from botocore.exceptions import ClientError
import json

from config import *

################################################# GET

@validate_token_middleware
def getActivityLogs():
  try:
    userId = g.userId
    if not validateAdminUser(userId):
      return unauthorised("user is not authorized")

    logs = ActivityLogs.get_activity_logs(request)

    return success(jsonify({
      "status": 200,
      "message": "success",
      "data": logs
    }))
  except Exception as e:
    print("Error: ", str(e))
    return unhandled()




def getSecretPayloadForAndroidApp():
    try:
        secret_name = ANDROID_SECRET_NAME
        # AWS_REGION = aws_region
        # AWS_ACCESS_KEY = ANDROID_SECRET_AWS_ACCESS_KEY
        # AWS_SECRET_KEY = ANDROID_SECRET_AWS_SECRET_KEY
        # Create a Secrets Manager client
        session = boto3.session.Session()
        # client = boto3.client(service_name = 'secretsmanager',
        #                     region_name=AWS_REGION,
        #                     aws_access_key_id = AWS_ACCESS_KEY, 
        #                     aws_secret_access_key = AWS_SECRET_KEY)
        client = boto3.client('secretsmanager')
        
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
        secretString = get_secret_value_response['SecretString']
        secretPayload = json.loads(secretString)
        return success(jsonify({
            "message": "success",
            "status": 200,
            "data" : secretPayload
        }))
    except Exception as e:
        return unhandled(f"error: {e}")

#################################################

