from flask import jsonify, request, g
import boto3
import json
from controllers.Middleware import validate_token_middleware

# local imports
from utilities.helpers import validateLoginToken, validateMerchantUser, success, publish_sns_message, get_ip_address
from utilities.errors import invalid, not_found, unhandled, unauthorised
import config

# config
manual_sync_queue_name = config.manual_sync_queue_name


def menuManualSync(merchantId, platformId):
  # try:
    _json = request.json  
    token = _json.get('token')
    downloadMenu = _json.get('downloadMenu')

    # validate the received values
    if token and request.method == 'POST': 
      userId = validateLoginToken(token)
      if not userId:
        return invalid("Invalid Token")

      # if (not validateMerchantUser(merchantId, userId)):
      #   return unauthorised("User Not authorised to access merchant information")

      '''
      TRIGGER SQS ManualSync...
      '''
      print("Triggering Manual Sync SQS...")
      sqs = boto3.resource('sqs')
      queue = sqs.get_queue_by_name(QueueName=manual_sync_queue_name)
      platformObj = {
          "merchantId": merchantId,
          "platformId":platformId,
          "userId": userId,
          "downloadMenu": downloadMenu
      }
      response = queue.send_message(MessageBody=json.dumps(platformObj))
      print("Message successfully sent to SQS")

      # Triggering activity_logs SNS
      print("Triggering sns - platform.manual_sync ...")
      sns_msg = {
        "event": "platform.manual_sync",
        "body": {
          "merchantId": merchantId,
          "userId": userId,
          "platformId": platformId,
          "downloadMenu": downloadMenu
        }
      }
      logs_sns_resp = publish_sns_message(topic=config.sns_activity_logs, message=str(sns_msg), subject="platform.manual_sync")

      return success()
    else:
      return not_found(params=["token"])
  # except Exception as e:
  #   print("Error: ", str(e))
  #   return unhandled()




########################### TESTING
########################### TESTING
########################### TESTING
from models.Sync import Sync

@validate_token_middleware
def testMenuManualSync(merchantId, platformId):
  try:
    userId = g.userId
    _json = request.json
    ip_address = get_ip_address(request)
    if _json.get("downloadMenu") and int(_json.get("downloadMenu")) == 1:
      return Sync.trigger_downloadMenu(merchantId, platformId, userId)
    else:
      return Sync.trigger_manualSync(merchantId, platformId, userId , request=request ,ip_address=ip_address )

  except Exception as e:
    return unhandled(str(e))