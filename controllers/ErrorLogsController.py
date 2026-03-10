from flask import jsonify, g, request

# local imports
from controllers.Middleware import validate_token_middleware
from models.ErrorLogs import ErrorLogs
from utilities.errors import unauthorised, unhandled
from utilities.helpers import success, validateAdminUser, publish_sns_message
import config


################################################# GET

@validate_token_middleware
def getErrorLogs():
  try:
    userId = g.userId
    if not validateAdminUser(userId):
      return unauthorised("user is not authorized")

    logs = ErrorLogs.get_error_logs(request)

    return success(jsonify({
      "status": 200,
      "message": "success",
      "data": logs
    }))
  except Exception as e:
    print("Error: ", str(e))
    return unhandled()

################################################# POST

@validate_token_middleware
def createErrorLogs():
  try:
    userId = g.userId
    _json = request.json
    
    # Triggering SNS
    print("Triggering SNS ...")
    sns_msg = {
        "event": "error_logs.entry",
        "body": {
            "userId": userId,
            "merchantId": _json.get("merchantId"),
            "errorName": _json.get("errorName"),
            "errorSource": _json.get("errorSource"),
            "errorStatus": _json.get("errorStatus"),
            "errorDetails": _json.get("errorDetails"),
            "orderExternalReference": _json.get("orderExternalReference")
        }
    }
    error_logs_sns_resp = publish_sns_message(topic=config.sns_error_logs, message=str(sns_msg), subject="error_logs.entry")

    return success()
  except Exception as e:
    print("Error: ", str(e))
    return unhandled()

