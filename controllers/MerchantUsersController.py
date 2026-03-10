from flask import jsonify, request
import json

# local imports
from models.MerchantUsers import MerchantUsers
from utilities.helpers import validateLoginToken, validateMerchantUser, success, publish_sns_message,create_log_data,get_ip_address
from utilities.errors import invalid, not_found, unhandled, unauthorised
import config


def getMerchantUsers(merchantId):
  try:
    token = request.args.get('token')
    ip_address = get_ip_address(request)
    create_log_data(level='[INFO]', Message=f"In the start of getMerchantUsers,IP address: {ip_address}, Token:{token}",
                    functionName="getMerchantUsers", request=request)
    
    if token and request.method == 'GET':
      userId = validateLoginToken(token)
      if not userId:
        return invalid("Invalid Token")
      
      if not validateMerchantUser(merchantId, userId):
        return unauthorised("User Not authorised to access merchant information")
      
      resp = MerchantUsers.get_merchantusers(merchantId=merchantId)
      create_log_data(level='[INFO]', Message=f"Successfully getMerchantUsers,IP address: {ip_address}, Token:{token}",
                    functionName="getMerchantUsers", request=request)
      return success(jsonify(resp))
    else:
      return not_found(params=["token"])
  except Exception as e:
    create_log_data(level='[INFO]', Message=f"Error : {e},IP address: {ip_address}, Token:{token}",
                    functionName="getMerchantUsers", request=request)
    print("Error: ", str(e))
    return unhandled("Unhandled Exception")


def createMerchantUser(merchantId):
  try:
    
    
    
    ip_address = None
    if request:
        ip_address = request.environ.get(
            'HTTP_X_FORWARDED_FOR', request.remote_addr)
    if ip_address:
        ip_address = ip_address.split(',')[0].strip()
    _json = request.json
    token = _json.get("token")
    userId = _json.get("userid")
    
    create_log_data(level='[INFO]', Message=f"In the start of createMerchantUser,IP address: {ip_address}, Token:{token}",
                    functionName="createMerchantUser", request=request)
    
    if token and userId and request.method == 'POST':
      currentUser = validateLoginToken(token)
      if not currentUser:
        return invalid("Invalid Token")
      
      if not validateMerchantUser(merchantId, currentUser):
        return unauthorised("User Not authorised to access merchant information")
      
      create_log_data(level='[INFO]', Message=f"Successfully createMerchantUser,IP address: {ip_address}, Token:{token}",
                    functionName="createMerchantUser", request=request)
      return MerchantUsers.post_merchantusers(userId=userId, merchantId=merchantId, ip_address=ip_address, currentUser=currentUser)
      
    else:
      fields = {
        "token": "required",
        "userid": "required"
      }
      return not_found(body=fields)
  except Exception as e:
    create_log_data(level='[INFO]', Message=f"Error occured {e},IP address: {ip_address}, Token:{token}",
                    functionName="createMerchantUser", request=request)
    print("Error: ", str(e))
    return unhandled("Unhandled Exception")


def removeMerchantUser(merchantId, userId):
  try:
    
    ip_address = None
    if request:
        ip_address = request.environ.get(
            'HTTP_X_FORWARDED_FOR', request.remote_addr)
    if ip_address:
        ip_address = ip_address.split(',')[0].strip()
    token = request.args.get('token')
    create_log_data(level='[INFO]', Message=f"In the start of removeMerchantUser,IP address: {ip_address}, Token:{token}",
                    functionName="removeMerchantUser", request=request)
    

    if token and request.method == 'DELETE':
      currentUser = validateLoginToken(token)
      if not currentUser:
        return invalid("Invalid Token")
      
      if not validateMerchantUser(merchantId, currentUser):
        return unauthorised("User Not authorised to access merchant information")
      
      resp = MerchantUsers.delete_merchantusers(userId=userId, merchantId=merchantId)
      create_log_data(level='[INFO]', Message=f"Successfully removeMerchantUser,IP address: {ip_address}, Token:{token}",
                    functionName="removeMerchantUser", request=request)
      # Triggering SNS - merchant.unassign_user
      print("Triggering sns - merchant.unassign_user ...")
      sns_msg = {
        "event": "merchant.unassign_user",
        "body": {
          "merchantId": merchantId,
          "userId": currentUser,
          "updatedUserId": userId,
          "ipAddr": ip_address
        }
      }
      logs_sns_resp = publish_sns_message(topic=config.sns_activity_logs, message=str(sns_msg), subject="merchant.unassign_user")

      return success()
    else:
      return not_found(params=["token"])
  except Exception as e:
    create_log_data(level='[INFO]', Message=f"Error at removeMerchantUser, Error: {e},IP address: {ip_address}, Token:{token}",
                    functionName="removeMerchantUser", request=request)
    print("Error: ", str(e))
    return unhandled("Unhandled Exception")
