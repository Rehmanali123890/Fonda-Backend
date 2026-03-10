from flask import jsonify, request, g

# local imports
import config
from utilities.helpers import validateLoginToken, validateMerchantUser, success, publish_sns_message, create_log_data
from utilities.errors import invalid, not_found, unhandled, unauthorised
from models.Addons import Addons

# config
sns_addon_notification = config.sns_addon_notification


################################################# GET

def getMerchantAddonByID(merchantId, addonId):
  try:
    token = request.args.get('token')
    if token and request.method == 'GET': 
      userId = validateLoginToken(token)
      if not userId:
        return invalid("Invalid Token")
      if (not validateMerchantUser(merchantId, userId)):
        return unauthorised("User Not authorised to access merchant information")

      addon = Addons.get_addon_by_id_with_options_str(addonId)
      return success(jsonify(addon))
    else:
      return not_found(params=["token"])
  except Exception as e:
    print("Error: ", str(e))
    return unhandled()


def getMerchantaddonListwithOptions(merchantId):
  try:
    token = request.args.get('token')

    limit = "2000"
    if (request.args.get('limit')):
      limit = request.args.get('limit')
    
    _from = "0"
    if (request.args.get('from')):
      _from = request.args.get('from')

    _addonName = None
    if (request.args.get('addonName')):
      _addonName = request.args.get('addonName') 
           
    if token and request.method == 'GET': 
      userId = validateLoginToken(token)
      if not userId:
        return invalid("Invalid Token")
      if (not validateMerchantUser(merchantId, userId)):
        return unauthorised("User Not authorised to access merchant information")

      addons = Addons.get_all_addons_with_options_str(merchantId=merchantId, limit=limit, _from=_from, addonName=_addonName)
      return success(jsonify(addons))
    else:
      return not_found(params=["token"])
  except Exception as e:
    print("Error: ", str(e))
    return unhandled()


def getMerchantaddonListwithoutOptions(merchantId):
  try:
    token = request.args.get('token')
    withitemcount= request.args.get('withitemcount') if "withitemcount" in request.args else 0
    limit = "2000"
    if (request.args.get('limit')):
      limit = request.args.get('limit')

    _from = "0"
    if (request.args.get('from')):
      _from = request.args.get('from')

    _addonName = None
    if (request.args.get('addonName')):
      _addonName = request.args.get('addonName')

    if token and request.method == 'GET':
      userId = validateLoginToken(token)
      if not userId:
        return invalid("Invalid Token")
      if (not validateMerchantUser(merchantId, userId)):
        return unauthorised("User Not authorised to access merchant information")

      addons = Addons.get_all_addons_without_options_str(merchantId=merchantId, limit=limit, _from=_from,
                                                      addonName=_addonName , withitemcount=withitemcount)
      return success(jsonify(addons))
    else:
      return not_found(params=["token"])
  except Exception as e:
    print("Error: ", str(e))
    return unhandled()
################################################# POST

def createMerchantAddon(merchantId):
  try:
    _json = request.json  
    token = _json.get('token')
    addon = _json.get('addon')

    ip_address = ''
    if request:
        ip_address = request.environ.get('HTTP_X_FORWARDED_FOR', request.remote_addr)
    if ip_address:
        ip_address = ip_address.split(',')[0].strip()

    userId = validateLoginToken(token)
    if not userId:
      return invalid("Invalid Token")
    
    if not validateMerchantUser(merchantId, userId):
      return unauthorised("user is not authorised to access merchant information") 

    return Addons.post_addon(merchantId, addon, userId=userId  , ip_address=ip_address)
    
  except Exception as e:
    print("Error: ", str(e)) 
    return unhandled()

################################################# PUT

def updateMerchantAddon(merchantId, addonId):
  try:
    ip_address = None
    if request:
        ip_address = request.environ.get('HTTP_X_FORWARDED_FOR', request.remote_addr)
    if ip_address:
        ip_address = ip_address.split(',')[0].strip()
    
    _json = request.json
    token = _json.get("token")
    addon = _json.get("addon")
    
    create_log_data(level='[INFO]', Message=f"In the start of updateMerchantAddon,IP address: {ip_address}, Token:{token}",
                    functionName="updateMerchantAddon", request=request)
    
    if token and addon and request.method == 'PUT': 
      userId = validateLoginToken(token)
      if not userId:
        return invalid("Invalid Token")
      if (not validateMerchantUser(merchantId, userId)):
        return unauthorised("User Not authorised to access merchant information")
      
      old_addon_details = Addons.get_addon_by_id_str(addonId)
      
      resp = Addons.put_addon(addonId=addonId, addon=addon, userId=userId )
      create_log_data(level='[INFO]', Message=f"Successfully updateMerchantAddon,IP address: {ip_address}, Token:{token}",
                    functionName="updateMerchantAddon", request=request)
      if not resp:
        return unhandled()

      # Triggering Addon SNS - addon.update
      print("Triggering item sns - addon.update ...")
      sns_msg = {
        "event": "addon.update",
        "body": {
          "merchantId": merchantId,
          "addonId": addonId,
          "userId": userId,
          "ipAddr": ip_address,
          "old_addon_details": old_addon_details
        }
      }
      sns_resp = publish_sns_message(topic=sns_addon_notification, message=str(sns_msg), subject="addon.update")
      publish_sns_message(topic=config.sns_activity_logs, message=str(sns_msg),
                                subject="addon.update")
      addon = Addons.get_addon_by_id_with_options_str(addonId)
      return success(jsonify(addon))
    else:
      return not_found(body={"token":"required", "addon": {}})
  except Exception as e:
    create_log_data(level='[INFO]', Message=f"Error {e},IP address: {ip_address}, Token:{token}",
                    functionName="updateMerchantAddon", request=request)
    return unhandled(f"Error: {e}")
  
################################################# DELETE

def deleteMerchantAddon(merchantId, addonId):
  try:
    token = request.args.get('token')
    ip_address = None
    if request:
        ip_address = request.environ.get('HTTP_X_FORWARDED_FOR', request.remote_addr)
    if ip_address:
        ip_address = ip_address.split(',')[0].strip()
    create_log_data(level='[INFO]', Message=f"In the start of deleting item addons , IP address:{ip_address}, Token:{token}",
                    functionName="deleteMerchantAddon", request=request)
    
    if token and request.method == 'DELETE': 
      userId = validateLoginToken(token)
      if not userId:
        create_log_data(level='[INFO]',
                        Message="The API token is invalid.",
                        messagebody=f"Unable to find the user on the basis of provided token., IP address:{ip_address}, Token:{token}",
                        functionName="deleteMerchantAddon", request=request, statusCode="400 Bad Request")
        return invalid("Invalid Token")
      if (not validateMerchantUser(merchantId, userId)):
        create_log_data(level='[INFO]', Message=f"User Not authorised to access merchant information, IP address:{ip_address}, Token:{token}",
                        functionName="deleteMerchantAddon", request=request, statusCode="400 Bad Request"
                        )
        return unauthorised("User Not authorised to access merchant information")
      
      addon_details = Addons.get_addon_by_id_str(addonId)
      create_log_data(level='[INFO]', Message=f"Successfully retrieved addon information, IP address:{ip_address}, Token:{token}",
                      functionName="deleteMerchantAddon", request=request, statusCode="400 Bad Request"
                      )
      resp = Addons.delete_addon_by_id(addonId)
      if not resp:
        return unhandled()

      # Triggering Addon SNS - addon.delete
      print("Triggering item sns - addon.delete ...")
      sns_msg = {
        "event": "addon.delete",
        "body": {
          "merchantId": merchantId,
          "addonId": addonId,
          "userId": userId,
          "ipAddr": ip_address,
          "addon_details": addon_details
        }
      }
      sns_resp = publish_sns_message(topic=sns_addon_notification, message=str(sns_msg), subject="addon.delete")
      publish_sns_message(topic=config.sns_activity_logs, message=str(sns_msg),
                          subject="addon.delete")
      create_log_data(level='[INFO]', Message="Successfully deleted addon",
                      functionName="deleteMerchantAddon", request=request, statusCode="200 OK"
                      )

      return success()
    else:
      create_log_data(level='[ERROR]', Message=f'Token not found in the request',
                      functionName="deleteMerchantAddon", request=request, statusCode="400 Bad Request"
                      )
      return not_found(params=["token"])
  except Exception as e:
    print("Error: ", str(e))
    create_log_data(level='[ERROR]', Message=f'Failed to delete category',
                    messagebody=f"An error occured while deleting addon: {str(e)}",
                    functionName="deleteMerchantAddon", request=request, statusCode="400 Bad Request"
                    )
    return unhandled()
