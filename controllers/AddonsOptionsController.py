from flask import jsonify, request, g

# local imports
import config
from controllers.Middleware import validate_token_middleware
from utilities.helpers import validateMerchantUser, success, publish_sns_message,create_log_data
from utilities.errors import not_found, unhandled, unauthorised
from models.AddonsOptions import AddonsOptions

# config
sns_addon_notification = config.sns_addon_notification


################################################# POST


@validate_token_middleware
def createOrDeleteAddonOption(merchantId, addonId):
  try:
    ip_address = None
    if request:
        ip_address = request.environ.get('HTTP_X_FORWARDED_FOR', request.remote_addr)
    if ip_address:
        ip_address = ip_address.split(',')[0].strip()
    _json = request.json
    
    token = g.token
    
    create_log_data(level='[INFO]', Message=f"In the start of createOrDeleteAddonOption,IP address: {ip_address}, Token:{token}",
                    functionName="createOrDeleteAddonOption", request=request)
    
    if 'itemId' in _json:
      itemId = _json.get("itemId")
    if 'AddonId' in _json:
      itemId = addonId
      addonId=_json.get("AddonId")


    isDelete = _json.get("delete")

    if itemId is not None and isDelete is not None and request.method == 'POST': 
      userId = g.userId
      if (not validateMerchantUser(merchantId, userId)):
        return unauthorised("User Not authorised to access merchant information")
      
      if isDelete == 0:
        # add
       
        create_log_data(level='[INFO]', Message=f"Adding addon,IP address: {ip_address}, Token:{token}",
                    functionName="createOrDeleteAddonOption", request=request)
        resp = AddonsOptions.post_addon_option(itemId=itemId, addonId=addonId, userId=userId,merchantId=merchantId, ip_address=ip_address)
        if resp:
          return success()
      elif isDelete == 1:
        # delete
        resp = AddonsOptions.delete_addon_option(itemId=itemId, addonId=addonId)
        create_log_data(level='[INFO]', Message=f"Deleting addon,IP address: {ip_address}, Token:{token}",
                    functionName="createOrDeleteAddonOption", request=request)
        if resp:

          # Triggering Addon SNS - addon.unassign_option
          print("Triggering item sns - addon.unassign_option ...")
          sns_msg = {
            "event": "addon.unassign_option",
            "body": {
              "merchantId": merchantId,
              "addonId": addonId,
              "itemId": itemId,
              "userId": userId,
              "ipAddr": ip_address
            }
          }
          sns_resp = publish_sns_message(topic=sns_addon_notification, message=str(sns_msg), subject="addon.unassign_option")
          publish_sns_message(topic=config.sns_activity_logs, message=str(sns_msg),
                          subject="addon.unassign_option")

          return success()
      return unhandled()
    else:
      return not_found(body={"itemId":"required", "delete":"required"})
  except Exception as e:
    create_log_data(level='[INFO]', Message=f"Error{e},IP address: {ip_address}, Token:{token}",
                    functionName="createOrDeleteAddonOption", request=request)
    print("Error")
    print(str(e))
    return unhandled()


@validate_token_middleware
def getusedAddonItems(merchantId, addonId):
  try:
    userId = g.userId
    if (not validateMerchantUser(merchantId, userId)):
      return unauthorised("User Not authorised to access merchant information")
    usedAddonItems=AddonsOptions.Get_used_addons_items(merchantId=merchantId, addonId=addonId)
    return success(jsonify(usedAddonItems))
  except Exception as e:
    print("Error")
    print(str(e))
    return unhandled()

@validate_token_middleware
def getOptionAddons(merchantId, optionId):
  try:
    userId = g.userId
    if (not validateMerchantUser(merchantId, userId)):
      return unauthorised("User Not authorised to access merchant information")
    usedAddons=AddonsOptions.Get_option_addonss( optionId=optionId)
    return success(jsonify({"addons_ids":usedAddons}))
  except Exception as e:
    print("Error")
    print(str(e))
    return unhandled()

@validate_token_middleware
def sortAddonOptions(merchantId, addonId):
  try:
    userId = g.userId

    if not validateMerchantUser(merchantId, userId):
      return unauthorised("user is not authorized")

    _json = request.json
    options = _json.get("options") if isinstance(_json.get("options"), list) else []

    return AddonsOptions.sort_addon_options(merchantId, addonId, options)
  except Exception as e:
    return unhandled(f"Error: {e}")
