from flask import jsonify, request, g

# local imports
import config
from controllers.Middleware import validate_token_middleware
from models.ProductsAddons import ProductsAddons
from utilities.helpers import validateMerchantUser, success, publish_sns_message
from utilities.errors import not_found, unhandled, unauthorised

# config
sns_item_notification = config.sns_item_notification

################################################# POST


@validate_token_middleware
def createOrDeleteProductAddon(merchantId, itemId):
  try:
    ip_address = None
    if request:
        ip_address = request.environ.get('HTTP_X_FORWARDED_FOR', request.remote_addr)
    if ip_address:
        ip_address = ip_address.split(',')[0].strip()
        
    print("addon start")
    _json = request.json
    if 'addonId' in _json:
      addonId=_json.get("addonId")
    elif 'itemids' in  _json:
      addonId=itemId
      itemId = _json.get("itemids")

    isDelete = _json.get("delete")

    if addonId is not None and isDelete is not None and request.method == 'POST': 
      userId = g.userId
      if (not validateMerchantUser(merchantId, userId)):
        return unauthorised("User Not authorised to access merchant information")
      
      if isDelete == 0:
        # add
        resp = ProductsAddons.post_item_addon(itemId=itemId, addonId=addonId, userId=userId,merchantId=merchantId)
        return success()
      elif isDelete == 1:
        # delete
        resp = ProductsAddons.delete_item_addon(itemId=itemId, addonId=addonId)
        if resp:

          print("Triggering item sns...")
          sns_msg = {
            "event": "item.unassign_addon",
            "body": {
              "merchantId": merchantId,
              "itemId": itemId,
              "addonId": addonId,
              "userId": userId,
              "ipAddr":ip_address
            }
          }
          sns_resp = publish_sns_message(topic=sns_item_notification, message=str(sns_msg), subject="item.unassign_addon")

          return success()
      return unhandled()
    else:
      return not_found(body={"addonId":"required", "delete":"required"})
  except Exception as e:
    print("Error")
    print(str(e))
    return unhandled()

  
@validate_token_middleware
def sortItemAddons(merchantId, itemId):
  try:
    userId = g.userId

    if not validateMerchantUser(merchantId, userId):
      return unauthorised("user is not authorized")

    _json = request.json
    addons = _json.get("addons") if isinstance(_json.get("addons"), list) else []

    return ProductsAddons.sort_item_addons(merchantId, itemId, addons)
  except Exception as e:
    return unhandled(f"Error: {e}")