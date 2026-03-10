from flask import jsonify, request, g


# local imports
from controllers.Middleware import validate_token_middleware
from models.Menus import Menus
from models.MenuMappings import MenuMappings
from utilities.helpers import validateMerchantUser, success, publish_sns_message, create_log_data
from utilities.errors import invalid, not_found, unhandled, unauthorised
import config
 


def newCreateMenuMappings(merchantId, menuId,platformType,userId):
  try:
    ip_address = None
    if request:
        ip_address = request.environ.get('HTTP_X_FORWARDED_FOR', request.remote_addr)
    if ip_address:
        ip_address = ip_address.split(',')[0].strip()
    
    if platformType :
      

      # check if menu is already assigned
      if int(platformType):
        data = MenuMappings.get_menumappings(menuId=menuId, platformType=platformType)
        if len(data):
          return invalid("Menu is already assigned to same platform")


      if int(platformType) == 2:
        # flipdish can only be assigned 1 menu
        data = MenuMappings.get_menumappings(merchantId=merchantId, platformType=platformType)
        if len(data):
          return invalid("Another menu is already assigned to flipdish")

      elif int(platformType) == 4:
        # clover can only be assigned 1 menu
        data = MenuMappings.get_menumappings(merchantId=merchantId, platformType=platformType)
        if len(data):
          return invalid("Another menu is already assigned to clover")
      
      elif int(platformType) == 11:
        # square can only be assigned 1 menu
        data = MenuMappings.get_menumappings(merchantId=merchantId, platformType=platformType)
        if len(data):
          return invalid("Another menu is already assigned to square")
      elif int(platformType) == 7:
        # GMB can only be assigned 1 menu
        data = MenuMappings.get_menumappings(merchantId=merchantId, platformType=platformType)
        if len(data):
          return invalid("Another menu is already assigned to GMB")

      elif int(platformType) == 50:
        # Storefront can only be assigned 1 menu
        data = MenuMappings.get_menumappings(merchantId=merchantId, platformType=platformType)
        
        if len(data):
          return invalid("Another menu is already assigned to Storefront")
      elif int(platformType) == 8:
        # Storefront can only be assigned 1 menu
        data = MenuMappings.get_menumappings(merchantId=merchantId, platformType=platformType)

        if len(data):
          return invalid("Another menu is already assigned to Stream")
      mappingId = MenuMappings.post_menumappings(
        merchantId=merchantId, 
        menuId=menuId, 
        platformType=platformType,
        userId=userId
      )
      if not mappingId:
        return unhandled("Unhandled exception while creating menu mappings")

      # SEND SNS RESPONSE
      sns_msg = {
        "event": "menu_mapping.update",
        "body": {
          "merchantId": merchantId,
          "menuId": menuId,
          "userId": userId,
          "platformType": platformType,
          "operation": "created",
          "ipAddr":ip_address
        }
      }
      sns_resp = publish_sns_message(topic=config.sns_menu_notification, message=str(sns_msg), subject="menu_mapping.update")

      menuResp = Menus.get_menu_by_id(menuId=menuId)
      return success(jsonify({
        "message": "success",
        "status": 200,
        "data": menuResp
      }))
    else:
      return not_found(body={"platformType": "required"})
  except Exception as e:
    return unhandled(str(e))



@validate_token_middleware
def createMenuMappings(merchantId, menuId):
  try:
    ip_address = None
    if request:
        ip_address = request.environ.get('HTTP_X_FORWARDED_FOR', request.remote_addr)
    if ip_address:
        ip_address = ip_address.split(',')[0].strip()
    
    token = g.token
    
    create_log_data(level='[INFO]', Message=f"In the start of update menu category,IP address: {ip_address}, Token:{token}",
                    functionName="createMenuMappings", request=request)
    _json = request.json
    platformType = _json.get("platformType")
    
    if platformType and request.method == "POST":
      userId = g.userId
      if (not validateMerchantUser(merchantId, userId)):
        create_log_data(level='[INFO]', Message=f"not authorize,IP address: {ip_address}, Token:{token}",
                    functionName="createMenuMappings", request=request)
        return unauthorised("User Not authorised to access merchant information")
      
      
      # check if menu is already assigned
      if int(platformType):
        data = MenuMappings.get_menumappings(menuId=menuId, platformType=platformType)
        if len(data):
          return invalid("Menu is already assigned to same platform")

      
      # if int(platformType) == 2:
      #   # flipdish can only be assigned 1 menu
      #   data = MenuMappings.get_menumappings(merchantId=merchantId, platformType=platformType)
      #   if len(data):
      #     return invalid("Another menu is already assigned to flipdish")

      elif int(platformType) == 4:
        # clover can only be assigned 1 menu
        data = MenuMappings.get_menumappings(merchantId=merchantId, platformType=platformType)
        if len(data):
          return invalid("Another menu is already assigned to clover")
      
      elif int(platformType) == 11:
        # square can only be assigned 1 menu
        data = MenuMappings.get_menumappings(merchantId=merchantId, platformType=platformType)
        if len(data):
          return invalid("Another menu is already assigned to square")
      elif int(platformType) == 7:
        # square can only be assigned 1 menu
        data = MenuMappings.get_menumappings(merchantId=merchantId, platformType=platformType)
        if len(data):
          return invalid("Another menu is already assigned to GMB")

      elif int(platformType) == 50:
        # Storefront can only be assigned 1 menu
        data = MenuMappings.get_menumappings(merchantId=merchantId, platformType=platformType)
        if len(data):
          return invalid("Another menu is already assigned to Storefront")

      mappingId = MenuMappings.post_menumappings(
        merchantId=merchantId, 
        menuId=menuId, 
        platformType=platformType,
        userId=userId
      )
      if not mappingId:
        return unhandled("Unhandled exception while creating menu mappings")

      create_log_data(level='[INFO]', Message=f"Successfully create menu mapping,IP address: {ip_address}, Token:{token}",
                    functionName="createMenuMappings", request=request)
      
      # SEND SNS RESPONSE
      sns_msg = {
        "event": "menu_mapping.update",
        "body": {
          "merchantId": merchantId,
          "menuId": menuId,
          "userId": userId,
          "platformType": platformType,
          "operation": "created",
          "ipAddr" : ip_address
        }
      }
      sns_resp = publish_sns_message(topic=config.sns_menu_notification, message=str(sns_msg), subject="menu_mapping.update")

      menuResp = Menus.get_menu_by_id(menuId=menuId)
      return success(jsonify({
        "message": "success",
        "status": 200,
        "data": menuResp
      }))
    else:
      return not_found(body={"platformType": "required"})
  except Exception as e:
    create_log_data(level='[INFO]', Message=f"Error,{e},Ip address: {ip_address}, Token:{token}",
                    functionName="createmenumapping", request=request)
    return unhandled(str(e))


@validate_token_middleware
def deleteMenuMapping(merchantId, menuId, mappingId):
  try:
    
    ip_address = None
    if request:
        ip_address = request.environ.get('HTTP_X_FORWARDED_FOR', request.remote_addr)
    if ip_address:
        ip_address = ip_address.split(',')[0].strip()
    userId = g.userId
    if (not validateMerchantUser(merchantId, userId)):
      return unauthorised("User Not authorised to access merchant information")
    
    mapping_details = MenuMappings.get_menumappings(mappingId=mappingId)
    mapping_details = mapping_details[0]

    resp = MenuMappings.delete_menumappings(mappingId=mappingId)
    if not resp:
      return unhandled("Unhandled exception while deleting menu mappings")
    
    # SEND SNS RESPONSE
    sns_msg = {
      "event": "menu_mapping.update",
      "body": {
        "merchantId": merchantId,
        "menuId": menuId,
        "userId": userId,
        "platformType": mapping_details["platformtype"],
        "operation": "deleted",
        "ipAddr": ip_address
      }
    }
    sns_resp = publish_sns_message(topic=config.sns_menu_notification, message=str(sns_msg), subject="menu_mapping.update")

    menuResp = Menus.get_menu_by_id(menuId=menuId)
    return success(jsonify({
      "message": "success",
      "status": 200,
      "data": menuResp
    }))
  except Exception as e:
    return unhandled(str(e))