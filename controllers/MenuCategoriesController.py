from flask import jsonify, request, g

# local imports
import config
from controllers.Middleware import validate_token_middleware
from models.MenuCategories import MenuCategories
from models.Menus import Menus
from utilities.helpers import validateLoginToken, validateMerchantUser, success, publish_sns_message, create_log_data,get_ip_address
from utilities.errors import invalid, not_found, unhandled, unauthorised

# import sns
sns_menu_notification = config.sns_menu_notification



def updateMenuCategories(merchantId, menuId):
  try:
    _json = request.json

    token = _json.get("token")
    categoryIds = _json.get("categoryIds")

    create_log_data(level='[INFO]', Message=f"In the start of update menu category, Token:{token}",
                    functionName="updateMenuCategories", request=request)
    
    if token and request.method == "PUT":
      userId = validateLoginToken(token)
      if not userId:
        return invalid("Invalid Token")

      if (not validateMerchantUser(merchantId, userId)):
        return unauthorised("User Not authorised to access merchant information")

      if categoryIds is not None and type(categoryIds) is list:
        resp = MenuCategories.update_menucategories(merchantId=merchantId, menuId=menuId, categoryIds=categoryIds)
        if resp:

          menuResp = Menus.get_menu_by_id(menuId=menuId)
          if not menuResp:
            return unhandled("Unhandled exception while getting the updated menu")
          
          create_log_data(level='[INFO]', Message=f"Successfuly update menu categories, Token:{token}",
                    functionName="updateMenuCategories", request=request)
            
          
          return success(jsonify({
            "message": "success",
            "status": 200,
            "data": menuResp
          }))
        else:
          return unhandled("Unhandled exception in update menu categories")
      else:
        return invalid("categoryIds (list) not found in json body")
    else:
      fields = {
        "token": "required", 
        "categoryIds": [
          'required'
        ]
      }
      return not_found(body=fields)

  except Exception as e:
    create_log_data(level='[INFO]', Message=f"Error: {e}, Token:{token}",
                    functionName="updateMenuCategories", request=request)
    return unhandled(f"Error: {e}")


@validate_token_middleware
def createOrDeleteMenuCategoryMapping(merchantId, categoryId):
  try:
    _json = request.json
    menuIds = _json.get("menuid")
    isDelete = _json.get("delete")
    ip_address = get_ip_address(request)
    if categoryId is not None and isDelete is not None and request.method == "POST":
      userId = g.userId
      if not validateMerchantUser(merchantId, userId):
        return unauthorised("User Not authorised to access merchant information")
      
      if isDelete == 0:
        # add
        if isinstance(menuIds, list):
          for menu in menuIds:
            resp = MenuCategories.post_menucategory(merchantId=merchantId, menuId=menu, categoryId=categoryId, platformType=1)
            if resp:

              print("Triggering menu sns...")
              sns_msg = {
                "event": "menu.assign_category",
                "body": {
                  "merchantId": merchantId,
                  "menuId": menu,
                  "categoryId": categoryId,
                  "userId": userId,
                  "ipAddr": ip_address,
                }
              }
              sns_resp = publish_sns_message(topic=sns_menu_notification, message=str(sns_msg), subject="menu.assign_category")
              publish_sns_message(topic=config.sns_activity_logs, message=str(sns_msg),
                          subject="menu.assign_category")
          
          return success()
    else:
      return not_found(body={"categoryId":"required", "delete": "required"})
  except Exception as e:
    return unhandled(f"Error: {e}")




@validate_token_middleware
def createOrDeleteMenuCategory(merchantId, menuId):
  try:
    _json = request.json
    categoryId = _json.get("categoryId")
    isDelete = _json.get("delete")
    token = g.token
    ip_address = None
    if request:
        ip_address = request.environ.get('HTTP_X_FORWARDED_FOR', request.remote_addr)
    if ip_address:
        ip_address = ip_address.split(',')[0].strip()
    
    create_log_data(level='[INFO]', Message=f"In the start of update menu category,Ip address: {ip_address}, Token:{token}",
                    functionName="createOrDeleteMenuCategory", request=request)
    
    if categoryId is not None and isDelete is not None and request.method == "POST":
      userId = g.userId
      if not validateMerchantUser(merchantId, userId):
        return unauthorised("User Not authorised to access merchant information")

      if isDelete == 0:
        # add
        resp = MenuCategories.post_menucategory(merchantId=merchantId, menuId=menuId, categoryId=categoryId, platformType=1)
        if resp:
          create_log_data(level='[INFO]', Message=f"Successfully update menu category,Ip address: {ip_address}, Token:{token}",
                    functionName="createOrDeleteMenuCategory", request=request)

          print("Triggering menu sns...")
          sns_msg = {
            "event": "menu.assign_category",
            "body": {
              "merchantId": merchantId,
              "menuId": menuId,
              "categoryId": categoryId,
              "userId": userId,
              "ipAddr": ip_address,
            }
          }
          sns_resp = publish_sns_message(topic=sns_menu_notification, message=str(sns_msg), subject="menu.assign_category")
          publish_sns_message(topic=config.sns_activity_logs, message=str(sns_msg),
                          subject="menu.assign_category")

          return success()
      elif isDelete == 1:
        # delete
        resp = MenuCategories.delete_menucategories(menuId=menuId, categoryId=categoryId, platformType=1)
        if resp:
          
          create_log_data(level='[INFO]', Message=f"Successfully update menu category, {resp},IP address: {ip_address}, Token:{token}",
                    functionName="createOrDeleteMenuCategory", request=request)

          print("Triggering menu sns...")
          sns_msg = {
            "event": "menu.unassign_category",
            "body": {
              "merchantId": merchantId,
              "menuId": menuId,
              "categoryId": categoryId,
              "userId": userId,
              "ipAddr": ip_address,
            }
          }
          sns_resp = publish_sns_message(topic=sns_menu_notification, message=str(sns_msg), subject="menu.unassign_category")
          publish_sns_message(topic=config.sns_activity_logs, message=str(sns_msg),
                          subject="menu.unassign_category")


          return MenuCategories.check_category_platform_mapping(menuId, categoryId)
    else:
      return not_found(body={"categoryId":"required", "delete": "required"})
  except Exception as e:
    create_log_data(level='[INFO]', Message=f"Error,{e},Ip address: {ip_address}, Token:{token}",
                    functionName="createOrDeleteMenuCategory", request=request)
    return unhandled(f"Error: {e}")
  

@validate_token_middleware
def sortMenuCategories(merchantId, menuId):
  try:
    userId = g.userId
    
    if not validateMerchantUser(merchantId, userId):
      return unauthorised("user is not authrozied!")
    
    _json = request.json
    categories = _json.get("categories") if isinstance(_json.get("categories"), list) else []

    return MenuCategories.sort_menu_categories(merchantId, menuId, categories)

  except Exception as e:
    return unhandled(f"Error: {e}")


