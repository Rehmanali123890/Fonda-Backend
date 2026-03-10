import json

import requests
from flask import jsonify, request, g

from controllers.MenuMappingsController import newCreateMenuMappings
# local imports
from controllers.Middleware import validate_token_middleware
from models.Platforms import Platforms
from models.VMerchantMenus import VMerchantMenus
from utilities.helpers import get_db_connection, publish_sns_message, validateAdminUser, create_log_data,get_ip_address
import config
from models.Menus import Menus
from models.ServiceAvailability import ServiceAvailability
from models.MenuCategories import MenuCategories
from models.MenuMappings import MenuMappings
from models.Merchants import Merchants
from models.ItemMappings import ItemMappings
from models.AddonMappings import AddonMappings
from utilities.helpers import validateLoginToken, validateMerchantUser, success
from utilities.errors import invalid, not_found, unhandled, unauthorised


# config
sns_menu_notification = config.sns_menu_notification


################################################# POST

def createMerchantMenu(merchantId):
  try:
    
    _json = request.json
    connection, cursor = get_db_connection()
    
    ip_address = None
    if request:
        ip_address = request.environ.get('HTTP_X_FORWARDED_FOR', request.remote_addr)
    if ip_address:
        ip_address = ip_address.split(',')[0].strip()
    
    # get data from json
    token = _json.get("token")
    
    create_log_data(level='[INFO]', Message=f"In the start of createmerchantMenu , IP address:{ip_address}, Token:{token}",
                    functionName="createMerchantMenu", request=request)
    
    name = _json.get("menu").get("name")
    cusine = _json.get("menu").get("cusines")
    description = _json.get("menu").get("description")
    metadata = _json.get("menu").get("metadata")
    menuPlatforms = _json.get("menu").get("menuPlatforms")
    vMerchantId = _json.get("menu").get("vmerchantId") if "vmerchantId" in _json.get("menu") else None

    if token and name and request.method == "POST":
      userId = validateLoginToken(token)
      if not userId:
        return invalid("Invalid Token")

      if (not validateMerchantUser(merchantId, userId)):
        return unauthorised("User Not authorised to access merchant information")

      menuId = Menus.post_menus(userId=userId, merchantId=merchantId, name=name, description=description, metadata=metadata, menuPlatforms=menuPlatforms, cusine=cusine)
      if not menuId:
        return unhandled("Unhandled Exception while creating menu")

      menuResp = Menus.get_menu_by_id(menuId=menuId)
      if not menuResp:
        create_log_data(level='[INFO]', Message=f"Error while creating menus , IP address:{ip_address}, Token:{token}",
                    functionName="createMerchantMenu", request=request)
        return unhandled("Unhandled Exception while creating menu")
      if vMerchantId and vMerchantId !='':
        VMerchantMenus.assign_new_menu_to_vmerchant(merchantId, vMerchantId, menuId, userId)


      if isinstance(menuPlatforms, list):
        print(f"Their is platform list .{menuPlatforms}")
        for platform in menuPlatforms:
          cursor.execute("""SELECT * FROM platformtype WHERE type=%s""", (platform))
          platform_id = cursor.fetchone()
          platform_id = platform_id['id']
          resp = newCreateMenuMappings(merchantId,menuId, platform_id,userId )
          print(resp)
      else:
          pass
      """
      TODO: 
      ... SEND SNS menu.create
      """
      
      create_log_data(level='[INFO]', Message=f"Succesfully menu created , IP address:{ip_address}, Token:{token}",
                    functionName="createMerchantMenu", request=request)

      sns_msg = {
        "event": "menu.create",
        "body": {
          "merchantId": merchantId,
          "menuId": menuId,
          "menuName": menuResp.get("name"),
          "userId": userId,
          "ipAddr": ip_address
        }
      }
      sns_resp = publish_sns_message(topic=sns_menu_notification, message=str(sns_msg), subject="menu.create")
      publish_sns_message(topic=config.sns_activity_logs, message=str(sns_msg),
                          subject="menu.create")

      return success(jsonify({
        "message": "success",
        "status": 200,
        "data": menuResp
      }))

    else:
      fields = {
        "token": "required",
        "menu": {"name": "required"}}
      return not_found(body=fields)
  except Exception as e:
    print("Error: ", str(e))
    return unhandled()


# get all types
def getAllConnectedPlatforms(merchantId):
  connection, cursor = get_db_connection()
  try:
    cursor.execute("""Select distinct platformtype from menumappings where merchantid=%s""", (merchantId))
    rows = cursor.fetchall()
    return success(jsonify({
      "message": "success",
      "status": 200,
      "data": rows
    }))
  except:
    return unhandled()

################################################# PUT

def updateMerchantMenu(merchantId, menuId):
  try:
    
    ip_address = None
    if request:
        ip_address = request.environ.get('HTTP_X_FORWARDED_FOR', request.remote_addr)
    if ip_address:
        ip_address = ip_address.split(',')[0].strip()
    connection, cursor = get_db_connection()

    _json = request.json
    token = _json.get("token")
    name = _json.get("menu").get("name")
    description = _json.get("menu").get("description")
    status = _json.get("menu").get("status")
    metadata = _json.get("menu").get("metadata")
    menuPlatforms = _json.get("menu").get("menuPlatforms")
    cusine = _json.get("menu").get("cusines")
    new_menu = _json.get("menu").get("new_menu")
    vMerchantId = _json.get("menu").get("vmerchantId") if "vmerchantId" in _json.get("menu") else None


    create_log_data(level='[INFO]', Message=f"In the start of updateMerchantMenu , IP address:{ip_address}, Token:{token}",
                    functionName="updateMerchantMenu", request=request)
    
    if token and name and request.method == "PUT":
      userId = validateLoginToken(token)
      if not userId:
        return invalid("Invalid Token")

      if (not validateMerchantUser(merchantId, userId)):
        return unauthorised("User Not authorised to access merchant information")

      old_menu_details = Menus.get_menu_by_id_fk(menuId)
      if new_menu:
        if isinstance(menuPlatforms, list):
          old_platform_list = MenuMappings.new_get_menumappings_str(menuId)
          new_platform_list = menuPlatforms
          differ_list_to_add = [platform for platform in new_platform_list if platform not in old_platform_list]
          differ_list_to_delete = [platform for platform in old_platform_list if platform not in new_platform_list]

          if len(differ_list_to_add):
            for platform in differ_list_to_add:
              cursor.execute("""SELECT * FROM platformtype WHERE type=%s""", (platform))
              platform_id = cursor.fetchone()
              platform_id = platform_id['id']
              resp = newCreateMenuMappings(merchantId,menuId, platform_id,userId )
              print(resp)

          if len(differ_list_to_delete):
            for platform in differ_list_to_delete:
              cursor.execute("""SELECT * FROM platformtype WHERE type=%s""", (platform))
              platform_id = cursor.fetchone()
              platform_id = platform_id['id']
              resp = MenuMappings.delete_menumappings(mappingId=None, menuId=menuId, merchantId=merchantId, platformType=platform_id)
              # SEND SNS RESPONSE
              sns_msg = {
                "event": "menu_mapping.update",
                "body": {
                  "merchantId": merchantId,
                  "menuId": menuId,
                  "userId": userId,
                  "platformType": platform_id,
                  "operation": "deleted",
                  "ipAddr":ip_address
                }
              }
              sns_resp = publish_sns_message(topic=config.sns_menu_notification, message=str(sns_msg),
                                             subject="menu_mapping.update")
                
      menuId = Menus.put_menuById(userId, menuId, merchantId, name, description, status, metadata, menuPlatforms, cusine)
      if not menuId:
        create_log_data(level='[INFO]', Message=f"Exception , IP address:{ip_address}, Token:{token}",
                    functionName="updateMerchantMenu", request=request)
        return unhandled("Unhandled Exception while updating menu")
      if vMerchantId and vMerchantId !='':
        cursor.execute("SELECT * FROM vmerchantmenus WHERE  menuid = %s", (menuId))
        row = cursor.fetchone()
        if row:
          if vMerchantId != row.get('vmerchantid'):
            VMerchantMenus.remove_menu_from_vmerchant( merchantId, vMerchantId, menuId)
            VMerchantMenus.assign_new_menu_to_vmerchant(merchantId, vMerchantId, menuId, userId)
        else:
           VMerchantMenus.assign_new_menu_to_vmerchant(merchantId, vMerchantId, menuId, userId)


      create_log_data(level='[INFO]', Message=f"Successfully update menu , IP address:{ip_address}, Token:{token}",
                    functionName="updateMerchantMenu", request=request)
      
      # SEND SNS RESPONSE
      print("Triggering menu sns...")
      sns_msg = {
        "event": "menu.update",
        "body": {
          "merchantId": merchantId,
          "menuId": menuId,
          "userId": userId,
          "ipAddr":ip_address,
          "old_menu_details": {
            "name": old_menu_details["name"],
            "description": old_menu_details["description"]
          }
        }
      }
      sns_resp = publish_sns_message(topic=sns_menu_notification, message=str(sns_msg), subject="menu.update")
      publish_sns_message(topic=config.sns_activity_logs, message=str(sns_msg),
                          subject="menu.update")

      menuResp = Menus.get_menu_by_id(menuId=menuId)
      if not menuResp:
        return unhandled("Unhandled Exception while getting the updated menu")

      return success(jsonify({
        "message": "successfully updated",
        "status": 200,
        "data": menuResp
      }))

    else:
      fields = {
        "token": "required",
        "menu": {"name": "required"}}
      return not_found(body=fields)
  except Exception as e:
    create_log_data(level='[INFO]', Message=f"Error occured {e}, IP address:{ip_address}, Token:{token}",
                    functionName="updateMerchantMenu", request=request)
    print("Error: ", str(e))
    return unhandled()


################################################# GET

def getMerchantMenuById(merchantId, menuId):
  try:
    
    ip_address = None
    if request:
        ip_address = request.environ.get('HTTP_X_FORWARDED_FOR', request.remote_addr)
    if ip_address:
        ip_address = ip_address.split(',')[0].strip()
        
    token = request.args.get("token")
    create_log_data(level='[INFO]', Message=f"In the start of getMerchantMenuById , IP address:{ip_address}, Token:{token}",
                    functionName="getMerchantMenuById", request=request)

    if token and request.method == "GET":
      userId = validateLoginToken(token)
      if not userId:
        return invalid("Invalid Token")

      if (not validateMerchantUser(merchantId, userId)):
        return unauthorised("User Not authorised to access merchant information")

      menuResp = Menus.get_menu_by_id(menuId=menuId)
      if not menuResp:
        create_log_data(level='[INFO]', Message=f"Error occured , IP address:{ip_address}, Token:{token}",
                    functionName="getMerchantMenuById", request=request)
        return unhandled()

      return success(jsonify({
        "message": "success",
        "status": 200,
        "data": menuResp
      }))

    else:
      return not_found(params=["token"])
  except Exception as e:
    create_log_data(level='[INFO]', Message=f"Error occured , IP address:{ip_address}, Token:{token}",
                    functionName="getMerchantMenuById", request=request)
    
    print("Error: ", str(e))
    return unhandled()


def downloadMenu(menuId):
  try:
    ip_address = None
    if request:
        ip_address = request.environ.get('HTTP_X_FORWARDED_FOR', request.remote_addr)
    if ip_address:
        ip_address = ip_address.split(',')[0].strip()
    create_log_data(level='[INFO]', Message=f"In the start of downlload menu pdf,IP address: {ip_address},",
                    functionName="downloadMenu", request=request)
    return Menus.get_menu_details(menuId=menuId)

  except Exception as e:
    create_log_data(level='[INFO]', Message=f"An Error occured while downlod pdf {e},IP address: {ip_address}",
                    functionName="downloadMenu", request=request)
    print("Error: ", str(e))
    return unhandled()

def uploadMenuToGoogle(merchantId):
  _json = request.json
  ip_address = None
  if request:
      ip_address = request.environ.get('HTTP_X_FORWARDED_FOR', request.remote_addr)
  if ip_address:
      ip_address = ip_address.split(',')[0].strip()
  location = _json.get("locationId")
  account = _json.get("accountId")
  sns_msg = {
    "event": "menu.upload_google",
    "body": {
      "merchantId": merchantId,
      "location": location,
      "account": account,
      "ipAddr": ip_address
    }
  }
  sns_resp = publish_sns_message(topic=sns_menu_notification, message=str(sns_msg), subject="menu.upload_google")

  return success(jsonify({
    "message": "success",
    "status": 200,
    "all_menu": {
      "menus": "ok"
    }
  }))


# except Exception as e:
#   print("Error: ", str(e))
#   return unhandled()


def getMerchantMenus(merchantId):
  try:
    token = request.args.get("token")
    if (request.args.get('limit')):
      limit = int(request.args.get('limit'))
    else:
      limit = 25

    if (request.args.get('from')):
      offset = int(request.args.get('from'))
    else:
      offset = 0

    if token and request.method == "GET":
      userId = validateLoginToken(token)
      if not userId:
        return invalid("Invalid Token")

      if (not validateMerchantUser(merchantId, userId)):
        return unauthorised("User Not authorised to access merchant information")

      allMenus = Menus.get_menus(merchantId=merchantId, limit=limit, offset=offset)
      if type(allMenus) is not list:
        return unhandled("Unhandled Exception")

      return success(jsonify({
        "message": "success",
        "status": 200,
        "data": allMenus
      }))

    else:
      return not_found(params=["token"])
  except Exception as e:
    print("Error: ", str(e))
    return unhandled()


#new menu 

def newGetMerchantMenus(merchantId):
  try:
    token = request.args.get("token")
    if (request.args.get('limit')):
      limit = int(request.args.get('limit'))
    else:
      limit = 25

    if (request.args.get('from')):
      offset = int(request.args.get('from'))
    else:
      offset = 0

    if token and request.method == "GET":
      userId = validateLoginToken(token)
      if not userId:
        return invalid("Invalid Token")

      if (not validateMerchantUser(merchantId, userId)):
        return unauthorised("User Not authorised to access merchant information")

      allMenus = Menus.new_get_menus(merchantId=merchantId, limit=limit, offset=offset)
      if type(allMenus) is not list:
        return unhandled("Unhandled Exception")

      return success(jsonify({
        "message": "success",
        "status": 200,
        "data": allMenus
      }))

    else:
      return not_found(params=["token"])
  except Exception as e:
    print("Error: ", str(e))
    return unhandled()



################################################# DELETE

def deleteMerchantMenuById(merchantId, menuId):
  try:
    
    ip_address = None
    if request:
        ip_address = request.environ.get('HTTP_X_FORWARDED_FOR', request.remote_addr)
    if ip_address:
        ip_address = ip_address.split(',')[0].strip()
        
    create_log_data(level='[INFO]', Message=f"In the start of deleting menu, IP address:{ip_address}",
                    functionName="deleteMerchantMenuById", request=request)
    token = request.args.get("token")

    if token and request.method == "DELETE":
      userId = validateLoginToken(token)
      if not userId:
        create_log_data(level='[ERROR]',
                        Message="The API token is invalid.",
                        messagebody=f"Unable to find the user on the basis of provided token., IP address:{ip_address}",
                        functionName="deleteMerchantMenuById", request=request, statusCode="400 Bad Request")
        return invalid("Invalid Token")

      if (not validateMerchantUser(merchantId, userId)):
        create_log_data(level='[INFO]', Message=f"User Not authorised to access merchant information, IP address:{ip_address}",
                        functionName="deleteMerchantMenuById", request=request, statusCode="400 Bad Request"
                        )
        return unauthorised("User Not authorised to access merchant information")

      # first delete menu mappings
      '''
      TODO: We are deleting all the menu's menumappings, menucategories, itemmappings, addonmappings details here
      '''
      # get menu mappings
      mappings = MenuMappings.get_menumappings_str(menuId=menuId)

      # get menu details
      menu_details = Menus.get_menu_by_id_fk(menuId)
      create_log_data(level='[INFO]', Message=f"Successfully retrieved menu details, IP address:{ip_address}",
                      functionName="deleteMerchantMenuById", request=request, statusCode="200 OK"
                      )

      # delete menu-mappings
      mappingsResp = MenuMappings.delete_menumappings(menuId=menuId)
      create_log_data(level='[INFO]', Message=f"Successfully delete menu mapping, IP address:{ip_address}",
                      functionName="deleteMerchantMenuById", request=request, statusCode="200 OK"
                      )
      if not mappingsResp:
        create_log_data(level='[ERROR]', Message=f"Failed to delete menu mapping, IP address:{ip_address}",
                        functionName="deleteMerchantMenuById", request=request, statusCode="400 Bad Request"
                        )
        return unhandled("Unhandled exception while deleting menu-mappings")

      # delete menu service availabilty
      saDelResp = ServiceAvailability.delete_serviceAvailabilityByMenuId(menuId)
      create_log_data(level='[INFO]', Message=f"Successfully delete service availability, IP address:{ip_address}",
                      functionName="deleteMerchantMenuById", request=request, statusCode="200 OK"
                      )
      if not saDelResp:
        create_log_data(level='[ERROR]', Message=f"Failed to delete service availability, IP address:{ip_address}",
                        functionName="deleteMerchantMenuById", request=request, statusCode="400 Bad Request"
                        )
        return unhandled("Unhandled exception while deleting menu service availability")

      # delete menu-categories
      menuCatResp = MenuCategories.delete_menucategories(menuId=menuId)
      create_log_data(level='[INFO]', Message=f"Successfully delete menu categories, IP address:{ip_address}",
                      functionName="deleteMerchantMenuById", request=request, statusCode="200 OK"
                      )
      if not menuCatResp:
        create_log_data(level='[ERROR]', Message=f"Failed to delete menu categories, IP address:{ip_address}",
                        functionName="deleteMerchantMenuById", request=request, statusCode="400 Bad Request"
                        )
        return unhandled("Unhandled exception while deleting menu-categories")

      # delete item-mappings
      itResp = ItemMappings.delete_itemmappings(menuId=menuId)
      create_log_data(level='[INFO]', Message=f"Successfully delete item mapping, IP address:{ip_address}",
                      functionName="deleteMerchantMenuById", request=request, statusCode="200 OK"
                      )
      if not itResp:
        create_log_data(level='[ERROR]', Message=f"Failed to delete item mapping, IP address:{ip_address}",
                        functionName="deleteMerchantMenuById", request=request, statusCode="400 Bad Request"
                        )
        return unhandled()

      # delete addons-mappings
      adResp = AddonMappings.delete_addonmappings(menuId=menuId)
      create_log_data(level='[INFO]', Message=f"Successfully delete menu addons, IP address:{ip_address}",
                      functionName="deleteMerchantMenuById", request=request, statusCode="200 OK"
                      )
      if not adResp:
        create_log_data(level='[ERROR]', Message=f"Failed to delete menu addons, IP address:{ip_address}",
                        functionName="deleteMerchantMenuById", request=request, statusCode="400 Bad Request"
                        )
        return unhandled()
      
      # delete the virtual-merchant-menus data
      vmDeletedRows = VMerchantMenus.delete_virtual_merchant_menu(menuId=menuId)
      create_log_data(level='[INFO]', Message=f"Successfully delete virtual merchant menu, IP address:{ip_address}",
                      functionName="deleteMerchantMenuById", request=request, statusCode="200 OK"
                      )
      if vmDeletedRows == False:
        create_log_data(level='[ERROR]', Message="Failed to delete virtual merchant menu",
                        functionName="deleteMerchantMenuById", request=request, statusCode="400 Bad Request"
                        )
        return unhandled()
      '''TODO: use vMerchantMenus data in future processings of autosync then'''

      # now delete the menu
      menuResp = Menus.delete_menu_by_id(menuId=menuId)
      create_log_data(level='[INFO]', Message="Successfully delete menu",
                      functionName="deleteMerchantMenuById", request=request, statusCode="200 OK"
                      )
      if not menuResp:
        create_log_data(level='[ERROR]', Message="Failed to delete  menu",
                        functionName="deleteMerchantMenuById", request=request, statusCode="400 Bad Request"
                        )
        return unhandled()

      # SEND SNS RESPONSE
      print("Triggering menu sns...")
      sns_msg = {
        "event": "menu.delete",
        "body": {
          "merchantId": merchantId,
          "menuId": menuId,
          "userId": userId,
          "mappings": mappings,
          "ipAddr":ip_address,
          "menu_details": {
            "name": menu_details["name"],
            "description": menu_details["description"]
          }
        }
      }
      print(sns_msg)
      print(sns_msg.get("body")['menu_details']['name'])
      sns_resp = publish_sns_message(topic=sns_menu_notification, message=str(sns_msg), subject="menu.delete")
      publish_sns_message(topic=config.sns_activity_logs, message=str(sns_msg),
                          subject="menu.delete")
      create_log_data(level='[INFO]', Message=f"Successfully deleted menu of merchant {merchantId}",
                      functionName="deleteMerchantMenuById", request=request, statusCode="200 OK"
                      )

      return success()
    else:
      create_log_data(level='[ERROR]', Message=f'Token not found in the request',
                      functionName="deleteMerchantMenuById", request=request, statusCode="400 Bad Request"
                      )
      return not_found(params=["token"])
  except Exception as e:
    print("Error: ", str(e))
    create_log_data(level='[ERROR]', Message=f'Failed to delete menu ',
                    messagebody=f"An error occured while deleting menu: {str(e)}",
                    functionName="deleteMerchantMenuById", request=request, statusCode="400 Bad Request"
                    )
    return unhandled()


################################################################ SERVICE AVAILABILITY

def addedUpdatedDeletedFields(beforeUpdated, afterupdated):
  day_mapping = {
    1: "Monday",
    2: "Tuesday",
    3: "Wednesday",
    4: "Thursday",
    5: "Friday",
    6: "Saturday",
    7: "Sunday"
  }
  days_before_updating = [entry['weekDay'] for entry in beforeUpdated]

  updated_fields = []
  for beforeUpdatedData in beforeUpdated:
    for afterUpdatedData in afterupdated:
      if beforeUpdatedData["weekDay"] == afterUpdatedData["weekDay"]:
        different_values = []
        for key in beforeUpdatedData:
          if key=='id':
            continue
          if beforeUpdatedData[key] != afterUpdatedData[key]:
            different_values.append(f"{key} <{beforeUpdatedData[key]}> to <{afterUpdatedData[key]}>")
        if different_values:
          updated_fields.append(f"{day_mapping[beforeUpdatedData['weekDay']]}: {','.join(different_values)}")
        break
    else:
      updated_fields.append(f"{day_mapping[beforeUpdatedData['weekDay']]} menu hours deleted ")
  for afterUpdatedData in afterupdated:
      if afterUpdatedData["weekDay"] not in days_before_updating:
        updated_fields.append(f"{day_mapping[afterUpdatedData['weekDay']]} added: starttime {afterUpdatedData['startTime']}, endtime {afterUpdatedData['endTime']}")
  print(updated_fields)
  return ','.join(updated_fields)


@validate_token_middleware
def createMenuServiceAvailability(merchantId, menuId):
  try:
    
    ip_address = None
    if request:
        ip_address = request.environ.get('HTTP_X_FORWARDED_FOR', request.remote_addr)
    if ip_address:
        ip_address = ip_address.split(',')[0].strip()
    _json = request.json
    availability = _json.get("serviceAvailability")

    if request.method == "POST":
      userId = g.userId
      if not validateMerchantUser(merchantId, userId):
        return unauthorised("User Not authorised to access merchant information")

      getservice =  ServiceAvailability.get_serviceAvailabilityByMenuId(menuId=menuId)
      delResp = ServiceAvailability.delete_serviceAvailabilityByMenuId(menuId)
      if len(availability) != 0:
        postResp = ServiceAvailability.post_serviceAvailability(menuId, availability)
        if not postResp:
          return unhandled()
      getservice_updated = ServiceAvailability.get_serviceAvailabilityByMenuId(menuId=menuId)
      messagebody = addedUpdatedDeletedFields(getservice, getservice_updated)
      
      if messagebody:
        messagebody += f", IP address:{ip_address}"
        sns_msg = {
          "event": "menu.update_hours",
          "body": {
            "merchantId": merchantId,
            "userId": userId,
            "eventName": "menu.update_hours",
            "eventDetails": messagebody
          }
        }
        logs_sns_resp = publish_sns_message(topic=config.sns_activity_logs, message=str(sns_msg),
                                            subject="menu.update_hours")
      if not getservice_updated and availability:
        return unhandled()

      return success(jsonify({
        "message": "success",
        "status": 200,
        "data": getservice_updated
      }))

    else:
      fields = {
        "serviceAvailability": [
          {
            "startTime": "required",
            "endTime": "required",
            "weekDay": "required"
          }
        ]
      }
      return not_found(body=fields)
  except Exception as e:
    print("Error: ", str(e))
    return unhandled()


@validate_token_middleware
def getMenuServiceAvailability(merchantId, menuId):
  try:
    if request.method == "GET":
      userId = g.userId
      if (not validateMerchantUser(merchantId, userId)):
        return unauthorised("User Not authorised to access merchant information")

      getResp = ServiceAvailability.get_serviceAvailabilityByMenuId(menuId=menuId)

      return success(jsonify({
        "message": "success",
        "status": 200,
        "data": getResp
      }))
    else:
      return not_found("invalid request method")
  except Exception as e:
    print("Error: ", str(e))
    return unhandled()

################################################# DOWNLOAD

@validate_token_middleware
def downloadMenuCsv(merchantId, menuId):
  try:
    userId = g.userId
    ip_address = None
    if request:
        ip_address = request.environ.get('HTTP_X_FORWARDED_FOR', request.remote_addr)
    if ip_address:
        ip_address = ip_address.split(',')[0].strip()
    create_log_data(level='[INFO]', Message=f"In the start of downlload menu csv,IP address: {ip_address},",
                    functionName="downloadMenuCsv", request=request)
    if not validateAdminUser(userId):
      return unauthorised("user is not authorized!")  

    return Menus.download_menu_csv(menuId)
  except Exception as e:
    create_log_data(level='[INFO]', Message=f"An Error occured while downlod csv,{e},IP address: {ip_address}",
                    functionName="downloadMenuCsv", request=request)
    return unhandled(f"Error: {e}")


@validate_token_middleware
def updateMerchantMenuStatus(merchantId, menuId):
  try:
    
    token=g.token
    ip_address = None
    if request:
        ip_address = request.environ.get('HTTP_X_FORWARDED_FOR', request.remote_addr)
    if ip_address:
        ip_address = ip_address.split(',')[0].strip()
    
    create_log_data(level='[INFO]', Message=f"In the start of updateMerchantMenuStatus,IP address: {ip_address}, Token:{token}",
                    functionName="updateMerchantMenuStatus", request=request)
    
    _json = request.json
    menu_status = _json.get('menuStatus')
    if menu_status is not None and request.method == 'PUT':
      userId = g.userId
      if (not validateMerchantUser(merchantId, userId)):
        return unauthorised("User Not authorised to access merchant information")
      resp=Menus.update_menu_status( menu_id=menuId, menu_status=menu_status, userId=userId)
      create_log_data(level='[INFO]', Message=f"Successfully updateMerchantMenuStatus,IP address: {ip_address}, Token:{token}",
                    functionName="updateMerchantMenuStatus", request=request)
      if resp:
        print("Triggering menu sns...")
        sns_msg = {
              "event": "menu.status",
              "body": {
                "merchantId": merchantId,
                "menuId": menuId,
                "userId": userId,
                "status": menu_status,
                "ipAddr": ip_address,
              }
            }
        publish_sns_message(topic=config.sns_activity_logs, message=str(sns_msg),
                            subject="menu.status")
      return success()
    else:
      return not_found(body={"menuStatus": "required"})
  except Exception as e:
    create_log_data(level='[INFO]', Message=f"Error Occured: {e},IP address: {ip_address}, Token:{token}",
                    functionName="updateMerchantMenuStatus", request=request)
    return unhandled(f"Error: {e}")
    
# get all cuisine types
def getConfigOption():
  ip_address = None
  if request:
    ip_address = request.environ.get('HTTP_X_FORWARDED_FOR', request.remote_addr)
  if ip_address:
    ip_address = ip_address.split(',')[0].strip()
  try:
    config_type = request.args.get('configType')   
    create_log_data(level='[INFO]', Message=f"In the start of getConfigOption,IP address: {ip_address}",
                    functionName="getConfigOption", request=request)
    
    resp = Menus.get_config_list(configType=config_type)
    create_log_data(level='[INFO]', Message=f"Successfully getConfigOption,IP address: {ip_address}",
                    functionName="getConfigOption", request=request)
    return success(jsonify({
        "message": "success",
        "status": 200,
        "data": resp
      }))
  except Exception as e:
    create_log_data(level='[ERROR]', Message=f"Error Occured: {e},IP address: {ip_address}",
                    functionName="getConfigOption", request=request)
    return unhandled(f"Error: {e}")
  
# add new cuisine types
def addConfigOption():
  ip_address = None
  if request:
    ip_address = request.environ.get('HTTP_X_FORWARDED_FOR', request.remote_addr)
  if ip_address:
    ip_address = ip_address.split(',')[0].strip()
  try:
    create_log_data(level='[INFO]', Message=f"In the start of addConfigOption,IP address: {ip_address}",
                    functionName="addConfigOption", request=request)
    _json = request.json
    configValue = _json.get('configValue')
    configType = _json.get('configType')
    if configValue:
      checkType = Menus.find_config(configValue=configValue, configType=configType)
      if checkType:
        return invalid(f"'{configValue}' value for {configType.capitalize()} Type is already registered.")
      else:
        resp = Menus.add_config(configType= configType,configValue=configValue)
        create_log_data(level='[INFO]', Message=f"Successfully addConfigOption,IP address: {ip_address}",
                        functionName="addConfigOption", request=request)
        return success(jsonify({
            "message": "success",
            "status": 200,
            "data": resp
          }))
    else:
      return unauthorised("Config value is not availabel!")  
  except Exception as e:
    create_log_data(level='[ERROR]', Message=f"Error Occured: {e},IP address: {ip_address}",
                    functionName="addConfigOption", request=request)
    return unhandled(f"Error: {e}")