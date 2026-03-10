import boto3
from flask import jsonify, request, g
import os
import threading
from datetime import datetime, timedelta
import pytz
import time
# local imports
import config
from controllers.Middleware import validate_token_middleware
from models.Merchants import Merchants
from models.Items import Items
from models.ProductsCategories import ProductsCategories
from models.ProductsAddons import ProductsAddons
from models.VirtualMerchants import VirtualMerchants
from utilities.helpers import validateLoginToken, validateMerchantUser, success, publish_sns_message, create_log_data, get_ip_address
from utilities.errors import invalid, not_found, unhandled, unauthorised
from utilities.sns_triggers import trigger_item_update_sns_topic
from models.ItemServiceAvailability import ItemServiceAvailability

# config
sns_item_notification = config.sns_item_notification


################################################# GET

def getMerchantItems(merchantId):
  try:
    main_merchant = VirtualMerchants.get_virtual_merchant(merchantId)
    if main_merchant:
      merchantId = main_merchant[0]['merchantid']


    token = request.args.get('token')
    
    ip_address = get_ip_address(request)
    
    create_log_data(level='[INFO]', Message=f"In the start of getMerchantItems,IP address: {ip_address}, Token:{token}",
                    functionName="getMerchantItems", request=request)
    if (request.args.get('limit')):
      limit = request.args.get('limit')
    else:
      limit = "25"

    if (request.args.get('from')):
      offset = request.args.get('from')
    else:
      offset = "0"

    if (request.args.get('itemName')):
      _itemName = request.args.get('itemName')
    else:
      _itemName = None;

    productId = request.args.get('productId')

    # validate the received values
    if token and request.method == 'GET':
      userId = validateLoginToken(token)
      if not userId:
        return invalid("Invalid Token")

      if (not validateMerchantUser(merchantId, userId)):
        return unauthorised("User Not authorised to access merchant information")

      resp = Items.get_items(merchantId=merchantId, limit=limit, offset=offset, itemName=_itemName, productId=productId)
      
      if isinstance(resp, tuple) or isinstance(resp, list):
        create_log_data(level='[INFO]', Message=f"successfully getMerchantItems,IP address: {ip_address}, Token:{token}",
                    functionName="getMerchantItems", request=request)
        return success(jsonify(resp))
      else:
        return unhandled("Unhandled exception while getting merchant items")
    else:
      return not_found(params=["token"])
  except Exception as e:
    print("Error")
    create_log_data(level='[INFO]', Message=f"Error: {e},IP address: {ip_address}, Token:{token}",
                    functionName="getMerchantItems", request=request)
    print(str(e))
    return unhandled()


def CreateTopItems():
   print("start CreateTopItems scheduler")
   print("starting thread")
   # thread = threading.Thread(target=Items.generating_top_items)
   # thread.start()
   print("after starting thread")
   time.sleep(5)
   return success()
def getMerchantTopItems(merchantId):
  try:


      resp = Items.get_topitemsFromDb(merchantId)
      if resp is not None:
        return success(jsonify(resp))
      else:
        return unhandled()

  except Exception as e:
    print("ERROR: ", str(e))
    return unhandled()
def getMerchantItemById(merchantId, itemId):
  try:
    token = request.args.get('token')
    platform_pricing_tab = request.headers.get('fromtab')
    print("flag: ", platform_pricing_tab)
    # if platform_pricing_tab:
    #   pass
    # validate the received values
    if token and request.method == 'GET':
      userId = validateLoginToken(token)
      if not userId:
        return invalid("Invalid Token")

      if (not validateMerchantUser(merchantId, userId)):
        return unauthorised("User Not authorised to access merchant information")

      resp = Items.get_itemDetailsByIdFromDb(fromtab=platform_pricing_tab,itemId=itemId , isnotdefaultimage=True,merchantId=merchantId)
      if resp:
        return success(jsonify(resp))
      else:
        return unhandled()
    else:
      return not_found(params=["token"])
  except Exception as e:
    print("ERROR: ", str(e))
    return unhandled()

################################################# POST


def createMerchantitem(merchantId):
  try:
    ip_address = None
    if request:
        ip_address = request.environ.get('HTTP_X_FORWARDED_FOR', request.remote_addr)
    if ip_address:
        ip_address = ip_address.split(',')[0].strip()
        
        
    _json = request.json
    token = _json.get('token')

    create_log_data(level='[INFO]', Message=f"In the start of createMerchantitem,IP address: {ip_address}, Token:{token}",
                    functionName="createMerchantitem", request=request)

    itemSku = _json.get("item").get("itemSKU")
    itemName = _json.get("item").get("itemName")
    posName = _json.get("item").get("posName")
    shortName = _json.get("item").get("shortName")
    itemDescription = _json.get("item").get("itemDescription")
    itemPrice = _json.get("item").get("itemUnitPrice")
    itemType = _json.get("item").get("itemType")
    if itemType is None:
      itemType = 0
    taxRate = _json.get("item").get("taxRate")
    itemStatus = _json.get("item").get("itemStatus")
    if itemStatus is None:
      itemStatus = 1
    metadata = _json.get("item").get("metadata")
    itemPriceMappings = _json.get("item").get("itemPriceMappings")

    # validate the received values
    if token and itemName and itemPrice is not None and request.method == 'POST':
      userId = validateLoginToken(token)
      if not userId:
        return invalid("Invalid Token")

      if not validateMerchantUser(merchantId, userId):
        return unauthorised("User Not authorised to access merchant information")

      itemId = Items.post_item(userId=userId, merchantId=merchantId, itemSku=itemSku,
                               itemName=itemName, posName=posName, shortName=shortName, itemDescription=itemDescription,
                               itemPrice=itemPrice, itemType=itemType, taxRate=taxRate, itemStatus=itemStatus,
                               metadata=metadata, itemPriceMappings=itemPriceMappings)
      if not itemId:
        return unhandled("Unhandled exception while adding new item")

      # Triggering SNS
      create_log_data(level='[INFO]', Message=f"Successfully createMerchantitem,IP address: {ip_address}, Token:{token}",
                    functionName="createMerchantitem", request=request)

      
      eventName = "item.create"
      print(f"Triggering sns - {eventName} ...")
      sns_msg = {
        "event": eventName,
        "body": {
          "merchantId": merchantId,
          "itemId": itemId,
          "userId": userId,
          "ipAddr": ip_address
        }
      }
      sns_resp = publish_sns_message(topic=sns_item_notification, message=str(sns_msg), subject=eventName)

      resp = Items.get_itemDetailsByIdFromDb(itemId)
      
      return success(jsonify(resp))

    else:
      fields = {
        "token": "required",
        "item": {
          "itemName": "required",
          "itemUnitPrice": "required"
        }
      }
      return not_found(body=fields)
  except Exception as e:
    create_log_data(level='[INFO]', Message=f"Error: {e},IP address: {ip_address}, Token:{token}",
                    functionName="createMerchantitem", request=request)

    print("Error: ", str(e))
    return unhandled()


################################################# PUT


def uploadMerchantItemImage(merchantId, itemId):
  try:
    ip_address = None
    if request:
        ip_address = request.environ.get('HTTP_X_FORWARDED_FOR', request.remote_addr)
    if ip_address:
        ip_address = ip_address.split(',')[0].strip()
    
    token = request.form.get("token")

    if request.files:
      imageFile = request.files["image"]
      print(imageFile.filename)
      print(imageFile.content_type)
    else:
      imageFile = None

    # validate the received values
    if token and imageFile and request.method == 'PUT':

      fileType = imageFile.content_type
      if "image/" not in fileType:
        return invalid("Invalid Image Type. Require image/* file types")

      userId = validateLoginToken(token)
      if not userId:
        return invalid("Invalid Token")
      if (not validateMerchantUser(merchantId, userId)):
        return unauthorised("User Not authorised to access merchant information")

      uploadResp = Items.upload_itemImage(userId=userId, merchantId=merchantId, itemId=itemId, imageFile=imageFile)
      if type(uploadResp) is str:

        # Triggering Item SNS - item.image_update
        print("Triggering item sns - item.image_update ...")
        sns_msg = {
          "event": "item.image_update",
          "body": {
            "merchantId": merchantId,
            "itemId": itemId,
            "userId": userId,
            "ipAddr":ip_address
          }
        }
        sns_resp = publish_sns_message(topic=sns_item_notification, message=str(sns_msg), subject="item.image_update")

        respItem = Items.get_itemDetailsByIdFromDb(itemId)
        return success(jsonify(respItem))
      else:
        return unhandled("Unhandled exception while uploading image")
    else:
      return not_found(form=["token", "image"])
  except Exception as e:
    print("Error")
    print(str(e))
    return unhandled()


def deleteMerchantItemImage(merchantId, itemId):
  try:
    token = request.args.get("token")
    ip_address = None
    if request:
        ip_address = request.environ.get('HTTP_X_FORWARDED_FOR', request.remote_addr)
    if ip_address:
        ip_address = ip_address.split(',')[0].strip()
    create_log_data(level='[INFO]', Message=f"In the start of deleting item image , IP address:{ip_address}, Token:{token}",
                    functionName="deleteMerchantItemImage", request=request)
    

    if token and request.method == 'DELETE':
      userId = validateLoginToken(token)
      if not userId:
        create_log_data(level='[ERROR]',
                        Message="The API token is invalid.",
                        messagebody=f"Unable to find the user on the basis of provided token., IP address:{ip_address}, Token:{token}",
                        functionName="deleteMerchantItemImage", request=request, statusCode="400 Bad Request")
        return invalid("Invalid Token")

      if (not validateMerchantUser(merchantId, userId)):
        create_log_data(level='[INFO]', Message=f"User Not authorised to access merchant information, IP address:{ip_address}, Token:{token}",
                        functionName="deleteMerchantItemImage", request=request, statusCode="400 Bad Request"
                        )
        return unauthorised("User Not authorised to access merchant information")

      item_details = Items.get_item_by_id(itemId)
      create_log_data(level='[INFO]', Message=f"Successfully retrieved item information, IP address:{ip_address}, Token:{token}",
                      functionName="deleteMerchantItemImage", request=request, statusCode="200 OK"
                      )
      deleteResp = Items.delete_itemImage(itemId=itemId)
      if deleteResp:

        # Triggering Item SNS - item.image_delete
        print("Triggering item sns - item.image_delete ...")
        sns_msg = {
          "event": "item.image_delete",
          "item_details":item_details,
          "body": {
            "merchantId": merchantId,
            "itemId": itemId,
            "userId": userId,
            "ipAddr":ip_address
          }
        }
        sns_resp = publish_sns_message(topic=sns_item_notification, message=str(sns_msg), subject="item.image_delete")
        # publish_sns_message(topic=config.sns_activity_logs, message=str(sns_msg),
        #                     subject="item.image_delete")
        print(f'{item_details["itemName"]} image deleted')
        create_log_data(level='[INFO]', Message=f'Successfully deleted item {item_details["itemName"]} image, Token:{token}',
                        functionName="deleteMerchantItemImage", request=request, statusCode="200 OK"
                        )

        return success()
      else:
        create_log_data(level='[ERROR]', Message=f'Failed to delete item {item_details["itemName"]} image, Token:{token}',
                        functionName="deleteMerchantItemImage", request=request, statusCode="400 Bad Request"
                        )
        return unhandled("Unhandled exception while deleting item image")
    else:
      return not_found(params=["token"])
  except Exception as e:
    print("Error: ", str(e))
    create_log_data(level='[ERROR]', Message=f'Failed to delete item image',
                    messagebody=f"An error occured while deleting item image : {str(e)}",
                    functionName="deleteMerchantItemImage", request=request, statusCode="400 Bad Request"
                    )
    return unhandled()


def updateMerchantItem(merchantId, itemId):
  try:
    
    ip_address = None
    if request:
        ip_address = request.environ.get('HTTP_X_FORWARDED_FOR', request.remote_addr)
    if ip_address:
        ip_address = ip_address.split(',')[0].strip()
        
    _json = request.json
    token = _json.get('token')
    item = _json.get("item")

    create_log_data(level='[INFO]', Message=f"In the start of updateMerchantItem,IP address: {ip_address}, Token:{token}",
                    functionName="updateMerchantItem", request=request)


    itemSku = _json.get("item").get("itemSKU")
    itemName = _json.get("item").get("itemName")
    posName = _json.get("item").get("posName")
    shortName = _json.get("item").get("shortName")
    itemDescription = _json.get("item").get("itemDescription")
    itemPrice = _json.get("item").get("itemUnitPrice")
    itemType = _json.get("item").get("itemType")
    taxRate = _json.get("item").get("taxRate")
    itemStatus = _json.get("item").get("itemStatus")
    metadata = _json.get("item").get("metadata")
    itemPriceMappings = _json.get("item").get("itemPriceMappings")

    # validate the received values
    if token and item and request.method == 'PUT':
      userId = validateLoginToken(token)
      if not userId:
        return invalid("Invalid Token")

      if not validateMerchantUser(merchantId, userId):
        return unauthorised("User Not authorised to access merchant information")

      old_item_details = Items.get_item_by_id(itemId)
      if not old_item_details:
        return invalid("item id is invalid!")

      updResp = Items.put_itemById(userId=userId, merchantId=merchantId, itemId=itemId,
                                   itemSku=itemSku, itemName=itemName, posName=posName, shortName=shortName, itemDescription=itemDescription,
                                   itemPrice=itemPrice, itemType=itemType, taxRate=taxRate, itemStatus=itemStatus,
                                   metadata=metadata, itemPriceMappings=itemPriceMappings)

      if updResp:

        create_log_data(level='[INFO]', Message=f"Successfully updateMerchantItem,IP address: {ip_address}, Token:{token}",
                    functionName="updateMerchantItem", request=request)
        sns_msg = {
          "event": "item.update",
          "body": {
            "merchantId": merchantId,
            "itemId": itemId,
            "userId": userId,
            "unchanged": None,
            "oldItemStatus": old_item_details['itemStatus'],
            "old_item_details": old_item_details,
            "source":"Fonda",
            "ipAddr": ip_address
          }
        }
        sns_resp = publish_sns_message(topic=config.sns_item_notification, message=str(sns_msg), subject="item.update")

        getResp = Items.get_itemDetailsByIdFromDb(itemId)
        getResp['hoursList'] = ItemServiceAvailability.get_serviceAvailabilityByitemId(itemId=itemId)
        return success(jsonify(getResp))
      else:
        return unhandled("Unhandled exception while updating the item")
    else:
      fields = {
        "token": "required",
        "item": {}
      }
      return not_found(body=fields)
  except Exception as e:
    create_log_data(level='[INFO]', Message=f"Error:{e},IP address: {ip_address}, Token:{token}",
                    functionName="updateMerchantItem", request=request)
    return unhandled(f"Error: {str(e)}")


@validate_token_middleware
def updateMerchantCategoryStatus(merchantId, categoryId):
  ip_address = None
  if request:
      ip_address = request.environ.get('HTTP_X_FORWARDED_FOR', request.remote_addr)
  if ip_address:
      ip_address = ip_address.split(',')[0].strip()
  
  _json = request.json
  category_status = _json.get('categoryStatus')
  if category_status is not None and request.method == 'PUT':
    userId = g.userId
    if (not validateMerchantUser(merchantId, userId)):
      return unauthorised("User Not authorised to access merchant information")
    items_to_update=Items.update_category_and_item_status(merchant_id=merchantId, category_id=categoryId, category_status=category_status, userId=userId)
    sns_client = boto3.client('sns')
    for item in items_to_update:
      # Triggering SNS
      eventName = "item.status_change"
      print(f"Triggering sns - {eventName} ...")
      sns_msg = {
        "event": eventName,
        "body": {
          "merchantId": merchantId,
          "itemId": item["id"],
          "userId": userId,
          "ipAddr":ip_address
        }
      }
      publish_sns_message(topic=sns_item_notification, message=str(sns_msg), subject=eventName,sns_client=sns_client)
    category_items=ProductsCategories.get_category_items(categoryId)
    return success(jsonify(category_items))
  else:
    return not_found(body={"categoryStatus": "required"})


@validate_token_middleware
def updateMerchantItemStatus(merchantId, itemId):
  try:

    ip_address = None
    if request:
        ip_address = request.environ.get('HTTP_X_FORWARDED_FOR', request.remote_addr)
    if ip_address:
        ip_address = ip_address.split(',')[0].strip()
    
    _json = request.json
    itemStatus = _json.get('itemStatus')

    itemPauseType = _json.get('itemPauseType')
    token= g.token
    create_log_data(level='[INFO]', Message=f"In the start of updateMerchantItemStatus,IP address: {ip_address}, Token:{token}",
                    functionName="updateMerchantItemStatus", request=request)
    
    if itemStatus is not None and request.method == 'PUT':
      userId = g.userId
      if (not validateMerchantUser(merchantId, userId)):
        return unauthorised("User Not authorised to access merchant information")

      itemResumeTime = None
      if itemStatus==0:
        if itemPauseType=='today':
          merchantDetail = Merchants.get_merchant_by_id(merchantId)
          merchantOpeningHours = Merchants.get_merchant_opening_hours_by_id(merchantId)
          merchant_tz = pytz.timezone(merchantDetail['timezone'])
          utc_tz = pytz.timezone('UTC')

          now = datetime.now(merchant_tz)
          nextDay = now + timedelta(days=1)
          nextDayName = nextDay.strftime("%A")
          if merchantOpeningHours and nextDayName is not None:
            for merchantOpeningHoursTemp in merchantOpeningHours:
              if itemResumeTime is None and merchantOpeningHoursTemp['day'].lower()==nextDayName.lower():
                businessOpeningTime = merchantOpeningHoursTemp['opentime']
                if businessOpeningTime is None or businessOpeningTime=='':
                  businessOpeningTime = '12:30 AM'
                businessOpeningTime = '12:30 AM'
                itemResumeTimeString = nextDay.strftime("%Y-%m-%d")+" "+businessOpeningTime
                itemResumeTimeDT = datetime.strptime(itemResumeTimeString, "%Y-%m-%d %I:%M %p")

                merchant_time = merchant_tz.localize(itemResumeTimeDT)

                utc_time = merchant_time.astimezone(utc_tz)

                itemResumeTime = utc_time.strftime("%Y-%m-%d %H:%M:%S")
      updated = Items.update_item_status(merchantId=merchantId, itemId=itemId, itemStatus=itemStatus, itemPauseType=itemPauseType, itemResumeTime=itemResumeTime, userId=userId)
      if updated:
        
        create_log_data(level='[INFO]', Message=f"Successfully updateMerchantItemStatus,IP address: {ip_address}, Token:{token}",
                    functionName="updateMerchantItemStatus", request=request)
        # Triggering SNS
        eventName = "item.status_change"
        print(f"Triggering sns - {eventName} ...")
        sns_msg = {
          "event": eventName,
          "body": {
            "merchantId": merchantId,
            "itemId": itemId,
            "userId": userId,
            "itemStatus": itemStatus,
            "ipAddr": ip_address
          }
        }
        sns_resp = publish_sns_message(topic=sns_item_notification, message=str(sns_msg), subject=eventName)
        getResp = Items.get_itemDetailsByIdFromDb(itemId)
        return success(jsonify(getResp))
      else:
        return unhandled()
    else:
      return not_found(body={"itemStatus": "required"})
  except Exception as e:
    create_log_data(level='[INFO]', Message=f"Error:{e},IP address: {ip_address}, Token:{token}",
                    functionName="updateMerchantItemStatus", request=request)
    print("Error: ", str(e))
    return unhandled()

def updateMerchantItemStatusInternal(merchantId, itemId, _json, userId='8c1e409d-5c17-4def-8f86-6c092aceb2b4'):
  try:

    itemStatus = _json.get('itemStatus')

    itemPauseType = _json.get('itemPauseType')

    if itemStatus is not None:
      itemResumeTime = None
      if itemStatus==0:
        if itemPauseType=='today':
          merchantDetail = Merchants.get_merchant_by_id(merchantId)
          merchantOpeningHours = Merchants.get_merchant_opening_hours_by_id(merchantId)
          merchant_tz = pytz.timezone(merchantDetail['timezone'])
          utc_tz = pytz.timezone('UTC')

          now = datetime.now(merchant_tz)
          nextDay = now + timedelta(days=1)
          nextDayName = nextDay.strftime("%A")
          if merchantOpeningHours and nextDayName is not None:
            for merchantOpeningHoursTemp in merchantOpeningHours:
              if itemResumeTime is None and merchantOpeningHoursTemp['day'].lower()==nextDayName.lower():
                businessOpeningTime = merchantOpeningHoursTemp['opentime']
                if businessOpeningTime is None or businessOpeningTime=='':
                  businessOpeningTime = '12:30 AM'
                businessOpeningTime = '12:30 AM'
                itemResumeTimeString = nextDay.strftime("%Y-%m-%d")+" "+businessOpeningTime
                itemResumeTimeDT = datetime.strptime(itemResumeTimeString, "%Y-%m-%d %I:%M %p")

                merchant_time = merchant_tz.localize(itemResumeTimeDT)

                utc_time = merchant_time.astimezone(utc_tz)

                itemResumeTime = utc_time.strftime("%Y-%m-%d %H:%M:%S")
      updated = Items.update_item_status(merchantId=merchantId, itemId=itemId, itemStatus=itemStatus, itemPauseType=itemPauseType, itemResumeTime=itemResumeTime, userId=userId)
      if updated:
        # Triggering SNS
        eventName = "item.status_change"
        print(f"Triggering sns - {eventName} ...")
        sns_msg = {
          "event": eventName,
          "body": {
            "merchantId": merchantId,
            "itemId": itemId,
            "userId": userId
          }
        }
        sns_resp = publish_sns_message(topic=sns_item_notification, message=str(sns_msg), subject=eventName)
        # logs_sns_resp = publish_sns_message(topic=config.sns_activity_logs, message=str(sns_msg), subject=eventName)

        getResp = Items.get_itemDetailsByIdFromDb(itemId)
        return success(jsonify(getResp))
      else:
        return unhandled()
    else:
      return not_found(body={"itemStatus": "required"})
  except Exception as e:
    print("Error: ", str(e))
    return unhandled()


################################################# DELETE


def deleteMerchantItem(merchantId, itemId):
  try:
    token = request.args.get("token")
    ip_address = None
    if request:
        ip_address = request.environ.get('HTTP_X_FORWARDED_FOR', request.remote_addr)
    if ip_address:
        ip_address = ip_address.split(',')[0].strip()
    
    create_log_data(level='[INFO]', Message=f"In the start of deleting item, IP address:{ip_address}, Token:{token}",
                    functionName="deleteMerchantItem", request=request)
    

    if token and request.method == 'DELETE':
      userId = validateLoginToken(token)
      if not userId:
        create_log_data(level='[ERROR]',
                        Message="The API token is invalid.",
                        messagebody=f"Unable to find the user on the basis of provided token., IP address:{ip_address}, Token:{token}",
                        functionName="deleteMerchantItem", request=request, statusCode="400 Bad Request")
        return invalid("Invalid Token")
      if (not validateMerchantUser(merchantId, userId)):
        create_log_data(level='[INFO]', Message=f"User Not authorised to access merchant information, IP address:{ip_address}, Token:{token}",
                        functionName="deleteMerchantItem", request=request, statusCode="400 Bad Request"
                        )
        return unauthorised("User Not authorised to access merchant information")

      item_details = Items.get_item_by_id(itemId)
      create_log_data(level='[INFO]', Message=f"Successfully retrieved item information, IP address:{ip_address}, Token:{token}",
                      functionName="deleteMerchantItem", request=request, statusCode="200 OK"
                      )

      addons_ids_list = list()
      product_addons = ProductsAddons.get_item_addon(itemId=itemId)
      for pa in product_addons:
        addons_ids_list.append(pa['addonid'])

      create_log_data(level='[INFO]', Message=f"Successfully make addon ids list, IP address:{ip_address}, Token:{token}",
                      messagebody=addons_ids_list,
                      functionName="deleteMerchantItem", request=request, statusCode="200 OK"
                      )
      delResp = Items.delete_item(itemId)
      ItemServiceAvailability.delete_serviceAvailabilityByitemId(itemId=itemId)
      if delResp:

        # Triggering Item SNS - item.delete
        print("Triggering item sns - item.delete ...")
        sns_msg = {
          "event": "item.delete",
          "body": {
            "merchantId": merchantId,
            "itemId": itemId,
            "userId": userId,
            "addons_ids_list": addons_ids_list,
            "item_details": item_details,
            "ipAddr":ip_address
          }
        }
        sns_resp = publish_sns_message(topic=sns_item_notification, message=str(sns_msg), subject="item.delete")
        # publish_sns_message(topic=config.sns_activity_logs, message=str(sns_msg),
        #                     subject="item.delete")
        print(sns_msg.get("body")["item_details"]["itemName"])
        create_log_data(level='[INFO]', Message=f'Successfully deleted item {item_details["itemName"]}, IP address:{ip_address}, Token:{token}',
                        functionName="deleteMerchantItem", request=request, statusCode="200 OK"
                        )

        return success()
      else:
        create_log_data(level='[ERROR]', Message=f'Failed to delete item {item_details["itemName"]}, IP address:{ip_address}, Token:{token}',
                        functionName="deleteMerchantItem", request=request, statusCode="400 Bad Request"
                        )
        return unhandled()
    else:
      create_log_data(level='[ERROR]', Message=f'Token not found in the request, IP address:{ip_address}, Token:{token}',
                     functionName="deleteMerchantItem", request=request, statusCode="400 Bad Request"
                     )
      return not_found(params=["token"])
  except Exception as e:
    print("Error: ", str(e))
    create_log_data(level='[ERROR]', Message=f'Failed to delete item ',
                    messagebody=f"An error occured while deleting item: {str(e)}, Token:{token}",
                    functionName="deleteMerchantItem", request=request, statusCode="400 Bad Request"
                    )
    return unhandled()

  ############################################### CSV


@validate_token_middleware
def generateItemsCsv(merchantId):
  try:
    userId = g.userId
    itemType = request.args.get("itemType")
    ip_address = get_ip_address(request)
    token = g.token
    create_log_data(level='[INFO]', Message=f"In the start of generateItemsCsv,IP address: {ip_address}, Token:{token}",
                    functionName="generateItemsCsv", request=request)
    if not validateMerchantUser(merchantId, userId):
      return unauthorised("User Not authorised to access merchant information")

    return Items.generate_items_csv(merchantId,itemType)

  except Exception as e:
    create_log_data(level='[INFO]', Message=f"Error:{e},IP address: {ip_address}, Token:{token}",
                    functionName="generateItemsCsv", request=request)
    print("Error: ", str(e))
    return unhandled()


@validate_token_middleware
def uploadItemsPriceMappings(merchantId):
  try:

    userId = g.userId
    token = g.token
    ip_address = get_ip_address(request)
    
    create_log_data(level='[INFO]', Message=f"In the start of uploadItemsPriceMappings,IP address: {ip_address}, Token:{token}",
                    functionName="uploadItemsPriceMappings", request=request)
    if not validateMerchantUser(merchantId, userId):
      return unauthorised("User Not authorised to access merchant information")

    if request.files and request.form.get("platformTypes"):
      platformTypes = request.form.get("platformTypes")
      csvFile = request.files.get("csvFile")
      print(csvFile.filename)
      print(csvFile.content_type)
      print(platformTypes)

      # fileType = csvFile.content_type
      # if fileType != "text/csv":
      #   return invalid("Invalid file type. Require text/csv file type!")

      return Items.upload_items_price_mappings(csvFile=csvFile, platformTypes=platformTypes, merchantId=merchantId, userId=userId , ip_address=ip_address)

    else:
      return not_found(form="csv or platforms_list not provided")
  except Exception as e:
    create_log_data(level='[INFO]', Message=f"Error:{e},IP address: {ip_address}, Token:{token}",
                    functionName="uploadItemsPriceMappings", request=request)
    print("Error: ", str(e))
    return unhandled()


def add_and_assign_suggest_item(merchantId, categoryId):
  try:
      _json = request.json
      token = _json.get('token')
      items= _json.get('items')
      print(_json)
      # validate the received values
      if request.method == 'POST':
        userId = validateLoginToken(token)
        if not userId:
          return invalid("Invalid Token")

        if not validateMerchantUser(merchantId, userId):
          return unauthorised("User Not authorised to access merchant information")
        if items:
          for item in items:
            itemId = Items.assign_and_add_suggest_item(item,merchantId,userId,categoryId)

        return success()
  except Exception as e:
        print("Error: ", str(e))
        return unhandled()

@validate_token_middleware
def createItemServiceAvailability(merchantId, itemId):
  try:
    
    ip_address = None
    if request:
        ip_address = request.environ.get('HTTP_X_FORWARDED_FOR', request.remote_addr)
    if ip_address:
        ip_address = ip_address.split(',')[0].strip()
    _json = request.json
    availability = _json.get("serviceAvailability")
    item_details = Items.get_item_by_id(itemId)

    if request.method == "POST":
      userId = g.userId
      if not validateMerchantUser(merchantId, userId):
        create_log_data(level='[ERROR]', Message=f"User Not authorised to access merchant information,IP address: {ip_address}",
                    functionName="createItemServiceAvailability", request=request)
        return unauthorised("User Not authorised to access merchant information")

      getservice =  ItemServiceAvailability.get_serviceAvailabilityByitemId(itemId=itemId)
      if len(availability) != 0:
        if getservice:
          array1_ids = {obj.get('id') for obj in getservice}
          array2_ids = {obj.get('id') for obj in availability}
          missing_ids = array1_ids - array2_ids
          if missing_ids:
            for id in missing_ids:
              ItemServiceAvailability.delete_serviceAvailabilityById(id)
          for row in availability:            
            if row.get('id'):
              update_item_scheduler=ItemServiceAvailability.put_serviceAvailabilityById(id=row.get('id'), endTime=row.get('endTime'),startTime=row.get("startTime"),weekDay=row.get("weekDay"),timezone=row.get("timezone"),itemId=itemId,groupDays=row.get('groupDays'),merchantId=merchantId)
              if not update_item_scheduler:
                return invalid('An error occurred while updating the scheduler.')
            else:
              create_item_scheduler=ItemServiceAvailability.post_serviceAvailability(itemId, [row],merchantId)
              if not create_item_scheduler:
                return invalid('An error occurred while creating the scheduler.')
        else:
          create_item_scheduler=ItemServiceAvailability.post_serviceAvailability(itemId, availability,merchantId)
          if not create_item_scheduler:
            return invalid('An error occurred while creating the scheduler.')
      else:
        if getservice:
          for row in getservice:
              if row.get('id'):
                ItemServiceAvailability.delete_serviceAvailabilityById(row.get('id'))
      
      getservice_updated = ItemServiceAvailability.get_serviceAvailabilityByitemId(itemId=itemId)
      messagebody = addedUpdatedDeletedFields(getservice, getservice_updated)
      if messagebody:
        messagebody = 'For ' + item_details.get('itemName') +', ' + messagebody
        messagebody += f", IP address:{ip_address}"
        print("Triggering item sns - item.hours_change ...")
        sns_msg = {
          "event": "item.hours_change",
          "body": {
            "merchantId": merchantId,
            "itemId": itemId,
            "userId": userId,
            "event_details": messagebody
          }
        }
        print(item_details,config.sns_activity_logs)
        sns_resp = publish_sns_message(topic=config.sns_activity_logs, message=str(sns_msg), subject="item.hours_change")

        create_log_data(level='[INFO]', Message=f"Item service availability add/updated successfully,IP address: {ip_address}",
                    functionName="createItemServiceAvailability", request=request)

      return success(jsonify({
        "message": "success",
        "status": 200
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

      create_log_data(level='[ERROR]', Message=f"Item service availability fields are missing,IP address: {ip_address}",
                    functionName="createItemServiceAvailability", request=request)
      return not_found(body=fields)
  except Exception as e:
    print("Error: ", str(e))
    create_log_data(level='[ERROR]', Message=f"ERROR: {str(e)},IP address: {ip_address}",
                    functionName="createItemServiceAvailability", request=request)
    return unhandled()


@validate_token_middleware
def getItemServiceAvailability(merchantId, itemId):
  try:
    if request.method == "GET":
      userId = g.userId
      if (not validateMerchantUser(merchantId, userId)):
        return unauthorised("User Not authorised to access merchant information")

      getResp = ItemServiceAvailability.get_serviceAvailabilityByitemId(itemId=itemId)

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
          if key=='id' or key == 'deactivateSchedulerID' or key == 'activateSchedulerID':
            continue
          if beforeUpdatedData[key] != afterUpdatedData[key]:
            keyName = key
            if key == 'starttime' or key == 'startTime':
              keyName = 'Start Time'
            if key == 'endtime' or key == 'endTime':
              keyName = 'End Time'
            different_values.append(f"{keyName} <{beforeUpdatedData[key]}> to <{afterUpdatedData[key]}>")
        if different_values:
          updated_fields.append(f"{day_mapping[beforeUpdatedData['weekDay']]}: {','.join(different_values)}")
        break
    else:
      updated_fields.append(f"{day_mapping[beforeUpdatedData['weekDay']]} item hours deleted ")
  for afterUpdatedData in afterupdated:
      if afterUpdatedData["weekDay"] not in days_before_updating:
        updated_fields.append(f"{day_mapping[afterUpdatedData['weekDay']]} added: Start Time {afterUpdatedData['startTime']}, End Time {afterUpdatedData['endTime']}")
  print(updated_fields)
  return ','.join(updated_fields)
