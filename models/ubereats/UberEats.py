from flask import request
import json
import requests
import datetime
import uuid

# local imports
import config
from models.Addons import Addons
import models.Platforms  # we have to avoid circular imports
from utilities.helpers import get_db_connection
from models.MenuCategories import MenuCategories
from models.Items import Items
from models.MenuMappings import MenuMappings
from models.Categories import Categories
from models.Merchants import Merchants
from models.Metadata import Metadata
from utilities.helpers import *


class UberEats():

  ############################################### UBEREATS STORE API

  @classmethod
  def ubereats_set_restaurant_status(cls, storeId, accessToken, payload):
    try:
      url = f"https://api.uber.com/v1/delivery/store/{storeId}/update-store-status"
      headers = {
        'Authorization': f'Bearer {accessToken}',
        'Content-Type': 'application/json'
      }
      # {
      #   "status": "PAUSED" or "ONLINE",
      #   "paused_until": "2022-01-06T03:02:11.999+12:00",
      #   "reason": "Store is unable to accept orders"
      # }
      response = requests.request("POST", url, headers=headers, data=payload)
      print(response.text)
      resp = True if response and response.status_code >= 200 and response.status_code < 300 else False
      return resp
    except Exception as e:
      print("Error UberEats: ", str(e))
      return False


  ############################################### UBEREATS MENU

  @classmethod
  def ubereats_post_menu(cls, storeId, accessToken, payload):
    try:
      url = f"https://api.uber.com/v2/eats/stores/{storeId}/menus"
      headers = {
        'Authorization': f'Bearer {accessToken}',
        'Content-Type': 'application/json'
      }
      response = requests.request("PUT", url, headers=headers, data=payload)
      print(response.text)
      if response and response.status_code >= 200 and response.status_code < 300:
        return True, response.status_code, "success"
      else:
        return False, response.status_code, response.json()
    except Exception as e:
      print("Error UberEats: ", str(e))
      return False, 500, str(e)
  

  @classmethod
  def ubereats_update_item(cls, storeId, itemId, accessToken, payload):
    try:
      url = f"https://api.uber.com/v2/eats/stores/{storeId}/menus/items/{itemId}"
      headers = {
        'Authorization': f'Bearer {accessToken}',
        'Content-Type': 'application/json'
      }
      response = requests.request("POST", url, headers=headers, data=payload)
      print(response.text)
      if response and response.status_code >= 200 and response.status_code < 300:
        return True, response.status_code, "success"
      else:
        return False, response.status_code, response.text
    except Exception as e:
      print("Error UberEats: ", str(e))
      return False, 500, str(e)
  
  ############################################### PROVISIONING / DE-PROVISIONING

  @classmethod
  def ubereats_deprovision_pos(cls, storeId, accessToken):
    try:
      create_log_data(level='[INFO]',
                      Message="In the beginning of ubereats_deprovision_pos code function to disconnect platform ",
                      messagebody=f"storeId : {storeId}" ,  functionName="ubereats_deprovision_pos" , request=request)
      url = f"https://api.uber.com/v1/eats/stores/{storeId}/pos_data"
      headers = {
        'Authorization': f'Bearer {accessToken}',
        'Content-Type': 'application/json'
      }
      response = requests.request("DELETE", url, headers=headers)
      create_log_data(level='[INFO]',
                      Message="Response from remove integration from uber eats ",
                      messagebody=response ,  functionName="ubereats_deprovision_pos" ,  request=request)
      data = True if response and response.status_code >= 200 and response.status_code < 300 else False
      return data
    except Exception as e:
      print("Error UberEats: ", str(e))
      return False


  ############################################### ACCESS TOKEN

  @classmethod
  def ubereats_refresh_access_token(cls, refresh_token):
    try:
      url = "https://login.uber.com/oauth/v2/token"
      headers = {
        'Content-Type': 'application/x-www-form-urlencoded' }
      
      grantType = "refresh_token"
      
      payload='client_id='+config.uber_client_id+'&' \
        'client_secret='+config.uber_client_secret+'&'+ \
        'grant_type='+grantType+'&' \
        'refresh_token='+refresh_token
      
      response = requests.request("POST", url, headers=headers, data=payload)
      data = response.json() if response and response.status_code >= 200 and response.status_code < 300 else False
      return data
    except Exception as e:
      print("Error UberEats: ", str(e))
      return False


  @classmethod
  def ubereats_generate_access_token(cls, grantType=None, scope=None):
    try:
      create_log_data(level='[INFO]',
                      Message="In the beginning of ubereats_generate_access_token",
                       functionName="ubereats_generate_access_token")
      url = "https://login.uber.com/oauth/v2/token"
      headers = {
        'Content-Type': 'application/x-www-form-urlencoded' }
      
      scope = "eats.order eats.store eats.store.orders.cancel eats.store.status.write eats.store.orders.read " if scope is None else scope
      grantType = "client_credentials" if grantType is None else grantType
      
      payload='client_id='+config.uber_client_id+'&' \
        'client_secret='+config.uber_client_secret+'&'+ \
        'grant_type='+grantType+'&' \
        'scope='+scope
      create_log_data(level='[INFO]',
                      Message="Before call the API to get access token",
                      messagebody=f"payload : {payload}", functionName="ubereats_check_and_get_access_token")
      response = requests.request("POST", url, headers=headers, data=payload)
      create_log_data(level='[INFO]',
                      Message="Response from the API to get access token",
                      messagebody=response, functionName="ubereats_check_and_get_access_token")
      print(response.text)
      data = response.json() if response and response.status_code >= 200 and response.status_code < 300 else False
      return data
    except Exception as e:
      print("Error UberEats: ", str(e))
      return False
  

  @classmethod
  def ubereats_check_and_get_access_token(cls, key: str = "ubereats_access_token"):
    try:
      """
        keys: ubereats_access_token, ubereats_reports_token
      """
      create_log_data(level='[INFO]',
                      Message="In the beginning of ubereats_check_and_get_access_token",
                      messagebody=key, functionName="ubereats_check_and_get_access_token")
      # get/create access_token in metadata table
      token_details = Metadata.get_metadata_by_key(key)

      # if token_details exists and everything goes well like token is not epired yet
      if token_details:
        create_log_data(level='[INFO]',
                        Message="Retrieve token detail from meta data successfully",
                        messagebody=token_details, functionName="ubereats_check_and_get_access_token")
        updated_datetime = token_details.get('updated_datetime')
        if isinstance(updated_datetime, datetime.datetime):
          current_datetime = datetime.datetime.now()
          duration = current_datetime - updated_datetime
          duration_days = duration.days
          if duration_days < 15:
            accessToken = token_details.get('value')
            print('access_token is valid')
            create_log_data(level='[INFO]',
                            Message="Access token is valid and not expired yet",
                            messagebody=accessToken, functionName="ubereats_check_and_get_access_token")
            return accessToken
      
      # if token_details not exists or token is expired then generate new one and store in metadata
      print('refreshing access token...')
      create_log_data(level='[INFO]',
                      Message="In token_detail access token is invalid so start generating new access token ",
                      messagebody=key, functionName="ubereats_check_and_get_access_token")
      if key == "ubereats_reports_token":
        resp = cls.ubereats_generate_access_token(scope="eats.store eats.report ")
      else:
        resp = cls.ubereats_generate_access_token()

      if not resp:
        create_log_data(level='[ERROR]',
                        Message="Error in generating  access token ",
                        messagebody=resp, functionName="ubereats_check_and_get_access_token")
        return False

      updated = Metadata.update_metadata_by_key(key=key, value=resp.get('access_token'))
      create_log_data(level='[INFO]',
                      Message="Successfully generate access token  ",
                      messagebody=resp, functionName="ubereats_check_and_get_access_token")
      return resp.get('access_token')
    except Exception as e:
      print("Error UberEats: ", str(e))
      create_log_data(level='[INFO]',
                      Message="Exception occured on get ubereast access token",
                      messagebody=str(e), functionName="ubereats_check_and_get_access_token")
      return False
  
  ############################################### POS PROVISION APIS
  '''
    Below APIs require token with (eats.pos_provisioning) scope
  '''

  @classmethod
  def ubereats_list_stores(cls, accessToken):
    try:
      create_log_data(level='[INFO]',
                      Message="In the beginning of ubereats_list_stores code function to get the list of ubereats store",
                      messagebody=f"access token is : {accessToken}" ,
                       functionName="ubereats_list_stores")
      next_page_token = None
      stores = list()

      while True:

        url = "https://api.uber.com/v1/delivery/stores"

        payload = {
          "Page_size": 10
        }
        if next_page_token:
          payload['next_page_token']=next_page_token

        headers = {
          'Authorization': 'Bearer ' + accessToken,
          'Content-Type': 'application/json'
        }
        payload=json.dumps(payload)
        response = requests.request("GET", url, headers=headers, data=payload)
        create_log_data(level='[INFO]',
                        Message="Response from get list stores API", messagebody=response.text, functionName="ubereats_list_stores")
        if response and response.status_code >= 200 and response.status_code < 300:
          response = response.json()
          stores.extend(response.get("stores"))

          if response.get("pagination_data") and "next_page_token" in response.get("pagination_data"):
            next_page_token = response['pagination_data']['next_page_token']
          else:
            break
        else:
          return False
        
      return stores
    except Exception as e:
      print("Error UberEats: ", str(e))
      create_log_data(level='[ERROR]',
                      Message="Exception occured. Failed to get the list of ubereats store",
                      messagebody=str(e) ,
                       functionName="ubereats_list_stores")
      return False
  

  @classmethod
  def ubereats_setup_pos_integration(cls, merchantId, storeId, accessToken):
    try:
      create_log_data(level='[INFO]',
                      Message="In the beginning of ubereats_setup_pos_integration code function to connect /activate integration with uber eats",
                      merchantID=merchantId, functionName="ubereats_setup_pos_integration" , request=request)
      url = f"https://api.uber.com/v1/eats/stores/{storeId}/pos_data"
      payload = json.dumps(
        {
          "allowed_customer_requests": {
            "allow_single_use_items_requests": False,
            "allow_special_instruction_requests": False
          },
          "integrator_brand_id": "XiE9fVR5Y8cxiy-QjxXsrUCL4joyzAmu",
          "integrator_store_id": merchantId,
          "is_order_manager": True,
          "merchant_store_id": merchantId,
          "require_manual_acceptance": False,
          "store_configuration_data": "string",
          "webhooks_config": {
            "order_release_webhooks": {
              "is_enabled": True
            },
            "schedule_order_webhooks": {
              "is_enabled": True
            },
            "delivery_status_webhooks": {
              "is_enabled": True
            }
          }
        }
      )
      create_log_data(level='[INFO]',
                      Message="Payload for call the activate integration API ",messagebody=payload,
                      merchantID=merchantId, functionName="ubereats_setup_pos_integration" , request=request)
      headers = {
        'Authorization': 'Bearer ' + accessToken,
        'Content-Type': 'application/json'
      }
      response = requests.request("POST", url, headers=headers, data=payload)
      create_log_data(level='[INFO]',
                      Message="Response from activate integration API", messagebody=response.text,
                      merchantID=merchantId, functionName="ubereats_setup_pos_integration", request=request)
      provisioned = True if response and response.status_code >= 200 and response.status_code < 300 else False
      print(" provisioned : ", provisioned)
      if provisioned:
        create_log_data(level='[INFO]',
                        Message="After successfull response from activate integration  now begingng for update integration configuration API ", messagebody=response.text,
                        merchantID=merchantId, functionName="ubereats_setup_pos_integration")
        resp = cls.ubereats_generate_access_token()
        if not resp:
          create_log_data(level='[ERROR]',
                          Message="Failed to generate access token",
                          merchantID=merchantId, functionName="ubereats_setup_pos_integration", request=request)
          print("error in generate access token")
          return False
        create_log_data(level='[INFO]',
                        Message="Successfull generate token",
                        messagebody=resp,
                        merchantID=merchantId, functionName="ubereats_setup_pos_integration", request=request)
        accessToken = resp.get('access_token')
        print("eats.store access token", accessToken)
        headers = {
          'Authorization': 'Bearer ' + accessToken,
          'Content-Type': 'application/json'
        }
        payload_dict = json.loads(payload)
        payload_dict["integration_enabled"] = True
        updated_payload_json = json.dumps(payload_dict)
        create_log_data(level='[INFO]',
                        Message="Payload for call the update integration configuration API ", messagebody=payload,
                        merchantID=merchantId, functionName="ubereats_setup_pos_integration", request=request)
        response = requests.request("PATCH", url, headers=headers, data=updated_payload_json)
        create_log_data(level='[INFO]',
                        Message="Response from update integration configuration API ", messagebody=response.text,
                        merchantID=merchantId, functionName="ubereats_setup_pos_integration", request=request)
        provisioned = True if response and response.status_code >= 200 and response.status_code < 300 else False
        print(" update provisioned : ", provisioned)
        if provisioned:
          create_log_data(level='[INFO]',
                          Message="After successfull response from update integration configuration API now begingng for Update the store status to ONLINE ",
                          messagebody=response.text,
                          merchantID=merchantId, functionName="ubereats_setup_pos_integration")
          access_token = cls.ubereats_check_and_get_access_token()
          if not access_token:
            create_log_data(level='[ERROR]',
                            Message="Failed to generate access token while updating store status",
                            merchantID=merchantId, functionName="ubereats_setup_pos_integration", request=request)
            print("error in generate access token while updating store status")
            return False
          create_log_data(level='[INFO]',
                          Message="Successfull generate token while updating store status",
                          messagebody=access_token,
                          merchantID=merchantId, functionName="ubereats_setup_pos_integration", request=request)
          print("eats.store access token", accessToken)
          url = f"https://api.uber.com/v1/delivery/store/{storeId}/update-store-status"
          headers = {
            'Authorization': 'Bearer ' + access_token,
            'Content-Type': 'application/json'
          }
          payload = json.dumps({
            "status": "ONLINE"
          })
          create_log_data(level='[INFO]',
                          Message="Payload for call the updating store status ", messagebody=payload,
                          merchantID=merchantId, functionName="ubereats_setup_pos_integration", request=request)
          response = requests.request("POST", url, headers=headers, data=payload)
          create_log_data(level='[INFO]',
                          Message="Response from updating store status ", messagebody=response.text,
                          merchantID=merchantId, functionName="ubereats_setup_pos_integration", request=request)
          print(response.text)
          resp = True if response and response.status_code >= 200 and response.status_code < 300 else False
          return resp


      # return True
    except Exception as e:
      print("Error UberEats: ", str(e))
      create_log_data(level='[ERROR]',
                      Message="Exception occured. Failed to connect /activate integration with uber eats",
                      messagebody=str(e),
                      merchantID=merchantId, functionName="ubereats_setup_pos_integration", request=request)
      return False

  ############################################### UBEREATS ORDER

  @classmethod
  def ubereats_get_order_details(cls, orderUrl, accessToken):
    try:
      create_log_data(level='[INFO]',
                      Message=f"In the beginning of ubereats_get_order_details for order : {orderUrl}",
                      messagebody=f"accessToken : {accessToken}", functionName="ubereats_get_order_details")
      headers = {
        'Authorization': f'Bearer {accessToken}'
      }
      response = requests.request("GET", orderUrl, headers=headers)
      create_log_data(level='[INFO]',
                      Message=f"Response from get order detail from ubereats api",
                      messagebody=response.text, functionName="ubereats_get_order_details")
      print(response.text)
      data = response.json() if response and response.status_code >= 200 and response.status_code < 300 else False
      return data , response.text
    except Exception as e:
      print("Error UberEats: ", str(e))
      return False , str(e)
  

  @classmethod
  def ubereats_accept_order(cls, orderId, accessToken , merchantId=None ):
    try:
      create_log_data(level='[INFO]',
                      Message=f"In the beginning of ubereats_accept_order for order id  : {orderId}",
                      messagebody=f"accessToken : {accessToken}", functionName="ubereats_accept_order")
      url = f'https://api.uber.com/v1/delivery/order/{orderId}/accept'
      headers = {
        'Authorization': f'Bearer {accessToken}',
        'Content-Type': 'application/json'
      }
      if merchantId:
        merchant_details = Merchants.get_merchant_by_id(merchantId)
        prep_time_minutes = int(merchant_details['preparationtime']) + int(merchant_details['orderdelaytime']) if merchant_details['busymode'] == 1 else int(merchant_details['preparationtime'])
        # Get the current UTC time
        now_utc = datetime.datetime.now(datetime.timezone.utc)
        # Add prep_time_minutes
        future_time = now_utc + datetime.timedelta(minutes=prep_time_minutes)
        # Format in RFC 3339 format with 'Z' to indicate UTC
        ready_for_pickup_time = future_time.isoformat(timespec='seconds').replace('+00:00', 'Z')

        print(ready_for_pickup_time)
        payload = json.dumps({
          "ready_for_pickup_time":ready_for_pickup_time,
          "accepted_by": "Order has been accepted."
        })
      else:
        payload = json.dumps({
          "accepted_by": "Order has been accepted."
        })
      print('Payload for accept ubereats order ' , payload)
      response = requests.request("POST", url, headers=headers, data=payload)
      create_log_data(level='[INFO]',
                      Message=f"Response from ubereat accept order api",
                      messagebody=response.text, functionName="ubereats_accept_order")
      print(response.text)
      data = True if response and response.status_code >= 200 and response.status_code < 300 else False
      return data , response.text
    except Exception as e:
      print("Error UberEats: ", str(e))
      return False , str(e)
  

  @classmethod
  def ubereats_deny_order(cls, orderId, accessToken, reasonCode=None, explanation=None):
    try:
      create_log_data(level='[INFO]',
                      Message=f"In the beginning of ubereats_deny_order for order id  : {orderId}",
                      messagebody=f"accessToken : {accessToken} , explanation :  {explanation}" , functionName="ubereats_deny_order")
      url = f'https://api.uber.com/v1/delivery/order/{orderId}/deny'
      headers = {
        'Authorization': f'Bearer {accessToken}',
        'Content-Type': 'application/json'
      }
      payload = json.dumps({
        "deny_reason": {
          "info": explanation if explanation else "No expalination is provided",
          "type": reasonCode if reasonCode else "POS_OFFLINE"
        }
      })
      response = requests.request("POST", url, headers=headers, data=payload)
      create_log_data(level='[INFO]',
                      Message=f"Response from ubereat deny order api",
                      messagebody=response.text, functionName="ubereats_deny_order")
      print(response.text)
      data = True if response and response.status_code >= 200 and response.status_code < 300 else False
      return data
    except Exception as e:
      print("Error UberEats: ", str(e))
      return False
  

  @classmethod
  def ubereats_cancel_order(cls, orderId, accessToken, reason=None):
    try:
      url = f'https://api.uber.com/v1/delivery/order/{orderId}/cancel'
      headers = {
        'Authorization': f'Bearer {accessToken}',
        'Content-Type': 'application/json'
      }
      payload = json.dumps({
        "type": reason if reason else "RESTAURANT_TOO_BUSY"
      })
      response = requests.request("POST", url, headers=headers, data=payload)
      print(response.text)
      data = True if response and response.status_code >= 200 and response.status_code < 300 else False
      return data
    except Exception as e:
      print("Error UberEats: ", str(e))
      return False
  
  ############################################### UBEREATS REPORTING

  @classmethod
  def ubereats_request_report(cls, accessToken, report_type, store_uuids, start_date, end_date, group_uuids=None):
    try:
      url = "https://api.uber.com/v1/eats/report"
      headers = {
        "Authorization": f"Bearer {accessToken}",
        "Content-Type": "application/json"
      }
      payload = json.dumps({
        "report_type": report_type,
        "store_uuids": store_uuids,
        "start_date": start_date,
        "end_date": end_date
      })
      response = requests.request("POST", url, headers=headers, data=payload)
      print(response.text)
      data = response.json() if response and response.status_code >= 200 and response.status_code < 300 else False
      return data
    except Exception as e:
      print("Error UberEats: ", str(e))
      return False

  ############################################### POST COMPLETE MENU

  @classmethod
  def post_complete_menu_ubereats(cls, platformId):
    try:
      print('-------------------------------------------------------------------')
      print('-------------------- UBEREATS MENU MANUAL SYNC --------------------')
      
      connection, cursor = get_db_connection()

      print("Get required details from platforms table...")
      row = models.Platforms.Platforms.get_platform_by_id(platformId)  # we do import like this because we have to avoid circular imports
      storeId = row["storeid"]
      platformType = row["platformtype"]
      syncMerchantId = row["merchantid"]

      # get main/sync merchant details
      merchant_details = Merchants.get_merchant_or_virtual_merchant(syncMerchantId)
      if merchant_details.get("isVirtual") == 1:
        mainMerchantId = merchant_details.get("merchantid")
        isVirtualMerchant = 1
      else:
        mainMerchantId = syncMerchantId
        isVirtualMerchant = 0

      print("Store id: ", storeId)
      print("Sync Merchant id: ", syncMerchantId)
      print("Main Merchant id: ", mainMerchantId)
      print("Platform Type: ", platformType)

      print('check if uber eats provision is done...')
      if not (storeId and int(platformType) == 3):
        return False, 400, "Ubereats is not provisioned for the merchant!"

      # check and refresh access token
      accessToken = cls.ubereats_check_and_get_access_token()
      if not accessToken:
        return False, 500, "Unhandled exception occured while checking for ubereats access token"
      print(accessToken)

      # form the payload
      payload, msg, all_ids = cls.create_menu_payload_ubereats(mainMerchantId, syncMerchantId, isVirtualMerchant, platformType)
      if not payload:
        print(msg)
        return False, 500, msg
      print(payload)

      # Status:  401
      # {"code":"unauthorized","message":"Invalid OAuth 2.0 credentials provided."}

      # post menu to ubereats
      resp, status_code, msg = cls.ubereats_post_menu(
        storeId=storeId,
        accessToken=accessToken,
        payload=payload
      )
      print("UberEats Post Menu Message: ", msg)
      print("Status: ", status_code)

      if not resp:
        return False, 500, msg.get("message") if msg.get("message") else msg

      # add items_ids to itemmappings table metadata field
      print("storing all items ids into itemmappings table for future changes in items...")
      cursor.execute("""DELETE FROM itemmappings WHERE merchantid=%s AND platformtype=%s""", (syncMerchantId, platformType))
      connection.commit()
      cursor.execute("""INSERT INTO itemmappings (id, merchantid, platformtype, metadata) 
        VALUES (%s,%s,%s,%s)""", (uuid.uuid4(), syncMerchantId, platformType, json.dumps(all_ids)))
      connection.commit()
      print("successfully stored")

      return True, 200, "success"
    except Exception as e:
      print("Error: ", str(e))
      return False, 500, str(e)
  





  @classmethod
  def create_menu_payload_ubereats(cls, mainMerchantId, syncMerchantId, isVirtualMerchant, platformType):
    try:
      connection, cursor = get_db_connection()

      ### some global variables
      uber_menus_node = list(dict())
      uber_categories_node = list(dict())
      uber_items_node = list(dict())
      uber_addons_node = list(dict())
      all_categories_ids = list()
      all_items_ids = list()
      all_addons_ids = list()
      all_options_ids = list()

      print("Getting menu-mappings by merchantId and platformType...")
      temp_mappings = MenuMappings.get_menumappings(merchantId=mainMerchantId, platformType=platformType)
      
      mappings = list()
      if isVirtualMerchant == 1:
        for tmapping in temp_mappings:
          # check if menu is assigned to specified virtual-merchant-id, then append it to list
          cursor.execute("""SELECT * FROM vmerchantmenus WHERE vmerchantid = %s AND menuid = %s""", (syncMerchantId, tmapping["menuid"]))
          row = cursor.fetchone()
          if row:
            mappings.append(tmapping)
      else:
        for tmapping in temp_mappings:
          # check if menu is assigned to any virtual-merchant, then skip it
          cursor.execute("""SELECT * FROM vmerchantmenus WHERE merchantid = %s AND menuid = %s""", (mainMerchantId, tmapping["menuid"]))
          row = cursor.fetchone()
          if not row:
            mappings.append(tmapping)

      if len(mappings) == 0:
        return False, "no menu is assigned to ubereats!!!", ""

      # get merchant details
      merchant_details = Merchants.get_merchant_by_id(mainMerchantId)

      # a. loop over all menu-mappings and get each menu details
      # b. get each menu categories
      print("constructing uber menus node...")
      for mapping in mappings:
        u_menu = dict()
        u_menu_categories = list()
        u_menu_availability = list()

        # get menu details by id
        cursor.execute("""SELECT * FROM menus WHERE id = %s """, (mapping['menuid']))
        menu_details = cursor.fetchone()
        
        cursor.execute("""SELECT serviceavailability.id, TIME_FORMAT(starttime, '%%H:%%i') starttime, TIME_FORMAT(endtime, '%%H:%%i') endtime, weekdays.day FROM serviceavailability, weekdays 
          WHERE serviceavailability.weekday = weekdays.id AND serviceavailability.menuId=%s""", (mapping['menuid']))
        sa_rows = cursor.fetchall()

        for sa in sa_rows:
          
          append = False
          for a in u_menu_availability:
            if a['day_of_week'] == sa['day'].lower():
              a['time_periods'].append({
                "start_time": sa['starttime'],
                "end_time": sa['endtime']
              })
              append = True
              break

          if not append:
            u_menu_availability.append({
              "day_of_week": sa['day'].lower(),
              'time_periods': [
                {
                  "start_time": sa['starttime'],
                  "end_time": sa['endtime']
                }
              ]
            })

        # get menu categories
        menu_categories = MenuCategories.get_menucategories_fk(menuId=mapping['menuid'], platformType=1, order_by=["sortid ASC"])#1=apptopus
        for mc in menu_categories:
          u_menu_categories.append(mc['categoryid'])
          all_categories_ids.append(mc['categoryid'])

        # create uber_menu and append it to the uber_menus_node
        u_menu['service_availability'] = u_menu_availability
        u_menu['category_ids'] = u_menu_categories
        u_menu['id'] = menu_details['id']
        u_menu['title'] = {
          'translations': {
            'en_us': menu_details['name']
          }
        }

        # append u_menu to uber_menus_node
        uber_menus_node.append(u_menu)


      # a. uniquify all_categories_ids
      # b. loop over all_categories_ids and get each category_details
      # c. get each category items ids and append to all_items_ids
      # d. form uber_categories_node
      all_categories_ids = list(set(all_categories_ids))
      print("\nUnique All Categories IDS: ", all_categories_ids)

      print('constructing uber categories node...')
      for categoryId in all_categories_ids:
        u_category = dict()
        u_category_items = list(dict())

        # get category details by id
        category_details = Categories.get_category_by_id(categoryId)
        if not category_details:
          continue

        # get category-items ids
        cursor.execute("""SELECT productid FROM productscategories WHERE categoryid=%s ORDER BY sortid ASC""", (categoryId))
        category_items = cursor.fetchall()
        for ca in category_items:
          u_category_items.append({
            'id': ca['productid'],
            'type': 'ITEM'
          })
          all_items_ids.append(ca['productid'])

        # create uber_category and append it to the uber_categories_node
        u_category = {
          'id': categoryId,
          'title': {
            'translations': {
              "en_us": category_details['categoryname']
            }
          },
          'subtitle': {
            'translations': {
              "en_us": category_details['categorydescription']
            }
          },
          'entities': u_category_items
        }

        # append u_category to uber_category_node
        uber_categories_node.append(u_category)


      # a. uniquify all_items_ids
      # b. loop over all_items_ids and append each item-addons to all_addons_ids
      # c. get each item details
      # d. for items_node
      all_items_ids = list(set(all_items_ids))
      print("\nUnique All Items Ids: ", all_items_ids)
      
      print('constructing uber items node...')
      for itemId in all_items_ids:
        u_item_addons = list()

        # get item details by id
        item_details = Items.get_item_by_id(itemId , isnotdefaultimage=True)
        if not item_details:
          continue
        itemPrice = None
        if type(item_details["itemPriceMappings"]) is list:
          for r in item_details["itemPriceMappings"]:
            if r.get("platformType") == platformType:
              itemPrice = r.get("platformItemPrice")
              break
        if itemPrice is None:
          itemPrice = item_details["itemUnitPrice"]

        # get item-addons ids
        cursor.execute("""SELECT addonid FROM productsaddons WHERE productid=%s ORDER BY sortid ASC""", (itemId))
        item_addons = cursor.fetchall()
        for ia in item_addons:
          u_item_addons.append(ia['addonid'])
          all_addons_ids.append(ia['addonid'])
        
        # create uber_item and append it to the uber_items_node
        u_item = {
          'id': itemId,
          'image_url':  item_details.get('imageUrl') ,
          'suspension_info': None,
          'external_data': "",
          'quantity_info': None,
          'title': {
            'translations': {
              'en_us': item_details['itemName']
            }
          },
          'description': {
            'translations': {
              'en_us': item_details['itemDescription']
            }
          },
          'price_info': {
            'price': int(round(float(itemPrice) * 100)),
            'overrides': []
          },
          'tax_info': {
            'tax_rate': float(merchant_details['taxrate']),
            'vat_rate_percentage': None
          },
          'modifier_group_ids': {
            'overrides': [],
            'ids': u_item_addons
          },
          'nutritional_info': {
            'allergens': None,
            'kilojoules': None,
            'calories': None
          }
        }

        # check if item is disable, change suspension info also
        if item_details['itemStatus'] == 0:
          u_item['suspension_info'] = {
            'suspension': {
              "suspend_until": 2147483647,
              "reason": "sold out"
            }
          }

        uber_items_node.append(u_item)
      
      
      # a. uniquify all_addons_ids
      # b. loop over all_addons_ids and append each addon_option to all_options_ids
      # c. get each addon details
      # d. for uber_addons_node
      all_addons_ids = list(set(all_addons_ids))
      print("\nUnique All Addons Ids: ", all_addons_ids)

      print('constructing uber addons node...')
      for addonId in all_addons_ids:
        u_addon_options = list(dict())
        
        # get addon details by id from db
        addon_details = Addons.get_addon_by_id(addonId)
        if not addon_details:
          continue

        # get addon-options ids
        cursor.execute("""SELECT itemid FROM addonsoptions WHERE addonid=%s ORDER BY sortid ASC""", (addonId))
        addon_options = cursor.fetchall()
        for ao in addon_options:
          u_addon_options.append({
            'id': ao['itemid'],
            'type': 'ITEM'
          })
          all_options_ids.append(ao['itemid'])
        
        # create uber_addon and append it to the uber_addons_node
        u_addon = {
          'id': addonId,
          'external_data': "",
          'title': {
            'translations': {
              'en_us': addon_details['addonname']
            }
          },
          'quantity_info': {
            'overrides': [],
            'quantity': {
              'max_permitted': addon_details['maxpermitted'],
              'min_permitted': addon_details['minpermitted'],
              'default_quantity': None,
              'charge_above': None,
              'refund_under': None
            }
          },
          'modifier_options': u_addon_options
        }

        uber_addons_node.append(u_addon)
      
      
      # a. uniquify all_options_ids
      # b. loop over all_options_ids
      # c. check if option_id is not present in all_items_ids array
      # c. if not present, then get option details
      # d. append each u_option to uber_items node
      all_options_ids = list(set(all_options_ids))
      print("\nUnique All Options Ids: ", all_options_ids)

      print('appending addon_options to uber_items_node...')
      for itemId in all_options_ids:
        
        # check if itemId is already processed
        if itemId in all_items_ids:
          continue

        # get item details by id from db
        item_details = Items.get_item_by_id(itemId , isnotdefaultimage=True)
        if not item_details:
          continue
        itemPrice = None
        if type(item_details["itemPriceMappings"]) is list:
          for r in item_details["itemPriceMappings"]:
            if r.get("platformType") == platformType:
              itemPrice = r.get("platformItemPrice")
              break
        if itemPrice is None:
          itemPrice = item_details["itemUnitPrice"]
        
        # create uber_item and append it to the uber_items_node
        u_item = {
          'id': itemId,
          'image_url':item_details.get('imageUrl') ,
          'suspension_info': None,
          'external_data': "",
          'quantity_info': None,
          'title': {
            'translations': {
              'en_us': item_details['itemName']
            }
          },
          'description': {
            'translations': {
              'en_us': item_details['itemDescription']
            }
          },
          'price_info': {
            'price': int(float(itemPrice) * 100),
            'overrides': []
          },
          'tax_info': {
            'tax_rate': float(merchant_details['taxrate']),
            'vat_rate_percentage': None
          },
          'modifier_group_ids': {
            'overrides': [],
            'ids': []
          },
          'nutritional_info': {
            'allergens': None,
            'kilojoules': None,
            'calories': None
          }
        }
        
        # check if item is disable, change suspension info also
        if item_details['itemStatus'] == 0:
          u_item['suspension_info'] = {
            'suspension': {
              "suspend_until": 2147483647,
              "reason": "sold out"
            }
          }

        uber_items_node.append(u_item)

      
      print("\nUberEats Menu payload: ")
      payload = json.dumps({
        "items": uber_items_node,
        "menus": uber_menus_node,
        "categories": uber_categories_node,
        "modifier_groups": uber_addons_node,
        "display_options": {
          "disable_item_instructions": True if merchant_details["acceptspecialinstructions"] == 0 else False
        }
      })

      # return all_items ids
      all_ids = [*all_items_ids, *all_options_ids]
      
      # return payload
      return payload, "success", all_ids

    except Exception as e:
      return False, f"Create Menu Payload Error: {e}", ""
