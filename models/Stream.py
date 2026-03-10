import boto3
from flask import jsonify

import config
import datetime
import jwt
import re
import uuid
from models.MenuMappings import MenuMappings
# local imports
from utilities.errors import invalid, unhandled , invalid_stream_response
from utilities.helpers import get_db_connection, success, publish_sns_message, is_float, create_log_data
import config

import hmac
import hashlib
import requests
import json

JWT_SECRET_KEY = config.JWT_SECRET_KEY
fonda_client_id=config.fonda_client_id
fonda_secret=config.fonda_secret

class Stream():

  ###############################################

  @classmethod
  def check_stream_tokens(cls, json , request=None ,  ip_address=None):
    try:
      print(" check_stream_tokens call " , json)
      create_log_data(level='[INFO]',
                      Message=f"In the start of check_stream_tokens,IP address: {ip_address}",messagebody=json,
                      functionName="check_stream_tokens", request=request)
      connection, cursor = get_db_connection()
      client_id = json.get('client_id')
      client_secret = json.get('client_secret')
      print(" cliend id " , client_id)
      if client_id == fonda_client_id and client_secret == fonda_secret:
        grant_type = json.get('grant_type')
        refresh_token=''
        print("grant type is  " , grant_type )
        if grant_type == 'authorization_code':
          refresh_token = json.get('code')
        elif grant_type == 'refresh_token':
          refresh_token = json.get('refresh_token')
        cursor.execute("SELECT refresh_token  ,access_token ,  user_id FROM UserTokens where refresh_token=%s", refresh_token)
        userTokenLoginrow = cursor.fetchone()
        create_log_data(level='[INFO]',
                        Message=f"Getting user Token Login row from DB,IP address: {ip_address}", messagebody=userTokenLoginrow,
                        functionName="check_stream_tokens", request=request)
        if (userTokenLoginrow):
          refresh_token_validity = Stream.check_jwt_token(userTokenLoginrow.get('refresh_token'))
          create_log_data(level='[INFO]',
                          Message=f"Getting  refresh_token_validity,IP address: {ip_address}",
                          messagebody=refresh_token_validity,
                          functionName="check_stream_tokens", request=request)
          if refresh_token_validity.get('expired') == False:
            access_token_validity = Stream.check_jwt_token(userTokenLoginrow.get('access_token'))
            create_log_data(level='[INFO]',
                            Message=f"Getting  access_token_validity,IP address: {ip_address}",
                            messagebody=access_token_validity,
                            functionName="check_stream_tokens", request=request)
            if access_token_validity.get('expired') == True:
              access_token , expire_time=cls.generate_jwt_tokens(refresh_token_validity.get('user_id') , forAccessToken=True)
              update_sql = "UPDATE UserTokens SET access_token=%s , updated_at = NOW() WHERE user_id = %s and token_type=%s"
              cursor.execute(update_sql, (access_token, userTokenLoginrow.get('user_id'), 'streams'))
              connection.commit()
            else:
              access_token = userTokenLoginrow.get('access_token')
              expire_time = access_token_validity.get('expire_time')
            cursor.execute("SELECT * FROM users where id=%s", userTokenLoginrow.get('user_id'))
            userDetail = cursor.fetchone()
            response = {
              "access_token": access_token,
              "refresh_token": userTokenLoginrow.get('refresh_token'),
              "expire_time": expire_time,
              "refresh_token_expire_time": refresh_token_validity.get('expire_time'),
              "id":  refresh_token_validity.get('user_id'),
              "sso_details": {
                "email": userDetail.get('email'),
                "full_name": userDetail.get('firstname'),
                "phone_number": userDetail.get('phone')
              }
            }
            create_log_data(level='[INFO]',
                            Message=f"Return Exchange token response ,IP address: {ip_address}",
                            messagebody=response,
                            functionName="check_stream_tokens", request=request)
            print(" return response " , response)
            return response , 201
          else:
            print(" error  in userTokenLoginrow")
            create_log_data(level='[ERROR]',
                            Message=f" error  in userTokenLoginrow ,IP address: {ip_address}",
                            functionName="check_stream_tokens", request=request)
            return invalid_stream_response()
        else:
          print(" Refresh token not found")
          create_log_data(level='[ERROR]',
                          Message=f" Refresh token not found ,IP address: {ip_address}",
                          functionName="check_stream_tokens", request=request)
          return invalid_stream_response(errmsg="Refresh token not found")
      else:
        create_log_data(level='[ERROR]',
                        Message=f" Client Id or Secret is invalid ,IP address: {ip_address}",messagebody=json,
                        functionName="check_stream_tokens", request=request)
        print("Client Id or Secret is invalid")
        return invalid("Client Id or Secret is invalid")
    except Exception as e:
      create_log_data(level='[ERROR]',
                      Message=f" Exception Occured ", messagebody=str(e),
                      functionName="check_stream_tokens", request=request)
      print("exception in check_stream_tokens")
      print(str(e))
      return invalid(str(e))

  @classmethod
  def generate_jwt_tokens(cls,user_id , forAccessToken=None ):

    # Define expiration times in milliseconds
    if forAccessToken:
      access_token_expiration = datetime.datetime.utcnow() + datetime.timedelta(days=7)  # 7 days for access token


      # Convert expiration times to milliseconds since the Unix epoch
      access_token_expire_time_ms = int(access_token_expiration.timestamp() * 1000)


      # Create the payload for the access token
      access_token_payload = {
        'user_id': user_id,
        'exp': access_token_expiration
      }

      # Generate the JWT for access token
      access_token = jwt.encode(access_token_payload, JWT_SECRET_KEY, algorithm='HS256')
      return access_token , access_token_expire_time_ms


    else:

      refresh_token_expiration = datetime.datetime.utcnow() + datetime.timedelta(days=30 * 10000)  # for long time
      refresh_token_expire_time_ms = int(refresh_token_expiration.timestamp() * 1000)
      # Create the payload for the refresh token
      refresh_token_payload = {
        'user_id': user_id,
        'exp': refresh_token_expiration
      }

      # Generate the JWT for refresh token
      refresh_token = jwt.encode(refresh_token_payload, JWT_SECRET_KEY, algorithm='HS256')

      # Construct the response JSON
      return refresh_token

  @classmethod
  def check_jwt_token(cls,token):
    try:
      # Decode the token without verifying expiration to extract the expiration time
      decoded_token = jwt.decode(token, JWT_SECRET_KEY, algorithms=['HS256'], options={"verify_exp": False})

      # Extract the expiration time from the token
      expiration_time = decoded_token.get('exp')
      user_id=decoded_token.get('user_id')

      # Current time in seconds
      current_time = datetime.datetime.utcnow().timestamp()

      # Convert expiration time to milliseconds
      expiration_time_ms = expiration_time * 1000 if expiration_time else None

      # Check if the token has expired
      if expiration_time and expiration_time < current_time:
        return {
          "expired": True,
          "expire_time": expiration_time_ms
        }
      else:
        return {
          "expired": False,
          "expire_time": expiration_time_ms,
          "user_id":user_id
        }

    except jwt.ExpiredSignatureError:
      # Token has expired
      return {
        "expired": True,
        "error": "Token has expired"
      }
    except jwt.InvalidTokenError as e:
      # Token is invalid
      return {
        "expired": True,
        "error": str(e)
      }


  @classmethod
  def get_user_id(cls,token):
    try:
      connection, cursor = get_db_connection()
      cursor.execute("SELECT user_id FROM UserTokens where access_token=%s", token)
      user_id = cursor.fetchone()
      return user_id
    except Exception as e:
      print("Error get_user_id", str(e))
      return None

  @classmethod
  def get_merchant_list_against_user(cls,userid):
    try:
      connection, cursor = get_db_connection()
      cursor.execute(
        "SELECT mu.merchantid FROM merchantusers mu join merchants m on mu.merchantid=m.id where mu.userid=%s and m.is_stream_enabled=1",
        userid)
      merchant_list = cursor.fetchall()
      if merchant_list is None:
        return None
      return merchant_list
    except Exception as e:
      print("Error get_merchant_list_against_user", str(e))
      return None

  @classmethod
  def format_address(cls,merchant_detail):
    country = "USA"  # Default country
    # Regex to match full addresses including street, city, state, and postal code
    return {
        "address_line_1": merchant_detail.get('businessaddressline'),
        "address_line_2": "",  # No line 2 provided
        "city": merchant_detail.get('businessaddresscity'),
        "state": merchant_detail.get('businessaddressstate'),
        "postal_code": merchant_detail.get('zipcode'),
        "country": country
    }

  @classmethod
  def get_merchant_or_virtual_merchant_for_stream(cls, merchantId):
    try:
      connection, cursor = get_db_connection()
      cursor.execute("""SELECT * FROM merchants WHERE id=%s and is_stream_enabled=1""", (merchantId))
      row = cursor.fetchone()
      if not row:
        cursor.execute(
          "SELECT * FROM virtualmerchants WHERE id = %s and is_stream_enabled=1", (merchantId))
        row = cursor.fetchone()

        if not row:
          return False
        row["mainMerchant"] = cls.get_merchant_by_id_for_stream(row["merchantid"])
        row["isVirtual"] = 1

      return row
    except Exception as e:
      print("error: ", str(e))
      return False


  @classmethod
  def generating_stream_menu_payload(cls,merchant_id , request=None ,  ip_address=None , user=None):
    connection, cursor = get_db_connection()
    create_log_data(level='[INFO]',
                    Message=f"In the start of generating_stream_menu_payload,IP address: {ip_address}",
                    functionName="generating_stream_menu_payload", request=request)
    ismainmerchant_merchant = Stream.get_merchant_or_virtual_merchant_for_stream(merchant_id)
    if not ismainmerchant_merchant:
      create_log_data(level='[ERROR]',
                      Message="Failed to get merchant detail",
                      merchantID=merchant_id, functionName="generating_stream_menu_payload", request=request)
      return invalid_stream_response(errmsg="Merchant Unavailable")
    create_log_data(level='[INFO]',
                    Message="Get merchant detail by merchant id", messagebody=ismainmerchant_merchant,
                    merchantID=merchant_id, functionName="generating_stream_menu_payload", request=request)
    isvmerchant = ismainmerchant_merchant.get("isVirtual")
    merchant_tax_rate=0
    if isvmerchant:
      mainmerchant=ismainmerchant_merchant.get('mainMerchant')
      merchant_tax_rate=float(mainmerchant.get('taxrate')) if mainmerchant else 0
      cursor.execute("""SELECT vm.* FROM dashboard.vmerchantmenus vm join menumappings mp on vm.menuid=mp.menuid
                    where vm.vmerchantid=%s and mp.platformtype=8 """, (merchant_id))
      menumappings = cursor.fetchone()
      if menumappings:
        create_log_data(level='[INFO]',
                        Message=f"Getting menu mappings,IP address: {ip_address}", messagebody=menumappings,
                        functionName="generating_stream_menu_payload", request=request)
        menu_id = menumappings.get('menuid')
      else:
        create_log_data(level='[INFO]',
                        Message=f"NO Vmerchant atteched to any menu , IP address: {ip_address}", messagebody=menumappings,
                        functionName="generating_stream_menu_payload", request=request)
        return invalid_stream_response(errmsg="NO menu attached to this platform")

    else:
      merchant_tax_rate=float(ismainmerchant_merchant.get('taxrate'))
      menumappings = MenuMappings.get_menumappings(merchantId=merchant_id, platformType=8)
      create_log_data(level='[INFO]',
                      Message=f"Getting menu mappings,IP address: {ip_address}",messagebody=menumappings,
                      functionName="generating_stream_menu_payload", request=request)
      if len(menumappings) == 0:
        create_log_data(level='[INFO]',
                        Message=f"NO menu attached to this platform,IP address: {ip_address}", messagebody=menumappings,
                        functionName="generating_stream_menu_payload", request=request)
        return invalid_stream_response(errmsg="NO menu attached to this platform")
      if len(menumappings) > 1:
        create_log_data(level='[INFO]',
                        Message=f"More than one menu attached to this platform,IP address: {ip_address}", messagebody=menumappings,
                        functionName="generating_stream_menu_payload", request=request)
        return invalid_stream_response(errmsg="More than one menu attached to this platform")
      menu_id = menumappings[0]['menuid']

    print("after db connection ")

    cursor.execute("""SELECT * FROM menus 
                     WHERE id = %s """, (menu_id))
    menu = cursor.fetchone()
    create_log_data(level='[INFO]',
                    Message=f"Getting menu ,IP address: {ip_address}", messagebody=menu,
                    functionName="generating_stream_menu_payload", request=request)
    print("menu  is ", menu)

    # --------------------- Tax rate ---------------
    # mecrhant_tax_rate_id=str(uuid.uuid4())
    tax_dict=[
        {
        "provider_id": "sales_tax",
        "name": "Sales Tax",
        "rate": merchant_tax_rate,
        "is_active": True,
        "is_default": True,
        "is_inclusive": False
        }
    ]

    category_payload = list()
    payload_items_family=list()
    all_categories_ids_list = list()
    all_items_mappings_tuple=list()
    cursor.execute("""SELECT DISTINCT categories.id id, categories.categoryname categoryName, categories.posname posName, categories.categorydescription categoryDescription, categories.status status 
                        FROM menucategories, categories
                        WHERE menucategories.categoryid=categories.id AND menucategories.menuid=%s  order by sortid asc """,
                   (menu["id"]))
    categories = cursor.fetchall() # Get all the categories related to menu
    create_log_data(level='[INFO]',
                    Message=f"Getting categories,IP address: {ip_address}", messagebody=categories,
                    functionName="generating_stream_menu_payload", request=request)
    print("categories ", categories)
    print("length of categories ", len(categories))
    cat_count = 0
    modifier_group = list()
    modifiers = list()
    InActiveItemsIds=list()
    mapping_item_ids_list=list()
    cursor.execute("SELECT * FROM platforms where merchantid=%s and platformtype=8",merchant_id)
    getplatfrom = cursor.fetchone()

    cursor.execute("SELECT itemid , addonoptionid FROM itemmappings where merchantid=%s and menuid=%s and platformtype=8 ",( merchant_id , menu["id"]))
    mapping_item_ids = cursor.fetchall()
    mapping_item_ids_list=[item['itemid'] if item['itemid'] != '' else item['addonoptionid'] for item in mapping_item_ids]

    for category in categories:
      cat_count=cat_count+1
      print("category count is " , cat_count)
      cursor.execute(
        "SELECT productscategories.productid id, items.itemname itemName, items.itemdescription itemDescription, items.itemsku itemSKU, convert(items.itemprice, CHAR) itemPrice, items.imageurl imageUrl, items.status itemStatus FROM productscategories, items WHERE items.id = productscategories.productid AND productscategories.categoryid = %s   order by sortid asc",
        (category['id']))
      allItems = cursor.fetchall()   # Get all the items related to categories
      item_ids_list = list()



      for item in allItems:
        modifier_group_ids = list()

        ##################################  Create modifiers and modifiers group ##################

        cursor.execute(
          """SELECT a.* FROM productsaddons pa join addons a on pa.addonid=a.id WHERE pa.productid=%s ORDER BY pa.sortid ASC""",
          (item.get('id')))
        item_addons = cursor.fetchall()  # Get all the addons  related to all items list
        for ia in item_addons:
          modifier_group_ids.append(ia.get('id'))
          cursor.execute(
            """SELECT i.* FROM addonsoptions ao  join items i on ao.itemid=i.id WHERE ao.addonid=%s ORDER BY ao.sortid ASC""",
            (ia.get('id')))
          addon_options = cursor.fetchall()  # Get all the addons options  related to addon
          modifiers_ids = list()
          for ao in addon_options:
            ###################################      Create list of  dsp modifier price mapping #################
            dsp_modifier_price_list = list()
            cursor.execute(
              """SELECT ipm.platformitemprice , pt.type from itempricemappings ipm join platformtype pt on ipm.platformtype=pt.id
                 where ipm.itemid=%s and pt.id in (3,5,6)""",
              (ao.get('id')))
            allIDspModifierPrices = cursor.fetchall()
            for DspModifierPrice in allIDspModifierPrices:
              Dsp_modifier_dict = {
                "dsp": 'uber' if DspModifierPrice.get('type') == 'ubereats' else DspModifierPrice.get('type'),
                "price_amount": round(float(DspModifierPrice.get('platformitemprice')) * 100) if DspModifierPrice.get(
                  'platformitemprice') else 0,
              }
              dsp_modifier_price_list.append(Dsp_modifier_dict)
            modifiers_ids.append(ao.get('id'))
            alreadyexist= ao.get('id') in mapping_item_ids_list
            InActiveItemsIds.append(ao.get('id')) if ao.get('status') != 1 and not alreadyexist else None
            optionstatus= True if ao.get('status') == 1 else False
            modifiers_dict = {
              "provider_id": ao.get('id'),
              "name": ao.get('itemname'),
              "is_active": optionstatus if alreadyexist else True,
              "price_amount": round(float(ao.get('itemprice')) * 100) if ao.get('itemprice') else 0,
              "price_currency": 'USD',
              "dsp_price_amount_overrides":dsp_modifier_price_list,
              "sku": ao.get('itemsku')
            }
            modifiers.append(modifiers_dict)
            if not alreadyexist:
              all_items_mappings_tuple.append((uuid.uuid4() , merchant_id ,menu["id"] ,'', ao.get('id') ,2, 8 ))

          modifier_group_dict = {
            "provider_id": ia.get('id'),
            "name": ia.get('addonname'),
            "is_active": True if ia.get('status') == 1 else False,
            "rules": {
              "selection_type": "multiple" if (ia.get('maxpermitted') or 0) - (ia.get('minpermitted') or 0) > 1 else "single",
              "default_modifier_quantities": {
                "modifier_id": 1
              },
              "minimum_unique_modifiers_allowed": ia.get('minpermitted'),
              "maximum_unique_modifiers_allowed": ia.get('maxpermitted'),
            },
            "modifier_ids": modifiers_ids
          }
          modifier_group.append(modifier_group_dict)

        ###################################      Create list of  dsp item price mapping #################
        dsp_item_price_list=list()
        cursor.execute(
          """SELECT ipm.platformitemprice , pt.type from itempricemappings ipm join platformtype pt on ipm.platformtype=pt.id
             where ipm.itemid=%s and pt.id in (3,5,6)""",
          (item.get('id')))
        allIDspItemPrices = cursor.fetchall()
        for DspItemPrice in allIDspItemPrices:
          Dsp_Item_dict= {
              "dsp": 'uber' if DspItemPrice.get('type') == 'ubereats' else DspItemPrice.get('type'),
              "price_amount": round(float(DspItemPrice.get('platformitemprice')) * 100) if DspItemPrice.get('platformitemprice') else 0,
            }
          dsp_item_price_list.append(Dsp_Item_dict)
        ####################################     Create list of  item family #################


        item_ids_list.append(item.get('id'))  # List of items that will add in item_family
        alreadyexist = item.get('id') in mapping_item_ids_list
        InActiveItemsIds.append(item.get('id')) if item.get('itemStatus') != 1 and not alreadyexist else None
        itemstatus = True if item.get('itemStatus')  == 1 else False
        item_family_dict = {
          "provider_id": item.get('id'),
          "name": item.get('itemName'),
          "is_active": itemstatus if alreadyexist else True,
          "price_amount": round(float(item.get('itemPrice')) * 100) if item.get('itemPrice') else 0,
          "price_currency": 'USD',
          "description":item.get('itemDescription') if item.get('itemDescription') else '',
          "is_alcohol":False,
          "dsp_price_amount_overrides": dsp_item_price_list,
          "modifier_group_ids":modifier_group_ids,
          "sku":item.get('itemSKU'),
          "images": [
            {
              "provider_url": item.get('imageUrl') if item.get('imageUrl') else ''
            }
          ],
          "tax_ids":["sales_tax"]
        }

        payload_items_family.append(item_family_dict)
        if not alreadyexist:
          all_items_mappings_tuple.append((uuid.uuid4(), merchant_id,menu["id"] ,  item.get('id'),'',1, 8))
      all_categories_ids_list.append(category.get('id'))
      category_dict = {
        "provider_id": category.get('id'),
        "name": category.get('categoryName'),
        "item_family_ids":item_ids_list
      }
      category_payload.append(category_dict)

    # Get menu business hours

    # Execute the query
    cursor.execute("""
        SELECT serviceavailability.id, TIME_FORMAT(starttime, '%%H:%%i') starttime,
               TIME_FORMAT(endtime, '%%H:%%i') endtime, weekdays.day 
        FROM serviceavailability, weekdays 
        WHERE serviceavailability.weekday = weekdays.id AND serviceavailability.menuId=%s
    """, (menu_id,))
    sa_rows = cursor.fetchall()
    create_log_data(level='[INFO]',
                    Message=f"Getting menu hours ,IP address: {ip_address}", messagebody=sa_rows,
                    functionName="generating_stream_menu_payload", request=request)
    # Initialize the menu data structure
    menu_data = {
      "provider_id": menu_id,
      "name": menu.get('name'),
      "schedule": {
        "monday": [],
        "tuesday": [],
        "wednesday": [],
        "thursday": [],
        "friday": [],
        "saturday": [],
        "sunday": []
      },
      "category_ids": all_categories_ids_list,
    }

    # Populate the schedule with time periods
    for sa in sa_rows:
      day_of_week = sa['day'].lower()  # Convert day name to lowercase
      time_period = f"{sa['starttime']}-{sa['endtime']}"  # Format as "start-end"
      menu_data["schedule"][day_of_week].append(time_period)




    # `output` now holds the desired data structure

    payload_complete_catalog = dict()
    payload_complete_catalog={

      "category":category_payload,
      "item_family":payload_items_family,
      "item":[],
      "modifier_group":modifier_group,
      "modifier":modifiers,
      "menu": [menu_data],
      "tax":tax_dict
    }
    print("return catalog response with " , payload_complete_catalog)
    create_log_data(level='[INFO]',
                    Message=f"Return catalog response ,IP address: {ip_address}", messagebody=payload_complete_catalog,
                    functionName="generating_stream_menu_payload", request=request)



    if len(all_items_mappings_tuple) != 0:
      all_items_mappings_tuple = list(set(all_items_mappings_tuple))
      cursor.executemany("""
             INSERT INTO itemmappings 
               (id, merchantid,menuid, itemid,addonoptionid, itemtype,platformtype)
               VALUES (%s,%s,%s,%s,%s,%s,%s)
             """, (all_items_mappings_tuple))
      connection.commit()
    if getplatfrom is None:
      platformId = uuid.uuid4()
      data = (platformId, merchant_id, 8, 1, 0,
              1, user.get('user_id'))
      cursor.execute("""INSERT INTO platforms
                               (id, merchantid,  platformtype, integrationstatus,syncstatus,
                                synctype, created_by)
                               VALUES (%s,%s,%s,%s,%s,%s,%s)""", data)
      connection.commit()


    # Inactive all InActiveItemsIds

    try:
      InActiveItemsIds = list(set(InActiveItemsIds))
      sqs_client = boto3.resource('sqs')
      queue = sqs_client.get_queue_by_name(QueueName=config.sqs_inActive_stream_menu_items)
      create_log_data(level='[INFO]',
                      Message=f"List of inActive items or modifiers , length of list {len(InActiveItemsIds)}", messagebody=InActiveItemsIds,
                      functionName="generating_stream_menu_payload", request=request)
      for InactiveItem in InActiveItemsIds:
        dataObj = {"merchantId": merchant_id, "itemId":InactiveItem }

        response = queue.send_message(
          MessageBody=json.dumps(dataObj),
          MessageGroupId=str(uuid.uuid4()),
          MessageDeduplicationId=str(uuid.uuid4())
        )
        print(response)
    except Exception as e:
      print("Error: ", str(e))
      create_log_data(level='[ERROR]',
                      Message=f"Exception occurred while enqueue the inactive items to sqs.",
                      functionName="generating_stream_menu_payload", messagebody=str(e))
      sns_msg = {
        "event": "error_logs.entry",
        "body": {
          "userId": None,
          "merchantId": merchant_id,
          "errorName": "Exception occurred while enqueue the inactive items to sqs.",
          "errorSource": "dashboard",
          "errorStatus": 500,
          "errorDetails": str(e)
        }
      }
      error_logs_sns_resp = publish_sns_message(topic=config.sns_error_logs, message=str(sns_msg),
                                                subject="error_logs.entry")
    return payload_complete_catalog,200

  @classmethod
  def get_merchant_by_id_for_stream(cls,merchantId ):
    connection, cursor = get_db_connection()
    cursor.execute("""SELECT * FROM merchants WHERE id=%s and is_stream_enabled=1""", (merchantId))
    row = cursor.fetchone()
    return row
  @classmethod
  def update_stream_order_status(cls ,orderid ,merchant_id ,status , ip_address=None):
    create_log_data(level='[INFO]',
                    Message=f"In the start of update order status to stream event api call,IP address: {ip_address}",
                    messagebody=f"Order Id : {orderid } , Order status : {status}", functionName="update_stream_order_status",
                    merchantID=merchant_id)
    # Replace with your actual webhook secret and Stream endpoint URL
    webhook_secret = config.stream_webhook_secret
    stream_webhook_url = config.stream_webhook_event_url

    try:
      # Example payload
      payload = {
        "type": "order.status.updated",
        "object": {
          "status": "ready_for_pickup" if status==7 else "merchant_canceled",
          "provider_id": orderid,
          "location_id": merchant_id,
        }
      }
      body = json.dumps(payload)

      # Generate HMAC-SHA256 signature
      signature = hmac.new(webhook_secret.encode(), body.encode(), hashlib.sha256).hexdigest()

      # Set headers
      headers = {
        "Content-Type": "application/json",
        "Stream-Webhook-Signature": signature
      }

      # Send request
      response = requests.post(stream_webhook_url, headers=headers, data=body)

      # Check response
      print("Status Code:", response.status_code)
      print("Response Body:", response.json())
      if response.status_code == 200:
        create_log_data(level='[INFO]',
                        Message=f"Update order status",
                        functionName="update_stream_order_status", merchantID=merchant_id)
        return True, 200, "success"
      else:
        create_log_data(level='[INFO]',
                        Message=f"Error occurred on stream api call",messagebody=response.text,
                        functionName="update_stream_order_status", merchantID=merchant_id)
        return False, response.status_code, response.text

    except Exception as e:
      create_log_data(level='[ERROR]',
                      Message=f"Error in Update order status to stream",
                      functionName="update_stream_order_status", merchantID=merchant_id)
      return False, 500, str(e)

  @classmethod
  def update_location_status_stream(cls,merchant_id,status,ip_address= None):
    create_log_data(level='[INFO]',
                    Message=f"In the start of update location status to stream event api call,IP address: {ip_address}",
                    messagebody=f"Location status is {status}", functionName="update_location_status_stream",
                    merchantID=merchant_id)
    webhook_secret = config.stream_webhook_secret
    stream_webhook_url = config.stream_webhook_event_url
    try:
      # Example payload
      payload = {
        "type": "location.status.updated",
        "object": {
          "status": 'active' if status else "inactive",
          "location_id": merchant_id
        }
      }
      body = json.dumps(payload)

      # Generate HMAC-SHA256 signature
      signature = hmac.new(webhook_secret.encode(), body.encode(), hashlib.sha256).hexdigest()
      
      # Set headers
      headers = {
        "Content-Type": "application/json",
        "Stream-Webhook-Signature": signature
      }
      # Send request
      response = requests.post(stream_webhook_url, headers=headers, data=body)
      # Check response
      print("Status Code:", response.status_code)
      # print("Response Body:", response.json())

      if response.status_code==200:
        create_log_data(level='[INFO]',
                        Message=f"Update location status to stream successfully, Request Body: {body},IP address: {ip_address}",
                        functionName="update_menu_status_stream", merchantID=merchant_id,
                      statusCode=response.status_code,
                      messagebody=response.json())
        return True, 200, "success"
      else:
        create_log_data(level='[INFO]',
                        Message=f"Error occurred on stream api call, Request Body: {body},IP address: {ip_address}",
                        functionName="update_menu_status_stream", merchantID=merchant_id,
                      statusCode=response.status_code,
                      messagebody=response.json())
        return False, response.status_code, response.text
    except Exception as e:
      create_log_data(level='[ERROR]',
                    Message=f"Error in Update location status to stream: {str(e)},IP address: {ip_address}",
                    functionName="update_menu_status_stream",merchantID=merchant_id)
      return False, 500, str(e)
    
  @classmethod
  def post_complete_menu_stream(cls,merchant_id,ip_address=None):
    create_log_data(level='[INFO]',
                    Message=f"In the start of post menu to stream event api call,IP address: {ip_address}",
                     functionName="post_complete_menu_stream", merchantID=merchant_id)
    webhook_secret = config.stream_webhook_secret
    stream_webhook_url = config.stream_webhook_event_url
    try:
      # Example payload
      payload = {
        "type": "location.catalog.updated",
        "object": {
          "location_id": merchant_id
        }
      }
      body = json.dumps(payload)

      # Generate HMAC-SHA256 signature
      signature = hmac.new(webhook_secret.encode(), body.encode(), hashlib.sha256).hexdigest()
      
      # Set headers
      headers = {
        "Content-Type": "application/json",
        "Stream-Webhook-Signature": signature
      }
      # Send request
      response = requests.post(stream_webhook_url, headers=headers, data=body)
      # Check response
      print("Status Code:", response.status_code)
      print("Response Body:", response.json())

      if response.status_code==200:
        create_log_data(level='[INFO]',
                        Message=f"Sync menu to stream successfully, Request Body: {body},IP address: {ip_address}",
                        functionName="post_complete_menu_stream", merchantID=merchant_id,
                      statusCode=response.status_code,
                      messagebody=response.json())
        return True, 200, "success"
      else:
        create_log_data(level='[INFO]',
                        Message=f"Error occurred on stream api call, Request Body: {body},,IP address: {ip_address}",
                        functionName="post_complete_menu_stream", merchantID=merchant_id,
                      statusCode=response.status_code,
                      messagebody=response.json())
        return False, response.status_code, response.text
    except Exception as e:
      create_log_data(level='[ERROR]',
                    Message=f"Error in Sync menu to stream:{str(e)},IP address: {ip_address}",
                    functionName="post_complete_menu_stream",merchantID=merchant_id)
      return False, 500, str(e)

  @classmethod
  def update_stream_menu_item_status(cls ,merchant_id, item_id, status,type,ip_address=None):
    create_log_data(level='[INFO]',
                    Message=f"In the start of update item status to stream event api call,IP address: {ip_address}",
                    messagebody=f"item_id: {item_id} , Item status is {status} , type : {type}",functionName="update_stream_menu_item_status", merchantID=merchant_id)
    print(f"update_stream_menu_item_status begin with item_id: {item_id} , Item status is {status} , type : {type}")
    # Replace with your actual webhook secret and Stream endpoint URL
    webhook_secret = config.stream_webhook_secret
    stream_webhook_url = config.stream_webhook_event_url
    try:
      # Example payload
      payload = {
      "type": "location.catalog.object.updated",
        "object": {
          "location_id": merchant_id,
            "object_id": item_id,
            "object_type":type,
            "object_update":
            {
            "is_active": status
            }
        }
      

      }
      create_log_data(level='[INFO]',
                      Message=f"Payload for update item status change,IP address: {ip_address}",
                      messagebody=payload,
                      functionName="update_stream_menu_item_status", merchantID=merchant_id)
      print("Payload for update item status change" , payload)
      body = json.dumps(payload)

      # Generate HMAC-SHA256 signature
      signature = hmac.new(webhook_secret.encode(), body.encode(), hashlib.sha256).hexdigest()

      # Set headers
      headers = {
        "Content-Type": "application/json",
        "Stream-Webhook-Signature": signature
      }

      # Send request
      print("calling the api with body " , body)
      response = requests.post(stream_webhook_url, headers=headers, data=body)

      # Check response
      print("response" , response)
      print("Status Code:", response.status_code)
      print("Response Body:", response.json())
      if response.status_code==200:
        create_log_data(level='[INFO]',
                      Message=f"Update menu item to stream successfully, Request Body: {body},IP address: {ip_address}",
                      functionName="update_stream_menu_item_status",merchantID=merchant_id,
                      statusCode=response.status_code,
                      messagebody=response.json())
        print(f"Update menu item to stream successfully, Request Body: {body}")
        return True, 200, "success"
      else:
        create_log_data(level='[INFO]',
                        Message=f"Error occurred on stream api call, Request Body: {body},IP address: {ip_address}",
                        functionName="update_stream_menu_item_status", merchantID=merchant_id,
                      statusCode=response.status_code,
                      messagebody=response.json())
        print(f"Error occurred on stream api call, Request Body: {body}")
        return False, response.status_code, response.text
    except Exception as e:
      create_log_data(level='[ERROR]',
                    Message=f"Error in update menu item to stream: {str(e)},IP address: {ip_address}",
                    functionName="update_stream_menu_item_status",merchantID=merchant_id)
      print(e)
      return False, 500, str(e)
