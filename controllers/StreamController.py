from flask import jsonify, g, request, redirect
import uuid
from models.Addons import Addons
from models.Items import Items
from models.Orders import Orders
from models.Stream import Stream
from models.Merchants import Merchants
from models.VirtualMerchants import VirtualMerchants
from models.Websockets import Websockets
from utilities.helpers import get_db_connection, get_ip_address, send_android_notification_api
from utilities.errors import unauthorised, unhandled, invalid, invalid_stream_response
from utilities.helpers import success, validateAdminUser, create_log_data, publish_sns_message, validateLoginToken
import config
import re
from datetime import datetime
import pytz

fonda_client_id=config.fonda_client_id
loginbaseurl=config.stream_loginbaseurl
def stream_authorize():
  try:
    ip_address = get_ip_address(request)
    client_id = request.args.get('client_id')
    response_type = request.args.get('response_type')
    redirect_uri = request.args.get('redirect_uri')
    create_log_data(level='[INFO]', Message=f"In the start of stream_authorize,IP address: {ip_address}, client_id:{client_id} , redirect_uri:{redirect_uri} , response_type:{response_type} ",
                    functionName="stream_authorize", request=request)
    if fonda_client_id==client_id and response_type=='code':
      login_url = f"{loginbaseurl}?redirect_uri={redirect_uri}&client_id={client_id}&response_type={response_type}"
      return redirect(login_url)
    else:
      create_log_data(level='[INFO]',
                      Message=f"Client Id or response_type is invalid ,IP address: {ip_address}",
                      functionName="stream_authorize", request=request)
      return invalid("Client Id or response_type is invalid")
  except Exception as e:
    create_log_data(level='[INFO]',
                    Message=f"Exception occurred",messagebody=str(e),
                    functionName="stream_authorize")
    print("Error: ", str(e))
    return invalid(str(e))

def generate_location_json():
    try:
      print("call generate_location_json")
      ip_address = get_ip_address(request)
      create_log_data(level='[INFO]',
                      Message=f"In the start of generate_location_json,IP address: {ip_address}",
                      functionName="generate_location_json", request=request)
      token = request.headers.get('Authorization').split('Bearer ')[1]
      create_log_data(level='[INFO]',
                      Message=f"Getting Token from Header. token is  {token} ,IP address: {ip_address}",
                      functionName="generate_location_json", request=request)
      location = []
      if not token:
        print("Token is missing")
        create_log_data(level='[INFO]',
                        Message=f"Token is missing in headers ,IP address: {ip_address}",
                        functionName="generate_location_json", request=request)
        return invalid_stream_response(errmsg="Token is missing")

      access_token_validity = Stream.check_jwt_token(token )
      create_log_data(level='[INFO]',
                      Message=f"Check token validation ,IP address: {ip_address}",messagebody=access_token_validity,
                      functionName="generate_location_json", request=request)
      user = Stream.get_user_id(token)
      create_log_data(level='[INFO]',
                      Message=f"Get user against  token : {token} ,IP address: {ip_address}", messagebody=user,
                      functionName="generate_location_json", request=request)
      if user is None:
        create_log_data(level='[INFO]',
                        Message=f"No user found against the provided token ,IP address: {ip_address}",
                        functionName="generate_location_json", request=request)
        print("No user found against the provided token")
        return invalid_stream_response(errmsg="No user found against the provided token")
      if access_token_validity.get('expired') == False:
        merchant_list = Stream.get_merchant_list_against_user(user['user_id'])
        create_log_data(level='[INFO]',
                        Message=f"Get merchant list agaisnt user : {user['user_id']} ,IP address: {ip_address}", messagebody=merchant_list,
                        functionName="generate_location_json", request=request)
        if merchant_list:
          for merchant in merchant_list:
            merchant_detail = Merchants.get_merchant_by_id(merchant['merchantid'])
            create_log_data(level='[INFO]',
                            Message=f"Get merchant detail agaisnt merchant id : {merchant['merchantid']} ,IP address: {ip_address}", messagebody=merchant_detail,
                            functionName="generate_location_json", request=request)
            if merchant_detail:
              prep_time_minutes= int(merchant_detail['preparationtime'])
              merchant_information = {'provider_id': merchant['merchantid'],
                                      'name': merchant_detail['merchantname'],
                                      'address': Stream.format_address(merchant_detail),
                                      'timezone': merchant_detail['timezone'],
                                      'prep_time_minutes':prep_time_minutes}
              location.append(merchant_information)
              virtual_merchant_list = VirtualMerchants.get_virtual_merchant(merchantId=merchant['merchantid'] , activeOnly=1 , stream=1)
              create_log_data(level='[INFO]',
                              Message=f"Get virtual_merchant_list agaisnt mecrhant : {merchant['merchantid']} ,IP address: {ip_address}",
                              messagebody=virtual_merchant_list,
                              functionName="generate_location_json", request=request)
              if virtual_merchant_list:
                for vmerchant in virtual_merchant_list:

                  merchant_information = {'provider_id': vmerchant['id'],
                                          'name': vmerchant['virtualname'],
                                          'address': Stream.format_address(merchant_detail),
                                          'timezone': merchant_detail['timezone'],
                                          'prep_time_minutes':prep_time_minutes}
                  location.append(merchant_information)
          print("return response for generate_location_json " , location)
          create_log_data(level='[INFO]',
                          Message=f"Return response for generate_location_json ,IP address: {ip_address}",
                          messagebody=location,
                          functionName="generate_location_json", request=request)
          return { "locations": location} , 200
        create_log_data(level='[INFO]',
                        Message=f"No merchants found against this user : {user} ,IP address: {ip_address}",
                        functionName="generate_location_json", request=request)
        print("No merchants found against this user")
        return invalid_stream_response(errmsg="No merchants found against this user")
      else:
        create_log_data(level='[ERROR]',
                        Message=f"Token is expired , Token : {token}, IP address: {ip_address}",
                        functionName="generate_location_json", request=request)
        print("Token is expired")
        return invalid_stream_response(errmsg="Token is expired")

    except Exception as e:
      create_log_data(level='[ERROR]',
                      Message=f"Exception occured ",messagebody=str(e),
                      functionName="generate_location_json", request=request)
      print("Error get_location", str(e))
      return invalid_stream_response(errmsg=str(e))


def stream_token_validation():
  try:
    ip_address = get_ip_address(request)
    create_log_data(level='[INFO]',
                    Message=f"In the start of stream_token_validation,IP address: {ip_address}",messagebody=request.json,
                    functionName="stream_token_validation", request=request)
    return Stream.check_stream_tokens(request.json , request=request , ip_address=ip_address)
  except Exception as e:
    create_log_data(level='[ERROR]',
                    Message=f"Exception occured ", messagebody=str(e),
                    functionName="stream_token_validation", request=request)
    print("exception in stream_token_validation")
    print("Error: ", str(e))
    return invalid_stream_response(errmsg=str(e))

def generate_menu_json():
    try:
      ip_address = get_ip_address(request)
      create_log_data(level='[INFO]',
                      Message=f"In the start of generate_menu_json,IP address: {ip_address}",
                      functionName="generate_menu_json", request=request)
      print("call generate_menu_json")
      token = request.headers.get('Authorization').split('Bearer ')[1]
      merchant_id = request.headers.get('X-Provider-Location-Id')
      create_log_data(level='[INFO]',
                      Message=f"Getting token: {token} and merchantid:{merchant_id} from Header , IP address: {ip_address}",
                      functionName="generate_menu_json", request=request)
      if not token:
        print("Token is missing")
        create_log_data(level='[INFO]',
                        Message=f"Token is missing , IP address: {ip_address}",
                        functionName="generate_menu_json", request=request)
        return invalid_stream_response(errmsg="Token is missing")

      user = Stream.get_user_id(token)
      create_log_data(level='[INFO]',
                      Message=f"Get user against  token : {token} ,IP address: {ip_address}", messagebody=user,
                      functionName="generate_menu_json", request=request)
      if user is None:
        create_log_data(level='[INFO]',
                        Message=f"No user found against the provided token ,IP address: {ip_address}",
                        functionName="generate_menu_json", request=request)
        print("No user found against the provided token")
        return invalid_stream_response(errmsg="No user found against the provided token")

      check_token_validity=Stream.check_jwt_token(token )
      create_log_data(level='[INFO]',
                      Message=f"Check token validation ,IP address: {ip_address}", messagebody=check_token_validity,
                      functionName="generate_menu_json", request=request)
      print("check_token_validity ", check_token_validity, 'token is ', token)


      if check_token_validity.get('expired') == False and merchant_id:

        return Stream.generating_stream_menu_payload(merchant_id,request=request, ip_address=ip_address , user=user)
      else:
        print("Token is expired")
        create_log_data(level='[INFO]',
                        Message=f"Token is expired ,IP address: {ip_address}", messagebody=check_token_validity,
                        functionName="generate_menu_json", request=request)
        return invalid_stream_response(errmsg="Token is expired")
    except Exception as e:
      create_log_data(level='[ERROR]',
                      Message=f"Exception occured ", messagebody=str(e),
                      functionName="generate_menu_json", request=request)
      print("Error generate_menu_json", str(e))
      return invalid_stream_response()



def stream_webhook():
  from datetime import datetime, timezone
  try:
    print("------------------------------   Call stream order webhook  ----------------------------")
    print("json is " , request.json)
    ip_address = get_ip_address(request)
    order_json=request.json
    create_log_data(level='[INFO]',
                    Message=f"In the start of stream_webhook,IP address: {ip_address}", messagebody=order_json,
                    functionName="stream_webhook", request=request)
    token = request.headers.get('Authorization').split('Bearer ')[1]
    create_log_data(level='[INFO]',
                    Message=f"Getting Token from Header. token is  {token} ,IP address: {ip_address}",
                    functionName="stream_webhook", request=request)
    location = []
    if not token:
      print("Token is missing")
      create_log_data(level='[INFO]',
                      Message=f"Token is missing in headers ,IP address: {ip_address}",
                      functionName="stream_webhook", request=request)
      stream_error_resp = {
        "posValidation":
          {
            "orderValidation": [{
              "order_error": "other",
              "order_error_reason": "Token is missing in headers"
            }]
          }
      }
      return stream_error_resp, 400

    access_token_validity = Stream.check_jwt_token(token)
    create_log_data(level='[INFO]',
                    Message=f"Check token validation ,IP address: {ip_address}", messagebody=access_token_validity,
                    functionName="stream_webhook", request=request)
    user = Stream.get_user_id(token)
    create_log_data(level='[INFO]',
                    Message=f"Get user against  token : {token} ,IP address: {ip_address}", messagebody=user,
                    functionName="stream_webhook", request=request)
    if user is None:
      create_log_data(level='[INFO]',
                      Message=f"No user found against the provided token ,IP address: {ip_address}",
                      functionName="stream_webhook", request=request)
      print("No user found against the provided token")
      stream_error_resp = {
        "posValidation":
          {
            "orderValidation": [{
              "order_error": "store_unavailable",
              "order_error_reason": "No user found against the provided token"
            }]
          }
      }
      return stream_error_resp, 400
    if access_token_validity.get('expired') == False:

      eventType=order_json.get('type') if order_json else None
      if eventType and eventType == "order.created":
        create_log_data(level='[INFO]',
                        Message=f"In the beginning of eventType == order.created ,IP address: {ip_address}",
                        functionName="stream_webhook")

        u_order_details=order_json.get('object')
        create_log_data(level='[INFO]',
                        Message="Get u_order_details detail from json", messagebody=u_order_details, functionName="stream_webhook")
        main_merchant = VirtualMerchants.get_virtual_merchant(id=u_order_details.get('location_id'))

        if main_merchant:
          create_log_data(level='[INFO]',
                          Message="Successfully get main_merchant ", messagebody=main_merchant,
                          functionName="stream_webhook")
          merchant_id = main_merchant[0]['merchantid']
        else:
          merchant_id = u_order_details.get('location_id')

        # get merchant details
        merchant_details = Stream.get_merchant_by_id_for_stream(merchant_id)
        if not merchant_details:
          create_log_data(level='[ERROR]',
                          Message="Failed to get merchant detail",
                          merchantID=merchant_id, functionName="stream_webhook")
          stream_error_resp = {
            "posValidation":
              {
                "orderValidation": [{
                  "order_error": "store_unavailable",
                  "order_error_reason": "Failed to get merchant detail"
                }]
              }
          }
          return stream_error_resp, 400
        create_log_data(level='[INFO]',
                        Message="Get merchant detail by merchant id", messagebody=merchant_details,
                        merchantID=merchant_id, functionName="stream_webhook")

        # init order dict


        order = {}
        source_order_id = u_order_details['source_order_id']
        source = "ubereats" if u_order_details['source']=='uber' else u_order_details['source']
        # loop over products
        items_list = []

        if source=='ubereats' or source=='doordash':
          print("Order source is doordash ")
          if merchant_details.get('doordashstream')==0:
            print("Order not created because Doordash stream connectivity is disabled")
            create_log_data(level='[INFO]',
                            Message="Order not created because Doordash stream connectivity is disabled",
                            merchantID=merchant_id, functionName="stream_webhook")
            stream_success_resp = {
              "order":
                {
                  "provider_id": str(uuid.uuid4()),
                  "prep_time_minutes": 30
                }
            }
            return stream_success_resp, 201

        elif source=='grubhub':
          print("Order source is grubhub ")
          if merchant_details.get('grubhubstream')==0:
            print("Order not created because grubhub stream connectivity is disabled")
            create_log_data(level='[INFO]',
                            Message="Order not created because Grubhub stream connectivity is disabled",
                            merchantID=merchant_id, functionName="stream_webhook")
            stream_success_resp = {
              "order":
                {
                  "provider_id": str(uuid.uuid4()),
                  "prep_time_minutes": 30
                }
            }
            return stream_success_resp, 201

        for u_item in u_order_details.get('line_items'):

          # get item details by id
          item_details = Items.get_item_by_id(u_item['provider_id'])
          if not item_details:
            create_log_data(level='[ERROR]',
                            Message="Failed to get item detail",
                            messagebody=f"Error: item details not found: for id {str(u_item['provider_id'])}",
                            merchantID=merchant_id, functionName="stream_webhook")
            print("Error: item details not found: ", str(u_item['provider_id']))
            product_not_found_error_notification(user.get('user_id') ,merchant_id , "Product Not Found" ,source , 404 , f'Product <{u_item["name"]}> not found in menu! ' , source_order_id )
            continue

          # loop over item-addons
          addons_list = []
          addon_options_list = []
          if len(u_item['modifiers']) > 0:
            for u_option in u_item['modifiers']:

              # get addon option details by id from db
              option_details = Items.get_item_by_id(u_option['provider_id'])
              if not option_details:
                create_log_data(level='[ERROR]',
                                Message="Failed to get addon-option detail",
                                messagebody=f"Error: addon-option details not found: for id {str(u_option['provider_id'])}",
                                merchantID=merchant_id, functionName="stream_webhook")
                print("Error: addon-option details not found: ", str(u_option['provider_id']))
                product_not_found_error_notification(user.get('user_id'), merchant_id, "Addon Option Not Found" , source, 404,
                                                     f'Addon Option <{u_option["name"]}> not found in menu! ', source_order_id)
                continue

              # append addon-option details
              option_quantity = u_option['quantity']

              addon_options_list.append({
                "addonOptionID": option_details['id'],
                "price": option_details['itemUnitPrice'] if int(
                  merchant_details['marketplacepricestatus']) == 0 else float(
                  u_option.get('price_amount') / 100),
                "qty": option_quantity,
                "cost": float(option_details['itemUnitPrice']) if int(
                  merchant_details['marketplacepricestatus']) == 0 else float(
                  u_option.get('price_amount') / 100)
              })

            if len(addon_options_list) > 0:
              addons_list.append({
                'addonoptions': addon_options_list,
                'addonID': '94644371-2a8b-42c6-96d6-3615c9eb1c23'
              })

          # append item to itemlist
          items_list.append({
            'productid': item_details['id'],
            'cost': item_details['itemUnitPrice'] if int(merchant_details['marketplacepricestatus']) == 0 else float(
              u_item.get('price_amount') / 100),
            'qty': u_item['quantity'],
            'addons': addons_list,
            'Total': float(item_details['itemUnitPrice']) * int(u_item['quantity']) if int(
              merchant_details['marketplacepricestatus']) == 0 else float(u_item.get('price_amount') / 100) * int(u_item['quantity']),
            'specialInstructions': u_item.get('special_instructions')
          })

        create_log_data(level='[INFO]',
                        Message="Items list", messagebody=items_list,
                        merchantID=merchant_id, functionName="stream_webhook")
        # calculate sub-total and tax

        if len(items_list)==0:
          stream_error_resp = {
            "posValidation":
              {
                "orderValidation": [{
                  "order_error":"invalid_cart",
                  "order_error_reason": "Products not found in fonda dashboard"
                }]
              }
          }
          print('Return order error response to stream', stream_error_resp)
          create_log_data(level='[ERROR]',
                          Message="Return order ERROR response to stream ", messagebody=stream_error_resp,
                          merchantID=merchant_id, functionName="stream_webhook")
          return stream_error_resp, 400

        orderItemsAmount = 0
        for prod in items_list:
          prod_price = float(prod["Total"])
          unit_prod_price = float(prod["cost"])
          if prod.get("addons"):
            for adon in prod.get("addons"):
              for opt in adon.get("addonoptions"):
                prod_price += float(opt["cost"]) * prod['qty'] * opt['qty']
                unit_prod_price += float(opt["cost"])

          orderItemsAmount += prod_price
          prod['cost'] = unit_prod_price
          prod['Total'] = unit_prod_price * prod['qty']

        orderTax = float(u_order_details.get('tax') / 100)
        orderTotalAmount = orderItemsAmount + orderTax if int(merchant_details['marketplacepricestatus']) == 0 else float(
          u_order_details.get('subtotal') / 100)
        orderDeliveryFee = 0

        # datetime
        # Assuming u_order_details['placed_at'] is in milliseconds (e.g., 1731324028000)
        timestamp_ms = u_order_details['placed_at']
        # Convert milliseconds to seconds
        dt2 = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
        # dt3 = dt2.replace(tzinfo=None)
        phone_number = ''
        order['orderitems'] = items_list
        if "customer_phone" in u_order_details:
          phone_number = u_order_details['customer_phone']

        customer_name = u_order_details['customer_name']
        customer_name = re.sub(r'^pickup-', '', customer_name, flags=re.IGNORECASE)

        order['orderCustomer'] = {
          'id': None,
          'customerName': customer_name,
          'customerPhone': phone_number
        }
        order['orderExternalReference'] = source_order_id
        order['orderProcessingId'] = u_order_details['source_order_id']
        order['orderMerchantID'] = merchant_id
        order['orderSource'] = source
        order['orderDateTime'] = dt2.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        order['specialInstructions'] = u_order_details.get('special_instructions')
        order['orderType'] = u_order_details['fulfillment_type']
        order['scheduled'] = 0

        # if auto accept of order is 1 then accept order by default, else assign status 10 (Acceptance Pending) to order
        if merchant_details['autoacceptorder'] == 1:
          order['orderStatus'] = 0
        else:
          order['orderStatus'] = 10

        order['orderDeliveryFee'] = orderDeliveryFee
        order['orderSubTotal'] = orderItemsAmount
        order['orderTax'] = orderTax
        order['orderTotal'] = orderTotalAmount if int(
          merchant_details['marketplacepricestatus']) == 0 else orderTotalAmount + orderTax

        if main_merchant:
          order['virtualMerchant'] = main_merchant[0]['id']

        if merchant_details.get('is_bogo')==1:
          total_discount = sum(d["amount"] for d in u_order_details.get("discounts", []) if "amount" in d)
          if total_discount:
            order['discount'] = total_discount / 100

        if u_order_details.get('is_future_order') and 'requested_for' in u_order_details:
          order['scheduled']=1
          order['scheduledTime']=format_timestamp(timestamp_ms=u_order_details['requested_for'] , timezone_str=merchant_details.get('timezone'))
        order = {'order': order}
        print(order)
        create_log_data(level='[INFO]',
                        Message="order detail before post_order function ", messagebody=order,
                        merchantID=merchant_id, functionName="stream_webhook")
        # post order
        resp = Orders.post_order(_json=order , userID=user.get('user_id'))
        json_response = resp.get_json()
        create_log_data(level='[INFO]',
                        Message="resposne from post order ", messagebody=json_response,
                        merchantID=merchant_id, functionName="stream_webhook")
        if resp.status_code != 200:
          create_log_data(level='[INFO]',
                          Message="Unable to create order",
                          merchantID=merchant_id, functionName="stream_webhook")

          error_msg= json_response.get('message') if 'message' in json_response else "Unable to create order"
          print("Unable to create order")
          # return resp
          stream_error_resp = {
            "posValidation":
              {
                "orderValidation": [{
                  "order_error": "other",
                  "order_error_reason": error_msg
                }]
              }
          }
          return stream_error_resp, 400
        elif resp.status_code == 200:
            other_env_order_json = order

            sns_msg = {
                "event": "other_env_order_creations",
                "body": {
                    "order_json": other_env_order_json
                }
            }
            order_sns_resp = publish_sns_message(topic=config.sns_create_order_in_other_env, message=str(sns_msg),
                                                      subject="other_env_order_creations")
        prep_time_minutes = int(merchant_details['preparationtime']) + int(merchant_details['orderdelaytime']) if merchant_details['busymode'] == 1 else int(merchant_details['preparationtime'])
        stream_success_resp={
          "order":
            {
              "provider_id": json_response.get('id'),
              "prep_time_minutes":prep_time_minutes
            }
        }
        print('Return order success response to stream' , stream_success_resp)
        create_log_data(level='[INFO]',
                        Message="Return order success response to stream ", messagebody=stream_success_resp,
                        merchantID=merchant_id, functionName="stream_webhook")
        return stream_success_resp, 201
      elif eventType in ("order.canceled", "order.updated"):

        u_order_details = order_json.get('object', {})
        print('Cancel order event trigger', u_order_details)
        create_log_data(level='[INFO]',
                        Message="Cancel order from stream event trigger ", messagebody=u_order_details,
                        functionName="stream_webhook")
        Cancellation_reason = ""
        if eventType == "order.updated" and u_order_details.get('status') in ('dsp_canceled', 'merchant_canceled'):
          Cancellation_reason = "dsp_cancelled"
        elif eventType == "order.canceled":
          Cancellation_reason = "customer cancelled"
        else:
          return 'success' , 201

        resp = Orders.get_order_details_str(u_order_details.get('order_id'))
        create_log_data(level='[INFO]',
                        Message=f"Getting order detail for order id : {u_order_details.get('order_id')} ", messagebody=resp,
                        functionName="stream_webhook")
        connections = Websockets.get_connection_by_mid_and_eventname(merchantId=u_order_details.get('location_id'), eventName="android.order")
        create_log_data(level='[INFO]',
                        Message=f"Getting websockets connection for merchantid : {u_order_details.get('location_id')} ",
                        messagebody=connections,
                        functionName="stream_webhook")
        if type(connections) is list:
          for connection in connections:
            deviceId = connection.get("connectionId")

            try:
              response = send_android_notification_api(deviceId=deviceId, subject="order.streamCancelled", orderId=u_order_details.get('order_id'), orderdetail=resp , Cancellation_reason=Cancellation_reason)
              # print(response.text)
              if response.status_code >= 200 and response.status_code < 300:
                create_log_data(level='[INFO]',
                                Message=f"Posted notification to android against orderid: {u_order_details.get('order_id')} and deviceid: {deviceId}",
                                messagebody=response,
                                functionName="stream_webhook")
                print(f"Posted notification to android against orderid: {u_order_details.get('order_id')} and deviceid: {deviceId}")
              else:
                create_log_data(level='[INFO]',
                                Message=f"Unable to posting notification to android against orderid: {u_order_details.get('order_id')} and Device Id:{deviceId}",
                                messagebody=response.text,
                                functionName="stream_webhook")
                print(
                  f"Unable to posting notification to android against orderid: {u_order_details.get('order_id')} and Device Id:{deviceId}, Response is {response.text}")
            except Exception as e:
              print("Error: ", str(e))
        # Orders.update_order_status(u_order_details.get('order_id'), 9, reason="OTHER", explanation='Stream webhook called for cancellation' , _json=u_order_details,
        #                             caller="Stream Webhook")


    else:
      create_log_data(level='[ERROR]',
                      Message=f"Token is expired , Token : {token}, IP address: {ip_address}",
                      functionName="stream_webhook", request=request)
      stream_error_resp = {
        "posValidation":
          {
            "orderValidation": [{
              "order_error": "other",
              "order_error_reason": "Token is expired"
            }]
          }
      }
      return stream_error_resp, 400

    return {} , 201
  except Exception as e:
    create_log_data(level='[ERROR]',
                    Message=f"Exception occured ", messagebody=str(e),
                    functionName="stream_webhook", request=request)
    print("Error on creating order", str(e))
    stream_error_resp = {
      "posValidation":
        {
          "orderValidation": [{
            "order_error": "other",
            "order_error_reason": f"Error on creating order {str(e)}"
          }]
        }
    }
    return stream_error_resp, 400


def test_stream_update_status():
  Stream.update_stream_order_status('4426fb19-9e4f-4277-ab77-89ba267717f7' , '86eaf4cf-a077-4f73-9d65-08802a512912' , 9)
  return "ok"

def product_not_found_error_notification(userId ,merchantId , errorName , errorSource , errorStatus , errorDetails , orderExternalReference ):
  print("Triggering SNS For product_not_found_error_notification")
  sns_msg = {
    "event": "error_logs.entry",
    "body": {
      "userId": userId,
      "merchantId": merchantId,
      "errorName": errorName,
      "errorSource":errorSource,
      "errorStatus": errorStatus,
      "errorDetails": errorDetails,
      "orderExternalReference": orderExternalReference
    }
  }
  error_logs_sns_resp = publish_sns_message(topic=config.sns_error_logs, message=str(sns_msg),
                                            subject="error_logs.entry")


def format_timestamp(timestamp_ms, timezone_str='US/Pacific'):
  # Convert milliseconds to seconds
  timestamp_sec = timestamp_ms / 1000
  # Create a timezone object
  local_tz = pytz.timezone(timezone_str)
  # Convert timestamp to datetime in the specified timezone
  dt = datetime.fromtimestamp(timestamp_sec, tz=local_tz)
  # Format the datetime
  return dt.strftime('%m-%d-%Y %H:%M:%S')