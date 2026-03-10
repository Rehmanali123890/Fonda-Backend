import json
from models.ProductsAddons import ProductsAddons
import requests
import stripe
from flask import jsonify, request, g

from io import BytesIO
import urllib.request
import gzip
from models.Orders import Orders
from models.Storefront import Storefront
from models.VirtualMerchants import VirtualMerchants
from utilities.errors import invalid, not_found, unhandled, unauthorised

# local imports
from utilities.helpers import *
from utilities.errors import *
from models.Storefront import *
from models.Merchants import Merchants
from controllers.Middleware import validate_token_middleware
from random import choices
from string import ascii_uppercase, digits
import datetime
from dateutil.relativedelta import relativedelta
from dateutil.tz import gettz
import dateutil.parser as parser
# from datetime import datetime, timedelta
import pdfkit
from flask import render_template
from flask import make_response
import config
import re

def getStoreFront(slug):
  try:
    return Storefront.get_storefront_details(slug)

  except Exception as e:
    print("Error")
    print(str(e))
    return invalid(str(e))


def getStoreFrontMenu(slug):
  try:
    create_log_data(level='[INFO]', Message="In the start of method getStoreFrontMenu to get store front menu ",
                    messagebody=f" Get Store Menu by slug {slug}", functionName="getStoreFrontMenu",request=request)
    store_front = Storefront.get_storefront_by_slug(slug)
    if not store_front:
      create_log_data(level='[ERROR]', Message="Failed to retrieve the merchant/storefront detail by slug. ",
                      messagebody=f"Unable to get the merchant by slug : {slug}", functionName="getStoreFrontMenu",
                      statusCode="400",
                      request=request)
      return invalid("Failed to retrieve store front")

    menu = Storefront.create_menu(store_front['id'])
    create_log_data(level='[INFO]', Message=f"Successfully retrieved store front menu by merchant {store_front['id']}",
                    messagebody=f"menu: {menu}", functionName="getStoreFrontMenu",
                    request=request , statusCode="200 Ok")

    return success_gzip({
      "message": "success",
      "status": 200,
      "menu": menu
    })


  except Exception as e:
    print("Error")
    print(str(e))
    create_log_data(level='[ERROR]',
                    Message="Exception occur. Failed to get store front menu ",
                    messagebody=str(e),
                    functionName="getStoreFrontMenu", statusCode=f"400 Bad Request", request=request)
    return False


def createStoreFrontOrder(slug):
  # try:
  _json = request.json

  # try:
  #     stripe.api_key = config.stripe_api_key
  #     stripe.api_version = "2020-08-27"
  #
  #     verification = stripe.Charge.retrieve(
  #         _json['order']['chargeId']
  #     )
  # except stripe.error.InvalidRequestError as e:
  #     return e.user_message, True
  create_log_data(level='[INFO]', Message="In the start of method createStoreFrontOrder to create order ",
                  messagebody=_json, functionName="createStoreFrontOrder", statusCode="200 Ok",
                  request=request)
  store_front = Storefront.get_storefront_by_slug(slug)
  if store_front:
    create_log_data(level='[INFO]', Message="Retrieve the merchant/storefront detail by slug. ",
                    messagebody=store_front, functionName="createStoreFrontOrder", statusCode="200 Ok",
                    request=request)
  else:
    create_log_data(level='[ERROR]', Message="Failed to retrieve the merchant/storefront detail by slug. ",
                    messagebody=f"Unable to get the merchant by slug : {slug}", functionName="createStoreFrontOrder", statusCode="400",
                    request=request)

  mid = store_front['id']
  main_merchant = VirtualMerchants.get_virtual_merchant(mid)

  if main_merchant:
    create_log_data(level='[INFO]', Message=f"Retrieve the virtual merchant by merchant id : {mid}. ",
                    messagebody=main_merchant, functionName="createStoreFrontOrder", statusCode="200 Ok",
                    request=request)
    merchant_id = main_merchant[0]['merchantid']
  else:
    merchant_id = mid

  _json['order']['orderMerchantID'] = merchant_id
  _json['url_slug'] = slug
  if main_merchant:
    _json['order']['virtualMerchant'] = main_merchant[0]['id']


  resp = Orders.post_order(_json=_json, userID='', storeFront=1, request=request)
  if resp.status_code == 200:
      other_env_order_json = _json

      sns_msg = {
          "event": "other_env_order_creations",
          "body": {
              "order_json": other_env_order_json
          }
      }
      order_sns_resp = publish_sns_message(topic=config.sns_create_order_in_other_env, message=str(sns_msg),
                                                subject="other_env_order_creations")
  return resp



# except Exception as e:
#     print("Error: ", str(e))
#     return unhandled()


def CreatePaymentIntent(slug):
  try:
    _json = request.json
    create_log_data(level='[INFO]',
                    Message="In the beginning of CreatePaymentIntent code function to create payment Intent",messagebody=_json,
                    functionName="CreatePaymentIntent", request=request)
    store_front = Storefront.get_storefront_by_slug(slug)
    if not store_front:
      create_log_data(level='[ERROR]',
                      Message=f"Failed to retieve store front by slug {slug}",
                      messagebody=f"Unable to find store front against slug {slug}",
                      functionName="CreatePaymentIntent", statusCode="400 Bad Request")
      return invalid("Failed to find store front detail")


    if store_front.get('status') == 0:
      print('merchant status is inactive and is not receiving orders!')
      return invalid('merchant status is inactive and is not receiving orders!')
    if store_front.get('marketstatus') == 0:
      print('merchant status is paused and is not receiving orders!')
      return invalid('merchant status is paused and is not receiving orders!')
    create_log_data(level='[INFO]',
                    Message="Successfully retrieve store front detail",
                    messagebody=store_front,
                    functionName="CreatePaymentIntent", request=request)
    statement_descriptor = '{:5.22}'.format(store_front['merchantname'])

    stripe.api_key = config.stripe_api_key
    stripe.api_version = "2020-08-27;link_beta=v1"
    token = stripe.PaymentIntent.create(
      amount=_json['amount'],
      currency='usd',
      payment_method_types=['card', 'link'],
      statement_descriptor= re.sub(r'[^a-zA-Z0-9\s]', '', statement_descriptor),
      metadata={
        "Order Source": "Storefront",
        "Restaurant Name": store_front['merchantname']
      }
    )
    create_log_data(level='[INFO]',
                    Message="Successfully create stripe payment intent",
                    messagebody=token,
                    functionName="CreatePaymentIntent", request=request)
    return success(jsonify(token))

  except Exception as e:
    print("Error: ", str(e))
    return unhandled()


def getDeliveryFee(slug):
  try:

    quote_id = ascii_uppercase + digits
    quote_id = str.join('', choices(quote_id, k=10))
    token = Storefront.get_doordash_jwt()
    address = request.json
    create_log_data(level='[INFO]',
                    Message="In the beginning of getDeliveryFee code function to get delivery fee for storefront order",
                    messagebody=address,
                    functionName="getDeliveryFee", request=request)


    store_front = Storefront.get_storefront_by_slug(slug)
    Static_delivery_fees_flag , Static_delivery_fees_amount = Storefront.get_static_delivery_fee_configuration()
    if not store_front:
      create_log_data(level='[ERROR]',
                      Message=f"Failed to retieve store front by slug {slug}",
                      messagebody=f"Unable to find store front against slug {slug}",
                      functionName="getDeliveryFee", statusCode="400 Bad Request")
      return invalid("Failed to find store front detail")

    pickup_time = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc).astimezone(gettz(store_front['timezone']))
    if address['tipAmount'] ==None:
      address['tipAmount']=0
      tip_amount=0
    else:
     tip_amount = address['tipAmount'] * (60 / 100) if address['tipAmount'] > 0 else 0

    if address['orderValue'] == None:
      address['orderValue']=0
    if store_front['busymode'] == 1:
      add_time = int(store_front['preparationtime']) + int(store_front['orderdelaytime'])
      print("busy mode")
    else:
      add_time = int(store_front['preparationtime'])

    pickup_time = pickup_time + datetime.timedelta(hours=0, minutes=add_time, seconds=0)
    print(pickup_time)
    pickup_time = pickup_time.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    print(pickup_time)
    # Get values from the dictionary
    address_line = store_front.get('businessaddressline', '')
    city = store_front.get('businessaddresscity', '')
    state = store_front.get('businessaddressstate', '')
    zipcode = store_front.get('zipcode', '')

    # Replace None or empty strings with empty space
    address_line = address_line if address_line else ' '
    city = city if city else ' '
    state = state if state else ' '
    zipcode = zipcode if zipcode else ' '

    # Concatenate the strings
    merchant_address = address_line + " " + city + " " + state + " " + zipcode

    url = config.DOOR_DASH_DRIVE + "drive/v2/quotes"
    payload = json.dumps({
      "external_delivery_id": quote_id,
      "pickup_address": merchant_address,
      "pickup_business_name": store_front['merchantname'],
      "pickup_phone_number": store_front['businessnumber'],
      "dropoff_address": address['address'],
      "dropoff_phone_number": address['phone'],
      "dropoff_instructions": address['instructions'],
      "dropoff_contact_given_name": address['name'],
      "dropoff_contact_send_notifications": True,
      "order_value": address['orderValue'],
      "currency": "USD",
      "pickup_time": pickup_time,
      "tip": int(tip_amount * 100),
      "contactless_dropoff": True if address['contactlessDropoff'] == 1 else False
      # "action_if_undeliverable": "return_to_pickup"
    }).encode('utf-8')
    headers = {
      'Content-Type': 'application/json',
      'Authorization': 'Bearer ' + token

    }

    # response = requests.request("POST", url, headers=headers, data=payload)
    # exception on this   ('Received response with content-encoding: gzip, but failed to decode it.', error('Error -3 while decompressing data: incorrect header check'))
    create_log_data(level='[INFO]',
                    Message="Call the api to get delivery fee address",
                    messagebody=payload,
                    functionName="getDeliveryFee", request=request)
    req = urllib.request.Request(url, data=payload, headers=headers)
    with urllib.request.urlopen(req) as response:
      # Check if the response is gzip-encoded
      if response.info().get('Content-Encoding') == 'gzip':
        compressed_data = response.read()
        response_text = compressed_data.decode('utf-8')

      else:
        response_text = response.read().decode('utf-8')

    print(response_text)
    response = json.loads(response_text)
    print(response)
    create_log_data(level='[INFO]',
                    Message="Response From API that get delivery fee",
                    messagebody=response,
                    functionName="getDeliveryFee", request=request)

    if "fee" in response:
      format = '%Y-%m-%dT%H:%M:%SZ'
      estimated_pickup = datetime.datetime.strptime(response['pickup_time_estimated'], format)
      estimated_delivery = datetime.datetime.strptime(response['dropoff_time_estimated'], format)

      diff = estimated_delivery - estimated_pickup
      estimated_delivery_time = (diff.seconds / 60) + add_time
      response['estimated_delivery_time'] = int(estimated_delivery_time)
      print(estimated_delivery_time)
      create_log_data(level='[INFO]',
                      Message="Successfully get delivery fee from API ",
                      messagebody=response,
                      functionName="getDeliveryFee", request=request)
      response['Static_delivery_fees_flag']=Static_delivery_fees_flag
      response['Static_delivery_fees_amount']=Static_delivery_fees_amount
      return success(jsonify({
        "message": "success",
        "status": 200,
        "data": response
      }))
    create_log_data(level='[ERROR]',
                    Message="Failed to get delivery fee from API ",
                    messagebody=response,
                    functionName="getDeliveryFee", request=request , statusCode="400 Bad Request")
    return invalid(response)

  except Exception as e:
    if e.code==400:
      create_log_data(level='[INFO]',
                      Message="Exception occur. Failed to get delivery fee from API ",
                      messagebody="dropoff phone is wrong",
                      functionName="getDeliveryFee",statusCode=f"{e.code} Bad Request", request=request  )
      return failed(jsonify({
        "message": "dropoff  phone  is wrong",
        "status": 400,
      }),status=400)
    elif e.code==422:
      create_log_data(level='[INFO]',
                      Message="Exception occur. Failed to get delivery fee from API ",
                      messagebody="The dropoff address is either incorrect or outside the coverage area.",
                      functionName="getDeliveryFee", statusCode=f"{e.code} Bad Request", request=request)
      return failed(jsonify({
        "message": "The dropoff address is either incorrect or outside the coverage area.",
        "status": 422,
      }),status=422)
    create_log_data(level='[INFO]',
                    Message="Exception occur. Failed to get delivery fee from API ",
                    messagebody=str(e),
                    functionName="getDeliveryFee", statusCode=f"400 Bad Request", request=request)
    print(str(e))
    return False


def qrStoreFront(merchentid):
  try:
    merchent = Storefront.get_storefront_by_id(merchentid)
    storefront_base = config.app_base_URL_storefront_food


    url = "https://api.qrserver.com/v1/create-qr-code/?data=" + storefront_base + "/" + merchent['slug'] +  "?type=qr" + "&size=300x300"

    html = render_template(
      "qrcodepdf.html",
      resp={
        "url": url,
        "merchant": merchent['merchantname']
      })
    path_wkhtmltopdf = os.environ.get("wkhtmltopdf_path")
    pdfconfig = pdfkit.configuration(wkhtmltopdf=path_wkhtmltopdf)
    pdf = pdfkit.from_string(html, False, configuration=pdfconfig)
    response = make_response(pdf)
    response.headers["Content-Type"] = "application/pdf"
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Access-Control-Allow-Headers'] = '*'
    response.headers['Access-Control-Allow-Methods'] = '*'
    response.headers["Content-Disposition"] = "inline; filename=menu.pdf"

    return response


  except Exception as e:
    print("Error")
    print(str(e))
    return False


def AddSourceQr(merchentid):
  ip_addr = None
  if request:
      ip_addr = request.environ.get('HTTP_X_FORWARDED_FOR', request.remote_addr)
  if ip_addr:
      ip_addr = ip_addr.split(',')[0].strip()
  try:
    create_log_data(level='[INFO]',
                    Message=f"In the beginning of AddSourceQr code function to create a new qr code for the specified merchant, IP address: {ip_addr}",
                    merchantID=merchentid, functionName="AddSourceQr", request=request)
    user_token = request.args.get('token')
    user = None
    if user_token:
      user = validateLoginToken(user_token, userFullDetail=1)
      if not user:
        create_log_data(level='[ERROR]',
                        Message="The API token is invalid.",
                        messagebody=f"Unable to find the user on the basis of provided token., IP address: {ip_addr}",
                        functionName="AddSourceQr", statusCode="400 Bad Request")
        return invalid("Invalid Token")
    else:
      create_log_data(level='[ERROR]',
                      Message="The API token is not found.",
                      messagebody=f"Unable to get api token in request argument, IP address: {ip_addr}",
                      functionName="AddSourceQr", statusCode="400 Bad Request")

    error = Storefront.add_qrCode(merchentid)

    if error:
      create_log_data(level='[ERROR]', Message=f"Unable to create a new qr code, IP address: {ip_addr}",
                      messagebody=error, merchantID=merchentid, user=user,
                      functionName="AddSourceQr", statusCode="400 Bad Request", request=request)
      return invalid(error)

    create_log_data(level='[INFO]', Message=f"Successfully created a new qr code, IP address: {ip_addr}",
                    messagebody=request.json, merchantID=merchentid, user=user,
                    functionName="AddSourceQr", statusCode="200 Ok", request=request)
    sns_msg = {
      "event": "merchant.AddSourceQr",
      "body": {
        "merchantId": merchentid,
        "userId": user['id'],
        "eventDetails": f"Qr code added successfully, IP address: {ip_addr}",
        "eventType": "activity",
        "eventName": "qr.add"
      }
    }

    publish_sns_message(topic=config.sns_activity_logs, message=str(sns_msg),
                        subject="QR_activity_logs")

    return success(jsonify({
      "message": "success",
      "status": 200,
      "data": "Qr code added successfully!"
    }))

  except Exception as e:
    print("Error: ", str(e))
    create_log_data(level='[ERROR]', Message=f"Exception occured. Failed to create a new qr code, IP address: {ip_addr}",
                    messagebody=e, merchantID=merchentid,
                    functionName="AddSourceQr", statusCode="400 Bad Request", request=request)
    return unhandled()
  
def DeleteSourceQr(sourceqrid):
  ip_addr = None
  if request:
      ip_addr = request.environ.get('HTTP_X_FORWARDED_FOR', request.remote_addr)
  if ip_addr:
      ip_addr = ip_addr.split(',')[0].strip()
  try:
    create_log_data(level='[INFO]', Message=f"In the beginning of DeleteSourceQr code function to create a new qr code for the specified merchant, IP address: {ip_addr}",
                        sourceqrId=sourceqrid, functionName="DeleteSourceQr",request=request)
    user_token = request.args.get('token')
    user = None
    if user_token:
      user = validateLoginToken(user_token, userFullDetail=1)
      if not user:
        create_log_data(level='[INFO]',
                        Message="The API token is invalid.",
                        messagebody=f"Unable to find the user on the basis of provided token., IP address: {ip_addr}",
                        functionName="DeleteSourceQr", statusCode="400 Bad Request")
        return invalid("Invalid Token")
    else:
      create_log_data(level='[INFO]',
                      Message="The API token is not found.",
                      messagebody=f"Unable to get api token in request argument, IP address: {ip_addr}",
                      functionName="DeleteSourceQr", statusCode="400 Bad Request")
    
    error = Storefront.delete_qrCode(sourceqrid)

    if error:
      create_log_data(level='[ERROR]', Message=f"Unable to delete a qr code, IP address: {ip_addr}",
                  messagebody=error, sourceqrId=DeleteSourceQr, user=user,
                  functionName="DeleteSourceQr", statusCode="400 Bad Request", request=request)
      return invalid(error)

    create_log_data(level='[INFO]', Message=f"Successfully delete a qr code, IP address: {ip_addr}",
                      sourceqrId=sourceqrid, user=user,
                      functionName="DeleteSourceQr", statusCode="200 Ok", request=request)
    
    
    return success(jsonify({
      "message": "success",
      "status": 200,
      "data": "Qr code deleted successfully!"
    }))

  except Exception as e:
    print("Error: ", str(e))
    create_log_data(level='[ERROR]', Message=f"Exception occured. Failed to create a new qr code, IP address: {ip_addr}",
            messagebody=e, sourceqrId=sourceqrid,
            functionName="AddSourceQr", statusCode="400 Bad Request", request=request)
    return unhandled()
  
def AddPromo(merchentid):
  ip_addr = None
  if request:
      ip_addr = request.environ.get('HTTP_X_FORWARDED_FOR', request.remote_addr)
  if ip_addr:
      ip_addr = ip_addr.split(',')[0].strip()
  try:
    create_log_data(level='[INFO]', Message=f"In the beginning of AddPromo code function to create a new promo for the specified merchant, IP address: {ip_addr}",
                        merchantID=merchentid, functionName="AddPromo",request=request)
    user_token = request.args.get('token')
    user = None
    if user_token:
      user = validateLoginToken(user_token, userFullDetail=1)
      if not user:
        create_log_data(level='[ERROR]',
                        Message="The API token is invalid.",
                        messagebody=f"Unable to find the user on the basis of provided token., IP address: {ip_addr}",
                        functionName="AddPromo", statusCode="400 Bad Request")
        return invalid("Invalid Token")
    else:
      create_log_data(level='[ERROR]',
                      Message="The API token is not found.",
                      messagebody=f"Unable to get api token in request argument, IP address: {ip_addr}",
                      functionName="AddPromo", statusCode="400 Bad Request")
    
    error = Storefront.add_promo(merchentid)

    if error:
      create_log_data(level='[ERROR]', Message=f"Unable to create a new promo code, IP address: {ip_addr}",
                  messagebody=error, merchantID=merchentid, user=user,
                  functionName="AddPromo", statusCode="400 Bad Request", request=request)
      return invalid(error)

    create_log_data(level='[INFO]', Message="Successfully created a new promo code",
                      messagebody=request.json, merchantID=merchentid, user=user,
                      functionName="AddPromo", statusCode="200 Ok", request=request)
    sns_msg = {
      "event": "merchant.addPromo",
      "body": {
        "merchantId": merchentid,
        "userId": user['id'],
        "eventDetails": f"Promo added successfully, IP address: {ip_addr}",
        "eventType": "activity",
        "eventName": "promo.add"
      }
    }

    publish_sns_message(topic=config.sns_activity_logs, message=str(sns_msg),
                        subject="Promo_activity_logs")
    
    return success(jsonify({
      "message": "success",
      "status": 200,
      "data": "Promo code added successfully!"
    }))

  except Exception as e:
    print("Error: ", str(e))
    create_log_data(level='[ERROR]', Message=f"Exception occured. Failed to create a new promo code, IP address: {ip_addr}",
            messagebody=e, merchantID=merchentid,
            functionName="AddPromo", statusCode="400 Bad Request", request=request)
    return unhandled()


def EditPromo(merchentid):
  ip_addr = None
  if request:
      ip_addr = request.environ.get('HTTP_X_FORWARDED_FOR', request.remote_addr)
  if ip_addr:
      ip_addr = ip_addr.split(',')[0].strip()
  try:
    create_log_data(level='[INFO]',
                    Message=f"In the beginning of EditPromo code function to edit a promo code for the specified merchant, IP address: {ip_addr}",
                    merchantID=merchentid, functionName="EditPromo", request=request)
    user_token = request.args.get('token')
    if user_token:
      user = validateLoginToken(user_token, userFullDetail=1)
      if not user:
        create_log_data(level='[ERROR]',
                        Message="The API token is invalid.",
                        messagebody=f"Unable to find the user on the basis of provided token., IP address: {ip_addr}",
                        functionName="EditPromo", statusCode="400 Bad Request")
        return invalid("Invalid Token")
    else:
      create_log_data(level='[ERROR]',
                      Message="The API token is not found.",
                      messagebody=f"Unable to get api token in request argument, IP address: {ip_addr}",
                      functionName="EditPromo", statusCode="400 Bad Request")
      return invalid("Api token not found")

    error = Storefront.edit_promo(merchentid, user, ipAddress=ip_addr)

    if error:
      create_log_data(level='[ERROR]', Message=f"Unable to edit a promo code for the specified merchant, IP address: {ip_addr}",
                      messagebody=error, merchantID=merchentid, user=user,
                      functionName="EditPromo", statusCode="400 Bad Request", request=request)
      return invalid(error)

    create_log_data(level='[INFO]', Message=f"Promo code updated successfully, IP address: {ip_addr}",
                    messagebody=request.json, merchantID=merchentid, user=user,
                    functionName="EditPromo", statusCode="200 Ok", request=request)

    return success(jsonify({
      "message": "success",
      "status": 200,
      "data": "Promo edited successfully!"
    }))

  except Exception as e:
    print("Error: ", str(e))
    create_log_data(level='[ERROR]', Message=f"Exception occur. Failed to create a new promo code, IP address: {ip_addr}",
                    messagebody=e, merchantID=merchentid,
                    functionName="EditPromo", statusCode="400 Bad Request", request=request)
    return unhandled()


def GetAllPromo(merchentid):
  ip_addr = None
  if request:
      ip_addr = request.environ.get('HTTP_X_FORWARDED_FOR', request.remote_addr)
  if ip_addr:
      ip_addr = ip_addr.split(',')[0].strip()
  try:
    create_log_data(level='[INFO]', Message=f"In the beginning of GetAllPromo code function to get all promo code for the specified merchant, IP address: {ip_addr}",
                    merchantID=merchentid, functionName="GetAllPromo",request=request)
    
    status = request.args.get('status') if request.args.get('status') else None
    limit = request.args.get('limit') if request.args.get('limit') else 0
    startdate = datetime.datetime.strptime(request.args.get('startdate'), "%Y-%m-%d") if request.args.get('startdate') else ""
    enddate = datetime.datetime.strptime(request.args.get('enddate'), "%Y-%m-%d") if request.args.get('enddate') else ""

    create_log_data(level='[INFO]', Message=f"Requesting to get all promo code for specified merchent, IP address: {ip_addr}",
                        messagebody=f"merchantid={merchentid} , status={status},limit={limit}, startdateoforder={startdate}, enddateoforder={enddate}",
                        merchantID=merchentid, functionName="GetAllPromo")
    promo = Storefront.get_all_promo(merchantid=merchentid, status=status, limit=limit, startdate=startdate, enddate=enddate)
    items = Storefront.get_menu_item(merchentid)
    freeItems= Storefront.get_menu_item(merchentid , freeitem=1)
    
    if isinstance(promo, Exception):
      print("Error: ", str(promo))
      create_log_data(level='[INFO]', Message=f"Unable to get all promo code for the specified merchant, IP address: {ip_addr}",
              messagebody=str(promo), merchantID=merchentid,
              functionName="GetAllPromo")
      return invalid( str(promo))
      

    create_log_data(level='[INFO]', Message=f"Successfully get all promo code for the specified merchant, IP address: {ip_addr}",
                      messagebody=promo, merchantID=merchentid,
                      functionName="GetAllPromo", statusCode="200 Ok", request=request)

    return success(jsonify({
      "message": "success",
      "status": 200,
      "data": promo,
      "items": items,
      "freeItems": freeItems
    }))

  except Exception as e:
    print("Error: ", str(e))
    create_log_data(level='[ERROR]', Message=f"Exception occur. Failed to get all Promos for the specified merchant, IP address: {ip_addr}",
            messagebody=e, merchantID=merchentid,
            functionName="GetAllPromo", statusCode="400 Bad Request", request=request)
    return unhandled()

def GetAllSourceQr(merchentid):
  ip_addr = None
  if request:
      ip_addr = request.environ.get('HTTP_X_FORWARDED_FOR', request.remote_addr)
  if ip_addr:
      ip_addr = ip_addr.split(',')[0].strip()
  try:
    create_log_data(level='[INFO]', Message=f"In the beginning of GetAllSourceQr code function to get all qr code for the specified merchant, IP address: {ip_addr}",
                    merchantID=merchentid, functionName="GetAllSourceQr",request=request)
    qr = Storefront.get_all_source_qr(merchantid=merchentid)
    
    if isinstance(qr, Exception):
      create_log_data(level='[ERROR]', Message=f"Unable to get all qr code for the specified merchant, IP address: {ip_addr}",
              messagebody=str(qr), merchantID=merchentid,
              functionName="GetAllSourceQr")

      return invalid("Unable to get all qr code for the specified merchant")

      

    create_log_data(level='[INFO]', Message=f"Successfully get all qr code for the specified merchant, IP address: {ip_addr}",
                      messagebody=qr, merchantID=merchentid,
                      functionName="GetAllSourceQr", statusCode="200 Ok", request=request)

    return success(jsonify({
      "message": "success",
      "status": 200,
      "data": qr
    }))

  except Exception as e:
    print("Error: ", str(e))
    create_log_data(level='[ERROR]', Message=f"Exception occur. Failed to get all Promos for the specified merchant, IP address: {ip_addr}",
            messagebody=e, merchantID=merchentid,
            functionName="GetAllSourceQr", statusCode="400 Bad Request", request=request)
    return unhandled()

def GetStorefrontPromo(merchentid):
  ip_addr = None
  if request:
      ip_addr = request.environ.get('HTTP_X_FORWARDED_FOR', request.remote_addr)
  if ip_addr:
      ip_addr = ip_addr.split(',')[0].strip()
  try:
    create_log_data(level='[INFO]', Message=f"In the beginning of GetStorefrontPromo code function to get Storefront promo code, IP address: {ip_addr}",
                    merchantID=merchentid, functionName="GetStorefrontPromo")
    
    create_log_data(level='[INFO]', Message=f"Requesting to get all promo code for Storefront, IP address: {ip_addr}",
                          merchantID=merchentid, functionName="GetStorefrontPromo")
    
    promo = Storefront.get_all_storefront_promo(merchentid)
    
    if isinstance(promo, Exception):
          print("Error: ", str(promo))
          create_log_data(level='[ERROR]', Message=f"Unable to to get Storefront promo code for specified merchant {merchentid}, IP address: {ip_addr}",
                  messagebody=str(promo), merchantID=merchentid,
                  functionName="GetStorefrontPromo")
          return invalid( str(promo))

    create_log_data(level='[INFO]', Message=f"Successfully get all promo code of Storefront for specified merchant, IP address: {ip_addr}",
                    messagebody=promo, merchantID=merchentid,
                    functionName="GetStorefrontPromo", statusCode="200 Ok", request=request)

    return success(jsonify({
      "message": "success",
      "status": 200,
      "data": promo
    }))

  except Exception as e:
    print("Error: ", str(e))
    create_log_data(level='[ERROR]', Message="Failed to get Storefront promo code",
                  messagebody=f"An error ocuured: {str(e)}", merchantID=merchentid,
                  functionName="GetStorefrontPromo")
    return unhandled()

def GenerateFrontEndLogs(merchantid):
  try:
    _json=request.json

    if _json.get("log_type") == 1:
        log_level='[INFO]'
        log_msg= f"Store front cart items on  {_json.get('type')} < {_json.get('productName')} >" if _json.get('type') !='checkout' else f"Store front cart items on {_json.get('type')}."
    elif _json.get("log_type") == 8:   # For stream opensearch logs from parser node js app
      log_level = '[INFO]'
      log_msg = _json.get("log_msg")
    else:
        log_level = '[INFO]'
        log_msg='On verifying customer phone number by twillio API'
    create_log_data(level=log_level,
                    Message=log_msg,
                    messagebody=_json.get("event_detail"),merchantID=merchantid, functionName="GenerateFrontEndLogs" , request=request)
    return success(jsonify({
      "message": "success",
      "status": 200,

    }))

  except Exception as e:
    print("Error: ", str(e))
    create_log_data(level='[ERROR]', Message="Failed to get Storefront promo code",
                    messagebody=f"An error ocuured: {str(e)}", merchantID=merchantid,
                    functionName="GetStorefrontPromo")
    return unhandled()
def CheckPromo(slug, promo):
  ip_addr = None
  if request:
      ip_addr = request.environ.get('HTTP_X_FORWARDED_FOR', request.remote_addr)
  if ip_addr:
      ip_addr = ip_addr.split(',')[0].strip()
  try:
    create_log_data(level='[INFO]',
                    Message=f"In the beginning of CheckPromo code function to check promo code, IP address: {ip_addr}",
                    functionName="CheckPromo", request=request)
    merchant = Storefront.get_storefront_by_slug(slug)

    valid_promo = None
    if merchant:
      valid_promo = Storefront.validate_promo(merchant['id'], promo)

    if valid_promo:
      create_log_data(level='[INFO]',
                      Message=f"Successfully validate the promo {valid_promo} against merchant {merchant}, IP address: {ip_addr}",
                      merchantID=merchant['id'], functionName="CheckPromo", request=request)
      happyhourstarttime =  Storefront.timedelta_to_24_hour_format(valid_promo['happyhourstarttime']) if valid_promo['ishappyhourenabled'] ==1 else ''
      happyhourendtime =  Storefront.timedelta_to_24_hour_format(valid_promo['happyhourendtime']) if valid_promo['ishappyhourenabled'] ==1 else ''


      return success(jsonify({
        "message": "success",
        "status": 200,
        "data": {
          "promo": valid_promo['promo'],
          "primaryItem": valid_promo['primaryitem'] if valid_promo['primaryitem'] else None,
          "freeitem": valid_promo['freeitem'] if valid_promo['freeitem'] else None,
          "primaryItemQuantity":valid_promo['primaryitemquantity'] if valid_promo['primaryitemquantity'] else 0,
          "freeItemQuantity":valid_promo['freeitemquantity'] if valid_promo['freeitemquantity'] else 0,
          "promoStartDate": valid_promo['promostartdate'].strftime('%m-%d-%Y'),
          "promoEndDate": valid_promo['promoenddate'].strftime('%m-%d-%Y'),
          "promoType": valid_promo['PromoType'],
          "promoDiscount": float(valid_promo['discount'] if valid_promo['discount'] else 0),
          "promoText": valid_promo['promotext'],
          "minPurchaseAmount": float(valid_promo['minpurchaseamount'] if valid_promo['minpurchaseamount'] else 0),
          "maxDiscount": float(valid_promo['maxdiscount'] if valid_promo['maxdiscount'] else 0),
          "isHappyHourEnabled":valid_promo['ishappyhourenabled'],
          "happyHourSartTime": happyhourstarttime,
          "happyHourEndTime": happyhourendtime,
          "days": valid_promo['days'] if valid_promo['ishappyhourenabled'] == 1 else []
        }
      }))

    create_log_data(level='[INFO]',
                    Message=f"Promo {promo} against merchant {merchant} is not valid, IP address: {ip_addr}",
                    merchantID=merchant['id'], functionName="CheckPromo", request=request)
    return invalid("Invalid Promo!")

  except Exception as e:
    print("Error")
    print(str(e))
    create_log_data(level='[ERROR]', Message=f"Failed to validate promo {promo}, IP address: {ip_addr}",
                    messagebody=f"An error ocuured: {e}",
                    functionName="CheckPromo")
    return False


def qrPromoCode(merchentid, promoid):
  try:
    merchent = Storefront.get_storefront_by_id(merchentid)
    promo = Storefront.get_storefront_promo_by_id(merchentid, promoid)
    redirectUrl = promo['redirecturl']

    if "source" in promo:
        promo['source'] = promo['source']
    else:
        promo['source'] = 'DIRECT'
    source = promo['source']  
    promo = merchent['slug'] + "?promo=" + promo['promo'] + "&type=qr&utm_source=" +  promo['source']

    storefront_base = config.app_base_URL_storefront_food

    storefront_url = storefront_base + "/" + promo
    
    if redirectUrl:
      storefront_url = redirectUrl

    url = "https://api.qrserver.com/v1/create-qr-code/?size=300x300&data=" + requests.utils.quote(storefront_url)
    
    html = render_template(
      "qrcodepdf.html",
      resp={
        "url": url,
        "merchant": merchent['merchantname'] + " - " + source
      })
    path_wkhtmltopdf = os.environ.get("wkhtmltopdf_path")
    pdfconfig = pdfkit.configuration(wkhtmltopdf=path_wkhtmltopdf)
    pdf = pdfkit.from_string(html, False, configuration=pdfconfig)
    response = make_response(pdf)
    response.headers["Content-Type"] = "application/pdf"
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Access-Control-Allow-Headers'] = '*'
    response.headers['Access-Control-Allow-Methods'] = '*'
    response.headers["Content-Disposition"] = "inline; filename=menu.pdf"

    return response


  except Exception as e:
    print("Error")
    print(str(e))
    return False
  

def GetSourceQrCode(sourceqrid):
  ip_addr = None
  if request:
      ip_addr = request.environ.get('HTTP_X_FORWARDED_FOR', request.remote_addr)
  if ip_addr:
      ip_addr = ip_addr.split(',')[0].strip()
  try:
    sourceqr = Storefront.check_sourceqr_byId(sourceqrid)
    if sourceqr:
      merchent = Storefront.get_storefront_by_id(sourceqr['merchantid'])
      if "source" in sourceqr:
        sourceqr['source'] = sourceqr['source']
      else:
        sourceqr['source'] = 'DIRECT'
      promo = merchent['slug'] + "?type=qr&utm_source=" + sourceqr['source']
      storefront_base = config.app_base_URL_storefront_food

      url = "https://api.qrserver.com/v1/create-qr-code/?size=300x300&data=" + requests.utils.quote(storefront_base + "/" + promo)

      html = render_template(
        "qrcodepdf.html",
        resp={
          "url": url,
          "merchant": merchent['merchantname'] + " - " + sourceqr['source']
        })
      path_wkhtmltopdf = os.environ.get("wkhtmltopdf_path")
      pdfconfig = pdfkit.configuration(wkhtmltopdf=path_wkhtmltopdf)
      pdf = pdfkit.from_string(html, False, configuration=pdfconfig)
      response = make_response(pdf)
      response.headers["Content-Type"] = "application/pdf"
      response.headers['Access-Control-Allow-Origin'] = '*'
      response.headers['Access-Control-Allow-Headers'] = '*'
      response.headers['Access-Control-Allow-Methods'] = '*'
      response.headers["Content-Disposition"] = "inline; filename=menu.pdf"
     
      if response.status_code==200:
        create_log_data(level='[INFO]', Message=f"Successfully download the storefront qr code, IP address: {ip_addr}",
                    functionName="GetSourceQrCode")
        return response
      else:
        create_log_data(level='[ERROR]', Message=f"Error in download Storefront qr code for specified id, IP address: {ip_addr}",
                    functionName="GetSourceQrCode")
        return invalid("Error: Unable to get Storefront qr code for specified id ")
    else:
        create_log_data(level='[ERROR]', Message=f"Unable to get Storefront qr code for specified id, IP address: {ip_addr}",
                    functionName="GetSourceQrCode")
        return invalid("Error: Unable to get Storefront qr code for specified id ")

  except Exception as e:
    print("Error")
    print(str(e))
    create_log_data(level='[ERROR]', Message=f"Failed to download strofront qr code, IP address: {ip_addr}",
                    messagebody=f"An error ocuured: {str(e)}",
                    functionName="GetSourceQrCode")
    return False
