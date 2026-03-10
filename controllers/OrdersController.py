from flask import jsonify, request, g
import datetime
from dateutil.tz import gettz
from utilities.helpers import get_db_connection, create_log_data, success,publish_sns_message
import config
import pdfkit
from flask import render_template
from flask import make_response, has_request_context
import os
import shutil
# local imports
from models.Orders import Orders, post_Toast_orders, check_order_by_orderexternalreference
from models.VirtualMerchants import VirtualMerchants
from models.ubereats.UberEats import UberEats
from utilities.errors import invalid, not_found, unhandled, unauthorised
from utilities.helpers import get_db_connection, success, validateLoginToken, validateMerchantUser, create_log_data
from controllers.Middleware import validate_token_middleware
# import base64
# import json
# import requests
# from google.cloud import aiplatform as vertexai
# from vertexai.generative_models import GenerativeModel, Part, FinishReason
# import vertexai.preview.generative_models as generative_models




################################################# POST

def createMerchantOrder():

    try:
        _json = request.json
        token = _json['token']
        create_log_data(level='[INFO]', Message="Trigger the function that create order",
                        messagebody=_json,
                        functionName="createMerchantOrder")
        if token and request.method == 'POST':
            userID = validateLoginToken(token)
            if not userID:
                create_log_data(level='[ERROR]',
                                Message=f"The API token {token} is invalid.",
                                messagebody="Unable to find the userId on the basis of provided token."
                                , functionName="createMerchantOrder", statusCode="400 Bad Request",
                                request=request)
                return invalid("Invalid Token")

            mid = _json['order']['orderMerchantID']
            if not validateMerchantUser(mid, userID):
                create_log_data(level='[INFO]', Message="User Not authorised to access Merchant Information"
                                , messagebody=f"Unable to access merchant information against merchantid {mid}",
                                statusCode="400 Bad Request",
                                functionName="createMerchantOrder",
                                request=request)
                return unauthorised("User Not authorised to access Merchant Information")

            main_merchant = VirtualMerchants.get_virtual_merchant(mid)
            if main_merchant:
                merchant_id = main_merchant[0]['merchantid']
            else:
                merchant_id = mid

            _json['order']['orderMerchantID'] = merchant_id
            if main_merchant:
                _json['order']['virtualMerchant'] = main_merchant[0]['id']

            resp = Orders.post_order(_json=_json, userID=userID, request=request)
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

        else:
            create_log_data(level='[ERROR]', Message="Token is not given in request"
                            , messagebody=f"Token is not found in the request made for create merchant order api",
                            functionName="createMerchantOrder",statusCode="400 BAD REQUEST",
                            request=request)
            return not_found("Token not found")
    except Exception as e:
        create_log_data(level='[ERROR]',
                        Message=f"Failed to create order against merchant {request.json.get('order')['orderMerchantID']}",
                        messagebody=f"In process of creating order against merchant {request.json.get('order')['orderMerchantID']} an error occured {e}",
                        functionName="post_order",
                        statusCode="500 INTERNAL SERVER ERROR",
                        request=request)
        print("Error: ", str(e))
        return unhandled()


def updateMerchantOrderStatus(orderId):
    try:
        create_log_data(level='[INFO]',
                        Message="In the beginning of updateMerchantOrderStatus code function to update order status.",
                        orderId=orderId, functionName="updateMerchantOrderStatus", request=request)
        _json = request.json
        token = _json['token']
        merchantId = _json.get('merchantID')
        orderStatus = _json['update']['orderStatus']
        reason = _json.get('update').get('reason')
        explanation = _json.get('update').get('explanation')
        stream_notification_datetime = _json.get('update').get('stream_notification_datetime')
        stream_notification = _json.get('update').get('stream_notification')
        if token and request.method == 'PUT':
            userId = validateLoginToken(token)
            if not userId:
                create_log_data(level='[ERROR]',
                                Message="Invalid Token.", messagebody=f" token : {token}",
                                orderId=orderId, functionName="updateMerchantOrderStatus", request=request)
                return invalid("Invalid Token")
            if not validateMerchantUser(merchantId, userId):
                create_log_data(level='[ERROR]',
                                Message=f"User Not authorised to access Merchant Information merchant id : {merchantId}",
                                messagebody= f"user id : {userId}",
                                orderId=orderId, functionName="updateMerchantOrderStatus", request=request)
                return unauthorised("User Not authorised to access Merchant Information")

            return Orders.update_order_status(orderId, orderStatus, reason=reason, explanation=explanation,
                                              userId=userId, caller="a User", _json=_json , stream_notification_datetime=stream_notification_datetime ,stream_notification=stream_notification, request=request)

        else:
            return not_found("Token not found")
    except Exception as e:
        print("Error: ", str(e))
        return unhandled()


################################################# GET
def create_square_order_test():
    Orders.send_to_square( request.json["orderId"], request.json["merchantID"])

def getMerchantOrder(orderId):
    user = None
    default_request = None
    try:

        if has_request_context():
            # Assign the actual request object if there's a request context
            default_request = request


        create_log_data(level='[INFO]', Message="Trigger the function that get order by order id", messagebody="",

                            functionName="getMerchantOrder", statusCode="200 Ok",orderId=orderId,request=default_request)
        token = request.args.get('token')
        merchantId = request.args.get('merchantID')
        historical = 0
        if request.args.get('historical'):
            historical = request.args.get('historical')

        if token and request.method == 'GET':
            user = validateLoginToken(token , userFullDetail=1)
            if not user:
                create_log_data(level='[ERROR]',
                                             Message="The API token is invalid.", messagebody="Unable to find the userId on the basis of provided token."
                                             ,functionName="getMerchantOrder" , statusCode="400 Bad Request",orderId=orderId,request=default_request)


                return invalid("Invalid Token")

            if not validateMerchantUser(merchantId, user['id']):
                create_log_data(level='[INFO]', Message="User Not authorised to access Merchant Information"
                                , messagebody=f"Unable to access merchant information against merchantid {merchantId}", functionName="getMerchantOrder", orderId=orderId,
                                request=default_request)
                return unauthorised("User Not authorised to access Merchant Information")

            create_log_data(level='[INFO]' ,Message="Get  order detail by order id."
                                        , messagebody="" ,functionName="getMerchantOrder",user=user,orderId=orderId,request=default_request)
            resp = Orders.get_order_details_str(orderId, historical=int(historical),user=user,request=default_request)

            return success(jsonify(resp))
        else:
            create_log_data(level='[INFO]',
                            Message=f"Failed to get token against merchant {merchantId}",
                            messagebody=f"Token is not avaliable in request for merchant {merchantId}",
                            functionName="getMerchantOrder",
                            statusCode="400 BAD REQUEST", orderId=orderId, request=default_request)
            return not_found(params=['token'])
    except Exception as e:
        create_log_data(level='[ERROR]',


                                       Message="Exception occured", messagebody=str(e) ,functionName="getMerchantOrder",
                                     statusCode="500 INTERNAL SERVER ERROR",user=user,orderId=orderId,request=default_request)


        print("Error: ", str(e))
        return unhandled()

def orderCompletionTime():
    try:
        merchantId = request.args.get('merchantId')
        orderId = request.args.get('orderId')
        time = request.args.get('time')
        token = request.args.get('token')
        if token and request.method == 'GET':
            userId = validateLoginToken(token)
            if not userId:
                return invalid("Invalid Token")
            if not validateMerchantUser(merchantId, userId):
                return unauthorised("User Not authorised to access merchant information")

            resp = Orders.update_order_time(orderId, time)
            if resp:
                return success(jsonify("Order completed time updated successfully"))

            return invalid("Order Id is incorrect")
        else:
            return not_found(params=["token"])
    except Exception as e:
        print("Error: ", str(e))
        return unhandled("Unhandled Exception")


def orderPdfDownload(orderId):
    try:
        resp = Orders.get_order_details_str(orderId)
        orderDateTime = resp["orderDateTimeFormatted"]
        print(resp)

        for order_item in resp["orderitems"]:
            total_price = 0
            for item in order_item["addons"]:
                for addon_option in item.get('addon', []).get('addonOptions', []):
                    # for addon_option in addon.get('addonOptions', []):
                    price = addon_option.get('price', 0)
                    quantity = addon_option.get('quantity', 0)
                    total_price += float(price) * quantity
            order_item['totalpriceSingleItem'] = "{:.2f}".format(total_price + float(order_item['cost']))
        html = render_template(
            "orderpdf.html",
            resp=resp,
            orderDateTime=orderDateTime)
        # path_wkhtmltopdf = r'C:\Program Files\wkhtmltopdf\bin\wkhtmltopdf.exe'
        path_wkhtmltopdf = os.environ.get("wkhtmltopdf_path")
        config = pdfkit.configuration(wkhtmltopdf=path_wkhtmltopdf)
        pdf = pdfkit.from_string(html, False, configuration=config)
        response = make_response(pdf)
        response.headers["Content-Type"] = "application/pdf"
        response.headers['Access-Control-Allow-Origin'] = '*'
        response.headers['Access-Control-Allow-Headers'] = '*'
        response.headers['Access-Control-Allow-Methods'] = '*'
        response.headers["Content-Disposition"] = "inline; filename=" + resp['orderMerchant']['merchantName'] + " " + \
                                                  resp['orderExternalReference'] + ".pdf"
        return response
    except Exception as e:
        print("Error: ", str(e))
        return unhandled()


def orderPdfRecipt():
    try:
        resp = Orders.get_order_details_str(request.args.get('orderId'))
        orderDateTime = resp["orderDateTimeFormatted"]
        if resp['orderType'] == "delivery":
            print(resp['staffTips'])
            resp['staffTips'] = "{:.2f}".format(float(resp['staffTips']) / 0.4)
            print(resp['staffTips'])

        total = float(resp['orderSubTotal']) + float(resp['tip_amount']) + float(resp['doordashDeliveryFee']) + float(
            resp['tax_and_fee']) + float(resp['promoDiscount'])
        rounded_total = round(total, 2)
        formatted_total = "{:.2f}".format(rounded_total)
        html = render_template(
            "order_recipt_pdf.html",
            resp=resp,
            orderDateTime=orderDateTime,
            total=formatted_total)
        # path_wkhtmltopdf = r'C:\Program Files\wkhtmltopdf\bin\wkhtmltopdf.exe'
        path_wkhtmltopdf = os.environ.get("wkhtmltopdf_path")
        configpdf = pdfkit.configuration(wkhtmltopdf=path_wkhtmltopdf)
        pdf = pdfkit.from_string(html, False, configuration=configpdf)
        response = make_response(pdf)
        response.headers["Content-Type"] = "application/pdf"
        response.headers['Access-Control-Allow-Origin'] = '*'
        response.headers['Access-Control-Allow-Headers'] = '*'
        response.headers['Access-Control-Allow-Methods'] = '*'
        response.headers["Content-Disposition"] = "inline; filename=" + resp['orderMerchant']['merchantName'] + " " + \
                                                  resp['orderExternalReference'] + ".pdf"
        return response
    except Exception as e:
        print("Error: ", str(e))
        return unhandled()


def updateMerchantOrder(orderId):
    try:
        _json = request.json

        return Orders.update_merchent_order(_json, orderId)

    except Exception as e:
        print("Error: ", str(e))
        return unhandled()


def importToastOrders(merchantId):
    try:
        _json = request.json
        token = _json['token']
        userID = validateLoginToken(token)
        mid = merchantId
        successList = []
        errorList = []
        alreadyList = []

        if not userID:
            return invalid("Invalid Token")

        if not validateMerchantUser(mid, userID):
            return unauthorised("User Not authorised to access Merchant Information")
        for order in _json['orderdataOBJ']:
            order['orderexternalreference'] = order['orderexternalreference'].strip()
            orderexternalreference = order['orderexternalreference']
            resp = check_order_by_orderexternalreference(orderexternalreference=orderexternalreference,
                                                         merchantId=merchantId)
            if resp:
                alreadyList.append(order['orderexternalreference'])
                continue
            else:
                response = post_Toast_orders(order=order, merchantId=merchantId, userID=userID)
                if response == True:
                    successList.append(order['orderexternalreference'])
                else:
                    errorList.append(order['orderexternalreference'])
        return success(jsonify({"successList": successList, "errorList": errorList, "alreadyList": alreadyList}))

    except Exception as e:
        print("Error: ", str(e))
        return unhandled()


@validate_token_middleware
def getOrderDeliveryInfo(orderId):
    try:
        userId = g.userId
        merchantId = request.args.get("merchantId")
        if not validateMerchantUser(merchantId, userId):
            return unauthorised("user is not authorized to access merchant information")

        order_details = Orders.get_order_by_id(orderId)
        if not order_details:
            return invalid("order id is invalid")

        # check if order source is ubereats
        if order_details['ordersource'] == "ubereats" and order_details['orderprocessingid']:

            orderUrl = f"https://api.uber.com/v2/eats/order/{order_details['orderprocessingid']}"
            accessToken = UberEats.ubereats_check_and_get_access_token()

            u_order_details , error_message = UberEats.ubereats_get_order_details(orderUrl, accessToken)
            if not u_order_details:
                return unhandled("error while getting order details from ubereats ," , error_message)

            driver_info = u_order_details.get('deliveries')

            return success(jsonify({
                "message": "success",
                "status": 200,
                "data": driver_info
            }))

        else:
            return invalid("ordersource is not from ubereats or order is created manually!!!")
    except Exception as e:
        print("Error: ", str(e))
        return unhandled(str(e))

@validate_token_middleware
def getOrderDeliveryInfoV2(orderId):
    try:
        userId = g.userId
        merchantId = request.args.get("merchantId")
        if not validateMerchantUser(merchantId, userId):
            return unauthorised("user is not authorized to access merchant information")

        order_details = Orders.get_order_by_id(orderId)
        if not order_details:
            return invalid("order id is invalid")

        # check if order source is ubereats
        if order_details['ordersource'] == "ubereats" and order_details['orderprocessingid']:

            orderUrl = f"https://api.uber.com/v1/delivery/order/{order_details['orderprocessingid']}?expand=deliveries"
            accessToken = UberEats.ubereats_check_and_get_access_token()

            u_order_details , error_message = UberEats.ubereats_get_order_details(orderUrl, accessToken)
            if not u_order_details:
                return unhandled(f"error while getting order details from ubereats ,error: {error_message}")

            driver_info = u_order_details.get('order').get('deliveries')

            return success(jsonify({
                "message": "success",
                "status": 200,
                "data": driver_info
            }))

        else:
            return invalid("ordersource is not from ubereats or order is created manually!!!")
    except Exception as e:
        print("Error: ", str(e))
        return unhandled(str(e))
def getAllMerchantsOrdersSummary():
    try:
        _json = request.json
        token = _json['token']
        mids = _json.get("merchants")

        limit = str(request.args.get('limit')) if request.args.get('limit') else "25"
        _from = str(request.args.get('from')) if (request.args.get('from')) else "0"
        conditions = []

        if mids:
            temp_list = list()
            for mid in mids:
                temp_list.append(f"'{mid}'")
            merchants_str = ','.join(temp_list)
            conditions.append(f"orders.merchantid IN ({merchants_str})")
        if request.args.get("status"): conditions.append(f'status IN ({request.args.get("status")})')
        if request.args.get("orderSource"): conditions.append(
            f'ordersource LIKE "%%{request.args.get("orderSource")}%%"')
        if request.args.get("shortOrderId"): conditions.append(
            f'short_order_id LIKE "%%{request.args.get("shortOrderId")}%%"')
        if request.args.get("customerName"): conditions.append(
            f'customername LIKE "%%{request.args.get("customerName")}%%"')
        if request.args.get("orderExternalReference"): conditions.append(
            f'orderexternalreference LIKE "%%{request.args.get("orderExternalReference")}%%"')

        if request.args.get("startDate") and request.args.get("endDate"):
            startDate = request.args.get("startDate")
            endDate = request.args.get("endDate")
            startDate = datetime.datetime.strptime(startDate, "%Y-%m-%d")
            endDate = datetime.datetime.strptime(endDate, "%Y-%m-%d") + datetime.timedelta(days=1)
            # startDate = startDate.replace(tzinfo=gettz("US/Pacific")).astimezone(datetime.timezone.utc)
            # endDate = endDate.replace(tzinfo=gettz("US/Pacific")).astimezone(datetime.timezone.utc) + datetime.timedelta(days=1)
            # conditions.append(f"orderdatetime BETWEEN '{startDate}' AND '{endDate}'")
            conditions.append(
                f"convert_tz(orderdatetime, '+00:00', (SELECT merchants.timezone FROM merchants where merchants.id = orders.merchantid LIMIT 1)) BETWEEN '{startDate}' AND '{endDate}'")

        # where clause handling
        where = ' AND '.join(conditions)
        if not where:
            where = "1"

        print(where)

        ################

        # validate the received values
        if token and request.method == 'POST':
            userID = validateLoginToken(token)
            if not userID:
                return invalid("Invalid Token")

            connection, cursor = get_db_connection()
            cursor.execute("SELECT role FROM users WHERE id=%s", userID)
            userrow = cursor.fetchone()
            if not userrow:
                return invalid("Invalid Token")

            fields = """
          orders.id,
          orders.merchantid,
          COALESCE((SELECT merchants.merchantname FROM merchants where merchants.id = orders.merchantid LIMIT 1), '') as merchantName,

          @dt1 := convert_tz(orderdatetime, '+00:00', (SELECT @timezone := merchants.timezone FROM merchants where merchants.id = orders.merchantid LIMIT 1)) dt1,

          date_format(@dt1, '%Y-%c-%dT%H:%i:%S') orderDateTime,
          CONCAT( date_format(@dt1, '%m-%d-%Y %H:%i:%S'), ' (', @timezone, ')' ) orderDateTimeFormatted,
          
          orders.customerid orderCustomerID, 
          orders.customername orderCustomerName, 
          orders.status orderStatus, 
          convert(orders.ordersubtotal,CHAR) orderSubTotal, 
          convert(orders.orderdeliveryfee,CHAR) orderDeliveryFee, 
          convert(orders.ordertotal,CHAR) orderTotal, 
          convert(orders.ordertax,CHAR) orderTax, 
          orders.orderexternalreference orderExternalReference, 
          orders.ordersource, 
          orders.short_order_id, 
          orders.order_preparation_time,
          orders.vmerchantid,
          orders.order_in_busy_mode, 
          convert(orders.errorcharge,CHAR) errorcharge, 
          convert(orders.adjustment,CHAR) adjustment, 
          orders.remarks, 
          orders.disputed,
          orders.is_bogo_enabled,
          convert(orders.refund_amount,CHAR) refundAmount,
          convert(orders.squarefee,CHAR) squarefee,
          ordertype,
          scheduled,
          scheduledtime
        """

            if (userrow['role'] == 1 or userrow['role'] == 2):
                cursor.execute(f"""
            SELECT {fields}
            FROM orders 
            WHERE {where} 
            ORDER by created_datetime DESC 
            LIMIT {limit} 
            OFFSET {_from}""")
            else:
                # else if user is merchant-user
                cursor.execute(f"""
            SELECT {fields}
            FROM orders, merchantusers 
            WHERE 
              orders.merchantid = merchantusers.merchantid 
              AND merchantusers.userid = '{userID}' 
              AND {where} 
            ORDER by orders.created_datetime DESC 
            LIMIT {limit} 
            OFFSET {_from}""")

            rows = cursor.fetchall()

            all_rows = []
            for row in rows:
                vmerchant_name = ''
                if row['vmerchantid']:
                    vmerchant_name = VirtualMerchants.get_virtual_merchant_str(id=row['vmerchantid'])
                    print(vmerchant_name)
                    vmerchant_name = vmerchant_name[0]['virtualName']

                all_rows.append({
                    "adjustment": row['adjustment'],
                    "dt1": row['dt1'],
                    "errorcharge": row['errorcharge'],
                    "id": row['id'],
                    "merchantName": row['merchantName'],
                    "merchantid": row['merchantid'],
                    "orderCustomerID": row['orderCustomerID'],
                    "orderCustomerName": row['orderCustomerName'],
                    "orderDateTime": row['orderDateTime'],
                    "orderDateTimeFormatted": row['orderDateTimeFormatted'],
                    "orderDeliveryFee": row['orderDeliveryFee'],
                    "orderExternalReference": row['orderExternalReference'],
                    "orderStatus": row['orderStatus'],
                    "orderSubTotal": row['orderSubTotal'],
                    "orderTax": row['orderTax'],
                    "orderTotal": row['orderTotal'],
                    "order_in_busy_mode": row['order_in_busy_mode'],
                    "order_preparation_time": row['order_preparation_time'],
                    "ordersource": row['ordersource'],
                    "refundAmount": row['refundAmount'],
                    "remarks": row['remarks'],
                    "short_order_id": row['short_order_id'],
                    "squarefee": row['squarefee'],
                    "virtualMerchantName": vmerchant_name,
                    "orderType": row["ordertype"],
                    "scheduled": row["scheduled"],
                    "scheduledTime": row["scheduledtime"],
                    "disputed":row['disputed'],
                    "is_bogo_enabled": row['is_bogo_enabled']
                })

            return success(jsonify(all_rows))

    except Exception as e:
        print("Error: ", str(e))
        return unhandled()


def getPollingOrders(merchantId):
    try:
        print(' ----------------------   Call the Polling Orders API   ------------------------------------------------')
        connection, cursor = get_db_connection()
        print('merchantId ------ ' , merchantId)
        print('request arguments---- ' , request.args)
        token=request.args.get('token')
        if not token:
            print('token is missing')
            return invalid('token is missing')
        userID = validateLoginToken(token)
        if not userID:
            return invalid("Invalid Token")
        if not merchantId:
            print('token is missing')
            return invalid('Merchant Id not found')
        print('getting polling orders')
        latest_polling_orders = str(request.args.get('latest_polling_orders')) if request.args.get('latest_polling_orders') else "10"
        _from='0'
        status='0'

        fields = """
        
          orders.id,
          @dt1 := convert_tz(orderdatetime, '+00:00', (SELECT @timezone := merchants.timezone FROM merchants where merchants.id = orders.merchantid LIMIT 1)) dt1,

          date_format(@dt1, '%Y-%c-%dT%H:%i:%S') orderDateTime,
          CONCAT( date_format(@dt1, '%m-%d-%Y %H:%i:%S'), ' (', @timezone, ')' ) orderDateTimeFormatted
        """
        conditions = []
        conditions.append(f'merchantid ="{merchantId}"')
        conditions.append(f'status ="{status}"')
        where = ' AND '.join(conditions)

        cursor.execute(f"""
                SELECT {fields}
                FROM orders 
                WHERE {where}
                ORDER by created_datetime DESC 
                LIMIT {latest_polling_orders} 
                OFFSET {_from}
        """)

        rows = cursor.fetchall()
        print('get orders from db ' ,rows )
        all_rows = []
        for row in rows:
            all_rows.append({
                "dt1": row['dt1'],
                "id": row['id'],
                "orderDateTime": row['orderDateTime'],
                "orderDateTimeFormatted": row['orderDateTimeFormatted']
            })
        print('Polling Orders Response ' , all_rows)
        return success(jsonify(all_rows))

    except Exception as e:
        print("Error: ", str(e))
        return unhandled()
# def grubhubEmailParser():
#   try:
#     # Get the JSON data from the request
#     json_data = request.get_json()
#
#     if not json_data or 'plain' not in json_data:
#       return jsonify({"error": "No plain text content found"}), 400
#
#     # Extract the plain text content from the email
#     plain_text_content = json_data['plain']
#
#     # Create a Part object with the plain text data
#     document1 = Part.from_data(
#       mime_type="text/plain",
#       data=plain_text_content.encode('utf-8'))
#
#     generation_config = {
#       "max_output_tokens": 8192,
#       "temperature": 1,
#       "top_p": 0.95,
#     }
#
#     safety_settings = {
#       generative_models.HarmCategory.HARM_CATEGORY_HATE_SPEECH: generative_models.HarmBlockThreshold.BLOCK_MEDIUM_AND_ABOVE,
#       generative_models.HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: generative_models.HarmBlockThreshold.BLOCK_MEDIUM_AND_ABOVE,
#       generative_models.HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: generative_models.HarmBlockThreshold.BLOCK_MEDIUM_AND_ABOVE,
#       generative_models.HarmCategory.HARM_CATEGORY_HARASSMENT: generative_models.HarmBlockThreshold.BLOCK_MEDIUM_AND_ABOVE,
#     }
#
#     vertexai.init(project="fonda-gmb-347613", location="us-central1")
#     model = GenerativeModel(
#       "gemini-1.5-flash-001",
#     )
#     prompt = """
#         Please parse the order and give me the JSON following element names.
#         {"reference": "the string after the text Order:, and it will be seperated by an -" , "dropoffName": customer, "itemCount": "2", "dropoffEarliestDateTime": pickup_time,
#         "dropoffPhone": phone, "job_date_time": pickup_date,
#         "dropoffFullAddress": {"original": "Payment: Pre-Paid", "found": false, "normalized": "Payment: Pre-Paid"},
#         "paymentMode": payment,
#         "product_id2":
#         [{"quantity": "1",
#         "description": "please use name, category and then concatenate addons separated with * ",
#         "unitPrice": "$1.00", "price":"$2.00"} ],
#         "subTotal":"$5.00", "tax": "$6.00", "grandTotal": "$5.00", "pickupName": restaurant,
#         "source": grubhub,
#         "orderType": "If the text 'GRUBHUB DELIVERY' appears in the document as a joint string, set 'orderType' to 'Delivery'; otherwise, set it to 'Pickup'"
#         }
#         """
#
#     responses = model.generate_content(
#       [document1, prompt],
#       generation_config=generation_config,
#       safety_settings=safety_settings,
#       stream=True,
#     )
#
#     result = ""
#     for response in responses:
#       result += response.text
#
#     # Assuming the response text is a JSON string, parse it into a Python dictionary
#     start_index = result.find('{')
#     end_index = result.rfind('}') + 1  # Adjust to include the closing curly brace
#     json_string = result[start_index:end_index]
#
#     print(json_string)
#
#     # Convert the JSON string to a JSON object
#     parsed_json = json.loads(json_string)
#
#     print(parsed_json)
#
#     # Send the parsed JSON to the specified API endpoint
#     response = requests.post(
#       'https://v1ai7tc4qe.execute-api.us-east-2.amazonaws.com/dev/api/order_v2',
#       headers={'Content-Type': 'application/json'},
#       data=json.dumps(parsed_json)
#     )
#
#     # Check for successful response
#     if response.status_code == 200:
#       return jsonify(parsed_json), 200
#     else:
#       return jsonify({"error": "Failed to send data to the API", "details": response.text}), response.status_code
#
#     return json_data
#   except Exception as e:
#     print("error", e)
#     return str(e), 500
#
#
# def doordashEmailParseur():
#   try:
#     # Get the JSON data from the request
#     json_data = request.get_json()
#
#     if not json_data or 'attachments' not in json_data or not json_data['attachments']:
#       return jsonify({"error": "No attachments found"}), 400
#
#     # Extract the base64-encoded PDF content from the attachments
#     pdf_content_base64 = json_data['attachments'][0]['content']
#
#     # Decode the base64-encoded PDF content
#     pdf_data = base64.b64decode(pdf_content_base64)
#
#     # Create a Part object with the PDF data
#     document1 = Part.from_data(
#       mime_type="application/pdf",
#       data=pdf_data)
#
#     generation_config = {
#       "max_output_tokens": 8192,
#       "temperature": 1,
#       "top_p": 0.95,
#     }
#
#     safety_settings = {
#       generative_models.HarmCategory.HARM_CATEGORY_HATE_SPEECH: generative_models.HarmBlockThreshold.BLOCK_MEDIUM_AND_ABOVE,
#       generative_models.HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: generative_models.HarmBlockThreshold.BLOCK_MEDIUM_AND_ABOVE,
#       generative_models.HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: generative_models.HarmBlockThreshold.BLOCK_MEDIUM_AND_ABOVE,
#       generative_models.HarmCategory.HARM_CATEGORY_HARASSMENT: generative_models.HarmBlockThreshold.BLOCK_MEDIUM_AND_ABOVE,
#     }
#
#     # vertexai.init(project="ethereal-art-425810-d5", location="us-central1")
#     vertexai.init(project="fonda-gmb-347613", location="us-central1")
#     model = GenerativeModel(
#       "gemini-1.5-flash-001",
#     )
#     prompt = """
#     Please parse the order and give me the JSON following element names.
#     {"reference": order_number, "dropoffName": customer, "itemCount": "2", "dropoffEarliestDateTime": pickup_time,
#     "dropoffPhone": phone, "job_date_time": pickup_date,
#     "dropoffFullAddress": {"original": "Payment: Pre-Paid", "found": false, "normalized": "Payment: Pre-Paid"},
#     "paymentMode": payment,
#     "product_id2":
#     [{"quantity": "1",
#     "description": "please use name, category and then  concatenate addons seperated with • ",
#     "unitPrice": "$1.00", "price":"$2.00"} ],
#     "subTotal":"$5.00", "tax": "$6.00", "grandTotal": "$5.00", "pickupName": restaurant,
#     "source": doordash or grubhub,
#     "PickupBy": "If the text "Customer Pickup" appears in the document as a joint string, set 'PickupBy' to 'Customer'; otherwise, set it to 'Doordasher'"
#     }
#     """
#
#     responses = model.generate_content(
#       [document1, prompt],
#       generation_config=generation_config,
#       safety_settings=safety_settings,
#       stream=True,
#     )
#     #
#     result = ""
#     for response in responses:
#       result += response.text
#     # Assuming the response text is a JSON string, parse it into a Python dictionary
#     start_index = result.find('{')
#     end_index = result.rfind('}') + 1  # Adjust to include the closing curly brace
#     # Extract the JSON string
#     json_string = result[start_index:end_index]
#
#     print(json_string)
#
#     # Convert the JSON string to a JSON object
#     parsed_json = json.loads(json_string)
#
#     # Add the 'orderType' field based on the 'PickupBy' value
#     if parsed_json.get('PickupBy') == 'Customer':
#       parsed_json['orderType'] = 'Pickup'
#     else:
#       parsed_json['orderType'] = 'Delivery'
#
#     print(parsed_json)
#
#     # Send the parsed JSON to the specified API endpoint
#     response = requests.post(
#       'https://v1ai7tc4qe.execute-api.us-east-2.amazonaws.com/dev/api/order_v2',
#       headers={'Content-Type': 'application/json'},
#       data=json.dumps(parsed_json)
#     )
#
#     # Check for successful response
#     if response.status_code == 200:
#       return jsonify(parsed_json), 200
#     else:
#       return jsonify({"error": "Failed to send data to the API", "details": response.text}), response.status_code
#
#     return json_data
#   except Exception as e:
#     print("error ", e)
#     return str(e)