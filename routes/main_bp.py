from collections import namedtuple
from random import choice
import pymysql
import xlsxwriter
from flask import Flask, jsonify, request, Response, Blueprint, current_app
import uuid
import datetime
from dateutil.tz import gettz

# local imports
import config
from controllers.WebHookController import create_ubereats_order
# from models import Merchants
from models.Merchants import Merchants
from models.Storefront import Storefront
from models.Orders import Orders
from models.Payouts import Payouts
from models.Platforms import Platforms
from models.clover.Clover import Clover
from utilities.errors import *
from utilities.helpers import *
from utilities.report_helper import generate_payouts_report, generate_menu_report, generate_revenue_report

# rds config
rds_host = config.db_host
username = config.db_username
password = config.db_password
database_name = config.db_name

# SNS
sns_order_notification = config.sns_order_notification

# init blueprint
main_bp = Blueprint('main_bp', __name__)
app = main_bp


@app.route("/test01", methods=["GET"])
def check_lambda():
    return success()


@app.route("/debug", methods=["POST"])
def debug_function():
    _json = request.json
    order = _json['order']
    marchant_id = _json['marchant_id']
    response = create_ubereats_order(marchant_id , None , order , 'order.notification')

    # response = generate_revenue_report('2023-04-01', '2023-04-02')

    # store_id = _json['store_id']
    # token = _json['token']
    # # tenders_verified = Clover.verify_tenders(store_id, token, tenders=['Fonda-DD', 'Fonda-UE', 'Fonda-GH', 'Fonda-SF'])
    # tender_id = Clover.get_tender_id(store_id, token, tender='Fonda-UE')

    # # response =Orders.refund_order_square(order_id, marchant_id)

    return success(jsonify({"message": "success", "status": 200, "data": response}))


@app.route("/dbhealth", methods=["GET"])
def get_db_health():
    connection = pymysql.connect(
        host=rds_host, user=username, passwd=password, db=database_name)
    with connection.cursor() as cur:
        cur.execute('select * from users LIMIT 1')
    return jsonify("OK")


@app.route("/quote", methods=["GET"])
def get_random_quote():
    pass
    # return jsonify(choice(quotes)._asdict())


@app.route('/merchant/<id>', methods=['DELETE'])
def inactivateMerchant(id):
    try:
        token = request.args.get('token')
        # validate the received values
        if token and request.method == 'DELETE':
            userID = validateLoginToken(token)
            if userID:
                if (validateMerchantUser(id, userID)):
                    connection, cursor = get_db_connection()
                    cursor.execute("SELECT id FROM merchants WHERE id=%s", id)
                    row = cursor.fetchone()
                    if (row):
                        data = (0, userID, id)
                        cursor.execute(
                            "update merchants set status = %s, updated_by = %s WHERE id=%s", data)
                        connection.commit()
                        resp = jsonify('ok')
                        resp.status_code = 200
                        resp.headers['Content-Type'] = 'application/json'
                        resp.headers['Access-Control-Allow-Origin'] = '*'
                        resp.headers['Access-Control-Allow-Headers'] = '*'
                        resp.headers['Access-Control-Allow-Methods'] = 'DELETE,GET,HEAD,OPTIONS,PATCH,POST,PUT'
                        resp.headers['Access-Control-Allow-Credentials'] = 'true'
                        return resp
                    else:
                        return not_found("Merchant ID Not Found")
                else:
                    return unauthorised("User Not authorised to access Merchant Information")
            else:
                return invalid("Invalid Token")
    except Exception as e:
        print(e)
        return unhandled("Unhandled Exception")


@app.route('/merchant/<mid>/openingHour', methods=['POST'])
def createMerchantOpeningHour(mid):
    try:
        _json = request.json
        token = _json['token']
        ip_address = get_ip_address(request)
  
        create_log_data(level='[INFO]', Message=f"In the start of createMerchantOpeningHour,IP address: {ip_address}, Token:{token}",
                    functionName="createMerchantOpeningHour", request=request)
        
        if token and request.method == 'POST':
            userID = validateLoginToken(token)
            if userID:
                if validateMerchantUser(mid, userID):
                    seqNo = _json['openinghour']['seqNo']
                    day = _json['openinghour']['day']
                    opentime = _json['openinghour']['openTime']
                    closetime = _json['openinghour']['closeTime']
                    closeforbusinessflag = _json['openinghour']['closeForBusinessFlag']

                    new_opening_hour = Storefront.time_to_minutes(opentime)
                    new_closing_hours = Storefront.time_to_minutes(closetime)
                    if new_closing_hours < new_opening_hour:
                        new_closing_hours += 1440

                    connection, cursor = get_db_connection()
                    cursor.execute("SELECT id FROM merchants WHERE id=%s", mid)
                    row = cursor.fetchone()
                    if (row):
                        cursor.execute(
                            "SELECT id FROM users WHERE id=%s", userID)
                        userrow = cursor.fetchone()
                        if (userrow):
                            cursor.execute(""" SELECT * FROM merchantopeninghrs WHERE merchantid=%s AND day=%s """, (mid, day))
                            merchant_opening_hour_row = cursor.fetchall()
                            for row in merchant_opening_hour_row:
                                start_time = Storefront.time_to_minutes(row['opentime'])
                                end_time = Storefront.time_to_minutes(row['closetime'])

                                if end_time < start_time:
                                    end_time += 1440
                                if start_time <= new_opening_hour < end_time or new_opening_hour <= start_time < new_closing_hours:
                                    return invalid(f"Overlapping hours detected for {day}")
                                elif start_time < new_closing_hours <= end_time or new_opening_hour < end_time <= new_closing_hours:
                                    return invalid(f"Overlapping hours detected for {day}")
                            merchantopeninghrGUID = uuid.uuid4()
                            data = (merchantopeninghrGUID, mid, seqNo, day,
                                    opentime, closetime, closeforbusinessflag, userID)
                            cursor.execute(
                                "INSERT INTO merchantopeninghrs (id, merchantid, daynumber, day, opentime, closetime, closeforbusinessflag, created_by) VALUES (%s, %s, %s,%s, %s, %s,%s,%s)", data)
                            connection.commit()
                            
                            create_log_data(level='[INFO]', Message=f"successfully  createMerchantOpeningHour,IP address: {ip_address}, Token:{token}",
                                functionName="createMerchantOpeningHour", request=request)
                            # Triggering SNS
                            print("Triggering sns - merchant.update_hours ...")
                            sns_msg = {
                                "event": "merchant.update_hours",
                                "body": {
                                    "merchantId": mid,
                                    "userId": userID,
                                    "eventDetails": f"Added new opening hours. Day: {day} , closeTime: <{closetime}> , openTime: <{opentime}> IP address:{ip_address}"
                                }
                            }
                            logs_sns_resp = publish_sns_message(topic=config.sns_activity_logs, message=str(
                                sns_msg), subject="merchant.update_hours")

                            resp = Response()
                            return success(resp)
                        else:
                            return not_found("User ID Not Found")
                    else:
                        return not_found("Merchant ID Not Found")
                else:
                    return unauthorised("User Not authorised to access Merchant Information")
            else:
                return invalid("Invalid Token")
    except Exception as e:
        print(e)
        create_log_data(level='[INFO]', Message=f"Error : {e},IP address: {ip_address}, Token:{token}",
                    functionName="createMerchantOpeningHour", request=request)
        return unhandled("Unhandled Exception")


@app.route('/merchant/<mid>/openingHour/<oid>', methods=['PUT'])
def updateMerchantOpeningHour(mid, oid):
    try:
        _json = request.json
        token = _json['token']

        ip_address = None
        if request:
            ip_address = request.environ.get(
                'HTTP_X_FORWARDED_FOR', request.remote_addr)
        if ip_address:
            ip_address = ip_address.split(',')[0].strip()

        create_log_data(level='[INFO]', Message=F"In the beginning of function to update merchant business hours,IP address:{ip_address}",
                        functionName="updateMerchantOpeningHour", merchantID=mid, request=request)
        merchant_openhours = getMerchantOpeningHoursbyid(mid, oid)
        merchant_openhours = merchant_openhours.json
        if token and request.method == 'PUT':
            user = validateLoginToken(token, userFullDetail=1)
            create_log_data(level='[INFO]', Message=F" Retrieved user details ,IP address:{ip_address}",
                            functionName="updateMerchantOpeningHour", merchantID=mid, user=user, request=request)
            if user:
                if (validateMerchantUser(mid, user['id'])):
                    seqNo = _json['openinghour']['seqNo']
                    day = _json['openinghour']['day']
                    opentime = _json['openinghour']['openTime']
                    closetime = _json['openinghour']['closeTime']
                    closeforbusinessflag = _json['openinghour']['closeForBusinessFlag']

                    new_opening_hour = Storefront.time_to_minutes(opentime)
                    new_closing_hours = Storefront.time_to_minutes(closetime)
                    if new_closing_hours < new_opening_hour:
                        new_closing_hours += 1440

                    connection, cursor = get_db_connection()
                    cursor.execute("SELECT id FROM merchants WHERE id=%s", mid)
                    row = cursor.fetchone()
                    if (row):
                        cursor.execute(""" SELECT * FROM merchantopeninghrs WHERE merchantid=%s AND day=%s AND id!=%s """,
                                      (mid, day, oid))
                        merchant_opening_hour_row = cursor.fetchall()
                        for row in merchant_opening_hour_row:
                            start_time = Storefront.time_to_minutes(row['opentime'])
                            end_time = Storefront.time_to_minutes(row['closetime'])

                            if end_time < start_time:
                                end_time += 1440

                            if start_time <= new_opening_hour < end_time or new_opening_hour <= start_time < new_closing_hours:
                                return invalid(f"Overlapping hours detected for {day}")
                            elif start_time < new_closing_hours <= end_time or new_opening_hour < end_time <= new_closing_hours:
                                return invalid(f"Overlapping hours detected for {day}")
                        merchantopeninghrGUID = uuid.uuid4()
                        data = (seqNo, day, opentime, closetime,
                                closeforbusinessflag, user['id'], oid)
                        cursor.execute(
                            "UPDATE merchantopeninghrs set   daynumber = %s,day= %s,opentime= %s,closetime= %s,closeforbusinessflag= %s, updated_by=%s  where id = %s", data)
                        connection.commit()
                        create_log_data(level='[INFO]', Message=F"Successfully update merchant business hours ,IP address:{ip_address}",
                                        functionName="updateMerchantOpeningHour", user=user, merchantID=mid, request=request)
                        merchant_updated_openhours = getMerchantOpeningHoursbyid(
                            mid, oid)
                        merchant_updated_openhours = merchant_updated_openhours.json
                        messagebody = Merchants.check_update_merchants_field(
                            merchant_openhours[0], merchant_updated_openhours[0])
                        if messagebody:
                            print("Triggering sns - merchant.update ...")
                            sns_msg = {
                                "event": "merchant.update_hours",
                                "body": {
                                    "merchantId": mid,
                                    "userId": user['id'],
                                    "eventDetails": f"{day} {messagebody} IP address:{ip_address}"
                                }
                            }
                            logs_sns_resp = publish_sns_message(topic=config.sns_activity_logs, message=str(sns_msg),
                                                                subject="merchant.update_hours")
                        resp = Response()
                        return success(resp)
                    else:
                        create_log_data(level='[ERROR]', Message=f"Invalid merchant id to update merchant business hours ,IP address:{ip_address}",
                                        functionName="updateMerchantOpeningHour", user=user, merchantID=mid, request=request)
                        return not_found("Merchant ID Not Found")
                else:
                    create_log_data(level='[ERROR]', Message=f"User is not authorized to update merchant business hours ,IP address:{ip_address}",
                                    functionName="updateMerchantOpeningHour", user=user, merchantID=mid, request=request)
                    return unauthorised("User Not authorised to access Merchant Information")
            else:
                create_log_data(level='[ERROR]', Message=f"Failed to retrieved user due to invalid token ,IP address:{ip_address}",
                                functionName="updateMerchantOpeningHour", user=user, merchantID=mid, request=request)
                return invalid("Invalid Token")
    except Exception as e:
        print(e)
        create_log_data(level='[ERROR]',
                        Message=f"Unable to update merchant business hours ,IP address:{ip_address}",
                        messagebody=f'An error occured {str(e)}',
                        functionName="updateMerchantOpeningHour", request=request)
        return unhandled("Unhandled Exception")


@app.route('/merchant/<mid>/openingHours', methods=['GET'])
def getMerchantOpeningHours(mid):
    try:
        ip_address = get_ip_address(request)
        create_log_data(level='[INFO]', Message=f"In the start of getMerchantOpeningHours,IP address: {ip_address}",
                    functionName="getMerchantOpeningHours", request=request)
        
        connection, cursor = get_db_connection()
        cursor.execute("SELECT id FROM merchants WHERE id=%s", mid)
        row = cursor.fetchone()
        if (row):
            cursor.execute(
                "SELECT id, daynumber seqNo, day, opentime openTime, closetime closeTime, closeforbusinessflag closeForBusinessFlag FROM merchantopeninghrs WHERE merchantid=%s order by daynumber",
                mid)
            rows = cursor.fetchall()
            resp = jsonify(rows)
            return success(resp)
        else:
            return not_found("Merchant ID Not Found")
    except Exception as e:
        print(e)
        create_log_data(level='[INFO]', Message=f"An error occured, {e},IP address: {ip_address}",
                    functionName="getMerchantOpeningHours", request=request)
        return unhandled("Unhandled Exception")


def getMerchantOpeningHoursbyid(mid, oid):
    try:
        connection, cursor = get_db_connection()
        cursor.execute("SELECT id FROM merchants WHERE id=%s", mid)
        row = cursor.fetchone()
        if (row):
            cursor.execute(
                "SELECT id, daynumber seqNo, day, opentime openTime, closetime closeTime, closeforbusinessflag closeForBusinessFlag FROM merchantopeninghrs WHERE merchantid=%s and id=%s order by daynumber",
                (mid, oid))
            rows = cursor.fetchall()
            resp = jsonify(rows)
            return success(resp)
        else:
            return not_found("Merchant ID Not Found")
    except Exception as e:
        print(e)
        return unhandled("Unhandled Exception")


@app.route('/merchant/<mid>/openingHour/<oid>', methods=['DELETE'])
def removeMerchantOpeningHour(mid, oid):
    try:
        token = request.args.get('token')
        ip_address = get_ip_address(request)
        create_log_data(level='[INFO]', Message=f"In the start of removeMerchantOpeningHour,IP address: {ip_address}, Token:{token}",
                    functionName="removeMerchantOpeningHour", request=request)
        merchant_openhours = getMerchantOpeningHoursbyid(mid, oid)
        merchant_openhours = merchant_openhours.json
        merchant_openhours_dict = merchant_openhours[0]
        # validate the received values
        if token and request.method == 'DELETE':
            userID = validateLoginToken(token)
            if userID:
                if (validateMerchantUser(mid, userID)):
                    connection, cursor = get_db_connection()
                    cursor.execute("SELECT id FROM merchants WHERE id=%s", mid)
                    row = cursor.fetchone()
                    if (row):
                        cursor.execute(
                            "DELETE  FROM merchantopeninghrs WHERE id =%s", oid)
                        connection.commit()
                        
                        
                        create_log_data(level='[INFO]', Message=f"successfully removeMerchantOpeningHour,IP address: {ip_address}, Token:{token}",
                            functionName="removeMerchantOpeningHour", request=request)
                        # Triggering SNS
                        print("Triggering sns - merchant.update_hours ...")
                        sns_msg = {
                            "event": "merchant.update_hours",
                            "body": {
                                "merchantId": mid,
                                "userId": userID,
                                "eventDetails": f"Deleted opening hours. Day: {merchant_openhours_dict.get('day')} , closeTime: <{merchant_openhours_dict.get('closeTime')}> ,openTime: <{merchant_openhours_dict.get('openTime')}> IP address:{ip_address}"
                            }
                        }
                        logs_sns_resp = publish_sns_message(topic=config.sns_activity_logs, message=str(
                            sns_msg), subject="merchant.update_hours")

                        return success()
                    else:
                        return not_found("Merchant Not Found")
                else:
                    return unauthorised("User Not authorised to access Merchant Information")
            else:
                return invalid("Invalid Token")
        else:
            return not_found("Token not found")
    except Exception as e:
        print(e)
        create_log_data(level='[INFO]', Message=f"Error {e},IP address: {ip_address}, Token:{token}",
                    functionName="removeMerchantOpeningHour", request=request)
        return unhandled("Unhandled Exception")


@app.route('/customer', methods=['POST'])
def createCustomer():
    try:
        _json = request.json
        token = _json['token']
        # validate the received values
        if token and request.method == 'POST':
            userID = validateLoginToken(token)
            if userID:
                merchantID = _json['customer']['merchantID']
                _fn = _json['customer']['firstName']
                _ln = _json['customer']['lastName']
                _email = _json['customer']['email']
                _phone = _json['customer']['phone']
                _address = _json['customer']['address']
                connection = pymysql.connect(
                    host=rds_host, user=username, passwd=password, db=database_name)
                custGUID = createCustomerInDB(
                    merchantID, _fn, _ln, _email, _phone, _address, userID, connection)
                if (custGUID != "error"):
                    connection.commit()
                    resp = getCustomerByIDfromDB(custGUID)
                    return success(resp)
                else:
                    return unhandled("Unhandled Exception")
            else:
                return invalid("Invalid Token")
    except Exception as e:
        print(e)
        return unhandled("Unhandled Exception")


def createCustomerInDB(merchantID, _fn, _ln, _email, _phone, _address, userID, connection):
    try:
        cursor = connection.cursor(pymysql.cursors.DictCursor)
        custGUID = uuid.uuid4()
        data = (custGUID, merchantID, _fn, _ln,
                _email, _address, _phone, userID)
        cursor.execute(
            "INSERT INTO customers (id, merchantid, firstname,lastname,email,address,phone,created_by) VALUES (%s,%s , %s,%s,%s,%s,%s,%s)", data)
        return custGUID
    except Exception as e:
        print(e)
        return "error"


@app.route('/customer/<id>', methods=['PUT'])
def updateCustomer(id):
    try:
        _json = request.json
        token = _json['token']
        # validate the received values
        if token and request.method == 'PUT':
            userID = validateLoginToken(token)
            if userID:
                _fn = _json['customer']['firstName']
                _ln = _json['customer']['lastName']
                _email = _json['customer']['email']
                _phone = _json['customer']['phone']
                _address = _json['customer']['address']

                connection, cursor = get_db_connection()

                cursor.execute("SELECT id FROM customers WHERE id=%s", id)
                row = cursor.fetchone()
                if (row):
                    data = (_fn, _ln, _address, _email, _phone, userID, id)
                    cursor.execute(
                        "UPDATE customers set  firstname = %s,lastname= %s,address= %s,email= %s,phone= %s, updated_by = %s  where id = %s", data)
                    connection.commit()
                    resp = getCustomerByIDfromDB(id)
                    return success(resp)
                else:
                    return not_found("Customer ID Not Found")
            else:
                return invalid("Invalid Token")
    except Exception as e:
        print(e)
        return unhandled("Unhandled Exception")


@app.route('/customer/<id>', methods=['DELETE'])
def removeCustomer(id):
    try:
        token = request.args.get('token')
        # validate the received values
        if token and request.method == 'DELETE':
            userID = validateLoginToken(token)
            if userID:

                connection, cursor = get_db_connection()
                cursor.execute("SELECT id  FROM customers WHERE id=%s", id)

                row = cursor.fetchone()
                if (row):
                    data = (id)
                    cursor.execute("delete from customers WHERE id=%s", data)
                    connection.commit()
                    resp = jsonify('ok')
                    resp.status_code = 200
                    resp.headers['Content-Type'] = 'application/json'
                    resp.headers['Access-Control-Allow-Origin'] = '*'
                    resp.headers['Access-Control-Allow-Headers'] = '*'
                    resp.headers['Access-Control-Allow-Methods'] = 'DELETE,GET,HEAD,OPTIONS,PATCH,POST,PUT'
                    resp.headers['Access-Control-Allow-Credentials'] = 'true'
                    return resp
                else:
                    return not_found("Customer ID Not Found")
            else:
                return invalid("Invalid Token")
    except Exception as e:
        print(e)
        return unhandled("Unhandled Exception")


@app.route('/customers', methods=['GET'])
def getCustomers():
    try:
        token = request.args.get('token')
        if (request.args.get('limit')):
            limit = request.args.get('limit')
        else:
            limit = "25"  # Default limit value

        if (request.args.get('from')):
            _from = request.args.get('from')
        else:
            _from = "0"  # Default limit value

        _email = None
        if (request.args.get('email')):
            _email = request.args.get('email')

        phone = None
        if (request.args.get('phone')):
            phone = request.args.get('phone')

        firstName = None
        if (request.args.get("firstName")):
            firstName = request.args.get("firstName")

        lastName = None
        if (request.args.get("lastName")):
            lastName = request.args.get("lastName")

        if (request.args.get('merchant')):
            merchant = request.args.get('merchant')

            # validate the received values
        if token and request.method == 'GET':
            userID = validateLoginToken(token)
            if userID:
                connection, cursor = get_db_connection()
                if (_email):
                    data = (merchant, "%" + _email + "%")
                    sqlstmt = "SELECT * FROM customers  WHERE  merchantid = %s and email like %s  ORDER by created_datetime DESC LIMIT " + \
                        limit + " OFFSET " + _from
                    cursor.execute(sqlstmt, data)
                elif (phone):
                    data = (merchant, "%" + phone + "%")
                    sqlstmt = "SELECT * FROM customers  WHERE  merchantid = %s and phone like %s  ORDER by created_datetime DESC LIMIT " + \
                        limit + " OFFSET " + _from
                    cursor.execute(sqlstmt, data)
                elif (firstName and lastName):
                    data = (merchant, firstName, lastName)
                    sqlstmt = "SELECT * FROM customers  WHERE  merchantid = %s and firstname = %s and lastname = %s ORDER by created_datetime DESC LIMIT " + limit + " OFFSET " + _from
                    cursor.execute(sqlstmt, data)
                else:
                    sqlstmt = "SELECT * FROM customers  where merchantid = %s   ORDER by created_datetime DESC LIMIT " + \
                        limit + " OFFSET " + _from
                    cursor.execute(sqlstmt, merchant)

                rows = cursor.fetchall()
                resp = jsonify(rows)
                return success(resp)
            else:
                return invalid("Invalid Token")
    except Exception as e:
        print(e)
        return unhandled("Unhandled Exception")


@app.route('/customer/<id>', methods=['GET'])
def getCustomerByID(id):
    try:
        token = request.args.get('token')
        # validate the received values
        if token and request.method == 'GET':
            userID = validateLoginToken(token)
            if userID:
                resp = getCustomerByIDfromDB(id)
                return success(resp)
            else:
                return invalid("Invalid Token")
    except Exception as e:
        print(e)
        return unhandled("Unhandled Exception")


def getCustomerByIDfromDB(id):
    try:
        connection, cursor = get_db_connection()
        cursor.execute(
            "SELECT id,firstname firstName,lastname lastName,email,address,phone FROM customers WHERE id=%s", id)
        row = cursor.fetchone()
        if (row):
            message = {
                'id': row['id'],
                'firstName': row['firstName'],
                'lastName': row['lastName'],
                'email': row['email'],
                'address': row['address'],
                'phone': row['phone']
            }
            resp = jsonify(message)
            return resp
        else:
            return not_found("Customer ID Not Found")
    except Exception as e:
        print(e)
        return unhandled("Unhandled Exception")


@app.route('/merchant/<mid>/categories', methods=['GET'])
def getMerchantCategoryList(mid):
    try:
        token = request.args.get('token')
        if (request.args.get('limit')):
            limit = request.args.get('limit')
        else:
            limit = "25"  # Default limit value

        if (request.args.get('from')):
            _from = request.args.get('from')
        else:
            _from = "0"  # Default limit value

        if (request.args.get('categoryName')):
            _categoryName = request.args.get('categoryName')
        else:
            _categoryName = None

        # validate the received values
        if token and request.method == 'GET':
            userID = validateLoginToken(token)
            if userID:
                if (validateMerchantUser(mid, userID)):
                    connection, cursor = get_db_connection()
                    cursor.execute("SELECT id FROM merchants WHERE id=%s", mid)
                    row = cursor.fetchone()
                    if (row):
                        if (_categoryName):
                            data = (mid, "%" + _categoryName + "%")
                            cursor.execute(
                                "SELECT id, categoryname categoryName, categorydescription categoryDescription, status categoryStatus  FROM categories WHERE merchantid = %s  and categoryname like %s ORDER by created_datetime DESC LIMIT " + limit + " OFFSET " + _from,
                                data)
                        else:
                            cursor.execute(
                                "SELECT id, categoryname categoryName, categorydescription categoryDescription, status categoryStatus  FROM categories WHERE merchantid = %s  ORDER by created_datetime DESC LIMIT " + limit + " OFFSET " + _from,
                                mid)

                        rows = cursor.fetchall()
                        resp = jsonify(rows)
                        resp.status_code = 200
                        return success(resp)
                    else:
                        return not_found("Merchant ID Not Found")
                else:
                    return unauthorised("User Not authorised to access Merchant Information")

            else:
                return invalid("Invalid Token")
    except Exception as e:
        print(e)
        return unhandled("Unhandled Exception")


@app.route('/merchant/<mid>/item/<pid>/linkcategory', methods=['POST'])
def addCateogoriesToItem(mid, pid):
    try:
        _json = request.json
        token = _json['token']
        # validate the received values
        if token and request.method == 'POST':
            userID = validateLoginToken(token)
            if userID:
                if (validateMerchantUser(mid, userID)):
                    connection, cursor = get_db_connection()
                    for category in _json['categories']:
                        if ('categoryID' in category):
                            cid = category['categoryID']
                            prodcatGUID = uuid.uuid4()
                            data = (prodcatGUID, pid, cid, userID)
                            cursor.execute(
                                "INSERT INTO productscategories (id, productid, categoryid, created_by) VALUES (%s,%s,%s,%s)", data)
                            connection.commit()
                    resp = jsonify('ok')
                    resp.status_code = 200
                    return success(resp)
                else:
                    return unauthorised("User Not authorised to access Merchant Information")
            else:
                return invalid("Invalid Token")
    except Exception as e:
        print(e)
        return unhandled("Unhandled Exception")


@app.route('/merchant/<mid>/item/<pid>/updatecategories', methods=['POST'])
def updateCateogoriesToItem(mid, pid):
    try:
        _json = request.json
        token = _json['token']
        # validate the received values
        if token and request.method == 'POST':
            userID = validateLoginToken(token)
            if userID:
                if (validateMerchantUser(mid, userID)):
                    connection, cursor = get_db_connection()
                    cursor.execute(
                        "DELETE FROM productscategories WHERE PRODUCTID = %s", pid)
                    for category in _json['categories']:
                        if ('categoryID' in category):
                            cid = category['categoryID']
                            prodcatGUID = uuid.uuid4()
                            data = (prodcatGUID, pid, cid, userID)
                            cursor.execute(
                                "INSERT INTO productscategories (id, productid, categoryid, created_by) VALUES (%s,%s,%s,%s)", data)
                            connection.commit()
                    resp = jsonify('ok')
                    resp.status_code = 200
                    return success(resp)
                else:
                    return unauthorised("User Not authorised to access Merchant Information")
            else:
                return invalid("Invalid Token")
    except Exception as e:
        print(e)
        return unhandled("Unhandled Exception")


@app.route('/merchant/<mid>/item/<pid>/unlinkcategory', methods=['POST'])
def removeCateogoriesToItem(mid, pid):
    try:
        _json = request.json
        token = _json['token']
        # validate the received values
        if token and request.method == 'POST':
            userID = validateLoginToken(token)
            if userID:
                if (validateMerchantUser(mid, userID)):
                    connection, cursor = get_db_connection()
                    for category in _json['categories']:
                        if ('categoryID' in category):
                            cid = category['categoryID']
                            prodcatGUID = uuid.uuid4()
                            data = (pid, cid)
                            cursor.execute(
                                "DELETE FROM productscategories WHERE productid = %s and categoryid = %s", data)
                            connection.commit()
                    resp = jsonify('ok')
                    resp.status_code = 200
                    return success(resp)
                else:
                    return unauthorised("User Not authorised to access Merchant Information")
            else:
                return invalid("Invalid Token")
    except Exception as e:
        print(e)
        return unhandled("Unhandled Exception")


@app.route('/merchant/<mid>/item/<pid>/linkaddons', methods=['POST'])
def addAddONsToItem(mid, pid):
    try:
        _json = request.json
        token = _json['token']
        # validate the received values
        if token and request.method == 'POST':
            userID = validateLoginToken(token)
            if userID:
                if (validateMerchantUser(mid, userID)):
                    connection, cursor = get_db_connection()
                    for addon in _json['addons']:
                        if ('addonID' in addon):
                            aid = addon['addonID']
                            prodcatGUID = uuid.uuid4()
                            data = (prodcatGUID, pid, aid, userID)
                            cursor.execute(
                                "INSERT INTO productsaddons (id, productid, addonid, created_by) VALUES (%s,%s,%s,%s)", data)
                            connection.commit()
                    resp = jsonify('ok')
                    resp.status_code = 200
                    return success(resp)
                else:
                    return unauthorised("User Not authorised to access Merchant Information")
            else:
                return invalid("Invalid Token")
    except Exception as e:
        print(e)
        return unhandled("Unhandled Exception")


@app.route('/merchant/<mid>/item/<pid>/updateaddons', methods=['POST'])
def updateAddonsToProduct(mid, pid):
    try:
        _json = request.json
        token = _json['token']
        # validate the received values
        if token and request.method == 'POST':
            userID = validateLoginToken(token)
            if userID:
                if (validateMerchantUser(mid, userID)):
                    connection, cursor = get_db_connection()
                    cursor.execute(
                        "DELETE FROM productsaddons WHERE PRODUCTID = %s", pid)
                    connection.commit()
                    for addon in _json['addons']:
                        if ('addonID' in addon):
                            aid = addon['addonID']
                            prodcatGUID = uuid.uuid4()
                            data = (prodcatGUID, pid, aid, userID)
                            cursor.execute(
                                "INSERT INTO productsaddons (id, productid, addonid, created_by) VALUES (%s,%s,%s,%s)", data)
                            connection.commit()
                    resp = jsonify('ok')
                    resp.status_code = 200
                    return success(resp)
                else:
                    return unauthorised("User Not authorised to access Merchant Information")
            else:
                return invalid("Invalid Token")
    except Exception as e:
        print(e)
        return unhandled("Unhandled Exception")


@app.route('/merchant/<mid>/item/<pid>/unlinkaddons', methods=['POST'])
def removeAddONsToItem(mid, pid):
    try:
        _json = request.json
        token = _json['token']
        # validate the received values
        if token and request.method == 'POST':
            userID = validateLoginToken(token)
            if userID:
                if (validateMerchantUser(mid, userID)):
                    connection, cursor = get_db_connection()
                    for addon in _json['addons']:
                        if ('addonID' in addon):
                            aid = addon['addonID']
                            data = (pid, aid)
                            cursor.execute(
                                "DELETE FROM productsaddons WHERE productid = %s and addonid = %s", data)
                            connection.commit()
                    resp = jsonify('ok')
                    resp.status_code = 200
                    return success(resp)
                else:
                    return unauthorised("User Not authorised to access Merchant Information")
            else:
                return invalid("Invalid Token")
    except Exception as e:
        print(e)
        return unhandled("Unhandled Exception")


@app.route('/merchant/<mid>/addons', methods=['GET'])
def getMerchantaddonList(mid):
    try:
        token = request.args.get('token')
        if (request.args.get('limit')):
            limit = request.args.get('limit')
        else:
            limit = "25"  # Default limit value

        if (request.args.get('from')):
            _from = request.args.get('from')
        else:
            _from = "0"  # Default limit value

        if (request.args.get('addonName')):
            _addonName = request.args.get('addonName')
        else:
            _addonName = None

        productId = request.args.get("productId")

        # validate the received values
        if token and request.method == 'GET':
            userID = validateLoginToken(token)
            if userID:
                if (validateMerchantUser(mid, userID)):
                    connection, cursor = get_db_connection()
                    if productId is not None and _addonName is not None:
                        data = (mid, productId, _addonName,
                                int(limit), int(_from))
                        cursor.execute("""
                      SELECT id, addonname addonName, addondescription addonDescription 
                        FROM addons WHERE merchantid = %s AND id IN (
                          SELECT addonid FROM productsaddons WHERE productid = %s
                          ) AND addonname=%s ORDER BY created_datetime LIMIT %s OFFSET %s
                      """, data)
                    elif productId is not None:
                        data = (mid, productId, int(limit), int(_from))
                        cursor.execute("""
                      SELECT id, addonname addonName, addondescription addonDescription 
                        FROM addons WHERE merchantid = %s AND id IN (
                          SELECT addonid FROM productsaddons WHERE productid = %s
                          ) ORDER BY created_datetime LIMIT %s OFFSET %s
                      """, data)
                    elif (_addonName):
                        data = (mid, _addonName)
                        cursor.execute(
                            "SELECT id, addonname addonName,  addondescription addonDescription, minpermitted minPermitted, maxpermitted maxPermitted FROM addons WHERE merchantid = %s and addonname = %s ORDER by created_datetime DESC LIMIT " + limit + " OFFSET " + _from,
                            data)
                    else:
                        data_1 = (mid)
                        cursor.execute(
                            "SELECT id, addonname addonName,  addondescription addonDescription, minpermitted minPermitted, maxpermitted maxPermitted FROM addons WHERE merchantid = %s ORDER by created_datetime DESC LIMIT " + limit + " OFFSET " + _from,
                            data_1)

                    rows = cursor.fetchall()
                    resp = jsonify(rows)
                    return success(resp)
                else:
                    return unauthorised("User Not authorised to access Merchant Information")
            else:
                return invalid("Invalid Token")
    except Exception as e:
        print(e)
        return unhandled("Unhandled Exception")


@app.route('/merchant/<mid>/addon/<aid>/linkitems', methods=['POST'])
def addItemsToAddon(mid, aid):
    try:
        _json = request.json
        token = _json['token']
        # validate the received values
        if token and request.method == 'POST':
            userID = validateLoginToken(token)
            if userID:
                if (validateMerchantUser(mid, userID)):
                    connection, cursor = get_db_connection()
                    for addonOption in _json['AddonOptions']:
                        if ('itemID' in addonOption):
                            itemid = addonOption['itemID']
                            itemaddonGUID = uuid.uuid4()
                            data = (itemaddonGUID, itemid, aid, userID)
                            cursor.execute(
                                "INSERT INTO addonsoptions (id, itemid, addonid, created_by) VALUES (%s,%s,%s,%s)", data)
                            connection.commit()
                            resp = jsonify('ok')
                            resp.status_code = 200
                            return success(resp)
                else:
                    return unauthorised("User Not authorised to access Merchant Information")
            else:
                return invalid("Invalid Token")
    except Exception as e:
        print(e)
        return unhandled("Unhandled Exception")


@app.route('/merchant/<mid>/addon/<aid>/updateitems', methods=['POST'])
def updateItemsToAddon(mid, aid):
    try:
        _json = request.json
        token = _json['token']
        # validate the received values
        if token and request.method == 'POST':
            userID = validateLoginToken(token)
            if userID:
                if (validateMerchantUser(mid, userID)):
                    connection, cursor = get_db_connection()
                    cursor.execute(
                        "DELETE FROM addonsoptions WHERE addonid = %s", aid)
                    connection.commit()
                    for addonOption in _json['AddonOptions']:
                        if ('itemID' in addonOption):
                            itemid = addonOption['itemID']
                            itemaddonGUID = uuid.uuid4()
                            data = (itemaddonGUID, itemid, aid, userID)
                            cursor.execute(
                                "INSERT INTO addonsoptions (id, itemid, addonid, created_by) VALUES (%s,%s,%s,%s)", data)
                            connection.commit()
                    resp = jsonify('ok')
                    resp.status_code = 200
                    return success(resp)
                else:
                    return unauthorised("User Not authorised to access Merchant Information")
            else:
                return invalid("Invalid Token")
    except Exception as e:
        print(e)
        return unhandled("Unhandled Exception")


@app.route('/merchant/<mid>/addon/<aid>/unlinkitems', methods=['POST'])
def removeItemsToAddon(mid, aid):
    try:
        _json = request.json
        token = _json['token']
        # validate the received values
        if token and request.method == 'POST':
            userID = validateLoginToken(token)
            if userID:
                if (validateMerchantUser(mid, userID)):
                    connection, cursor = get_db_connection()
                    for addonOption in _json['AddonOptions']:
                        if ('itemID' in addonOption):
                            itemid = addonOption['itemID']
                            prodcatGUID = uuid.uuid4()
                            data = (itemid, aid)
                            cursor.execute(
                                "DELETE from addonsoptions WHERE   itemid=%s and addonid = %s", data)
                            connection.commit()
                            resp = jsonify('ok')
                            resp.status_code = 200
                            return success(resp)
                else:
                    return unauthorised("User Not authorised to access Merchant Information")
            else:
                return invalid("Invalid Token")
    except Exception as e:
        print(e)
        return unhandled("Unhandled Exception")


@app.route('/orders', methods=['GET'])
def getMerchantOrdersSummary():
    try:
        token = request.args.get('token')
        if (request.args.get('limit')):
            limit = request.args.get('limit')
        else:
            limit = "25"  # Default limit value

        if (request.args.get('from')):
            _from = request.args.get('from')
        else:
            _from = "0"  # Default limit value

        if (request.args.get('status')):
            _status = request.args.get('status')
        else:
            _status = "0"  # Default limit value

        if (request.args.get('merchantID')):
            mid = request.args.get('merchantID')
        else:
            mid = None  # Default limit value

        # validate the received values
        if token and request.method == 'GET':
            userID = validateLoginToken(token)
            if userID:
                connection, cursor = get_db_connection()
                cursor.execute("SELECT role FROM users WHERE id=%s", userID)
                userrow = cursor.fetchone()
                if (userrow):
                    if not (userrow['role'] == 1 or userrow['role'] == 2):
                        if (mid):
                            if not (validateMerchantUser(mid, userID)):
                                return unauthorised("User Not authorised to access Merchant Information")
                        else:
                            return invalid("Invalid Merchant ID In the Search Parameter")

                    if (mid):
                        if (_status.count(",") > 0):
                            format_strings = ','.join(
                                ['%s'] * (_status.count(",") + 1))
                        else:
                            format_strings = '%s'
                        my_list = _status.split(",")
                        print(format_strings)
                        print(tuple(my_list))
                        dataArray = [mid]
                        tupleArray = dataArray + my_list
                        print(tuple(tupleArray))
                        data = (tuple(tupleArray))
                        cursor.execute(
                            "SELECT id,merchantid, date_format(orderdatetime, '%%Y-%%c-%%dT%%H:%%i:%%S') orderDateTime,customerid orderCustomerID, customername orderCustomerName, status orderStatus, convert(ordersubtotal,CHAR) orderSubTotal, convert(orderdeliveryfee,CHAR) orderDeliveryFee, convert(ordertotal,CHAR) orderTotal, convert(ordertax,CHAR) orderTax, orderexternalreference orderExternalReference, ordersource orderSource, short_order_id from orders where merchantid = %s and status IN (" + format_strings + ") ORDER by created_datetime DESC LIMIT " + limit + " OFFSET " + _from,
                            data)
                    else:
                        if (_status.count(",") > 0):
                            format_strings = ','.join(
                                ['%s'] * (_status.count(",") + 1))
                        else:
                            format_strings = '%s'
                        query = "SELECT id,merchantid,date_format(orderdatetime, '%%Y-%%c-%%dT%%H:%%i:%%S') orderDateTime,customerid orderCustomerID, customername orderCustomerName, status orderStatus, convert(ordersubtotal,CHAR) orderSubTotal, convert(orderdeliveryfee,CHAR) orderDeliveryFee, convert(ordertotal,CHAR) orderTotal, convert(ordertax,CHAR) orderTax, orderexternalreference orderExternalReference, ordersource orderSource, short_order_id from orders where status IN (" + format_strings + ") ORDER by created_datetime DESC LIMIT " + limit + " OFFSET " + _from
                        my_list = _status.split(",")
                        cursor.execute(query, tuple(my_list))

                    rows = cursor.fetchall()
                    ordersmessage = []
                    for order in rows:
                        data = (order['merchantid'])
                        cursor.execute(
                            "SELECT id, merchantname  FROM merchants where id = %s", data)
                        mercharntrow = cursor.fetchone()
                        order['merchantName'] = mercharntrow['merchantname']
                        order['short_order_id'] = order['short_order_id']
                        ordersmessage.append(order)

                    resp = jsonify(ordersmessage)
                    resp.status_code = 200
                    return success(resp)
            else:
                return invalid("Invalid Token")
    except Exception as e:
        print(e)
        return unhandled("Unhandled Exception")


@app.route('/webhook', methods=['POST'])
def createWebhook():
    try:
        _json = request.json
        token = _json['token']
        # validate the received values
        if token and request.method == 'POST':
            userID = validateLoginToken(token)
            if userID:
                _webhookName = _json['webhook']['webhookName']
                _webhookEvent = _json['webhook']['webhookEvent']
                _webhookUrl = _json['webhook']['webhookUrl']
                _merchantID = _json['webhook']['merchantID']
                _webhookheadersignaturekey = _json['webhook']['webhookheadersignaturekey']
                connection = pymysql.connect(
                    host=rds_host, user=username, passwd=password, db=database_name)
                webhookGUID = createWebhookInDB(
                    _webhookName, _webhookEvent, _webhookUrl, _merchantID, _webhookheadersignaturekey, userID, connection)
                if (webhookGUID != "error"):
                    connection.commit()
                    resp = getWebhookByIDfromDB(webhookGUID)
                    return success(resp)
                else:
                    return unhandled("Unhandled Exception")
            else:
                return invalid("Invalid Token")
    except Exception as e:
        print(e)
        return unhandled("Unhandled Exception")


def createWebhookInDB(_webhookName, _webhookEvent, _webhookUrl, _merchantID, _webhookheadersignaturekey, userID, connection):
    try:
        cursor = connection.cursor(pymysql.cursors.DictCursor)
        webhookGUID = uuid.uuid4()
        data = (webhookGUID, _webhookName, _webhookEvent, _webhookUrl,
                _webhookheadersignaturekey, _merchantID, userID)
        cursor.execute(
            "INSERT INTO webhooks (id, webhookname,webhookevent,webhookurl,webhookheadersignaturekey,merchantID,created_by) VALUES (%s,%s,%s,%s,%s,%s,%s)", data)
        return webhookGUID
    except Exception as e:
        print(e)
        return "error"


@app.route('/webhook/<id>', methods=['DELETE'])
def removeWebhook(id):
    try:
        token = request.args.get('token')
        # validate the received values
        if token and request.method == 'DELETE':
            userID = validateLoginToken(token)
            if userID:

                connection, cursor = get_db_connection()
                cursor.execute("SELECT id  FROM webhooks WHERE id=%s", id)

                row = cursor.fetchone()
                if (row):
                    data = (id)
                    cursor.execute("delete from webhooks WHERE id=%s", data)
                    connection.commit()
                    resp = jsonify('ok')
                    resp.status_code = 200
                    resp.headers['Content-Type'] = 'application/json'
                    resp.headers['Access-Control-Allow-Origin'] = '*'
                    resp.headers['Access-Control-Allow-Headers'] = '*'
                    resp.headers['Access-Control-Allow-Methods'] = 'DELETE,GET,HEAD,OPTIONS,PATCH,POST,PUT'
                    resp.headers['Access-Control-Allow-Credentials'] = 'true'
                    return resp
                else:
                    return not_found("Webhook ID Not Found")
            else:
                return invalid("Invalid Token")
    except Exception as e:
        print(e)
        return unhandled("Unhandled Exception")


@app.route('/webhooks', methods=['GET'])
def getWebhooks():
    try:
        token = request.args.get('token')
        if (request.args.get('limit')):
            limit = request.args.get('limit')
        else:
            limit = "25"  # Default limit value

        if (request.args.get('from')):
            _from = request.args.get('from')
        else:
            _from = "0"  # Default limit value

        # validate the received values
        if token and request.method == 'GET':
            userID = validateLoginToken(token)
            if userID:
                connection, cursor = get_db_connection()
                sqlstmt = "SELECT id,webhookname webhookName,webhookevent webhookEvent,webhookUrl,webhookheadersignaturekey,merchantID FROM webhooks  ORDER by created_datetime DESC LIMIT " + limit + " OFFSET " + _from
                cursor.execute(sqlstmt)
                rows = cursor.fetchall()
                resp = jsonify(rows)
                return success(resp)
            else:
                return invalid("Invalid Token")
    except Exception as e:
        print(e)
        return unhandled("Unhandled Exception")


@app.route('/webhook/<id>', methods=['GET'])
def getWebhookByID(id):
    try:
        token = request.args.get('token')
        # validate the received values
        if token and request.method == 'GET':
            userID = validateLoginToken(token)
            if userID:
                resp = getWebhookByIDfromDB(id)
                return success(resp)
            else:
                return invalid("Invalid Token")
    except Exception as e:
        print(e)
        return unhandled("Unhandled Exception")


def getWebhookByIDfromDB(id):
    try:
        connection, cursor = get_db_connection()
        cursor.execute(
            "SELECT id,webhookname webhookName,webhookevent webhookEvent,webhookUrl,webhookheadersignaturekey,merchantID FROM webhooks WHERE id=%s", id)
        row = cursor.fetchone()
        if (row):
            message = {
                'id': row['id'],
                'webhookName': row['webhookName'],
                'webhookEvent': row['webhookEvent'],
                'webhookUrl': row['webhookUrl'],
                'webhookheadersignaturekey': row['webhookheadersignaturekey'],
                'merchantID': row['merchantID']
            }
            resp = jsonify(message)
            return resp
        else:
            return not_found("Webhook ID Not Found")
    except Exception as e:
        print(e)
        return unhandled("Unhandled Exception")
