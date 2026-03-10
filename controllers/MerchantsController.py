import json
from twilio.rest import Client
import pymysql.cursors
import datetime
import pytz
import math

import stripe
from flask import jsonify, request, g
from dateutil.relativedelta import relativedelta
from dateutil.tz import gettz

# local imports
from models.Payouts import Payouts
from models.VirtualMerchants import VirtualMerchants
from utilities.helpers import validateLoginToken, validateMerchantUser, get_db_connection, success, publish_sns_message, \
    validateAdminUser, is_float, add_trassuary_features_to_connect_account, \
    create_finacial_account, get_balances, get_finacial_account_details, \
    issue_card, get_issued_cards, get_financial_account_transections, funds_transfer_to_connected_account, \
    add_financial_account_as_default_external_account, funds_transfer_to_financial_account, generate_otp, \
    funds_transfer_to_external_bank_account, get_external_accounts, add_external_account, \
    funds_transfer_to_stored_external_bank_account, delete_external_account, update_connect_account_tos, \
    get_connect_account_details, create_log_data,get_ip_address
from controllers.Middleware import validate_token_middleware
from utilities.errors import invalid, not_found, unhandled, unauthorised
from models.Merchants import Merchants
from models.Users import Users
from models.LoyaltyPoints import LoyaltyPoints
from models.esper.Esper import Esper
from models.twilio.Twilio import Twilio

import config
import boto3
import logging
logger = logging.getLogger(__name__)
# eventbridge = boto3.client('events')

# def lambda_handler(counter):
#     user_data = {
#         "user_id": "saim",
#         "notification_time": {"hour": 9, "minute": 0},
#         "custom_data": {"key1": "value1"}  # Add custom data as needed
#     }
#
#     # Calculate the time for notification based on user-specific data
#     # notification_time = datetime.datetime.utcnow().replace(
#     #     hour=user_data["notification_time"]["hour"],
#     #     minute=user_data["notification_time"]["minute"],
#     #     second=0,
#     #     microsecond=0
#     # )
#     notification_time = datetime.datetime.utcnow() + datetime.timedelta(minutes=2)
#
#     # Round the notification time to the nearest minute
#     notification_time = notification_time.replace(second=0, microsecond=0)
#     # Create EventBridge rule for the user "saim"
#     rule_name = 'NotificationRule_saim'+counter
#     response = eventbridge.put_rule(
#         Name=rule_name,
#         ScheduleExpression=f'cron({notification_time.minute} {notification_time.hour} * * ? *)',
#         # Run every day at the specified time
#         State='ENABLED',  # Ensure the rule is enabled
#         Description=f'Notification rule for {user_data["user_id"]}',
#         EventBusName='default',  # Or specify your event bus name
#         Tags=[{
#             "Key": "User_id",
#             "Value": "1"
#         },{
#             "Key": "user_name",
#             "Value": "Saim"
#         }],
#         # RoleArn='arn:aws:iam::123456789012:role/service-role/EventBridgeLambdaRole',  # Replace with your IAM role ARN
#         # Pass custom data to the rule using Input parameter
#         # This custom data will be available in the event sent to the Lambda function
#         # You can pass any JSON serializable data here
#         # Input=json.dumps({"custom_data": user_data["custom_data"]})
#     )
#
#     # Set Lambda function as the target for the rule
#     target_arn = config.send_notification_function
#     eventbridge.put_targets(
#         Rule=rule_name,
#         Targets=[
#             {
#                 'Id': target_arn,
#                 'Arn': config.send_notification_function,
#                 'InputTransformer': {
#                     'InputPathsMap': {
#                         'eventMessage': '$.detail.message',
#                         'eventSubject': '$.detail.subject'
#                     },
#                     'InputTemplate': json.dumps({
#                         'default': 'Event occurred with message: <eventMessage> and subject: <eventSubject>'
#                     })
#                 }
#             }
#         ]
#     )
# GET


def get_rds_token(hostname, port, username, region):
    client = boto3.client('rds', region_name=region)
    token = client.generate_db_auth_token(
        DBHostname=hostname, Port=port, DBUsername=username)
    return token


def connect_to_rds(hostname, port, username, region, database):
    token = get_rds_token(hostname, port, username, region)
    import os
    current_dir = os.path.dirname(os.path.realpath(__file__))

    # Construct the full path to the CA bundle file
    # ca_bundle_path = os.path.join(current_dir, 'rds-combined-ca-bundle.pem')
    conn = pymysql.connect(host=hostname, port=port, user=username, database=database,
                           password=token)
    return conn


def testDB():
    try:
        hostname = 'devtest-dbcluster-instance-1-old1.cbzj3czadroc.us-east-2.rds.amazonaws.com'
        port = 3306
        username = 'saim'
        region = 'us-east-2'
        database = 'dashboard'

        conn = connect_to_rds(hostname, port, username, region, database)

        with conn.cursor() as cursor:
            # Example query
            sql = "SELECT * FROM users;"
            cursor.execute(sql)
            result = cursor.fetchall()
        return success(jsonify(result))

    except Exception as e:
        print("Error: ", str(e))
        return unhandled()


def getMerchants():
    return Merchants.get_merchants(request)


def getMerchantsV2():
    _json = request.json

    esperDeviceIds = None
    openingHoursFilter = {"filter": 0}
    if _json:
        esperDeviceIds = _json.get("esperDeviceIds") if isinstance(
            _json.get("esperDeviceIds"), list) else None
        openingHoursFilter = _json.get(
            "openingHoursFilter") or openingHoursFilter  # {"filter": 0 OR 1, "startTime": "00:00:00" OR NONE, "endTime": "23:59:59" OR NONE}

    return Merchants.get_merchants(request, esperDeviceIds=esperDeviceIds, openingHoursFilter=openingHoursFilter)


def insert_into_subscription_table(id, date, amount, status, frequency, istrail, remarks=''):
    connection, cursor = get_db_connection()
    cursor.execute(
        """INSERT INTO subscriptions ( merchantId, amount, date, status, frequency, istrail,waiveoff_remarks) VALUES (%s,%s,%s,%s,%s,%s,%s) """,
        (id, amount, date, status, frequency, istrail, remarks))
    connection.commit()
    return True


def insert_into_subscription_table_by_split(id, date, amount, status, frequency, istrail, split_status, remarks):
    connection, cursor = get_db_connection()
    cursor.execute(
        """INSERT INTO subscriptions ( merchantId, amount, date, status, frequency, istrail,split_remarks,split_status) VALUES (%s,%s,%s,%s,%s,%s,%s,%s) """,
        (id, amount, date, status, frequency, istrail, remarks, split_status))
    connection.commit()
    return True


def update_logs_subscription(merchantId, detail):
    sns_msg = {
        "event": "subscription.create_record",
        "body": {
            "merchantId": merchantId,
            "detail": detail
        }
    }
    logs_sns_resp = publish_sns_message(topic=config.sns_audit_logs, message=str(sns_msg),
                                        subject="subscription.create_record")


def initBatteryStatusAlert(merchantId):

    try:
        _json = request.json
        # print(_json)
        # quit()

        batteryPercent = _json.get("batteryPercent")
        chargingStatus = _json.get("chargingStatus")

        log_level = config.log_level if config.log_level is not None and config.log_level != "" else 'logging.ERROR'
        # logger.setLevel(eval(log_level))

        create_log_data(level='[DEBUG]', Message="Trigger the function that initiate the battery charging status alert",
                        messagebody="", functionName="initBatteryStatusAlert", statusCode="200 Ok",
                        merchantID=merchantId, request=request)

        userId = g.userId

        # print(default_request)

        if batteryPercent is None:
            create_log_data(level='[ERROR]', Message="batteryPercent is required.",
                            messagebody="batteryPercent is not mentioned in the request",
                            functionName="initBatteryStatusAlert", statusCode="404 Not Found", merchantID=merchantId,
                            request=request)
            return not_found(body={"batteryPercent": "required"})
        if chargingStatus is None:
            create_log_data(level='[ERROR]', Message="chargingStatus is required.",
                            messagebody="chargingStatus is not mentioned in the request",
                            functionName="initBatteryStatusAlert", statusCode="404 Not Found", merchantID=merchantId,
                            request=request)
            return not_found(body={"chargingStatus": "required"})

        if not validateMerchantUser(merchantId, userId):
            create_log_data(level='[INFO]', Message="User Not authorised to access merchant information",
                            messagebody="failed to validate the merchant user", functionName="initBatteryStatusAlert",
                            statusCode="403 Forbidden", merchantID=merchantId, request=request)
            return unauthorised("User Not authorised to access merchant information")

        smsText = None
        if chargingStatus == 'not-charging' and batteryPercent <= config.battery_alert_threshold:
            sns_msg = {
                "event": "merchant.battery_charge_required_alert",
                "body": {
                    "merchantId": merchantId,
                    "userId": userId,
                    "chargingStatus": chargingStatus,
                    "batteryPercent": batteryPercent
                }
            }
            create_log_data(level='[INFO]', Message="Battery not charging and below threshold", messagebody="",
                            functionName="initBatteryStatusAlert", merchantID=merchantId, request=request)

            logs_sns_resp = publish_sns_message(topic=config.sns_activity_logs, message=str(sns_msg),
                                                subject="merchant.battery_status_change")
            # merchant_sns_resp = publish_sns_message(topic=config.sns_merchant_notification, message=str(sns_msg),
            #                                         subject="merchant.battery_status_change")
        elif chargingStatus == 'charging' and batteryPercent <= config.battery_alert_threshold:
            sns_msg = {
                "event": "merchant.battery_charge_ok_alert",
                "body": {
                    "merchantId": merchantId,
                    "userId": userId,
                    "chargingStatus": chargingStatus,
                    "batteryPercent": batteryPercent
                }
            }
            create_log_data(level='[INFO]', Message="Battery charging and below threshold", messagebody="",
                            functionName="initBatteryStatusAlert", merchantID=merchantId, request=request)
            logs_sns_resp = publish_sns_message(topic=config.sns_activity_logs, message=str(sns_msg),
                                                subject="merchant.battery_status_change")
            # merchant_sns_resp = publish_sns_message(topic=config.sns_merchant_notification, message=str(sns_msg),
            #                                         subject="merchant.battery_status_change")
        else:
            create_log_data(level='[INFO]', Message="Battery status logging", messagebody="",
                            functionName="initBatteryStatusAlert", merchantID=merchantId, request=request)
            # pauseTime = 'Today'

        create_log_data(level='[INFO]', Message="Battery status api response", messagebody="",
                        functionName="initBatteryStatusAlert", statusCode="200 ok", merchantID=merchantId,
                        request=request)
        return success(jsonify({
            "message": "success",
            "status": 200
        }))

    except Exception as e:
        create_log_data(level='[ERROR]', Message="Exception occured", messagebody=str(e),
                        functionName="initBatteryStatusAlert",
                        statusCode="500 INTERNAL SERVER ERROR", merchantID=merchantId, request=request)
        return unhandled(f"error: {e}")


def calculate_subscription(merchantId):

    # current_datetime = datetime.datetime.now()
    # utc_today_date = current_datetime.replace(tzinfo=datetime.timezone.utc)

    # db connection
    connection, cursor = get_db_connection()

    # # get all merchants whose status = subscriptionstatus = 1 (active and in-business)
    # cursor.execute("SELECT * FROM merchants WHERE merchants.status = 1 AND merchants.subscriptionstatus = 1")
    # rows = cursor.fetchall()

    cursor.execute(
        "SELECT * FROM merchants WHERE id = %s AND merchants.subscriptionstatus = 1", (merchantId))
    rows = cursor.fetchall()

    for row in rows:
        try:

            # today_date = utc_today_date.astimezone(gettz(row["timezone"])).date()
            # Convert the string to a date
            today_date = '2024-10-08'
            date_format = "%Y-%m-%d"
            today_date = datetime.datetime.strptime(today_date, date_format)
            today_date = today_date.date()
            if row['AutoWaivedStatus'] == 1:
                cursor.execute(f""" SELECT  SUM(total) as total FROM
                                (
                                    (SELECT
                                    SUM(ordertotal) as total
                                    FROM orders WHERE status=7
                                    and merchantid=%s and orderdatetime < %s)
                                    UNION
                                   (SELECT  SUM(ordertotal) as total
                                  FROM ordershistory WHERE status=7
                                    and merchantid=%s and orderdatetime < %s)
                                ) AS result
                                              """, (merchantId, today_date, merchantId, today_date))
                lifetime_total_revenue = cursor.fetchone()
                if lifetime_total_revenue['total'] == None:
                    lifetime_total_revenue['total'] = 0

                cursor.execute(f"""SELECT 
                                           SUM(totalTime) as total ,
                                           SUM(pauseTime) as totalpauseTime ,
                                           SUM(resumeTime) as totalresumeTime 
                                           FROM downtime 
                                           WHERE merchantId = %s and date >= DATE_SUB(NOW(), INTERVAL 30 DAY)""",
                               (merchantId))
                DownTimeData = cursor.fetchone()
                # DownTimeData['totalpauseTime'] = 0 if DownTimeData['totalpauseTime'] is None else DownTimeData[
                #     'totalpauseTime']
                # DownTimeData['total'] = 0 if DownTimeData['total'] is None else DownTimeData[
                #     'total']
                downtime = 0
                if DownTimeData['totalpauseTime'] != None and DownTimeData['total'] != None:
                    downtime = (
                        DownTimeData['totalpauseTime'] / DownTimeData['total']) * 100
                    downtime = round(downtime, 2)

            subscription_start_date = row['subscriptionstartdate']
            next_charge_date = row['nextsubscriptionchargedate']
            subscription_frequency = int(row['subscriptionfrequency'])
            subscription_trail_period = int(row['subscriptiontrialperiod'])
            subscription_amount = row['subscriptionamount']
            merchantId = row['id']

            print(f"\n----- merchant_name : {row['merchantname']} -----")
            print("TODAY: ", today_date)
            print("SUB START DATE: ", subscription_start_date)
            print("NXT CHG DATE: ", next_charge_date)
            print("SUB TRAIL: ", str(subscription_trail_period))
            print("SUB FREQ: ", str(subscription_frequency))
            print("TRAIL PERIOD LAST DATE: ",
                  subscription_start_date + relativedelta(months=subscription_trail_period, days=-1))

            # addition check for -> beta
            if next_charge_date is None:
                next_charge_date = subscription_start_date

            # check if subscript start date is correct
            if subscription_start_date is None:
                print("error: subscription start date is incorrect!!!")
                update_logs_subscription(
                    merchantId, 'error: subscription start date is incorrect!!!')
                continue

            # if today date is greater than the subscription start date
            if today_date >= subscription_start_date:
                print("0000000000000000000")

                cursor.execute("""SELECT * FROM subscriptions WHERE merchantid= %s ORDER BY date DESC LIMIT 1""",
                               (row['id']))
                previous_subscription = cursor.fetchall()

                # if this is the first time subscription charge
                if previous_subscription.__len__() == 0:
                    print("AAAAAAAAAAAAAAAAAAAA")
                    # no previous subscription exists, now add data in subscription table based on merchant subscription startdate from merchant table
                    if subscription_trail_period == 0:
                        print("NO TRAIL AND FIRST SUBSCRIPTION -> MONEY CHARGED")

                        if row['AutoWaivedStatus'] == 1 and lifetime_total_revenue['total'] < row['minimumLifetimeRevenue'] and downtime < row['DownTimeThreshold']:
                            print(
                                "total revenue is less than the minimumlife time revenue and downtime % less than the downtime threshold")
                            remarks = f"The minimum revenue and downtime threshold requirement is not met as the current revenue ( ${lifetime_total_revenue['total']} ) and downtime ( {downtime}%) falls below the minimum lifetime revenue threshold ( ${row['minimumLifetimeRevenue']} ) and downtime threshold ( {row['DownTimeThreshold']}% ) respectively."
                            insert_into_subscription_table(merchantId, subscription_start_date, subscription_amount, 4,
                                                           subscription_frequency, 0, remarks=remarks)

                        else:
                            print(
                                "total revenue is equal or greater than the minimumlife time revenue")
                            insert_into_subscription_table(merchantId, subscription_start_date, subscription_amount, 0,
                                                           subscription_frequency, 0)
                        if subscription_frequency == 4:
                            next_charge_date = next_charge_date + datetime.timedelta(weeks=1)  # Adds 1 week
                        else:
                            next_charge_date = next_charge_date + relativedelta(months=subscription_frequency)
                        update_logs_subscription(merchantId,
                                                 'Trail period is zero. subscription record created for first time with amount ' + str(
                                                     subscription_amount))

                    else:
                        print("IN TRAIL AND FIRST SUBSCRIPTION -> NO MONEY CHARGED")
                        insert_into_subscription_table(
                            merchantId, subscription_start_date, 0, 0, 1, 1)
                        next_charge_date = subscription_start_date + \
                            relativedelta(months=1)
                        update_logs_subscription(row['id'],
                                                 'Subscription record created for first time with zero amount')

                    # TODO: update merchants table -> nextChargeDate
                    cursor.execute("UPDATE merchants SET nextsubscriptionchargedate = %s WHERE id = %s",
                                   (next_charge_date, merchantId))
                    connection.commit()
                    # END TODO

                else:
                    print("BBBBBBBBBBBBBBBBBBBBBB")

                    # if today date is greater or equal to next charge date
                    if today_date >= next_charge_date:

                        # compare start_date + trail_period <= next_charge_date
                        cursor.execute(
                            """SELECT COUNT(*) as trails_count FROM subscriptions WHERE merchantid=%s AND istrail=1 AND date >= %s""",
                            (merchantId, subscription_start_date))
                        trails_count = cursor.fetchone()['trails_count']
                        print(trails_count)
                        # if subscription_start_date + relativedelta(months=subscription_trail_period, days=-1) >= next_charge_date:
                        if trails_count < subscription_trail_period:
                            # means we are still in trail period
                            print("IN TRAIL -> NO MONEY CHARGED")
                            insert_into_subscription_table(
                                merchantId, next_charge_date, 0, 0, 1, 1)
                            next_charge_date = next_charge_date + \
                                relativedelta(months=1)
                            update_logs_subscription(merchantId,
                                                     'Trail is not over, subscription record created with zero amount')
                        else:
                            print("NO TRAIL -> MONEY CHARGED")
                            if row['AutoWaivedStatus'] == 1 and lifetime_total_revenue['total'] < row['minimumLifetimeRevenue'] and downtime < row['DownTimeThreshold']:
                                print(
                                    "total revenue is less than the minimumlife time revenue")
                                remarks = f"The minimum revenue and downtime threshold requirement is not met as the current revenue ( ${lifetime_total_revenue['total']} ) and downtime ( {downtime}%) falls below the minimum lifetime revenue threshold ( ${row['minimumLifetimeRevenue']} ) and downtime threshold ( {row['DownTimeThreshold']}% ) respectively."
                                insert_into_subscription_table(merchantId, next_charge_date, subscription_amount,
                                                               4,
                                                               subscription_frequency, 0, remarks=remarks)

                            else:
                                print(
                                    "total revenue is equal or greater than the minimumlife time revenue")
                                insert_into_subscription_table(merchantId, next_charge_date, subscription_amount,
                                                               0,
                                                               subscription_frequency, 0)
                            if subscription_frequency == 4:
                                next_charge_date = next_charge_date + datetime.timedelta(weeks=1)  # Adds 1 week
                            else:
                                next_charge_date = next_charge_date + relativedelta(
                                    months=subscription_frequency)
                            update_logs_subscription(merchantId, 'subscription record created with amount: ' + str(
                                subscription_amount))

                        # TODO: update merchants table -> nextChargeDate
                        cursor.execute("UPDATE merchants SET nextsubscriptionchargedate = %s WHERE id = %s",
                                       (next_charge_date, merchantId))
                        connection.commit()
                        # END TODO

                    else:
                        # YOUR TIME IS NOT YET ARRIVED
                        print("YOUR TIME IS NOT YET ARRIVED")
            else:
                # today date is not greater than subscription start date.
                # NOTHING TO DO
                pass

        except Exception as e:
            print(f"Error: {row['merchantname']} -> ", str(e))

    return success()


def schedulerFunction():
    #date = request.args.get('date')
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')

    # Convert the string dates into datetime objects
    start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d')
    end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d')
    date_list = []

    # Loop to generate each date between start_date and end_date
    current_date = start_date
    while current_date <= end_date:
        date_list.append(current_date.strftime('%Y-%m-%d'))
        current_date += datetime.timedelta(days=1)

    # Print the list of dates
    print(date_list)

    for date in date_list:
        resumeMinutes, pauseMinutes, total = Merchants.scheduler_function(date)
        print(date)
        # print(resumeMinutes,pauseMinutes,total)
    # return {"resumeMinutes": resumeMinutes, "pauseMinutes": pauseMinutes, "total": total}
    return jsonify(date_list)


def getMerchantByID(merchantId):
    try:

        token = request.args.get('token')
        if token and request.method == "GET":
            userId = validateLoginToken(token)
            if (not validateMerchantUser(merchantId, userId)):
                return unauthorised("User Not authorised to access merchant information")

            merchant = Merchants.get_merchant_by_id_str(merchantId)
            return success(jsonify(merchant))
        else:
            return not_found(params=["token"])
    except Exception as e:
        return unhandled(f"Error: {e}")


def getMerchantByIDWithoutToken(merchantId):
    try:
        if request.method == "GET":
            merchant = Merchants.get_merchant_by_id_str(merchantId)
            return success(jsonify(merchant))

    except Exception as e:
        return unhandled(f"Error: {e}")


def getMerchantAccountDetails(merchantId):
    try:
        create_log_data(level='[INFO]', Message="In the beginning of function to get merchant account details",
                        functionName="getMerchantAccountDetails", merchantID=merchantId, request=request)
        token = request.args.get("token")
        if token and request.method == "GET":
            user = validateLoginToken(token, userFullDetail=1)
            if user:
              if (not validateMerchantUser(merchantId, user['id'])):
                  create_log_data(level='[ERROR]',
                                  Message=f"User {user['firstname']}{user['lastname']} is not authorized to get merchant account details",
                                  functionName="getMerchantAccountDetails", merchantID=merchantId, user=user, request=request)
                  return unauthorised("User Not authorised to access merchant information")
              merchant_account_details = Merchants.get_merchants_account_detail(merchantId)
              create_log_data(level='[INFO]',
                              Message=f"Successfully get merchant account fields data",
                              messagebody=merchant_account_details,
                              functionName="getMerchantAccountDetails", merchantID=merchantId, user=user, request=request)
              return success(jsonify(merchant_account_details))
            else:
              create_log_data(level='[ERROR]',
                              Message=f"User Access Denied ,  Invalid token.",
                              functionName="getMerchantAccountDetails", merchantID=merchantId, user=user,
                              request=request)
              return invalid('User Access Denied')
        else:
            create_log_data(level='[ERROR]',
                            Message=f"Token or merchant id is missing",
                            functionName="getMerchantAccountDetails", merchantID=merchantId, request=request)
            return not_found(body={"token": "required", "merchant": "required"})

    except Exception as e:
        create_log_data(level='[ERROR]',
                        Message=f"Unable to get merchant account detail",
                        messagebody=f'An error occured {str(e)}',
                        functionName="getMerchantAccountDetails", merchantID=merchantId, request=request)
        print("Error: ", str(e))
        return unhandled()


# PUT
def saveMerchantSettings(merchantId):
    try:

        ip_address = None
        if request:
            ip_address = request.environ.get(
                'HTTP_X_FORWARDED_FOR', request.remote_addr)
        if ip_address:
            ip_address = ip_address.split(',')[0].strip()

        create_log_data(level='[INFO]', Message=f"In the beginning of function to save merchant setting, ,IP address:{ip_address}",
                        functionName="saveMerchantSettings", request=request)
        _json = request.json
        jsonLen = len(_json['merchant'])
        token = _json.get('token')
        merchant = _json.get('merchant')
        if token and merchant and request.method == "PUT":
            user = validateLoginToken(token, userFullDetail=1)
            if user:
              if (not validateMerchantUser(merchantId, user['id'])):
                  create_log_data(level='[ERROR]',
                                  Message=f"User {user['firstname']}{user['lastname']} is not authorized to update merchant settings",
                                  functionName="saveMerchantSettings", request=request)
                  return unauthorised("User Not authorised to access merchant information")
              message, error = Merchants.put_merchant_Settings(merchantId, merchant=merchant, user=user, jsonLen = jsonLen)
              if error:
                  create_log_data(level='[ERROR]',
                                  Message=f"Unable to update merchant setting details",
                                  messagebody=f"An error occured {message}",
                                  functionName="saveMerchantSettings", request=request)
                  return invalid(message)
              merchant_details = Merchants.get_merchant_settings_details(merchantId,jsonLen)


              create_log_data(level='[INFO]',
                              Message=f"Successfully update settings detail of merchant {merchantId}",
                              functionName="saveMerchantSettings", request=request)
              return success(jsonify(merchant_details))
            else:
              create_log_data(level='[ERROR]',
                              Message=f"User Access Denied ,  Invalid token.",
                              functionName="saveMerchantSettings", merchantID=merchantId, user=user,
                              request=request)
              return invalid('User Access Denied')
        else:
            create_log_data(level='[ERROR]',
                            Message=f"Token or merchant id is missing ,IP address:{ip_address}",
                            functionName="saveMerchantSettings", request=request)
            return not_found(body={"token": "required", "merchant": "required"})
    except Exception as e:
        print("Error: ", str(e))
        create_log_data(level='[ERROR]',
                        Message=f"Unable to update merchant settings details ,IP address:{ip_address}",
                        messagebody=f'An error occured {str(e)}',
                        functionName="saveMerchantSettings", request=request)
        return unhandled()


def connectMerchantStripe(merchantId):
    try:
        create_log_data(level='[INFO]', Message="In the beginning of function to connect stripe account",
                        functionName="connectMerchantStripe", merchantID=merchantId, request=request)
        _json = request.json
        token = _json.get('token')
        if token and request.method == "PUT":
            user = validateLoginToken(token, userFullDetail=1)
            if (not validateMerchantUser(merchantId, user['id'])):
                create_log_data(level='[ERROR]',
                                Message=f"User {user['firstname']}{user['lastname']} is not authorized to connect merchant stripe",
                                functionName="connectMerchantStripe", merchantID=merchantId, user=user, request=request)
                return unauthorised("User Not authorised to access merchant information")
            merchant = Merchants.get_merchant_by_id_str(merchantId)
            if not merchant:
                create_log_data(level='[ERROR]',
                                Message=f"Merchant not found",
                                functionName="connectMerchantStripe", merchantID=merchantId, user=user, request=request)
                return unauthorised("Merchant not found")
            message, error = Merchants.connect_stripe_account(
                merchantId, merchant=merchant, user=user)
            if error:
                create_log_data(level='[ERROR]',
                                Message=f"Unable to create merchant stripe account",
                                messagebody=f"An error occured {message}",
                                functionName="connectMerchantStripe", request=request)
                return invalid(message)
            sns_msg = {
                "event": "stripe.connect",
                "body": {
                    "merchantId": merchantId,
                    "userId": user['id']
                }
            }
            logs_sns_resp = publish_sns_message(topic=config.sns_activity_logs, message=str(sns_msg),
                                                subject="stripe.connect")
            merchant_details = Merchants.get_merchants_account_detail(
                merchantId)
            create_log_data(level='[INFO]',
                            Message=f"Successfully create stripe account of merchant {merchantId}",
                            functionName="connectMerchantStripe", merchantID=merchantId, user=user, request=request)
            return success(jsonify(merchant_details))
        else:
            create_log_data(level='[ERROR]',
                            Message=f"Token or merchant id is missing",
                            functionName="connectMerchantStripe", merchantID=merchantId, request=request)
            return not_found(body={"token": "required", "merchant": "required"})
    except Exception as e:
        print("Error: ", str(e))
        create_log_data(level='[ERROR]',
                        Message=f"Unable to connect stripe account",
                        messagebody=f'An error occured {str(e)}',
                        functionName="connectMerchantStripe", merchantID=merchantId, request=request)
        return unhandled()


def updateMerchantAccount(merchantId):
    try:

        ip_address = None
        if request:
            ip_address = request.environ.get(
                'HTTP_X_FORWARDED_FOR', request.remote_addr)
        if ip_address:
            ip_address = ip_address.split(',')[0].strip()
        create_log_data(level='[INFO]', Message=f"In the beginning of function to update merchant account details , IP address:{ip_address}",
                        functionName="updateMerchantAccount", merchantID=merchantId, request=request)
        _json = request.json
        token = _json.get('token')
        merchant = _json.get('merchant')
        if token and merchant and request.method == "PUT":
            user = validateLoginToken(token, userFullDetail=1)
            if user:
              if (not validateMerchantUser(merchantId, user['id'])):
                  create_log_data(level='[ERROR]',
                                 Message=f"User {user['firstname']}{user['lastname']} is not authorized to update merchant account details",
                                 functionName="updateMerchantAccount",merchantID=merchantId, user=user,request=request)
                  return unauthorised("User Not authorised to access merchant information")
              message, error = Merchants.put_merchant_account(merchantId, merchant=merchant, user=user)
              if error:
                  create_log_data(level='[ERROR]',
                                  Message=f"Unable to update merchant account detail", messagebody=f"An error occured {error}",
                                  functionName="updateMerchantAccount",merchantID=merchantId, user=user, request=request)
                  return invalid(message)
              merchant_account_details = Merchants.get_merchants_account_detail(merchantId)
              create_log_data(level='[INFO]',
                            Message=f"Successfully update merchant account fields data", messagebody=merchant_account_details,
                            functionName="updateMerchantAccount", merchantID=merchantId, user=user, request=request)
              return success(jsonify(merchant_account_details))
            else:
              create_log_data(level='[ERROR]',
                              Message=f"User Access Denied ,  Invalid token.",
                              functionName="updateMerchantAccount", merchantID=merchantId, user=user,
                              request=request)
              return invalid('User Access Denied')

        else:
            create_log_data(level='[ERROR]',
                            Message=f"Token or merchant id is missing ,IP address:{ip_address}",
                            functionName="updateMerchantAccount", merchantID=merchantId, request=request)
            return not_found(body={"token": "required", "merchant": "required"})
    except Exception as e:
        create_log_data(level='[ERROR]',
                        Message=f"Unable to update merchant account detail ,IP address:{ip_address}",
                        messagebody=f'An error occured {str(e)}',
                        functionName="updateMerchantAccount", merchantID=merchantId, request=request)
        print("Error: ", str(e))
        return unhandled()


def updateMerchant(merchantId):
    try:
        
        ip_address = None
        if request:
            ip_address = request.environ.get('HTTP_X_FORWARDED_FOR', request.remote_addr)
        if ip_address:
            ip_address = ip_address.split(',')[0].strip()
        
        _json = request.json

        token = _json.get('token')
        merchant = _json.get('merchant')
        create_log_data(level='[INFO]', Message=f"In the beginning of function to update merchant details, IP address:{ip_address}, Token:{token}",
                        functionName="updateMerchant", merchantID=merchantId, request=request)
        if token and merchant and request.method == "PUT":
            user = validateLoginToken(token, userFullDetail=1)
            if (not validateMerchantUser(merchantId, user['id'])):
                create_log_data(level='[ERROR]',
                                Message=f"User {user['firstname']}{user['lastname']} is not authorized to update merchant details, IP address:{ip_address}, Token:{token}",
                                functionName="updateMerchant", merchantID=merchantId, user=user, request=request)
                return unauthorised("User Not authorised to access merchant information")

            message, error = Merchants.put_merchant(
                merchantId, merchant=merchant, user=user, userId=user['id'], ip_address=ip_address)
            if error:
                create_log_data(level='[ERROR]',
                                Message=f"Unable to update merchant detail, IP address:{ip_address}, Token:{token}",
                                messagebody=f'An error occured {str(message)}',
                                functionName="updateMerchant", user=user, merchantID=merchantId, request=request)
                return invalid(message)

            # Triggering SNS - merchant.update
            # print("Triggering sns - merchant.update ...")
            # sns_msg = {
            #     "event": "merchant.update",
            #     "body": {
            #         "merchantId": merchantId,
            #         "userId": userId,
            #         "eventDetails": "Updated merchant information"
            #     }
            # }
            # logs_sns_resp = publish_sns_message(topic=config.sns_activity_logs, message=str(sns_msg),
            #                                     subject="merchant.update")

            merchant_details = Merchants.get_merchant_by_id_str(merchantId)
            create_log_data(level='[INFO]',
                            Message=f"Successfully update merchant data, IP address:{ip_address}, Token:{token}",
                            messagebody=merchant_details,
                            functionName="updateMerchant", merchantID=merchantId, user=user, request=request)
            return success(jsonify(merchant_details))
        else:
            create_log_data(level='[ERROR]',
                            Message=f"Token or merchant id is missing, IP address:{ip_address}, Token:{token}",
                            functionName="updateMerchant", merchantID=merchantId, request=request)
            return not_found(body={"token": "required", "merchant": "required"})
    except Exception as e:
        print("Error: ", str(e))
        create_log_data(level='[ERROR]',
                        Message=f"Unable to update merchant detail, IP address:{ip_address}, Token:{token}",
                        messagebody=f'An error occured {str(e)}',
                        functionName="updateMerchant", merchantID=merchantId, request=request)
        return unhandled()


def storefront_slug_change(merchantId):
    try:
        ip_address = get_ip_address(request)
        _json = request.json
        token = _json.get('token')
        slug = _json.get('slug')
        create_log_data(level='[INFO]', Message=f"In the beginning of function to storefront_slug_change, IP address {ip_address}",
                        functionName="storefront_slug_change", merchantID=merchantId, request=request)
        if token and request.method == "PUT":
            user = validateLoginToken(token, userFullDetail=1)
            if (not validateMerchantUser(merchantId, user['id'])):
                create_log_data(level='[ERROR]',
                                Message=f"User {user['firstname']}{user['lastname']} is not authorized to update storefront_slug, IP address {ip_address}",
                                functionName="storefront_slug_change", merchantID=merchantId, user=user, request=request)
                return unauthorised("User Not authorised to access merchant information")

            message = Merchants.put_storefront_slug(slug, merchantId, user=user, userId=user['id'], ip_address=ip_address)
            
            if message:
                create_log_data(level='[INFO]',
                            Message=f"Successfully update storefront slug, IP address {ip_address}",
                            functionName="storefront_slug_change", merchantID=merchantId, user=user, request=request)
                return success()
            else: 
                create_log_data(level='[ERROR]',
                                Message=f"Unable to update merchant storefront slug , IP address {ip_address}",
                                messagebody=f'An error occured {str(message)}',
                                functionName="storefront_slug_change", user=user, merchantID=merchantId, request=request)
                return invalid()                
    except Exception as e:
        print("Error: ", str(e))
        create_log_data(level='[ERROR]',
                        Message=f"Unable to update merchant slug, IP address {ip_address}",
                        messagebody=f'An error occured {str(e)}',
                        functionName="storefront_slug_change", merchantID=merchantId, request=request)
        return unhandled()


def storefront_status_change(merchantId):
    try:
        ip_address = get_ip_address(request)
        _json = request.json
        token = _json.get('token')
        status = _json.get('storefrontStatus')
        status  = int(status)
        create_log_data(level='[INFO]',
                        Message=f"In the beginning of function to storefront_status_change, IP address {ip_address}",
                        functionName="storefront_status_change", merchantID=merchantId, request=request)
        if token and request.method == "PUT":
            user = validateLoginToken(token, userFullDetail=1)
            if (not validateMerchantUser(merchantId, user['id'])):
                create_log_data(level='[ERROR]',
                                Message=f"User {user['firstname']}{user['lastname']} is not authorized to update storefront_status_change, IP address {ip_address}",
                                functionName="storefront_status_change", merchantID=merchantId, user=user,
                                request=request)
                return unauthorised("User Not authorised to access merchant information")

            message = Merchants.put_storefront_status(status, merchantId, user=user, userId=user['id'],
                                                    ip_address=ip_address)

            if message:
                create_log_data(level='[INFO]',
                                Message=f"Successfully  storefront_status_change, IP address {ip_address}",
                                functionName="storefront_status_change", merchantID=merchantId, user=user,
                                request=request)
                return success()
            else:
                create_log_data(level='[ERROR]',
                                Message=f"Unable to update merchant storefront status , IP address {ip_address}",
                                messagebody=f'An error occured {str(message)}',
                                functionName="storefront_status_change", user=user, merchantID=merchantId,
                                request=request)
                return invalid()
    except Exception as e:
        print("Error: ", str(e))
        create_log_data(level='[ERROR]',
                        Message=f"Unable to update storefront status, IP address {ip_address}",
                        messagebody=f'An error occured {str(e)}',
                        functionName="storefront_status_change", merchantID=merchantId, request=request)
        return unhandled()



@validate_token_middleware
def updateMerchantBusinessInfo(merchantId):
    try:

        ip_address = None
        if request:
            ip_address = request.environ.get(
                'HTTP_X_FORWARDED_FOR', request.remote_addr)
        if ip_address:
            ip_address = ip_address.split(',')[0].strip()
        _json = request.json
        merchant = _json.get("merchant")
        create_log_data(level='[INFO]', Message=f"In the beginning of function to update merchant business details,,IP address:{ip_address}",
                        functionName="updateMerchantBusinessInfo", merchantID=merchantId, request=request)

        userId = g.userId
        user = Users.get_user_by_userid(userId)
        if not validateMerchantUser(merchantId, userId):
            create_log_data(level='[ERROR]',
                            Message=f"User {userId} is not authorized to update merchant business details,IP address:{ip_address}",
                            functionName="updateMerchantBusinessInfo", user=user, merchantID=merchantId, request=request)
            return unauthorised("User Not authorised to access merchant information")

        resp = Merchants.update_merchant_business_info(
            merchantId, merchant=merchant, userId=userId, user=user)
        if resp == 'Duplicate Entry':
            create_log_data(level='[ERROR]',
                            Message=f"The stripe id {merchant.get('stripeAccountId')} is connected to another merchant,IP address:{ip_address}",
                            functionName="updateMerchantBusinessInfo", user=user, merchantID=merchantId,
                            request=request)
            return invalid('This stripe connect id is assigned to another merchant.')
        if not resp:
            return unhandled()

        merchant_details = Merchants.get_merchant_business_info(merchantId)
        create_log_data(level='[INFO]',
                        Message=f"Successfuly updated merchant business details,IP address:{ip_address}",
                        functionName="updateMerchantBusinessInfo", user=user, merchantID=merchantId,
                        request=request)
        return success(jsonify(merchant_details))

    except Exception as e:
        print("Error: ", str(e))
        create_log_data(level='[ERROR]',
                        Message=f"Unable to update merchant business detail,IP address:{ip_address}",
                        messagebody=f'An error occured {str(e)}',
                        functionName="updateMerchantBusinessInfo", merchantID=merchantId, request=request)
        return unhandled()


@validate_token_middleware
def updateMerchantSubscription(merchantId):
    try:
        create_log_data(
            level="[INFO]",
            Message=f"In the start of function to update merchant subscription detail of merchant {merchantId}",
            functionName="updateMerchantSubscription",
        )
        ip_address = get_ip_address(request)
        _json = request.json
        subscriptionStatus = int(_json.get("subscriptionStatus"))
        subscriptionTrialPeriod = _json.get("subscriptionTrialPeriod") if is_float(
            _json.get("subscriptionTrialPeriod")) else 0
        subscriptionStartDate = _json.get("subscriptionStartDate") if _json.get(
            "subscriptionStartDate") else "9999-12-12"
        subscriptionFrequency = _json.get("subscriptionFrequency") if is_float(
            _json.get("subscriptionFrequency")) else 1
        userId = g.userId
        create_log_data(
            level="[INFO]",
            Message=f"Get Subscription Details such as subscription status {subscriptionStatus} subscription trial period {subscriptionTrialPeriod} subscription start date {subscriptionStartDate} and frequency in which subscription is charged {subscriptionFrequency}",
            functionName="updateMerchantSubscription",
        )

        if subscriptionStatus is None:
            create_log_data(
                level="[ERROR]",
                Message=f"Subscription status is not found",
                functionName="updateMerchantSubscription",
            )
            return not_found(body={"subscriptionStatus": "required"})

        subscriptionStartDate = datetime.datetime.strptime(
            subscriptionStartDate, "%Y-%m-%d").date()

        if not validateAdminUser(userId):
            create_log_data(
                level="[ERROR]",
                Message=f"User is not authorised to access this information",
                functionName="updateMerchantSubscription",
            )
            return unauthorised("User Not authorised to access this information")

        resp = Merchants.update_subscription_status(merchantId, subscriptionStatus, subscriptionTrialPeriod,
                                                    subscriptionFrequency, subscriptionStartDate, userId=userId)
        if not resp:
            create_log_data(
                level="[ERROR]",
                Message=f"Unable to update merchant subscription details",
                functionName="updateMerchantSubscription",
            )
            return unhandled()
        elif resp:
            create_log_data(
                level="[INFO]",
                Message=f"Successfully update merchant subscription details",
                functionName="updateMerchantSubscription",
            )

        # Triggering SNS - merchant.subscription_change
        print("Triggering sns - merchant.subscription_change ...")
        sns_msg = {
            "event": "merchant.subscription_change",
            "body": {
                "merchantId": merchantId,
                "userId": userId,
                "ipAddr": ip_address,
            }
        }
        logs_sns_resp = publish_sns_message(topic=config.sns_audit_logs, message=str(sns_msg),
                                            subject="merchant.subscription_change")

        merchant_details = Merchants.get_merchant_by_id_str(merchantId)
        return success(jsonify(merchant_details))
    except Exception as e:
        print("Error: ", str(e))
        create_log_data(level='[ERROR]',
                        Message=f"Unable to update merchant subscription details",
                        messagebody=f'An error occured {str(e)}',
                        functionName="updateMerchantSubscription", merchantID=merchantId, request=request)
        return unhandled()


@validate_token_middleware
def updateMerchantMarketplaceStatus(merchantId):
    try:

        ip_address = None
        if request:
            ip_address = request.environ.get(
                'HTTP_X_FORWARDED_FOR', request.remote_addr)
        if ip_address:
            ip_address = ip_address.split(',')[0].strip()
        create_log_data(level='[INFO]', Message=f"In the beginning of function to update merchant market place status, ,IP address:{ip_address}",
                        functionName="updateMerchantMarketplaceStatus", merchantID=merchantId, request=request)
        _json = request.json
        marketStatus = _json.get("marketStatus")
        pauseTime = _json.get("pauseTime") or 0
        caller = _json.get("caller") if _json.get("caller") else "android"
        userId = g.userId

        # pauseTime_duration = _json.get("pauseTime_duration") if _json.get("pauseTime_duration") else None
        pause_reason = _json.get("pause_reason") if _json.get(
            "pause_reason") else ""
        if marketStatus is None:
            create_log_data(level='[ERROR]',
                            Message=f"Market Status is missing in request ,IP address:{ip_address}",
                            functionName="updateMerchantMarketplaceStatus", merchantID=merchantId, request=request)
            return not_found(body={"marketStatus": "required"})

        if not validateMerchantUser(merchantId, userId):
            create_log_data(level='[INFO]',
                            Message=f"User Not authorised to access merchant information ,IP address:{ip_address}",
                            functionName="updateMerchantMarketplaceStatus", merchantID=merchantId, request=request)
            return unauthorised("User Not authorised to access merchant information")
        if int(pauseTime) < 1440:
            pause_reason = ''
            pauseTime = convert_minutes(int(pauseTime))
        else:
            pauseTime = 'Today'

        resp = Merchants.update_marketplace_status(merchantId, marketStatus, pause_reason=pause_reason,
                                                   pauseTime_duration=pauseTime, caller=caller, userId=userId)
        if not resp:
            create_log_data(level='[ERROR]',
                            Message=f"Unable to update merchant market status,IP address:{ip_address}",
                            functionName="updateMerchantMarketplaceStatus", merchantID=merchantId, request=request)
            return unhandled()
        if marketStatus:
            eventtype = "RESUMED"
        else:
            eventtype = "PAUSED"
        resp1 = Merchants.post_PauseResumeTime(
            userid=userId, merchantid=merchantId, eventtype=eventtype)

        market_status = "resumed" if marketStatus else "paused"
        merchant_details = Merchants.get_merchant_by_id_str(merchantId)
        print(f' market_status : {market_status} , pauseTime : {pauseTime} ')

        #creating auto resume merchant status
        if market_status =='paused' and pauseTime=='Today':
          try:
            print('In the start of creating auto resume merchant status scheduler')
            create_log_data(level='[INFO]',
                            Message=f"In the start of creating auto resume merchant status scheduler, ,IP address:{ip_address}",
                            functionName="updateMerchantMarketplaceStatus", merchantID=merchantId, request=request)


            is_scheduler_created, msg = Merchants.create_auto_resume_merchant_scheduler(merchantId,merchant_details['timezone'])
            if is_scheduler_created:
              create_log_data(level='[INFO]',
                              Message=f"Auto resume scheduler created successfully, ,IP address:{ip_address}"
                              ,messagebody=f" Scheduler detail : {msg}",
                              functionName="updateMerchantMarketplaceStatus", merchantID=merchantId, request=request)
              sns_msg = {
                "event": "merchant.auto_resume_merchant_scheduler",
                "body": {
                  "merchantId": merchantId,
                  "userId": userId,
                  "schedulerDetail": f"Scheduler Created Successfully. {msg}",
                  "ipAddr": ip_address
                }
              }
              logs_sns_resp = publish_sns_message(topic=config.sns_activity_logs, message=str(sns_msg),
                                                  subject="merchant.auto_resume_merchant_scheduler")

            else:
              create_log_data(level='[ERROR]',
                              Message=f"Unable to crate auto resume scheduler, ,IP address:{ip_address}"
                              , messagebody=f" Scheduler detail : {msg}",
                              functionName="updateMerchantMarketplaceStatus", merchantID=merchantId, request=request)
              sns_msg = {
                "event": "error_logs.entry",
                "body": {
                  "userId": userId,
                  "merchantId": merchantId,
                  "errorName": 'Error on creating auto resume merchant store scheduler',
                  "errorSource": 'dashboard',
                  "errorStatus": 400,
                  "errorDetails": msg
                }
              }
              error_logs_sns_resp = publish_sns_message(topic=config.sns_error_logs, message=str(sns_msg),
                                                        subject="error_logs.entry")


          except Exception as e:
            create_log_data(level='[ERROR]',
                            Message=f"Exception on creating auto resume merchant status scheduler ,IP address:{ip_address}",
                            messagebody=f"An error occured: {str(e)}",
                            functionName=f"updateMerchantMarketplaceStatus", merchantID=merchantId, request=request)
            sns_msg = {
              "event": "error_logs.entry",
              "body": {
                "userId": userId,
                "merchantId": merchantId,
                "errorName": 'Exception on creating auto resume merchant status scheduler',
                "errorSource": 'dashboard',
                "errorStatus": 400,
                "errorDetails": str(e)
              }
            }
            error_logs_sns_resp = publish_sns_message(topic=config.sns_error_logs, message=str(sns_msg),
                                                      subject="error_logs.entry")

        # Triggering SNS - merchant.status_change

        sns_msg = {
            "event": "merchant.status_change",
            "body": {
                "merchantId": merchantId,
                "userId": userId,
                "pauseTime": pauseTime,
                "marketStatus":marketStatus,
                "caller": caller,
                "ipAddr": ip_address
            }
        }
        logs_sns_resp = publish_sns_message(topic=config.sns_activity_logs, message=str(sns_msg),
                                            subject="merchant.status_change")
        merchant_sns_resp = publish_sns_message(topic=config.sns_merchant_notification, message=str(sns_msg),
                                                subject="merchant.status_change")

        create_log_data(level='[INFO]', Message=f"Successfully updated merchant market status ,IP address:{ip_address}",
                        messagebody=f"market status changed to {market_status}",
                        functionName="updateMerchantMarketplaceStatus", merchantID=merchantId, request=request)


        return success(jsonify(merchant_details))

    except Exception as e:
        create_log_data(level='[ERROR]',
                        Message=f"Unable to update merchant market status ,IP address:{ip_address}",
                        messagebody="An error occured: {str(e)}",
                        functionName=f"updateMerchantMarketplaceStatus", merchantID=merchantId, request=request)
        return unhandled(f"error: {e}")

@validate_token_middleware
def updateMerchantstreamPlatformStatus(merchantId):
    try:

        ip_address = None
        if request:
            ip_address = request.environ.get(
                'HTTP_X_FORWARDED_FOR', request.remote_addr)
        if ip_address:
            ip_address = ip_address.split(',')[0].strip()
        create_log_data(level='[INFO]', Message=f"In the beginning of function to update stream platfrom connectivity status, ,IP address:{ip_address}",
                        functionName="updateMerchantstreamPlatformStatus", merchantID=merchantId, request=request)
        _json = request.json
        Status = _json.get("Status")
        platform = _json.get("platform")
        userId = g.userId

        if Status is None or platform is None:
            create_log_data(level='[ERROR]',
                            Message=f"Status or platform is missing in request ,IP address:{ip_address}",
                            functionName="updateMerchantstreamPlatformStatus", merchantID=merchantId, request=request)
            return not_found(body={"Status or platform": "required"})

        if not validateMerchantUser(merchantId, userId):
            create_log_data(level='[INFO]',
                            Message=f"User Not authorised to access merchant information ,IP address:{ip_address}",
                            functionName="updateMerchantstreamPlatformStatus", merchantID=merchantId, request=request)
            return unauthorised("User Not authorised to access merchant information")
        is_virtual_merchant = VirtualMerchants.get_virtual_merchant(id=merchantId)
        is_main_merchant=True
        VmerchantId=None
        if is_virtual_merchant:
          create_log_data(level='[INFO]',
                          Message="Successfully get virtual merchant ", messagebody=is_virtual_merchant,
                          functionName="updateMerchantstreamPlatformStatus")
          is_main_merchant = False
          VmerchantId = is_virtual_merchant[0]['id']
        resp = Merchants.update_stream_platform_status(merchantId, Status, platform ,  userId=userId ,is_main_merchant=is_main_merchant , VmerchantId=VmerchantId )
        if not resp:
            create_log_data(level='[ERROR]',
                            Message=f"Unable to update stream merchant platform status,IP address:{ip_address}",
                            functionName="updateMerchantstreamPlatformStatus", merchantID=merchantId, request=request)
            return unhandled()
        # Triggering SNS - merchant.status_change
        statuschange= "Active" if Status==1 else "Inactive"
        sns_msg = {
            "event": "merchant.stream_platform_status_change",
            "body": {
                "merchantId": merchantId,
                "userId": userId,
                "eventDetails": f"{platform} status is changed to {statuschange}"
            }
        }
        logs_sns_resp = publish_sns_message(topic=config.sns_activity_logs, message=str(sns_msg),
                                            subject="merchant.stream_platform_status_change")
        merchant_details = Merchants.get_merchant_by_id_str(merchantId)
        return success(jsonify(merchant_details))

    except Exception as e:
        create_log_data(level='[ERROR]',
                        Message=f"Unable to update merchant market status ,IP address:{ip_address}",
                        messagebody="An error occured: {str(e)}",
                        functionName=f"updateMerchantMarketplaceStatus", merchantID=merchantId, request=request)
        return unhandled(f"error: {e}")
def convert_minutes(minutes):
    days = minutes // (24 * 60)  # Calculate the number of days
    remaining_minutes = minutes % (24 * 60)  # Calculate the remaining minutes

    hours = remaining_minutes // 60  # Calculate the number of hours
    minutes = remaining_minutes % 60  # Calculate the remaining minutes

    result = []

    if days > 0:
        if days == 1:
            result.append(f"{days} day")
        else:
            result.append(f"{days} days")

    if hours > 0:
        if hours == 1:
            result.append(f"{hours} hour")
        else:
            result.append(f"{hours} hours")

    if minutes > 0:
        if minutes == 1:
            result.append(f"{minutes} minute")
        else:
            result.append(f"{minutes} minutes")

    return ", ".join(result)


# POST


def createMerchant():
    try:
        _json = request.json
        token = _json.get('token')
        merchant = _json.get('merchant')
        if token and merchant and request.method == "POST":
            userId = validateLoginToken(token)

            user = Users.get_users(conditions=[f"id = '{userId}'"])
            if not user:
                return not_found("User not found")
            user = user[0]

            if user['role'] == 1 or user['role'] == 2:

                merchantId = Merchants.post_merchant(merchant, userId)
                if not merchantId:
                    return unhandled()
                # Triggering SNS - merchant.create
                # print("Triggering sns - merchant.create ...")
                # sns_msg = {
                #     "event": "merchant.create",
                #     "body": {
                #         "merchantId": merchantId,
                #         "userId": userId,
                #         "eventDetails": f"New Merchant with name <{merchant_details['merchantname']}> Created"
                #     }
                # }
                # logs_sns_resp = publish_sns_message(topic=config.sns_activity_logs, message=str(sns_msg),
                #                                     subject="merchant.create")

                merchant_details = Merchants.get_merchant_by_id_str(merchantId)
                return success(jsonify(merchant_details))
            else:
                return unauthorised("User is not authorized to create merchant")
        else:
            return not_found(body={"token": "required", "merchant": "required"})
    except Exception as e:
        print("Error: ", str(e))
        return unhandled()


def onBoardMerchant():
    try:
        _json = request.json
        merchant = _json.get('merchant')
        create_log_data(level='[INFO]', Message="Triggering merchant onboarding method.",
                        messagebody=_json,
                        functionName="onBoardMerchant", request=request)
        user_token = request.args.get('token')
        if user_token:
            user = validateLoginToken(user_token, userFullDetail=1)
            if not user:
                create_log_data(level='[ERROR]',
                                Message="The API token is invalid.",
                                messagebody="Unable to find the user on the basis of provided token.",
                                functionName="onBoardMerchant", statusCode="400 Bad Request")
                return invalid("Invalid Token")
        else:
            create_log_data(level='[ERROR]',
                            Message="The API token is not found.",
                            messagebody="Unable to get api token in request argument",
                            functionName="onBoardMerchant", statusCode="400 Bad Request")
            return invalid("Api token not found")
        error = False
        merchantId, error = Merchants.post_merchant(
            merchant, token_user=user, onBoard=True)

        if error:
            return invalid(merchantId)

        create_log_data(level='[INFO]',
                        Message=f"Successfully onboarded merchant {merchant['merchantName']} ",

                        functionName="onBoardMerchant", statusCode="200 OK Request", request=request)

        merchant_details = Merchants.get_merchant_by_id_str(merchantId)
        sns_msg = {
            "event": "merchant.create",
            "body": {
                "userId": user['id'],
                "merchantId": merchant_details['id'],
                "eventType": "activity",
                "eventName": "merchant.create",
                "eventDetails": f"Merchant {merchant_details['merchantName']} successfully onboarded"
            }
        }

        publish_sns_message(topic=config.sns_activity_logs, message=str(sns_msg),
                            subject="merchant.create")

        return success(jsonify(merchant_details))

    except Exception as e:
        print("Error: ", str(e))
        create_log_data(level='[ERROR]',
                        Message=f"Unable to do onboarding of merchant {request.json.get('merchant')['merchantName']}",
                        messagebody=str(e),
                        functionName="onBoardMerchant", request=request)
        return unhandled(errorMsg=e)


def onBoardMerchantUser():
    try:
        _json = request.json
        merchant = _json.get('merchant')
        userId = _json.get('userId')
        create_log_data(level='[INFO]', Message="Triggering merchant onboarding method.",
                        messagebody=_json,
                        functionName="onBoardMerchant", request=request)

        merchantId, error = Merchants.post_merchant_user(
            merchant, userId=userId, onBoard=True)

        if error:
            return invalid(merchantId)

        create_log_data(level='[INFO]',
                        Message=f"Successfully onboarded merchant {merchant['merchantName']} ",

                        functionName="onBoardMerchant", statusCode="200 OK Request", request=request)

        merchant_details = Merchants.get_merchant_by_id_str(merchantId)
        sns_msg = {
            "event": "merchant.create",
            "body": {
                "merchantId": merchant_details['id'],
                "eventType": "activity",
                "eventName": "merchant.create",
                "eventDetails": f"Merchant {merchant_details['merchantName']} successfully onboarded"
            }
        }

        publish_sns_message(topic=config.sns_activity_logs, message=str(sns_msg),
                            subject="merchant.create")

        return success(jsonify(merchant_details))

    except Exception as e:
        print("Error: ", str(e))
        create_log_data(level='[ERROR]',
                        Message=f"Unable to do onboarding of merchant {request.json.get('merchant')['merchantName']}",
                        messagebody=str(e),
                        functionName="onBoardMerchant", request=request)
        return unhandled(errorMsg=e)


@validate_token_middleware
def getStripeBalances(merchantId):
    try:

        merchant = Merchants.get_merchant_by_id_str(merchantId)
        if not merchant:
            return invalid(merchantId)

        if not merchant['stripeAccountId'] or not (len(merchant['stripeAccountId']) > 0):
            return unhandled(f"Error: merchant does not have any stripe account", 200,
                             {"connectedAccountMissing": True})

        response, is_error = get_balances(
            connected_account_id=merchant['stripeAccountId'])
        if is_error:
            if response == "financial account missing":
                return unhandled(f"Error: merchant does not have any stripe financial account", 200,
                                 {"financialAccountMissing": True})
            else:
                raise Exception(response)

        return success(jsonify(response))
    except Exception as e:
        return unhandled(f"Error: {e}")


@validate_token_middleware
def issueCard(merchantId):
    try:
        ip_addr = request.environ.get(
            'HTTP_X_FORWARDED_FOR', request.remote_addr)
        userId = g.userId
        if not validateAdminUser(userId):
            return unauthorised("user is not authorized")

        merchant = Merchants.get_merchant_by_id_str(merchantId)
        if not merchant:
            return invalid(merchantId)

        if not merchant['stripeAccountId'] or not (len(merchant['stripeAccountId']) > 0):
            return unhandled(f"Error: merchant does not have any stripe account", 200,
                             {"connectedAccountMissing": True})

        card_type = request.args.get('cardType')
        if not card_type:
            card_type = 2
        response, is_error = issue_card(connected_account_id=merchant['stripeAccountId'], merchant_details=merchant,
                                        card_type=card_type)

        if is_error:
            if response == "financial account missing":
                return unhandled(f"Error: merchant does not have any stripe financial account", 200,
                                 {"financialAccountMissing": True})
            else:
                raise Exception(response)

        sns_msg = {
            "event": "stripe.card_issued",
            "body": {
                "merchantId": merchant['id'],
                "userId": userId,
                "cardId": response['id'],
                "ipAddr": ip_addr
            }
        }
        logs_sns_resp = publish_sns_message(topic=config.sns_financial_logs, message=str(sns_msg),
                                            subject="stripe.card_issued")
        if logs_sns_resp:
            print("stripe.card_issued event sucessfull")

        return success(jsonify(response))
    except Exception as e:
        return unhandled(f"Error: {e}")


@validate_token_middleware
def getIssuedCards(merchantId):
    try:

        merchant = Merchants.get_merchant_by_id_str(merchantId)
        if not merchant:
            return invalid(merchantId)

        if not merchant['stripeAccountId'] or not (len(merchant['stripeAccountId']) > 0):
            return unhandled(f"Error: merchant does not have any stripe account", 200,
                             {"connectedAccountMissing": True})

        response, is_error = get_issued_cards(
            connected_account_id=merchant['stripeAccountId'])
        if is_error:
            if response == "financial account missing":
                return unhandled(f"Error: merchant does not have any stripe financial account", 200,
                                 {"financialAccountMissing": True})
            else:
                raise Exception(response)

        return success(jsonify(response))
    except Exception as e:
        return unhandled(f"Error: {e}")


@validate_token_middleware
def getFinancialAccountTransections(merchantId):
    try:

        merchant = Merchants.get_merchant_by_id_str(merchantId)
        if not merchant:
            return invalid(merchantId)

        if not merchant['stripeAccountId'] or not (len(merchant['stripeAccountId']) > 0):
            return unhandled(f"Error: merchant does not have any stripe account", 200,
                             {"connectedAccountMissing": True})

        response, is_error = get_financial_account_transections(
            connected_account_id=merchant['stripeAccountId'])
        # response, is_error = get_connected_account_transections(connected_account_id=merchant['stripeAccountId'])
        if is_error:
            if response == "financial account missing":
                return unhandled(f"Error: merchant does not have any stripe account", 2)
            else:
                raise Exception(response)

        return success(jsonify(response))
    except Exception as e:
        return unhandled(f"Error: {e}")


@validate_token_middleware
def getFinacialAccountDetails(merchantId):
    try:
        ip_addr = request.environ.get(
            'HTTP_X_FORWARDED_FOR', request.remote_addr)
        userId = g.userId
        if not validateAdminUser(userId):
            return unauthorised("user is not authorized")

        merchant = Merchants.get_merchant_by_id_str(merchantId)
        if not merchant:
            return invalid(merchantId)

        if not merchant['stripeAccountId'] or not (len(merchant['stripeAccountId']) > 0):
            return unhandled(f"Error: merchant does not have any stripe account", 200,
                             {"connectedAccountMissing": True})

        response, is_error = get_finacial_account_details(
            connected_account_id=merchant['stripeAccountId'])
        if is_error:
            if response == "financial account missing":
                return unhandled(f"Error: merchant does not have any stripe financial account", 200,
                                 {"financialAccountMissing": True})
            else:
                raise Exception(response)

        sns_msg = {
            "event": "stripe.financial_account_retrieved",
            "body": {
                "merchantId": merchant['id'],
                "userId": userId,
                "ipAddr": ip_addr
            }
        }

        print("triggering stripe.financial_account_retrieved sns")
        logs_sns_resp = publish_sns_message(topic=config.sns_financial_logs, message=str(sns_msg),
                                            subject="stripe.financial_account_retrieved")
        if logs_sns_resp:
            print("stripe.financial_account_retrieved event successful")
        else:
            print("stripe.financial_account_retrieved event unsuccessful")

        user_detail = Users.get_user_by_id(userId)
        response['has_treasury_edit_access'] = user_detail.get(
            "has_treasury_edit_access")
        return success(jsonify(response))
    except Exception as e:
        return unhandled(f"Error: {e}")


@validate_token_middleware
def getStripeConnectAccount(merchantId):
    try:
        userId = g.userId
        if not validateAdminUser(userId):
            return unauthorised("user is not authorized")

        merchant = Merchants.get_merchant_by_id(merchantId)
        if not merchant:
            return invalid(merchantId)

        if not merchant['stripeaccountid'] or not (len(merchant['stripeaccountid']) > 0):
            return unhandled(f"Error: merchant does not have any stripe account", 200,
                             {"connectedAccountMissing": True})

        response, is_error = get_connect_account_details(
            connected_account_id=merchant['stripeaccountid'])
        if is_error:
            if response == "connect account missing":
                return unhandled(f"Error: merchant does not have any stripe connect account", 200,
                                 {"connectAccountMissing": True})
            else:
                raise Exception(response)
        return success(jsonify({"account-detail": response['business_profile'], 'connected-account-id': merchant['stripeaccountid']}))
    except Exception as e:
        return unhandled(f"Error: {e}")


@validate_token_middleware
def activateTrasuary(merchantId):
    try:
        ip_addr = request.environ.get(
            'HTTP_X_FORWARDED_FOR', request.remote_addr)
        userId = g.userId
        if not validateAdminUser(userId):
            return unauthorised("user is not authorized")

        merchant = Merchants.get_merchant_by_id_str(merchantId)
        if not merchant:
            return invalid(merchantId)

        if not merchant['stripeAccountId'] or not (len(merchant['stripeAccountId']) > 0):
            return unhandled(f"Error: merchant does not have any stripe account", 200,
                             {"connectedAccountMissing": True})

        response, is_error = add_trassuary_features_to_connect_account(
            connected_account_id=merchant['stripeAccountId'])
        if is_error:
            raise Exception(response)

        response, is_error = update_connect_account_tos(
            connected_account_id=merchant['stripeAccountId'])
        if is_error:
            raise Exception(response)

        sns_msg = {
            "event": "stripe.treasury_features_added",
            "body": {
                "merchantId": merchant['id'],
                "userId": userId,
                "ipAddr": ip_addr
            }
        }
        logs_sns_resp = publish_sns_message(topic=config.sns_financial_logs, message=str(sns_msg),
                                            subject="stripe.treasury_features_added")
        if logs_sns_resp:
            print("stripe.treasury_features_added event sucessfull")

        return success(jsonify({'message': "Treasury features successfully updated"}))
    except Exception as e:
        return unhandled(f"Error: {e}")


@validate_token_middleware
def createFinancialAccount(merchantId):
    try:
        ip_addr = request.environ.get(
            'HTTP_X_FORWARDED_FOR', request.remote_addr)
        userId = g.userId
        if not validateAdminUser(userId):
            return unauthorised("user is not authorized")

        merchant = Merchants.get_merchant_by_id_str(merchantId)
        if not merchant:
            return invalid(merchantId)

        if not merchant['stripeAccountId'] or not (len(merchant['stripeAccountId']) > 0):
            return unhandled(f"Error: merchant does not have any stripe account", 200,
                             {"connectedAccountMissing": True})

        response, is_error = create_finacial_account(
            connected_account_id=merchant['stripeAccountId'])
        if is_error:
            raise Exception(response)

        sns_msg = {
            "event": "stripe.financial_account_created",
            "body": {
                "merchantId": merchant['id'],
                "userId": userId,
                "financialAccId": response['id'],
                "ipAddr": ip_addr
            }
        }
        logs_sns_resp = publish_sns_message(topic=config.sns_financial_logs, message=str(sns_msg),
                                            subject="stripe.financial_account_created")
        if logs_sns_resp:
            print("stripe.financial_account_created event sucessfull")

        return success(jsonify({'message': "Financial account successfully created"}))
    except Exception as e:
        return unhandled(f"Error: {e}")


@validate_token_middleware
def fundsTransferToMerchant(merchantId):
    try:
        ip_addr = request.environ.get(
            'HTTP_X_FORWARDED_FOR', request.remote_addr)
        userId = g.userId
        if not validateAdminUser(userId):
            return unauthorised("user is not authorized")

        merchant = Merchants.get_merchant_by_id_str(merchantId)
        if not merchant:
            return invalid(merchantId)

        if not merchant['stripeAccountId'] or not (len(merchant['stripeAccountId']) > 0):
            return unhandled(f"Error: merchant does not have any stripe account", 200,
                             {"connectedAccountMissing": True})

        amount = request.json.get('amount')
        if not amount:
            return invalid('amount is missing')

        response, is_error = funds_transfer_to_connected_account(connected_account_id=merchant['stripeAccountId'],
                                                                 amount=amount)
        if is_error:
            raise Exception(response)

        sns_msg = {
            "event": "stripe.funds_transfer_to_merchant",
            "body": {
                "merchantId": merchant['id'],
                "userId": userId,
                "payoutId": response['id'],
                "amount": amount,
                "ipAddr": ip_addr
            }
        }
        logs_sns_resp = publish_sns_message(topic=config.sns_financial_logs, message=str(sns_msg),
                                            subject="stripe.funds_transfer_to_merchant")

        if logs_sns_resp:
            print("stripe.funds_transfer_to_merchant event successfull")

        return success(jsonify({'message': "Funds Transferred successfully"}))
    except Exception as e:
        return unhandled(f"Error: {e}")


@validate_token_middleware
def addFinancialAccountDefaultExternalAccount(merchantId):
    try:
        ip_addr = request.environ.get(
            'HTTP_X_FORWARDED_FOR', request.remote_addr)
        userId = g.userId
        if not validateAdminUser(userId):
            return unauthorised("user is not authorized")

        merchant = Merchants.get_merchant_by_id_str(merchantId)
        if not merchant:
            return invalid(merchantId)

        if not merchant['stripeAccountId'] or not (len(merchant['stripeAccountId']) > 0):
            return unhandled(f"Error: merchant does not have any stripe account", 200,
                             {"connectedAccountMissing": True})

        response, is_error = add_financial_account_as_default_external_account(
            connected_account_id=merchant['stripeAccountId'])
        if is_error:
            raise Exception(response)

        sns_msg = {
            "event": "stripe.add_financial_account_as_default_external_account",
            "body": {
                "merchantId": merchant['id'],
                "userId": userId,
                "ipAddr": ip_addr
            }
        }
        logs_sns_resp = publish_sns_message(topic=config.sns_financial_logs, message=str(sns_msg),
                                            subject="stripe.add_financial_account_as_default_external_account")

        if logs_sns_resp:
            print(
                "stripe.add_financial_account_as_default_external_account event successfull")

        return success(jsonify({'message': "Financial Account created as External Bank Account successfully"}))
    except Exception as e:
        return unhandled(f"Error: {e}")


@validate_token_middleware
def fundsTransferToFinancialAccount(merchantId):
    try:
        ip_addr = request.environ.get(
            'HTTP_X_FORWARDED_FOR', request.remote_addr)
        userId = g.userId
        if not validateAdminUser(userId):
            return unauthorised("user is not authorized")

        merchant = Merchants.get_merchant_by_id_str(merchantId)
        if not merchant:
            return invalid(merchantId)

        if not merchant['stripeAccountId'] or not (len(merchant['stripeAccountId']) > 0):
            return unhandled(f"Error: merchant does not have any stripe account", 200,
                             {"connectedAccountMissing": True})

        amount = request.json.get('amount')
        if not amount:
            return invalid('amount is missing')

        response, is_error = funds_transfer_to_financial_account(connected_account_id=merchant['stripeAccountId'],
                                                                 amount=amount)
        if is_error:
            raise Exception(response)

        sns_msg = {
            "event": "stripe.funds_transfer_to_financial_account",
            "body": {
                "merchantId": merchant['id'],
                "userId": userId,
                "payoutId": response['id'],
                "amount": amount,
                "ipAddr": ip_addr
            }
        }
        logs_sns_resp = publish_sns_message(topic=config.sns_financial_logs, message=str(sns_msg),
                                            subject="stripe.funds_transfer_to_financial_account")

        if logs_sns_resp:
            print("stripe.funds_transfer_to_financial_account event successfull")

        return success(jsonify({'message': "Funds Transferred to Financial Account successfully"}))
    except Exception as e:
        return unhandled(f"Error: {e}")


@validate_token_middleware
def fundsTransferToExternalBankAccount(merchantId):
    try:
        ip_addr = request.environ.get(
            'HTTP_X_FORWARDED_FOR', request.remote_addr)
        userId = g.userId
        if not validateAdminUser(userId):
            return unauthorised("user is not authorized")

        merchant = Merchants.get_merchant_by_id_str(merchantId)
        if not merchant:
            return invalid(merchantId)

        if not merchant['stripeAccountId'] or not (len(merchant['stripeAccountId']) > 0):
            return unhandled(f"Error: merchant does not have any stripe account", 200,
                             {"connectedAccountMissing": True})

        _json = request.json
        if not _json:
            return invalid('payload is missing')

        amount = _json.get('amount')
        routing_number = _json.get('routingNumber')
        account_number = _json.get('accountNumber')
        name = _json.get('name')
        note = _json.get('note')
        otp = _json.get('otp')
        if not amount or not routing_number or not account_number or not name:
            return invalid('invalid payload')

        if merchant['trasuaryPhoneValid'] == 0:
            raise Exception(
                "Merchant Treasury Authentication phone number is unverified!")

        if otp != merchant['latestOtp']:
            return invalid(errorMsg="invalid otp", data={})

        response, is_error = funds_transfer_to_external_bank_account(connected_account_id=merchant['stripeAccountId'],
                                                                     name=name, routingNumber=routing_number,
                                                                     accountNumber=account_number, amount=amount,
                                                                     note=note)
        if is_error:
            raise Exception(response)

        sns_msg = {
            "event": "stripe.funds_transfer_to_external_bank_account",
            "body": {
                "merchantId": merchant['id'],
                "userId": userId,
                "payoutId": response['id'],
                "amount": amount,
                "accountHolder": name,
                "ipAddr": ip_addr
            }
        }
        logs_sns_resp = publish_sns_message(topic=config.sns_financial_logs, message=str(sns_msg),
                                            subject="stripe.funds_transfer_to_external_bank_account")

        if logs_sns_resp:
            print("stripe.funds_transfer_to_external_bank_account event successfull")

        return success(jsonify({'message': "Funds Transferred to External Bank Account successfully"}))
    except Exception as e:
        return unhandled(f"Error: {e}")


# @validate_token_middleware
# def updateConnectedAccountOwner(merchantId):
#     try:
#
#         owner_ssn = request.form['ownerssn']
#         merchant_in_db = Merchants.get_merchant_by_id_str(merchantId)
#         if not merchant_in_db:
#             return invalid(merchantId)
#
#         if not merchant_in_db['stripeAccountId'] or not (len(merchant_in_db['stripeAccountId']) > 0):
#             return unhandled(f"Error: merchant does not have any stripe account", 200,
#                              {"connectedAccountMissing": True})
#
#         files = request.files
#         if files:
#             front = files['identityFront']
#             back = files['identityBack']
#         else:
#             return invalid(files)
#
#         identity_refs = {}
#         response, is_error = upload_file(connected_account_id=merchant_in_db['stripeAccountId'], file=front,
#                                          purpose='identity_document')
#         if is_error:
#             raise Exception(response)
#         identity_refs['identity_front_file_id'] = response
#
#         response, is_error = upload_file(connected_account_id=merchant_in_db['stripeAccountId'], file=back,
#                                          purpose='identity_document')
#         if is_error:
#             raise Exception(response)
#         identity_refs['identity_back_file_id'] = response
#
#         if not (len(identity_refs) == 2):
#             raise Exception("identity files could not be uploaded")
#
#         response, is_error = update_owner_of_connect_account(connected_account_id=merchant_in_db['stripeAccountId'],
#                                                              merchant_details=merchant_in_db,
#                                                              owner_ssn=owner_ssn,
#                                                              identity_refs=identity_refs)
#         if is_error:
#             raise Exception(response)
#
#         return success(jsonify({'message': "Account Owner successfully updated"}))
#     except Exception as e:
#         return unhandled(f"Error: {e}")


@validate_token_middleware
def getRecipients(merchantId):
    try:
        ip_addr = request.environ.get(
            'HTTP_X_FORWARDED_FOR', request.remote_addr)
        userId = g.userId
        if not validateAdminUser(userId):
            return unauthorised("user is not authorized")

        merchant = Merchants.get_merchant_by_id_str(merchantId)
        if not merchant:
            return invalid(merchantId)

        if not merchant['stripeAccountId'] or not (len(merchant['stripeAccountId']) > 0):
            return unhandled(f"Error: merchant does not have any stripe account", 200,
                             {"connectedAccountMissing": True})

        response, is_error = get_external_accounts(
            connected_account_id=merchant['stripeAccountId'] )
        if is_error:
            raise Exception(response)

        sns_msg = {
            "event": "stripe.retrieve_recipients_for_connected_account",
            "body": {
                "merchantId": merchant['id'],
                "userId": userId,
                "ipAddr": ip_addr
            }
        }
        logs_sns_resp = publish_sns_message(topic=config.sns_financial_logs, message=str(sns_msg),
                                            subject="stripe.retrieve_recipients_for_connected_account")

        if logs_sns_resp:
            print("stripe.retrieve_recipients_for_connected_account event successfull")

        return success(jsonify(response))
    except Exception as e:
        return unhandled(f"Error: {e}")


@validate_token_middleware
def addRecipient(merchantId):
    try:
        ip_addr = request.environ.get(
            'HTTP_X_FORWARDED_FOR', request.remote_addr)
        userId = g.userId
        if not validateAdminUser(userId):
            return unauthorised("user is not authorized")

        merchant = Merchants.get_merchant_by_id_str(merchantId)
        if not merchant:
            return invalid(merchantId)

        if not merchant['stripeAccountId'] or not (len(merchant['stripeAccountId']) > 0):
            return unhandled(f"Error: merchant does not have any stripe account", 200,
                             {"connectedAccountMissing": True})

        _json = request.json
        if not _json:
            return invalid('payload is missing')
        account_holder_name = _json.get('accountHolderName')
        routing_number = _json.get('routingNumber')
        account_number = _json.get('accountNumber')
        default = _json.get('default')
        _otp = _json.get('otp')
        if not account_holder_name or not routing_number or not account_number or not _otp:
            return invalid('invalid payload')

        if merchant.get("latestOtp") == _otp:
            response, is_error = add_external_account(connected_account_id=merchant['stripeAccountId'],
                                                      accountHolderName=account_holder_name,
                                                      routingNumber=routing_number, accountNumber=account_number, default=default)
            if is_error:
                raise Exception(response)
        else:
            return unhandled(f"Error: otp did not matched", 200, {"otpUnmatched": True})

        sns_msg = {
            "event": "stripe.add_recipient_to_connected_account",
            "body": {
                "merchantId": merchant['id'],
                "userId": userId,
                "ipAddr": ip_addr,
                "accountHolderName": account_holder_name
            }
        }
        logs_sns_resp = publish_sns_message(topic=config.sns_financial_logs, message=str(sns_msg),
                                            subject="stripe.add_recipient_to_connected_account")

        if logs_sns_resp:
            print("stripe.add_recipient_to_connected_account event successfull")

        return success(jsonify({'message': "Recipient created successfully"}))
    except Exception as e:
        return unhandled(f"Error: {e}")


# @validate_token_middleware
# def updateRecipient(merchantId):
#     try:
#         ip_addr = request.environ.get('HTTP_X_FORWARDED_FOR', request.remote_addr)
#         userId = g.userId
#         if not validateAdminUser(userId):
#             return unauthorised("user is not authorized")
#
#         merchant = Merchants.get_merchant_by_id_str(merchantId)
#         if not merchant:
#             return invalid(merchantId)
#
#         if not merchant['stripeAccountId'] or not (len(merchant['stripeAccountId']) > 0):
#             return unhandled(f"Error: merchant does not have any stripe account", 200,
#                              {"connectedAccountMissing": True})
#
#         _json = request.json
#         if not _json:
#             return invalid('payload is missing')
#         account_holder_name = _json.get('accountHolderName')
#         external_account_id = _json.get('externalAccountId')
#         _otp = _json.get('otp')
#         if not account_holder_name or not external_account_id or not _otp:
#             return invalid('invalid payload')
#
#         if merchant.get("latestOtp") == _otp:
#             response, is_error,  = update_external_account(connected_account_id=merchant['stripeAccountId'],
#                                                       externalAccountId=external_account_id,
#                                                       accountHolderName=account_holder_name)
#             if is_error:
#                 raise Exception(response)
#         else:
#             return unhandled(f"Error: otp did not matched", 200, {"otpUnmatched": True})
#
#         sns_msg = {
#             "event": "stripe.update_recipient_for_connected_account",
#             "body": {
#                 "merchantId": merchant['id'],
#                 "userId": userId,
#                 "ipAddr": ip_addr,
#                 "accountHolderName": account_holder_name
#             }
#         }
#         logs_sns_resp = publish_sns_message(topic=config.sns_financial_logs, message=str(sns_msg),
#                                             subject="stripe.update_recipient_for_connected_account")
#
#         if logs_sns_resp: print("stripe.update_recipient_for_connected_account event successfull")
#
#         return success(jsonify({'message': "Recipient updated successfully"}))
#     except Exception as e:
#         return unhandled(f"Error: {e}")


@validate_token_middleware
def deleteRecipient(merchantId):
    try:
        ip_addr = request.environ.get(
            'HTTP_X_FORWARDED_FOR', request.remote_addr)
        userId = g.userId
        if not validateAdminUser(userId):
            return unauthorised("user is not authorized")

        merchant = Merchants.get_merchant_by_id_str(merchantId)
        if not merchant:
            return invalid(merchantId)

        if not merchant['stripeAccountId'] or not (len(merchant['stripeAccountId']) > 0):
            return unhandled(f"Error: merchant does not have any stripe account", 200,
                             {"connectedAccountMissing": True})

        _json = request.json
        if not _json:
            return invalid('payload is missing')

        external_account_id = _json.get('externalAccountId')
        _otp = _json.get('otp')
        if not external_account_id or not _otp:
            return invalid('invalid payload')

        if merchant.get("latestOtp") == _otp:
            response, is_error, account_holder_name = delete_external_account(connected_account_id=merchant['stripeAccountId'],
                                                                              externalAccountId=external_account_id)
            if is_error:
                raise Exception(response)
        else:
            return unhandled(f"Error: otp did not matched", 200, {"otpUnmatched": True})

        sns_msg = {
            "event": "stripe.remove_recipient_for_connected_account",
            "body": {
                "merchantId": merchant['id'],
                "userId": userId,
                "ipAddr": ip_addr,
                "accountHolderName": account_holder_name
            }
        }
        logs_sns_resp = publish_sns_message(topic=config.sns_financial_logs, message=str(sns_msg),
                                            subject="stripe.remove_recipient_for_connected_account")

        if logs_sns_resp:
            print("stripe.remove_recipient_for_connected_account event successfull")

        return success(jsonify({'message': "Recipient removed successfully"}))
    except Exception as e:
        return unhandled(f"Error: {e}")


@validate_token_middleware
def transferFundsToRecipient(merchantId):
    try:
        ip_addr = request.environ.get(
            'HTTP_X_FORWARDED_FOR', request.remote_addr)
        userId = g.userId
        if not validateAdminUser(userId):
            return unauthorised("user is not authorized")

        merchant = Merchants.get_merchant_by_id_str(merchantId)
        if not merchant:
            return invalid(merchantId)

        if not merchant['stripeAccountId'] or not (len(merchant['stripeAccountId']) > 0):
            return unhandled(f"Error: merchant does not have any stripe account", 200,
                             {"connectedAccountMissing": True})

        _json = request.json
        if not _json:
            return invalid('payload is missing')

        note = _json.get('note')
        amount = _json.get('amount')
        external_account_id = _json.get('externalAccountId')
        _otp = _json.get('otp')
        if not amount or not external_account_id or not _otp:
            return invalid('invalid payload')

        if merchant.get("latestOtp") == _otp:
            response, is_error, account_holder_name = funds_transfer_to_stored_external_bank_account(connected_account_id=merchant['stripeAccountId'],
                                                                                                     externalAccountId=external_account_id,
                                                                                                     amount=amount, note=note)
            if is_error:
                if response.__contains__('accountNumber'):
                    return unhandled(f"Error: this recipient is invalid so remove and add it again", 200, {"accountNumberMissing": True})
                raise Exception(response)
        else:
            return unhandled(f"Error: otp did not matched", 200, {"otpUnmatched": True})

        sns_msg = {
            "event": "stripe.transfer_funds_to_recipient_for_connected_account",
            "body": {
                "merchantId": merchant['id'],
                "userId": userId,
                "ipAddr": ip_addr,
                "accountHolderName": account_holder_name,
                "amount": amount
            }
        }
        logs_sns_resp = publish_sns_message(topic=config.sns_financial_logs, message=str(sns_msg),
                                            subject="stripe.transfer_funds_to_recipient_for_connected_account")

        if logs_sns_resp:
            print(
                "stripe.transfer_funds_to_recipient_for_connected_account event successfull")

        return success(jsonify({'message': "Funds to recipient transferred successfully"}))
    except Exception as e:
        return unhandled(f"Error: {e}")


@validate_token_middleware
def addTrasuaryAuthPhone(merchantId):
    try:
        ip_addr = request.environ.get(
            'HTTP_X_FORWARDED_FOR', request.remote_addr)
        userId = g.userId
        if not validateAdminUser(userId):
            return unauthorised("user is not authorized")

        merchant = Merchants.get_merchant_by_id_str(merchantId)
        if not merchant:
            return invalid(merchantId)

        _json = request.json
        if not _json:
            return invalid("payload is missing")

        _phone = _json.get("phone")
        if not _phone:
            return invalid("invalid payload", {"phone": "treasury authentication phone number missing"})

        response = Merchants.update_trasuary_auth_phone(merchantId, _phone)
        if response:
            response = Merchants.change_trasuary_auth_phone_validation_status(
                merchantId, 0)

        if response:
            sns_msg = {
                "event": "merchants.add_trasuary_auth_phone",
                "body": {
                    "merchantId": merchant['id'],
                    "userId": userId,
                    "phone": _phone,
                    "ipAddr": ip_addr
                }
            }
            logs_sns_resp = publish_sns_message(topic=config.sns_financial_logs, message=str(sns_msg),
                                                subject="merchants.add_trasuary_auth_phone")

            if logs_sns_resp:
                print("merchants.add_trasuary_auth_phone event successfull")

            return success(jsonify({'message': "Treasury authentication phone number added successfully"}))
        else:
            raise Exception(
                "database error occurred while adding Treasury authentication phone number")
    except Exception as e:
        return unhandled(f"Error: {e}")


@validate_token_middleware
def sendTrasuaryAuthOtp(merchantId):
    try:
        ip_addr = request.environ.get(
            'HTTP_X_FORWARDED_FOR', request.remote_addr)
        userId = g.userId
        if not validateAdminUser(userId):
            return unauthorised("user is not authorized")

        merchant = Merchants.get_merchant_by_id_str(merchantId)
        if not merchant:
            return invalid(merchantId)

        _json = request.json
        if _json:
            _to = _json.get("phone")
            if not _to or not (len(_to) > 0):
                return invalid("invalid payload", {"phone": "treasury authentication phone number missing"})
        else:
            _to = merchant['trasuaryAuthPhone']
            if not _to or not (len(_to) > 0):
                raise Exception(
                    "treasury authentication phone number missing for merchant")

        _from = config.twilio_sender_phone_number
        latest_otp = generate_otp(6)
        _message = f"Treasury Authentication OTP: {latest_otp}"

        resp_status, resp_data = Twilio.send_message(_from, _to, _message)

        if resp_status >= 200 and resp_status < 300:
            response = Merchants.update_trasuary_auth_otp(
                merchantId, latest_otp)
            if response:
                sns_msg = {
                    "event": "merchants.send_trasuary_auth_otp",
                    "body": {
                        "merchantId": merchant['id'],
                        "userId": userId,
                        "ipAddr": ip_addr
                    }
                }
                logs_sns_resp = publish_sns_message(topic=config.sns_financial_logs, message=str(sns_msg),
                                                    subject="merchants.send_trasuary_auth_otp")

                if logs_sns_resp:
                    print("merchants.send_trasuary_auth_otp event successfull")

                return success(jsonify({"message": "success", "status": 200, "data": resp_data}))
            else:
                raise Exception(
                    "database error occurred while updating Treasury authentication OTP")
        else:
            return invalid(errorMsg=resp_data.get("message"), data=resp_data)
    except Exception as e:
        return unhandled(f"Error: {e}")


@validate_token_middleware
def changeTrasuaryAuthPhone(merchantId):
    try:
        ip_addr = request.environ.get(
            'HTTP_X_FORWARDED_FOR', request.remote_addr)
        userId = g.userId
        if not validateAdminUser(userId):
            return unauthorised("user is not authorized")

        merchant = Merchants.get_merchant_by_id_str(merchantId)
        if not merchant:
            return invalid(merchantId)

        _json = request.json
        if _json:
            _newPhone = _json.get("newPhone")
            if not _newPhone or not (len(_newPhone) > 0):
                return invalid("invalid payload", {"newPhone": "new treasury authentication phone number missing"})
        else:
            return invalid("payload missing")

        _oldPhone = merchant['trasuaryAuthPhone']
        if not _oldPhone or not (len(_oldPhone) > 0):
            raise Exception(
                "old treasury authentication phone number missing for merchant")

        _from = config.twilio_sender_phone_number
        latest_otp1 = generate_otp(6)
        latest_otp2 = generate_otp(6)
        _message1 = f"Treasury Authentication OTP: {latest_otp1}"
        _message2 = f"Treasury Authentication OTP: {latest_otp2}"

        resp_status_old_phone, resp_data_old_phone = Twilio.send_message(
            _from, _oldPhone, _message1)
        resp_status_new_phone, resp_data_new_phone = Twilio.send_message(
            _from, _newPhone, _message2)

        if resp_status_old_phone >= 200 and resp_status_old_phone < 300:
            if resp_status_new_phone >= 200 and resp_status_new_phone < 300:
                response_old_phone_otp = Merchants.update_trasuary_auth_otp(
                    merchantId, latest_otp1)
                response_new_new_otp = Merchants.update_trasuary_auth_changed_phone_otp(
                    merchantId, latest_otp2)
                response_phone = Merchants.update_trasuary_auth_changed_phone(
                    merchantId, _newPhone)
                if response_old_phone_otp and response_new_new_otp and response_phone:
                    sns_msg = {
                        "event": "merchants.change_trasuary_auth_phone",
                        "body": {
                            "merchantId": merchant['id'],
                            "userId": userId,
                            "oldPhone": _oldPhone,
                            "newPhone": _newPhone,
                            "ipAddr": ip_addr
                        }
                    }
                    logs_sns_resp = publish_sns_message(topic=config.sns_financial_logs, message=str(sns_msg),
                                                        subject="merchants.change_trasuary_auth_phone")

                    if logs_sns_resp:
                        print(
                            "merchants.change_trasuary_auth_phone event successfull")

                    return success(jsonify({'message': "Treasury authentication phone number changed successfully"}))
                else:
                    raise Exception(
                        "database error occurred while changing Treasury authentication phone number")
            else:
                return invalid(errorMsg=resp_data_new_phone.get("message"), data=resp_data_new_phone)
        else:
            return invalid(errorMsg=resp_data_old_phone.get("message"), data=resp_data_old_phone)
    except Exception as e:
        return unhandled(f"Error: {e}")


@validate_token_middleware
def updateTrasuaryAuthPhone(merchantId):
    try:
        ip_addr = request.environ.get(
            'HTTP_X_FORWARDED_FOR', request.remote_addr)
        userId = g.userId
        if not validateAdminUser(userId):
            return unauthorised("user is not authorized")

        merchant = Merchants.get_merchant_by_id_str(merchantId)
        if not merchant:
            return invalid(merchantId)

        _json = request.json
        if not _json:
            return invalid("payload is missing")

        _otp_old = _json.get("otpOld")
        _otp_new = _json.get("otpNew")
        if not _otp_old or not _otp_new:
            return invalid("invalid payload", {"_otp_old": "treasury authentication current phone otp missing",
                                               "_otp_new": "treasury authentication changed phone otp missing"})

        if merchant['latestOtp'] == _otp_old and merchant['changedPhoneOtp'] == _otp_new:
            response_phone_change = Merchants.update_trasuary_auth_phone(
                merchantId, merchant['trasuaryAuthPhoneChanged'])
            response_valid_status = Merchants.change_trasuary_auth_phone_validation_status(
                merchantId, 1)
        else:
            return unhandled(f"Error: otp did not matched", 200, {"otpUnmatched": True})

        if response_phone_change and response_valid_status:
            sns_msg = {
                "event": "merchants.update_trasuary_auth_phone",
                "body": {
                    "merchantId": merchant['id'],
                    "userId": userId,
                    "oldPhone": merchant['trasuaryAuthPhone'],
                    "newPhone": merchant['trasuaryAuthPhoneChanged'],
                    "ipAddr": ip_addr
                }
            }
            logs_sns_resp = publish_sns_message(topic=config.sns_financial_logs, message=str(sns_msg),
                                                subject="merchants.update_trasuary_auth_phone")

            if logs_sns_resp:
                print("merchants.update_trasuary_auth_phone event successfull")

            return success(jsonify({'message': "Treasury authentication phone number updated successfully"}))
        else:
            raise Exception(
                "database error occurred while updating Treasury authentication phone number")
    except Exception as e:
        return unhandled(f"Error: {e}")


@validate_token_middleware
def validateTrasuaryAuthPhone(merchantId):
    try:
        userId = g.userId
        if not validateAdminUser(userId):
            return unauthorised("user is not authorized")

        merchant = Merchants.get_merchant_by_id_str(merchantId)
        if not merchant:
            return invalid(merchantId)

        _json = request.json
        if _json:
            _otp = _json.get("otp")
            if not _otp or not (len(_otp) > 0):
                return invalid("invalid payload", {"otp": "otp missing"})

        latest_otp = merchant['latestOtp']
        if not latest_otp or not (len(latest_otp) > 0):
            return success(
                jsonify({"message": "wrong request", "status": 200, "data": {"treasuryAuthOtpMissing": True}}))

        if _otp == latest_otp:
            response = Merchants.change_trasuary_auth_phone_validation_status(
                merchantId, 1)
            if response:
                return success(jsonify(
                    {"message": "Treasury authentication phone validated successfully", "status": 200, "data": {}}))
            else:
                raise Exception(
                    "database error occurred while updating Treasury authentication phone validation")
        else:
            return invalid(errorMsg="invalid otp", data={})
    except Exception as e:
        return unhandled(f"Error: {e}")


def onBoardMerchentFee():
    try:
        stripe.api_key = config.stripe_api_key
        stripe.api_version = "2020-08-27;link_beta=v1"
        token = stripe.PaymentIntent.create(
            amount=config.stripe_onboard_fee_total,
            currency='usd',
            payment_method_types=['card', 'link'],
        )

        return success(jsonify(token))

    except Exception as e:
        print("Error: ", str(e))
        return unhandled()


def paymentValidation(merchantId):
    try:
        _json = request.json
        chargeId = _json.get('chargeId')
        Merchants.update_merchant_payment(merchantId, chargeId)
        merchant_details = Merchants.get_merchant_by_id_str(merchantId)

        details = {}
        details['remarks'] = 'Merchant onboard reward'
        details['points'] = 200
        details['merchantId'] = merchantId
        details['remove'] = 0

        LoyaltyPoints.add_remove_points(details, '')

        return success(jsonify(merchant_details))

    except Exception as e:
        print("Error: ", str(e))
        return unhandled()


def reminderEmail(merchantId):
    try:
        Merchants.reminder_email(merchantId)
        return success(jsonify({
            "message": "Reminder email has been sent"
        }))

    except Exception as e:
        print("Error: ", str(e))
        return unhandled()


# Esper
@validate_token_middleware
def getAllEsperDevices():
    try:
        userId = g.userId

        state = request.args.get("state")
        if state is None or state == "" or int(state) not in (1, 20):
            state = ""

        limit = request.args.get("limit") or 500

        devices = Esper.get_all_devices_in_an_enterprise(
            limit=limit, state=state)

        return success(jsonify({
            "message": "success",
            "status": 200,
            "data": devices
        }))

    except Exception as e:
        return unhandled(f"Error: {e}")


@validate_token_middleware
def connectDisconnectEsperDevice(merchantId):
    try:
        userId = g.userId

        if not validateAdminUser(userId):
            return unauthorised("user is not authorized!")

        _json = request.json
        disconnect = int(_json.get("disconnect")) if _json.get(
            "disconnect") else 0
        esperDeviceId = _json.get("esperDeviceId")

        esperConnectivityStatus = Merchants.connect_disconnect_esper_device(
            merchantId, esperDeviceId, disconnect)
        if esperConnectivityStatus.status_code == 200:
            sns_msg = {
                "event": "esperdevice.connectivity_status",
                "body": {
                    "merchantId": merchantId,
                    "userId": userId,
                    "eventName": "esperdevice.connectivity_status",
                    "eventDetails": "Esper device disconnected" if disconnect == 1 else "Esper device connected"
                }
            }
            logs_sns_resp = publish_sns_message(topic=config.sns_activity_logs, message=str(sns_msg),
                                                subject="esperdevice.connectivity_status")
        return esperConnectivityStatus

    except Exception as e:
        return unhandled(f"Error: {e}")


@validate_token_middleware
def sendMessageWithTwilio(merchantId):
    try:
        userId = g.userId
        _json = request.json

        if not validateAdminUser(userId):
            return unauthorised("user is not authorized!")

        _from = config.twilio_sender_phone_number
        _to = _json.get("to")
        _message = _json.get("message")

        resp_status, resp_data = Twilio.send_message(_from, _to, _message)

        if resp_status >= 200 and resp_status < 300:
            return success(jsonify({
                "message": "success",
                "status": 200,
                "data": resp_data
            }))
        else:
            return invalid(errorMsg=resp_data.get("message"), data=resp_data)

    except Exception as e:
        return unhandled(f"Error: {e}")


@validate_token_middleware
def initBatteryStatusAlert(merchantId):
    try:
        _json = request.json
        # print(_json)
        # quit()

        batteryPercent = _json.get("batteryPercent")
        chargingStatus = _json.get("chargingStatus")

        from flask import has_request_context
        if has_request_context():
            # Assign the actual request object if there's a request context
            default_request = request
        log_level = config.log_level if config.log_level is not None and config.log_level != "" else 'logging.ERROR'
        logger.setLevel(eval(log_level))

        create_log_data(level='[DEBUG]', Message="Trigger the function that initiate the battery charging status alert", messagebody="",
                        functionName="initBatteryStatusAlert", statusCode="200 Ok", merchantID=merchantId, request=default_request)

        userId = g.userId

        # print(default_request)

        if batteryPercent is None:
            create_log_data(level='[ERROR]', Message="batteryPercent is required.", messagebody="batteryPercent is not mentioned in the request",
                            functionName="initBatteryStatusAlert", statusCode="404 Not Found", merchantID=merchantId, request=default_request)
            return not_found(body={"batteryPercent": "required"})
        if chargingStatus is None:
            create_log_data(level='[ERROR]', Message="chargingStatus is required.", messagebody="chargingStatus is not mentioned in the request",
                            functionName="initBatteryStatusAlert", statusCode="404 Not Found", merchantID=merchantId, request=default_request)
            return not_found(body={"chargingStatus": "required"})

        if not validateMerchantUser(merchantId, userId):
            create_log_data(level='[INFO]', Message="User Not authorised to access merchant information", messagebody="failed to validate the merchant user",
                            functionName="initBatteryStatusAlert", statusCode="403 Forbidden", merchantID=merchantId, request=default_request)
            return unauthorised("User Not authorised to access merchant information")

        smsText = None
        if chargingStatus == 'not-charging' and batteryPercent <= config.battery_alert_threshold:
            sns_msg = {
                "event": "merchant.battery_charge_required_alert",
                "body": {
                    "merchantId": merchantId,
                    "userId": userId,
                    "chargingStatus": chargingStatus,
                    "batteryPercent": batteryPercent
                }
            }
            create_log_data(level='[INFO]', Message="Battery not charging and below threshold", messagebody="",
                            functionName="initBatteryStatusAlert", merchantID=merchantId, request=default_request)

            logs_sns_resp = publish_sns_message(topic=config.sns_activity_logs, message=str(sns_msg),
                                                subject="merchant.battery_status_change")
            # merchant_sns_resp = publish_sns_message(topic=config.sns_merchant_notification, message=str(sns_msg),
            #                                         subject="merchant.battery_status_change")
        elif chargingStatus == 'charging' and batteryPercent <= config.battery_alert_threshold:
            sns_msg = {
                "event": "merchant.battery_charge_ok_alert",
                "body": {
                    "merchantId": merchantId,
                    "userId": userId,
                    "chargingStatus": chargingStatus,
                    "batteryPercent": batteryPercent
                }
            }
            create_log_data(level='[INFO]', Message="Battery charging and below threshold", messagebody="",
                            functionName="initBatteryStatusAlert", merchantID=merchantId, request=default_request)
            logs_sns_resp = publish_sns_message(topic=config.sns_activity_logs, message=str(sns_msg),
                                                subject="merchant.battery_status_change")
            # merchant_sns_resp = publish_sns_message(topic=config.sns_merchant_notification, message=str(sns_msg),
            #                                         subject="merchant.battery_status_change")
        else:
            create_log_data(level='[INFO]', Message="Battery status logging", messagebody="",
                            functionName="initBatteryStatusAlert", merchantID=merchantId, request=default_request)
            # pauseTime = 'Today'

        create_log_data(level='[INFO]', Message="Battery status api response", messagebody="",
                        functionName="initBatteryStatusAlert", statusCode="200 ok", merchantID=merchantId, request=default_request)
        return success(jsonify({
            "message": "success",
            "status": 200
        }))

    except Exception as e:
        create_log_data(level='[ERROR]', Message="Exception occured", messagebody=str(e), functionName="initBatteryStatusAlert",
                        statusCode="500 INTERNAL SERVER ERROR", merchantID=merchantId, request=default_request)
        return unhandled(f"error: {e}")


@validate_token_middleware
def offlineStatusAlert(merchantId):
    try:
        _json = request.json
        # print(_json)
        # quit()

        lastOfflineTs = _json.get("lastOfflineTs")
        currentOnlineTs = _json.get("currentOnlineTs")
        offlineDurationMessage = _json.get("offlineDurationMessage")

        from flask import has_request_context
        if has_request_context():
            # Assign the actual request object if there's a request context
            default_request = request
        log_level = config.log_level if config.log_level is not None and config.log_level != "" else 'logging.ERROR'
        logger.setLevel(eval(log_level))

        create_log_data(level='[DEBUG]', Message="Trigger the function that initiate the slack notification for Merchant tablet offline information",
                        messagebody="", functionName="offlineStatusAlert", statusCode="200 Ok",
                        merchantID=merchantId, request=default_request)

        userId = g.userId

        # print(default_request)

        if lastOfflineTs is None:
            create_log_data(level='[ERROR]', Message="lastOfflineTs is required.",
                            messagebody="lastOfflineTs is not mentioned in the request",
                            functionName="offlineStatusAlert", statusCode="404 Not Found", merchantID=merchantId,
                            request=default_request)
            return not_found(body={"lastOfflineTs": "required"})
        if currentOnlineTs is None:
            create_log_data(level='[ERROR]', Message="currentOnlineTs is required.",
                            messagebody="currentOnlineTs is not mentioned in the request",
                            functionName="offlineStatusAlert", statusCode="404 Not Found", merchantID=merchantId,
                            request=default_request)
            return not_found(body={"currentOnlineTs": "required"})
        if offlineDurationMessage is None:
            create_log_data(level='[ERROR]', Message="offlineDurationMessage is required.",
                            messagebody="offlineDurationMessage is not mentioned in the request",
                            functionName="offlineStatusAlert", statusCode="404 Not Found", merchantID=merchantId,
                            request=default_request)
            return not_found(body={"offlineDurationMessage": "required"})

        if not validateMerchantUser(merchantId, userId):
            create_log_data(level='[INFO]', Message="User Not authorised to access merchant information",
                            messagebody="failed to validate the merchant user", functionName="offlineStatusAlert",
                            statusCode="403 Forbidden", merchantID=merchantId, request=default_request)
            return unauthorised("User Not authorised to access merchant information")

        sns_msg = {
            "event": "merchant.tablet_offline_status",
            "body": {
                "merchantId": merchantId,
                "userId": userId,
                "offlineDurationMessage": offlineDurationMessage
            }
        }
        create_log_data(level='[INFO]', Message=offlineDurationMessage, messagebody="",
                        functionName="offlineStatusAlert", merchantID=merchantId, request=default_request)
        logs_sns_resp = publish_sns_message(topic=config.sns_activity_logs, message=str(sns_msg),
                                            subject="merchant.tablet_offline_status")

        create_log_data(level='[INFO]', Message="Offline status api response", messagebody="",
                        functionName="offlineStatusAlert", statusCode="200 ok", merchantID=merchantId,
                        request=default_request)
        return success(jsonify({
            "message": "success",
            "status": 200
        }))

    except Exception as e:
        create_log_data(level='[ERROR]', Message="Exception occured", messagebody=str(e),
                        functionName="offlineStatusAlert",
                        statusCode="500 INTERNAL SERVER ERROR", merchantID=merchantId, request=default_request)
        return unhandled(f"error: {e}")


@validate_token_middleware
def offlineStatusAlert(merchantId):
    try:
        _json = request.json
        # print(_json)
        # quit()

        lastOfflineTs = _json.get("lastOfflineTs")
        currentOnlineTs = _json.get("currentOnlineTs")
        offlineDurationMessage = _json.get("offlineDurationMessage")

        from flask import has_request_context
        if has_request_context():
            # Assign the actual request object if there's a request context
            default_request = request
        log_level = config.log_level if config.log_level is not None and config.log_level != "" else 'logging.ERROR'
        logger.setLevel(eval(log_level))

        create_log_data(level='[DEBUG]', Message="Trigger the function that initiate the slack notification for Merchant tablet offline information",
                        messagebody="", functionName="offlineStatusAlert", statusCode="200 Ok",
                        merchantID=merchantId, request=default_request)

        userId = g.userId

        # print(default_request)

        if lastOfflineTs is None:
            create_log_data(level='[ERROR]', Message="lastOfflineTs is required.",
                            messagebody="lastOfflineTs is not mentioned in the request",
                            functionName="offlineStatusAlert", statusCode="404 Not Found", merchantID=merchantId,
                            request=default_request)
            return not_found(body={"lastOfflineTs": "required"})
        if currentOnlineTs is None:
            create_log_data(level='[ERROR]', Message="currentOnlineTs is required.",
                            messagebody="currentOnlineTs is not mentioned in the request",
                            functionName="offlineStatusAlert", statusCode="404 Not Found", merchantID=merchantId,
                            request=default_request)
            return not_found(body={"currentOnlineTs": "required"})
        if offlineDurationMessage is None:
            create_log_data(level='[ERROR]', Message="offlineDurationMessage is required.",
                            messagebody="offlineDurationMessage is not mentioned in the request",
                            functionName="offlineStatusAlert", statusCode="404 Not Found", merchantID=merchantId,
                            request=default_request)
            return not_found(body={"offlineDurationMessage": "required"})

        if not validateMerchantUser(merchantId, userId):
            create_log_data(level='[ERROR]', Message="User Not authorised to access merchant information",
                            messagebody="failed to validate the merchant user", functionName="offlineStatusAlert",
                            statusCode="403 Forbidden", merchantID=merchantId, request=default_request)
            return unauthorised("User Not authorised to access merchant information")

        sns_msg = {
            "event": "merchant.tablet_offline_status",
            "body": {
                "merchantId": merchantId,
                "userId": userId,
                "offlineDurationMessage": offlineDurationMessage
            }
        }
        create_log_data(level='[INFO]', Message=offlineDurationMessage, messagebody="",
                        functionName="offlineStatusAlert", merchantID=merchantId, request=default_request)
        logs_sns_resp = publish_sns_message(topic=config.sns_activity_logs, message=str(sns_msg),
                                            subject="merchant.tablet_offline_status")

        create_log_data(level='[INFO]', Message="Offline status api response", messagebody="",
                        functionName="offlineStatusAlert", statusCode="200 ok", merchantID=merchantId,
                        request=default_request)
        return success(jsonify({
            "message": "success",
            "status": 200
        }))

    except Exception as e:
        create_log_data(level='[ERROR]', Message="Exception occured", messagebody=str(e),
                        functionName="offlineStatusAlert",
                        statusCode="500 INTERNAL SERVER ERROR", merchantID=merchantId, request=default_request)
        return unhandled(f"error: {e}")


@validate_token_middleware
def manageLoyaltyPoints():
    try:
        ip_address = None
        if request:
            ip_address = request.environ.get(
                'HTTP_X_FORWARDED_FOR', request.remote_addr)
        if ip_address:
            ip_address = ip_address.split(',')[0].strip()

        userId = g.userId
        data = request.json
        data['ip_address'] = ip_address
        message = "Loyalty points added successfully"
        create_log_data(level='[INFO]', Message=f"In the start of managing loyalty points function, ,IP address:{ip_address}",
                        functionName="manageLoyaltyPoints", request=request)
        if data['remove'] == 1:
            message = "Loyalty points redeemed successfully"
        points = LoyaltyPoints.add_remove_points(data, userId)

        if points:
            return success(jsonify({
                "message": "success",
                "status": 200,
                "data": message
            }))
        create_log_data(level='[ERROR]',
                        Message=f"Not enough balance points to remove, ,IP address:{ip_address}",
                        functionName="manageLoyaltyPoints", request=request)
        return invalid(errorMsg="You cannot redeemed more than balance points!")

    except Exception as e:
        create_log_data(level='[ERROR]',
                        Message=f"An error occured {str(e)},IP address:{ip_address}",
                        functionName="manageLoyaltyPoints",  request=request)
        return unhandled(f"Error: {e}")


def getAllLoyaltyPoints(merchantId):
    try:
        points = LoyaltyPoints.get_all_points(merchantId)
        currentpoints = LoyaltyPoints.get_total_points(merchantId)

        return success(jsonify({
            "message": "success",
            "status": 200,
            "data": {
                "currentPointa": str(currentpoints['point']),
                "details": points

            }
        }))

    except Exception as e:
        return unhandled(f"Error: {e}")

@validate_token_middleware
def deleteLoyaltyPoints(merchantId, pointId):
    try:
        userId = g.userId
        ip_address = get_ip_address(request)
        LoyaltyPoints.delete_points(merchantId, pointId)
        message = "Loyalty points with this id does not exists"
        create_log_data(level='[INFO]', Message=f"In the start of deleteLoyaltyPoints,IP address: {ip_address}",
                    functionName="deleteLoyaltyPoints", request=request)
        if LoyaltyPoints:
            message = "Loyalty points deleted successfully"
            create_log_data(level='[INFO]', Message=f"Successfully deleteLoyaltyPoints,IP address: {ip_address}",
                    functionName="deleteLoyaltyPoints", request=request)
            
            print("Triggering sns - loyalty.points ...")
            
            sns_msg = {
                "event": "loyalty.points",
                "body": {
                    "merchantId": merchantId,
                    "status": "delete",
                    "ipAddr":ip_address,
                    "userId": userId
                    }
            }
            
            logs_sns_resp = publish_sns_message(topic=config.sns_activity_logs, message=str(sns_msg),
                                                subject="loyalty.points")
        return success(jsonify({
            "message": "success",
            "status": 200,
            "data": message
        }))

    except Exception as e:
        create_log_data(level='[INFO]', Message=f"Error: {e},IP address: {ip_address}",
                    functionName="deleteLoyaltyPoints", request=request)
        return unhandled(f"Error: {e}")


def updateLoyaltyPoints():
    try:
        data = request.json
        ip_address = get_ip_address(request)
        create_log_data(level='[INFO]', Message=f"In the start of deleteLoyaltyPoints,IP address: {ip_address}",
                            functionName="updateLoyaltyPoints", request=request)
        LoyaltyPoints.update_points(data)
        create_log_data(level='[INFO]', Message=f"Successfully updateLoyaltyPoints,IP address: {ip_address}",
                    functionName="updateLoyaltyPoints", request=request)
        return success(jsonify({
            "message": "success",
            "status": 200,
            "data": "Loyalty points updated successfully"
        }))

    except Exception as e:
        create_log_data(level='[INFO]', Message=f"Error {e},IP address: {ip_address}",
                    functionName="updateLoyaltyPoints", request=request)
        return unhandled(f"Error: {e}")


def transferHistory(merchantId):
    try:
        data = request.json
        Payouts.downloadTransferHistory(merchantId, data)

        return success(jsonify({
            "message": "success",
            "status": 200,
            "data": "You will receive the report on your email shortly!"
        }))

    except Exception as e:
        return unhandled(f"Error: {e}")


def draftPayout(merchantId):
    try:
        data = request.json
        Payouts.post_draft(merchantId, data)

        return success(jsonify({
            "message": "success",
            "status": 200,
        }))

    except Exception as e:
        return unhandled(f"Error: {e}")


def storefrontLogo(merchantId):
    try:
        logo = None

        if request.files["logo"].filename != '':
            logo = request.files["logo"]

        Merchants.storefront_logo_update(merchantId, logo)

        return success(jsonify({
            "message": "success",
            "status": 200,
            "data": "Logo updated successfully"
        }))

    except Exception as e:
        return unhandled(f"Error: {e}")


def storefrontBanner(merchantId):
    try:

        url = Merchants.storefront_banner_update(
            merchantId, request.form["banner"], request.form['uploaded_url'])

        return success(jsonify({
            "message": "success",
            "status": 200,
            "data": url
        }))

    except Exception as e:
        return unhandled(f"Error: {e}")


def removeMedia():
    try:
        s3_apptopus_bucket = config.s3_apptopus_bucket
        images_folder = config.s3_images_folder
        client = boto3.client("s3")

        url = request.args.get("url")
        print("Deleting old image...")
        oldImageName = url.split("/")[-1]
        client.delete_object(Bucket=s3_apptopus_bucket,
                             Key=f"{images_folder}/{oldImageName}")
        print("Old image delete from s3")

        return success(jsonify({
            "message": "success",
            "status": 200,
            "data": "Media deleted successfully"
        }))

    except Exception as e:
        return unhandled(f"Error: {e}")


def onBoardNewMerchent():
    try:
        _json = request.json
        merchant = _json.get('merchant')
        error = False
        merchantId, error = Merchants.onboardnew_merchant(
            merchant, onBoard=True)

        if error:
            return invalid(merchantId)

        merchant_details = Merchants.get_merchant_by_id_str(merchantId)
        return success(jsonify(merchant_details))

    except Exception as e:
        print("Error: ", str(e))
        return unhandled()


def merchantonboard_media():
    try:

        media = request.files["media"]
        mediaUrl = Merchants.upload_media(media, None)

        return success(jsonify({
            "message": "success",
            "status": 200,
            "data": mediaUrl
        }))

    except Exception as e:
        return unhandled(f"Error: {e}")
