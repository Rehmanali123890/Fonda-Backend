import time
import datetime
import config
from models.Stream import Stream
import logging

env = config.env
from app import app
import json
from flask import jsonify, g
import boto3
from botocore.exceptions import ClientError
import requests

from controllers.AnalyticsController import email_merchant_summary_report
from models.Payouts import Payouts
# local imports
from models.Sync import Sync
from models.flipdish.Flipdish import Flipdish
from models.Platforms import Platforms
from models.Websockets import Websockets
from models.Merchants import Merchants
from models.Items import Items
from models.ubereats.UberEats import UberEats
from models.Orders import Orders
import config
from utilities.errors import invalid, unhandled, unauthorised
from utilities.helpers import success, validateMerchantUser, is_float, send_android_notification_api, \
  publish_sns_message,openDbconnection,closeDbconnection
  
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def trigger_manual_sync(event, context):
  with app.app_context():
    print("---------------------- manual sync triggered -------------------------")
    print(event)
    for record in event['Records']:
      print('Message body: ' + record["body"])
      platformObj = json.loads(record["body"])
      merchantId = platformObj['merchantId']
      platformId = platformObj['platformId']
      userId = platformObj['userId']
      downloadMenu = platformObj['downloadMenu']

      # check whether download or upload the menu
      if downloadMenu is not None and int(downloadMenu) == 1:
        resp = Sync.trigger_downloadMenu(merchantId, platformId, userId)
      else:
        resp = Sync.trigger_manualSync(merchantId, platformId, userId)
      print("Done")


def process_flipdish_item(event, context):
  with app.app_context():
    print("--------------------- process flipdish item --------------------------")
    print(event)
    for record in event['Records']:
      print('Message body: ' + record["body"])
      dataObj = json.loads(record["body"])
      platformId = dataObj['platformId']
      menuId = dataObj["menuId"]
      categoryId = dataObj["categoryId"]
      itemId = dataObj["itemId"]
      itemSortId = dataObj["itemSortId"]
      fMenuId = dataObj["fMenuId"]
      fCategoryId = dataObj["fCategoryId"]
      fTaxRateId = dataObj["fTaxRateId"]
      mainMerchantId = dataObj["mainMerchantId"]
      syncMerchantId = dataObj["syncMerchantId"]

      resp = Flipdish.flipdish_post_complete_product(
        platformId=platformId,
        menuId=menuId,
        categoryId=categoryId,
        itemId=itemId,
        itemSortId=itemSortId,
        fMenuId=fMenuId,
        fCategoryId=fCategoryId,
        fTaxRateId=fTaxRateId,
        mainMerchantId=mainMerchantId,
        syncMerchantId=syncMerchantId
      )

      print("Done")


def order_notification_subscriber(event, context):
  with app.app_context():
    print("--------------------- process order notification --------------------------")
    print(event)
    for record in event['Records']:

      # init websocket client
      websocketUrl = config.orders_websocket_url
      ws_client = boto3.client("apigatewaymanagementapi", endpoint_url=websocketUrl)

      subject = record.get("Sns").get("Subject")
      message = eval(record.get("Sns").get("Message"))
      print(message)

      """
        TODO: Send Notification to merchant heroku app webhook """

      #r = order_notification_to_heroku_webhook(subject, message)

      """ END """

      if subject == "order.create" or subject == "order.status":

        """
          Send notification to android
        """
        android_resp = send_order_notification_to_android(subject, message)
        """
          End -> send notification to android
        """

        # get merchantId
        merchantId = message.get("body").get("order").get("orderMerchant").get("id")

        # get all the socket connections ids from database
        connections = Websockets.get_connection_by_mid_and_eventname(merchantId=merchantId, eventName="order")

        if type(connections) is list:
          # loop over the websockets connections
          print(connections)
          for connection in connections:
            connectionId = connection.get("connectionId")
            try:
              response = ws_client.post_to_connection(ConnectionId=connectionId, Data=json.dumps(message))
              print("Posted to connection")
            except Exception as e:
              print("Unable to calling the post to connection operation. Deleting the connectionId from db")

              # delete connection key from mysql db, from websocket table
              deleted = Websockets.delete_websocketById(id=connectionId)
              if deleted:
                print("deleted the connectionId from db")

      if subject == "order.status":
        print("--------------- ORDER.STATUS SNS CALLED ------------------")

        # 7-completed, 8-rejected, 9-cancelled

        # get order by id
        orderId = message.get('body').get('order').get('id')
        order_details = Orders.get_order(orderId=orderId)

        if order_details['vmerchantid']:
          merchantId = order_details['vmerchantid']

        orderStatus = message.get("body").get("order").get("orderStatus")
        orderSource = message.get("body").get("order").get("orderSource")
        orderExternalRef = message.get("body").get("order").get("orderExternalReference")
        orderProcessingId = message.get("body").get("order").get("orderProcessingId")
        orderTotal = message.get("body").get("order").get("orderTotal")

        metadata = json.loads(order_details['metadata']) if order_details['metadata'] else None
        cancelReason = None
        cancelExplaination = None
        if metadata is not None:
          cancelReason = metadata['cancellation']['reason']
          cancelExplaination = metadata['cancellation']['explanation']

        if orderStatus == 0 and orderProcessingId is not None and orderProcessingId != "":
          # order status is 0 (pending)
          if orderSource.lower() == "ubereats":
            # accept order on ubereats also
            platform_details = Platforms.get_platform_by_merchantid_and_platformtype(merchantId, 3)
            if platform_details:
              access_token = UberEats.ubereats_check_and_get_access_token()
              if access_token:
                accept_resp , error_message = UberEats.ubereats_accept_order(
                  message.get("body").get("order").get("orderProcessingId"),
                  access_token,
                  merchantId= message.get("body").get("order").get("orderMerchant").get("id")
                )
                if accept_resp:
                  print("order has been accepted")
                else:
                  print("error occured while accepting order!" , error_message)
              else:
                print("error while getting ubereats access_token")

        elif orderStatus == 7 and orderProcessingId is not None and orderProcessingId != "":
          # order status is 7 (completed)

          # check if order source is flipdish
          if orderSource.lower() == "flipdish":
            platform_details = Platforms.get_platform_by_merchantid_and_platformtype(merchantId, 2)
            if platform_details:
              access_token = platform_details['accesstoken']
              url = config.flipdish_base_url + "/orders/" + orderProcessingId + "/dispatch"
              headers = {
                "Accept": "application/json",
                "Authorization": "Bearer " + access_token
              }
              response = requests.request("POST", url, headers=headers)
              print(response.text)

        elif orderStatus == 8 and orderProcessingId is not None and orderProcessingId != "":
          # order status is 8 (rejected)
          if orderSource.lower() == "ubereats":
            # reject order on ubereats also
            platform_details = Platforms.get_platform_by_merchantid_and_platformtype(merchantId, 3)
            if platform_details:
              access_token = UberEats.ubereats_check_and_get_access_token()
              if access_token:
                resp = UberEats.ubereats_deny_order(
                  message.get("body").get("order").get("orderProcessingId"),
                  access_token,
                  reasonCode=cancelReason,
                  explanation=cancelExplaination
                )
                if resp:
                  print("order has been rejected on ubereats")
                else:
                  print("error while rejecting order on ubereats")
              else:
                print("error while getting ubereats access_token")


        elif orderStatus == 9 and orderProcessingId is not None and orderProcessingId != "":
          # order status is 9 (cancelled)

          if orderSource.lower() == "flipdish":
            platform_details = Platforms.get_platform_by_merchantid_and_platformtype(merchantId, 2)
            if platform_details:
              access_token = platform_details['accesstoken']

              # get flipdish order details and flipdish order_total
              f_order_details = Flipdish.flipdish_get_order_by_id(access_token, orderProcessingId)

              fOrderTotal = f_order_details.get("Data").get("Amount")

              url = config.flipdish_base_url + "/orders/" + orderProcessingId + "/refund"
              payload = {
                "RefundReason": "Store is Busy",
                "RefundAmount": float(fOrderTotal),
                "NotifyCustomer": True
              }
              headers = {
                "Accept": "application/json",
                "Content-Type": "text/json",
                "Authorization": "Bearer " + access_token
              }
              response = requests.request("POST", url, json=payload, headers=headers)
              print(response.text)
          elif orderSource.lower() == "ubereats":
            # cancel order on ubereats also
            platform_details = Platforms.get_platform_by_merchantid_and_platformtype(merchantId, 3)
            if platform_details:
              access_token = UberEats.ubereats_check_and_get_access_token()
              if access_token:
                resp = UberEats.ubereats_cancel_order(
                  message.get("body").get("order").get("orderProcessingId"),
                  access_token,
                  reason=cancelReason
                )
                if resp:
                  print("order has been cancelled on ubereats")
                else:
                  print("error while cancelling order on ubereats")
              else:
                print("error while getting ubereats access_token")

        if orderStatus in  (7,9) and orderSource in ('ubereats' , 'doordash' ,'grubhub'):
          print(f" Update the stream status to orderStatus:  {orderStatus} , Merchantid : {merchantId} , orderExternalRef: {orderExternalRef}")
          merchant_details = Stream.get_merchant_by_id_for_stream(merchantId)
          if merchant_details:
            platform = Platforms.get_platform_by_merchantid_and_platformtype(merchantId, platformtype=8)
            if platform:
              stream,stream_status_code,msg=Stream.update_stream_order_status(orderId,merchantId ,orderStatus )
              if not stream:
                print(f"Error occurred while updating the order status on Stream.: Merchantid : {merchantId} , orderExternalRef: {orderExternalRef}")
                sns_msg = {
                  "event": "error_logs.entry",
                  "body": {
                    "userId": None,
                    "merchantId": merchantId,
                    "errorName": "Error occurred while updating the order status on Stream.",
                    "errorSource": "dashboard",
                    "errorStatus": stream_status_code,
                    "errorDetails": msg,
                    "orderExternalReference":orderExternalRef
                  }
                }
                error_logs_sns_resp = publish_sns_message(topic=config.sns_error_logs, message=str(sns_msg),
                                                          subject="error_logs.entry")
            else:
              print(f"Platform connection is not found:: Merchantid : {merchantId} , orderExternalRef: {orderExternalRef}")
          else:
            print(f"Stream is not connected with merchant:Merchantid : {merchantId} , orderExternalRef: {orderExternalRef}")
      else:
        pass
    return {
      'statusCode': 200,
      'body': json.dumps('Lambda -> Order Notification Handler!')
    }


def send_order_notification_to_android(subject, message):
  try:
    print("Sending notification to android...")
    # get merchantId
    merchantId = message.get("body").get("order").get("orderMerchant").get("id")
    orderId = message.get("body").get("order").get("id")

    # get all the socket connections ids from database
    connections = Websockets.get_connection_by_mid_and_eventname(merchantId=merchantId, eventName="android.order")

    if type(connections) is list:
      for connection in connections:
        deviceId = connection.get("connectionId")

        try:
          response=send_android_notification_api(deviceId=deviceId , subject=subject , orderId=orderId,datatype=1)
          #print(response.text)          
          if response.status_code >= 200 and response.status_code < 300:
            print(f"Posted notification to android , Device Id:{deviceId},Response is {response.text} , status_code is {response.status_code} ,  Function Name:send_order_notification_to_android")
          else:
            print(f"Unable to posting notification to android , Device Id:{deviceId}, Response is {response.text},  Function Name:send_order_notification_to_android")
        except Exception as e:
          print("Error: ", str(e))
    return True
  except Exception as e:
    print(str(e))
    return False


def item_ses_email_event(event, context):
  with app.app_context():
    print("---------------------- item_ses_email_event is triggered -------------------------")

    for record in event['Records']:
      subject = record.get("Sns").get("Subject")
      message = eval(record.get("Sns").get("Message"))
      print("Subject: ", subject)

      # extract details from event
      merchantId = message.get("body").get("merchantId")
      itemId = message.get("body").get("itemId")

      # get merchant details
      merchant_details = Merchants.get_merchant_by_id(merchantId=merchantId)
      merchantName = merchant_details['merchantname']

      # get item details
      item_details = Items.get_item_by_id(itemId=itemId)
      itemName = item_details['itemName']
      itemPrice = item_details['itemUnitPrice']
      itemStatus = "Enable" if int(item_details['itemStatus']) == 1 else "Disable"

      if subject == "item.status_change":

        # send email to the user
        SENDER = f"[NoReply] Fonda <{config.ses_sender_email}>"
        RECIPIENT = config.ses_receiver_email
        SUBJECT = f"Item Status Change Notification for Merchant <{merchantName}>"
        BODY_TEXT = (
          f"It is to notify you that the item <{itemName}> with price <{itemPrice}> status is changed to <{itemStatus}> for the merchant <{merchantName}>."
          )
        BODY_HTML = f"""
            <html>
                <head></head>
                <body>
                    <h1>Item Status Change</h1><br>
                    <h3>Merchant Name: {merchantName} </h3>
                    <h3>Item Name: {itemName} </h3>
                    <h3>Item Price: {itemPrice} </h3>
                    <h3>Item Status: {itemStatus} </h3>
                </body>
            </html>
        """
        CHARSET = "UTF-8"
        ses_client = boto3.client('ses')

        try:
          # Provide the contents of the email.
          response = ses_client.send_email(
            Destination={
              'ToAddresses': [
                RECIPIENT,
              ],
            },
            Message={
              'Body': {
                'Html': {
                  'Charset': CHARSET,
                  'Data': BODY_HTML,
                },
                'Text': {
                  'Charset': CHARSET,
                  'Data': BODY_TEXT,
                },
              },
              'Subject': {
                'Charset': CHARSET,
                'Data': SUBJECT,
              },
            },
            Source=SENDER,
          )
        # Display an error if something goes wrong.
        except ClientError as e:
          print(e.response['Error']['Message'])
          return success(jsonify(e.response['Error']['Message']))
        else:
          print("Email sent! Message ID:"),
          print(response['MessageId'])

      print("Done")


#def order_notification_to_heroku_webhook(subject, order_message):
#  try:
#    heroku_url = config.merchant_app_base_url_heroku
#    if heroku_url == "":
#      return True
#    headers = {"Accept": "application/json"}

#    if subject == "order.create":
#      full_url = f"{heroku_url}/orderCreatedOrChanged"
#      response = requests.request("POST", full_url, headers=headers, json=order_message)
#      print(response.text)

#    elif subject == "order.status":
#      full_url = f"{heroku_url}/orderStatusChanged"
#      response = requests.request("POST", full_url, headers=headers, json=order_message)
#      print(response.text)

#    return True
#  except Exception as e:
#    print("Heroku Webhook Error: ", str(e))
#    return False


def merchant_email_notification(event, context):
  print("----------------------- merchant email notification triggered --------------------------")

  with app.app_context():
    print(event)

    for record in event['Records']:
      try:
        time.sleep(1)

        print('message body: ' + record["body"])
        try:
          dataObj = json.loads(record["body"])
          startDate = dataObj['startDate']
          endDate = dataObj['endDate']
          merchantId = dataObj['merchantId']
          payoutType = dataObj['payouttype']
          payoutType = int(dataObj['payouttype']) if is_float(dataObj['payouttype']) else 1
          print("start date is ", startDate)
          print("endDate is ", endDate)
          print("merchantId is ", merchantId)
          print("payoutTypeis ", payoutType)

          # convert datetime to python format
          startDate = datetime.datetime.strptime(startDate, "%Y-%m-%d")
          endDate = datetime.datetime.strptime(endDate, "%Y-%m-%d")

          if endDate < startDate:
            print("invalid endate startdate")
            return invalid("invalid date range")

          # receivers emails
          if env == "production":
            emails = Payouts.get_emailDistributionList(merchantId=merchantId)
            email = emails[0]['emaildistributionlist'].split(';')
            static_emails = ["awais@mifonda.io", "sarah@mifonda.io", "khuzaima@paalam.co.uk"]
            email.extend(static_emails)

          else:
            # email = ["khuzaimabdul1994@gmail.com"]
            emails = Payouts.get_emailDistributionList(merchantId=merchantId)
            email = emails[0]['emaildistributionlist'].split(';')
            static_emails = ["khuzaimabdul1994@gmail.com", "khuzaima@paalam.co.uk"]
            email.extend(static_emails)
            print("email ", email)

          email_merchant_summary_report(merchantId, startDate, endDate, payoutType, email)
          print("successfull")

        except Exception as e:
          return unhandled(f"Error: {e}")
        # message = json.loads(record["body"])
        # print(message)

        # subject = message.get("event")
        # merchantId = message.get("merchantId")

      except Exception as e:
        print("Error: ", str(e))


def truncate_temporary_transaction_table(event, context):
    try:
        ### loggings
        current_time = datetime.datetime.now().time()
        logger.info("Your cron function (to tuncate temporary transaction table) starts running at " + str(current_time))
        print("Your cron function (to tuncate temporary transaction table) starts running")
        ### database config
        connection, cursor = openDbconnection()

        cursor.execute("""TRUNCATE TABLE temporarytransactiontable;""")

        connection.commit()

        print("Rows Affected: " + str(cursor.rowcount))
        print("Finished")

    except Exception as e:
        print("ERROR: ", str(e))
    finally:
        closeDbconnection(connection)