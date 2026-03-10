import uuid

from app import app
import json
# from flask import Flask
import datetime
from dateutil.tz import gettz
from decimal import *
import boto3
from botocore.exceptions import ClientError
import random
import string
import csv
import time

from models.ActivityLogs import ActivityLogs
from utilities.helpers import *
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from models.Finance import Finance
import os


# local imports
from models.Payouts import Payouts
from models.Users import Users
from controllers.PayoutsController import *
from controllers.FinanceController import *
from utilities.helpers import get_db_connection
import config

# init flask in order to keep events from crashing
# app = Flask(__name__)
# app.config.from_object(config)


def bulk_payout_event(event, context):
  with app.app_context():
    print("--------------------- - --------------------------")
    connection, cursor = get_db_connection()
    for record in event['Records']:
      try:

        subject = record.get("Sns").get("Subject")
        message = eval(record.get("Sns").get("Message"))

        print(subject)
        print(message)

        userId = message.get("body").get("userId")
        childEvent = message.get("body").get("childEvent")
        startDate = message.get("body").get("startDate")
        endDate = message.get("body").get("endDate")
        createTransfer = message.get("body").get("createTransfer")
        payoutType = message.get("body").get("payoutType")

        user_details = Users.get_user_by_id(userId)

        message_list = list(dict())

        # get all merchants
        cursor.execute("SELECT * FROM merchants")
        merchants = cursor.fetchall()

        ### clean entries from payouts table with entry status as 0
        resp = Payouts.delete_bulk_payout_entries()
        sqs_client = boto3.resource('sqs')
        queue = sqs_client.get_queue_by_name(QueueName=config.sqs_merchant_calculateBulkPayout)
        for merchant in merchants:
          dataObj = {
            "merchantId": merchant['id'], "startDate": startDate, "endDate": endDate, "createTransfer": createTransfer,
            "userId": userId, "payoutType": payoutType}

          response = queue.send_message(
            MessageBody=json.dumps(dataObj),
            MessageGroupId=str(uuid.uuid4()),
            MessageDeduplicationId=str(uuid.uuid4())
          )
          print(response)

        return success(jsonify({"message": "success", "status": 200, "data": response}))

        # response = merchantPayoutReportByDate(merchant['id'], startDate, endDate, createTransfer, userId, payoutType=payoutType, payoutAdjustments=0, remarks='')
        # response_json = response.get_json()
        # print(" response json is " , response_json)
        # if response.status_code == 200:
        #
        #   # store data in payouts table with status as 0 if createTransfer is 0
        #   if createTransfer == 0:
        #     resp = Payouts.post_payout(merchant['id'], startDate, endDate, 0, response_json.get('data'), userId, {}) #status=0 means that its bulk payout record
        #
        # else:
        #   ### audit logs entry for error
        #   sns_msg = {
        #     "event": "payout.create_error",
        #     "body": {
        #       "merchantId": merchant['id'],
        #       "userId": userId,
        #       "eventDetails": response_json.get("message")
        #     }
        #   }
        #   logs_sns_resp = publish_sns_message(topic=config.sns_audit_logs, message=str(sns_msg), subject="payout.create_error")

        # append report to list of dict
        #   if createTransfer == 1:
        #     response_report = response_json.get('data') if response_json.get('data') else {}
        #     message_list.append({
        #       'MerchantId': merchant.get('id'),
        #       'MerchantName': merchant.get('merchantname'),
        #       'PayoutStatus': response_json.get('message') if response.status_code == 200 else f"failed!!! {response_json.get('message')}",
        #       'NumberOfOrders': response_report.get('numberOfOrders'),
        #       'SubTotal': response_report.get('subTotal'),
        #       'StaffTips': response_report.get('staffTips'),
        #       'Tax': response_report.get('tax'),
        #       'ProcessingFee': response_report.get('processingFee'),
        #       'Commission': response_report.get('commission'),
        #       'MarketplaceTax': response_report.get('marketplaceTax'),
        #       'ErrorCharges': response_report.get('errorCharges'),
        #       'OrderAdjustments': response_report.get('orderAdjustments'),
        #       'SubscriptionAdjustments': response_report.get('subscriptionAdjustments'),
        #       'NetPayout': response_report.get('netPayout')
        #     })
        #
        # ### send email to the user that bulk payout process in completed
        # if createTransfer == 1:
        #   email_resp = send_bulk_payout_email(startDate=startDate, endDate=endDate, user_details=user_details, message_list=message_list)

      except Exception as e:
        print("Error: ", str(e))


def new_bulk_payout_event(event, context):
  with app.app_context():
    print("--------------------- - --------------------------")
    connection, cursor = get_db_connection()
    for record in event['Records']:
      try:
        create_log_data(
          level="[INFO]",
          Message="In the start of function (new_bulk_payout_event) to pass merchants in queue for new bulk payout creation",
          functionName="new_bulk_payout_event",
        )
        subject = record.get("Sns").get("Subject")
        message = eval(record.get("Sns").get("Message"))

        print(f"Sns subject of new bulk payout: {subject}")
        print(f"Sns message of new Bulk payout: {message}")

        userId = message.get("body").get("userId")
        childEvent = message.get("body").get("childEvent")
        startDate = message.get("body").get("startDate")
        endDate = message.get("body").get("endDate")
        createTransfer = message.get("body").get("createTransfer")
        payoutType = message.get("body").get("payoutType")
        create_log_data(
          level="[INFO]",
          Message="Successfully get sns message for new bulk payout calculation",
          functionName="new_bulk_payout_event",
        )
        user_details = Users.get_user_by_id(userId)

        message_list = list(dict())

        # get all merchants
        cursor.execute("SELECT * FROM merchants")
        merchants = cursor.fetchall()
        create_log_data(
          level="[INFO]",
          Message="Get all merchants from database",
          functionName="new_bulk_payout_event",
        )
        ### clean entries from payouts table with entry status as 0
        resp = Payouts.delete_new_bulk_payout_entries()
        sqs_client = boto3.resource('sqs')
        queue = sqs_client.get_queue_by_name(QueueName=config.sqs_merchant_calculateNewBulkPayout)
        for merchant in merchants:
          create_log_data(
            level="[INFO]",
            Message=f"Merchant pass in queue for new payout calculations {merchant}",
            functionName="new_bulk_payout_event",
          )
          print(f"Merchant details in sns {merchant}")
          dataObj = {
            "merchantId": merchant['id'], "startDate": startDate, "endDate": endDate, "createTransfer": createTransfer,
            "userId": userId, "payoutType": payoutType}
          print(dataObj)

          response = queue.send_message(
            MessageBody=json.dumps(dataObj),
            MessageGroupId=str(uuid.uuid4()),
            MessageDeduplicationId=str(uuid.uuid4())
          )
          print(response)
        create_log_data(
          level="[INFO]",
          Message="Successfully pass all merchants for new payout calculation",
          functionName="new_bulk_payout_event",
        )
        return success(jsonify({"message": "success", "status": 200, "data": response}))

        # response = merchantPayoutReportByDate(merchant['id'], startDate, endDate, createTransfer, userId, payoutType=payoutType, payoutAdjustments=0, remarks='')
        # response_json = response.get_json()
        # print(" response json is " , response_json)
        # if response.status_code == 200:
        #
        #   # store data in payouts table with status as 0 if createTransfer is 0
        #   if createTransfer == 0:
        #     resp = Payouts.post_payout(merchant['id'], startDate, endDate, 0, response_json.get('data'), userId, {}) #status=0 means that its bulk payout record
        #
        # else:
        #   ### audit logs entry for error
        #   sns_msg = {
        #     "event": "payout.create_error",
        #     "body": {
        #       "merchantId": merchant['id'],
        #       "userId": userId,
        #       "eventDetails": response_json.get("message")
        #     }
        #   }
        #   logs_sns_resp = publish_sns_message(topic=config.sns_audit_logs, message=str(sns_msg), subject="payout.create_error")

        # append report to list of dict
        #   if createTransfer == 1:
        #     response_report = response_json.get('data') if response_json.get('data') else {}
        #     message_list.append({
        #       'MerchantId': merchant.get('id'),
        #       'MerchantName': merchant.get('merchantname'),
        #       'PayoutStatus': response_json.get('message') if response.status_code == 200 else f"failed!!! {response_json.get('message')}",
        #       'NumberOfOrders': response_report.get('numberOfOrders'),
        #       'SubTotal': response_report.get('subTotal'),
        #       'StaffTips': response_report.get('staffTips'),
        #       'Tax': response_report.get('tax'),
        #       'ProcessingFee': response_report.get('processingFee'),
        #       'Commission': response_report.get('commission'),
        #       'MarketplaceTax': response_report.get('marketplaceTax'),
        #       'ErrorCharges': response_report.get('errorCharges'),
        #       'OrderAdjustments': response_report.get('orderAdjustments'),
        #       'SubscriptionAdjustments': response_report.get('subscriptionAdjustments'),
        #       'NetPayout': response_report.get('netPayout')
        #     })
        #
        # ### send email to the user that bulk payout process in completed
        # if createTransfer == 1:
        #   email_resp = send_bulk_payout_email(startDate=startDate, endDate=endDate, user_details=user_details, message_list=message_list)

      except Exception as e:
        create_log_data(
          level="[ERROR]",
          Message=f"An exception occur while passing merchants in queue for calculation {str(e)}",
          functionName="new_bulk_payout_event",
        )
        print("Error new_bulk_payout_event: ", str(e))
### EMAIL SEND FOR BULK PAYOUT
def send_bulk_payout_email(startDate, endDate, user_details, message_list):

  temp_file_name = "bulk-payout-"+''.join(random.choices(string.ascii_letters + string.digits, k=7))+".csv"
  temp_file_path = "/tmp/"+temp_file_name

  with open(temp_file_path, mode='w', newline='') as temp_csv:
    writer1 = csv.writer(temp_csv, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
    writer1.writerow(["Payout Date Range:", f"{startDate} to {endDate}"])
    writer1.writerow(["Payout Creation Date: ", f"{datetime.datetime.now().astimezone(gettz('UTC')).date().isoformat()}"])
    writer1.writerow([ "Payout Created By User: ", f"{user_details['username']}" ])
    writer1.writerow([])
    writer1.writerow([])

    # write rows
    fieldnames = ['MerchantId', 'MerchantName', 'PayoutStatus', 'NumberOfOrders', 'SubTotal', 'StaffTips',
      'Tax', 'ProcessingFee', 'Commission', 'MarketplaceTax', 'ErrorCharges', 'OrderAdjustments', 'SubscriptionAdjustments', 
      'NetPayout'
    ]
    writer2 = csv.DictWriter(temp_csv, fieldnames=fieldnames, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
    writer2.writeheader()
    for row in message_list:
        writer2.writerow(row)


  # send email

  SENDER = f"[NoReply] Fonda <{config.ses_sender_email}>"
  SUBJECT = f"Bulk Payout Transfer Report"
  BODY_TEXT = (f"Please and download find the below attachment to check the results of bulk payout report.")
  BODY_HTML = f"""
    <html>
      <head></head>
      <body>
        <h1>Bulk Payout Report</h1>
        <p><b>Bulk payout transfer is completed! Please download the below attachment to see individual merchant payout status</b></p>
      </body>
    </html>
  """
  RECIPIENTS = [user_details['email'], ]

  msg = create_multipart_message(
    sender=SENDER, 
    recipients=RECIPIENTS,
    title=SUBJECT,
    text=BODY_TEXT,
    html=BODY_HTML,
    attachments=[temp_file_path]
  )

  ses_client = boto3.client('ses')
  try:
    response = ses_client.send_raw_email(
      Source=SENDER,
      Destinations=RECIPIENTS,
      RawMessage={'Data': msg.as_string()}
    )

  except ClientError as e:
    print(e.response['Error']['Message'])
    return False
  except Exception as e:
    print("Error: ", str(e))
    return False
  else:
    print("Email sent! Message ID:")
    print(response['MessageId'])
    return True


def create_multipart_message(sender: str, recipients: list, title: str, text: str=None, html: str=None, attachments: list=None) -> MIMEMultipart:
    """
    Creates a MIME multipart message object.
    Uses only the Python `email` standard library.
    Emails, both sender and recipients, can be just the email string or have the format 'The Name <the_email@host.com>'.
 
    :param sender: The sender.
    :param recipients: List of recipients. Needs to be a list, even if only one recipient.
    :param title: The title of the email.
    :param text: The text version of the email body (optional).
    :param html: The html version of the email body (optional).
    :param attachments: List of files to attach in the email.
    :return: A `MIMEMultipart` to be used to send the email.
    """
    multipart_content_subtype = 'alternative' if text and html else 'mixed'
    msg = MIMEMultipart(multipart_content_subtype)
    msg['Subject'] = title
    msg['From'] = sender
    msg['To'] = ', '.join(recipients)
 
    # Record the MIME types of both parts - text/plain and text/html.
    # According to RFC 2046, the last part of a multipart message, in this case the HTML message, is best and preferred.
    if text:
      part = MIMEText(text, 'plain')
      msg.attach(part)
    if html:
      part = MIMEText(html, 'html')
      msg.attach(part)
 
    # Add attachments
    for attachment in attachments or []:
      with open(attachment, 'rb') as f:
        part = MIMEApplication(f.read())
        part.add_header('Content-Disposition', 'attachment', filename=os.path.basename(attachment))
        msg.attach(part)
 
    return msg

def merchant_calculate_BulkPayout(event, context):
  print("----------------------- merchant calculate bulk payout triggered --------------------------")

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
          userId = dataObj['userId']
          createTransfer = dataObj['createTransfer']
          merchantId = dataObj['merchantId']
          payoutType = int(dataObj['payoutType']) if is_float(dataObj['payoutType']) else 1
          print("start date is " , startDate)
          print("endDate is ", endDate)
          print("merchantId is ", merchantId)
          print("userId is ", userId)
          print("createTransfer is ", createTransfer)
          print("payoutType is ", payoutType)

          response = merchantPayoutReportByDate(merchantId, startDate, endDate, createTransfer, userId,
                                                payoutType=payoutType, payoutAdjustments=0, remarks='')
          response_json = response.get_json()
          print(" response json is ", response_json)
          if response.status_code == 200:

            # store data in payouts table with status as 0 if createTransfer is 0
            if createTransfer == 0:
              resp = Payouts.post_payout(merchantId, startDate, endDate, 0, response_json.get('data'), userId,
                                         {})  # status=0 means that its bulk payout record

          else:
            ### audit logs entry for error
            sns_msg = {
              "event": "payout.create_error",
              "body": {
                "merchantId": merchantId,
                "userId": userId,
                "eventDetails": response_json.get("message")
              }
            }
            logs_sns_resp = publish_sns_message(topic=config.sns_audit_logs, message=str(sns_msg),
                                                subject="payout.create_error")


        except Exception as e:
          return unhandled(f"Error merchant_calculate_BulkPayout: {e}")

      except Exception as e:
        print("Error merchant_calculate_BulkPayout: ", str(e))

def merchant_calculate_NewBulkPayout(event, context):
  print("----------------------- merchant calculate New bulk payout triggered --------------------------")

  with app.app_context():
    print(event)

    for record in event['Records']:
      try:
        time.sleep(1)
        print('message body: ' + record["body"])
        try:
          dataObj = json.loads(record["body"])
          create_log_data(level='[INFO]',
                          Message="In the beginning of function (merchant_calculate_NewBulkPayout) to calculate new bulk payout and insert into database table",
                          merchantID=dataObj['merchantId'], functionName="merchant_calculate_NewBulkPayout")
          startDate = dataObj['startDate']
          endDate = dataObj['endDate']
          userId = dataObj['userId']
          createTransfer = dataObj['createTransfer']
          merchantId = dataObj['merchantId']
          payoutType = int(dataObj['payoutType']) if is_float(dataObj['payoutType']) else 1
          print("start date is " , startDate)
          print("endDate is ", endDate)
          print("merchantId is ", merchantId)
          print("userId is ", userId)
          print("createTransfer is ", createTransfer)
          print("payoutType is ", payoutType)

          merchant_status_and_name = Finance.get_merchant_status(merchantId)
          create_log_data(level='[INFO]',
                          Message=f"Successfully get merchant status and name for merchant id {merchantId}",
                          merchantID=merchantId, functionName="merchant_calculate_NewBulkPayout")
          # response = merchantPayoutReportByDate(merchantId, startDate, endDate, createTransfer, userId,
          #                                       payoutType=payoutType, payoutAdjustments=0, remarks='')
          # response_json = response.get_json()

          dashboard_response = Finance.newPayoutReportByDate(merchantId, startDate, endDate, createTransfer, userId,
                                                             payoutType,
                                                             payoutAdjustments=0, remarks='', transferType=1,
                                                             orderSource='', unlocked_only=2)
          create_log_data(level='[INFO]',
                          Message=f"Calculated dashboard values of each platform for merchant {merchantId}",
                          messagebody=f'{dashboard_response}',
                          merchantID=merchantId, functionName="merchant_calculate_NewBulkPayout")
          csv_responce = Finance.newBulkPayoutReportByDate(merchantId, startDate, endDate, createTransfer, userId,
                                                           payoutType,
                                                           payoutAdjustments=0, remarks='', orderSource='')
          create_log_data(level='[INFO]',
                          Message=f"Calculated csv values of each platform for merchant {merchantId}",
                          messagebody=f'{csv_responce}',
                          merchantID=merchantId, functionName="merchant_calculate_NewBulkPayout")
          dashboard_response_json = dashboard_response.get_json()
          print(f"Dashboard responce : {dashboard_response_json}")
          csv_responce_json = csv_responce.get_json()
          print(f"CSV responce : {csv_responce_json}")
          # print(" response json is ", response_json)
          if dashboard_response_json['status'] == 200 and csv_responce_json['status'] == 200:
            # store data in payouts table with status as 0 if createTransfer is 0
            if createTransfer == 0:
              resp = Payouts.post_NewBulkPayout(merchantId, startDate, endDate, merchant_status_and_name,
                                                csv_responce_json['data'], dashboard_response_json['data'], userId)

          else:
            create_log_data(level='[ERROR]',
                            Message=f"Error while calculating new bulk payout",
                            messagebody=f"{dashboard_response_json['message']}, {csv_responce_json['message']}",
                            merchantID=merchantId, functionName="merchant_calculate_NewBulkPayout")
            ### audit logs entry for error
            sns_msg = {
              "event": "payout.create_error",
              "body": {
                "merchantId": merchantId,
                "userId": userId,
                "eventDetails": f"{dashboard_response_json['message']}, {csv_responce_json['message']}"
              }
            }
            logs_sns_resp = publish_sns_message(topic=config.sns_audit_logs, message=str(sns_msg),
                                                subject="payout.create_error")

        except Exception as e:
          return unhandled(f"Error merchant_calculate_NewBulkPayout: {e}")

      except Exception as e:
        create_log_data(level='[INFO]',
                        Message=f"Calculated dashboard values of each platform for merchant {merchantId}",
                        messagebody=f'{dashboard_response}',
                        merchantID=merchantId, functionName="merchant_calculate_NewBulkPayout")
        print("Error merchant_calculate_NewBulkPayout: ", str(e))


def treasury_transfer_event(event, context):
  print("----------------------- Treasury Transfer Event triggered --------------------------")

  with app.app_context():
    print("--------------------- - --------------------------")
    for record in event['Records']:
      try:
        subject = record.get("Sns").get("Subject")
        message = eval(record.get("Sns").get("Message"))

        print(subject)
        print(message)

        merchantId = message.get("body").get("merchantId")
        userId = message.get("body").get("userId")
        payoutId = message.get("body").get("payoutId")
        shortfall_remarks = message.get("body").get("shortfall_remarks")
        result = transfer_to_bank_finance_payout.__wrapped__(merchantId, payoutId , userid=userId , is_sns=True , shortfall_remarks=shortfall_remarks)


      except Exception as e:
        print("Error treasury_transfer_event: ", str(e))


