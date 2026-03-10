from app import app
import json
# from flask import Flask
import requests
import datetime
from dateutil.tz import gettz

# local imports
from models.Merchants import Merchants
from models.Websockets import Websockets
from models.twilio.Twilio import Twilio
import config
from utilities.helpers import success, get_db_connection, send_android_notification_api


# init flask in order to keep events from crashing
# app = Flask(__name__)


def merchant_autosync_event(event, context):
  with app.app_context():
    print("---------- merchant_autosync_event ----------")
    print(event)
    utcDatetime = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)

    for record in event['Records']:
      subject = record.get("Sns").get("Subject")
      message = eval(record.get("Sns").get("Message"))
      print("Subject: ", subject)

      merchantId = message.get("body").get("merchantId")
      pauseTime = message.get("body").get("pauseTime")
      caller = message.get("body").get("caller")

      """
          SEND NOTIFICATION TO ANDROID APP
      """
      # if subject == "merchant.status_change" and caller != "android":
      if subject == "merchant.status_change":

        # get merchant details
        merchant_details = Merchants.get_merchant_by_id(merchantId)

        marketStatus = merchant_details['marketstatus']

        # get all the socket connections ids from database
        connections = Websockets.get_connection_by_mid_and_eventname(
          merchantId=merchantId,
          eventName="android.order")

        if type(connections) is list:

          expired_device_ids = list()  # list of expired connection ids

          for connection in connections:
            deviceId = connection.get("connectionId")

            try:
              response = send_android_notification_api(deviceId=deviceId, subject=subject,merchantId=merchantId,
                                                       marketStatus=marketStatus,pauseTime=pauseTime,caller=caller)

              print("response is " , response.text)
              if response.status_code >= 200 and response.status_code < 300:

                response_json = response.json()
                failure = response_json.get("failure")
                
                if failure is not None and failure >= 1:
                  if len(response_json.get("results")) and response_json.get("results")[0].get(
                      "error") == "NotRegistered":
                    expired_device_ids.append(deviceId)

              else:
                print(f"Unable to posting notification to android , Device Id: {deviceId}, Subject:{subject}, Function Name:merchant_autosync_event()")

            except Exception as e:
              print("error: ", str(e))

          # delete expired device ids
          if len(expired_device_ids):
            print("expired ids: ");
            print(expired_device_ids);
            con, cursor = get_db_connection()
            cursor.execute("""DELETE FROM websockets WHERE connectionid IN %s""", (tuple(expired_device_ids),))
            con.commit()

        """
            send message to merchant phone using twilio
        """
        if merchant_details['phone'] != "":
          send_message_to_merchant_phone(merchant=merchant_details)


def send_message_to_merchant_phone(merchant):
  try:
    _from = config.twilio_sender_phone_number
    _to_list = []
    _to_list.append(merchant['phone'])
    _to_list.append(config.Back_office_number)

    if merchant["marketstatus"]:
      status = "active"
      message = "accepting"
    else:
      status = "paused"
      message = "not accepting"
    if status == "paused":
      merchant['pauseStarted_datetime'] = merchant['pauseStarted_datetime'].astimezone(gettz('US/Pacific')).strftime(
        "%m-%d-%Y %H:%M (%Z)")
      if merchant['caller'] == "dashboard":
        _message = "Alert: Your restaurant [" + merchant[
          "merchantname"] + "] is " + status + " in the Fonda system as of " + str(merchant[
                                                                                                 'pauseStarted_datetime']) + " and is " + message + " orders. You could be missing up to $100 in sales for every hour your tablet is paused. If this was a mistake, contact support at +1(888)366-3280."
      else:
        if merchant['pause_reason'] == "":
          _message = "Alert: Your restaurant [" + merchant[
            "merchantname"] + "> is " + status + " in the Fonda system as of " + str(
            merchant['pauseStarted_datetime']) + " for " + merchant[
                       'pauseTime_duration'] + " and is " + message + " orders. You could be missing up to $100 in sales for every hour your tablet is paused. If this was a mistake, contact support at +1(888)366-3280."
        else:
          _message = "Alert: Your restaurant [" + merchant[
            "merchantname"] + "] is " + status + " in the Fonda system as of " + str(
            merchant['pauseStarted_datetime']) + " for " + merchant[
                       'pauseTime_duration'] + " and is " + message + " orders. " + "Reason: " + merchant[
                       'pause_reason'] + ". You could be missing up to $100 in sales for every hour your tablet is paused. If this was a mistake, contact support at +1(888)366-3280."

    else:
      _message = "Great news! Your restaurant [" + merchant[
        "merchantname"] + "] is now " + status + " in the Fonda system and " + message + " orders. Keeping your tablet on means you could be making up to $100 in sales per hour. Need help? Contact support at 1(888)366-3280."

    for _to in _to_list:
      resp_status, resp_data = Twilio.send_message(_from, _to, _message)
      print(resp_status)
      print(resp_data)


  except Exception as e:
    print(f"error: {e}")
