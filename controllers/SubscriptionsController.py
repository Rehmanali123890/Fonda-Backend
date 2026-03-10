from flask import g, jsonify, request
import json

from controllers.MerchantsController import insert_into_subscription_table, insert_into_subscription_table_by_split
# local imports
from controllers.Middleware import validate_token_middleware
from utilities.errors import invalid, unauthorised, unhandled
from utilities.helpers import success, validateAdminUser, validateMerchantUser, is_float, publish_sns_message,get_ip_address
import config
from models.Subscriptions import Subscriptions


@validate_token_middleware
def waiveoffSubscriptionAmount(merchantId, subscriptionId):
  try:
    ip_address = get_ip_address(request)
    userId = g.userId
    if not validateMerchantUser(merchantId, userId):
      return unauthorised("user is not authorized to access merchant information")

    ### get subscription by id
    subscription_details = Subscriptions.get_subscription_by_id(subscriptionId)

    if not subscription_details:
      return invalid("subscription id is invalid!")

    if subscription_details['status'] == 1 and subscription_details['payoutid'] is not None:
      return invalid("subscription is already adjusted in payout!")
    ### unpack json body

    _json = request.json
    remarks = _json.get("remarks")

    if _json.get("action")=="waiveoff":
      resp = Subscriptions.mark_subscriptions_as_waiveoff(subscriptionId=subscriptionId, waiveoff_remarks=remarks, userId=userId)
      sns_msg = {
        "event": "subscription.waiveoff",
        "body": {
          "merchantId": merchantId,
          "userId": userId,
          "subscriptionId": subscriptionId,
          "ipAddr": ip_address
        }
      }
      logs_sns_resp = publish_sns_message(topic=config.sns_audit_logs, message=str(sns_msg), subject="subscription.waiveoff")
    elif _json.get("action")=="markPay":
      resp = Subscriptions.mark_subscriptions_as_paid_by_user(subscriptionId=subscriptionId, remarks=remarks, userId=userId)
      sns_msg = {
        "event": "subscription.markPay",
        "body": {
          "merchantId": merchantId,
          "userId": userId,
          "subscriptionId": subscriptionId,
          "ipAddr": ip_address
        }
      }
      logs_sns_resp = publish_sns_message(topic=config.sns_audit_logs, message=str(sns_msg), subject="subscription.markpay")


    
    ### mark subscription as wave-off

    if not resp:
      return unhandled("error occured while marking subscription as waive-off!")
    
    ### entry in audit logs



    return success()
  except Exception as e:
    print("Error: ", str(e))
    return unhandled()


@validate_token_middleware
def splitSubscriptionAmount(merchantId, subscriptionId):
  try:
    userId = g.userId
    if not validateMerchantUser(merchantId, userId):
      return unauthorised("user is not authorized to access merchant information")

    ### get subscription by id
    subscription_details = Subscriptions.get_subscription_by_id(subscriptionId)

    if not subscription_details:
      return invalid("subscription id is invalid!")

    if subscription_details['status'] == 1 and subscription_details['payoutid'] is not None:
      return invalid("subscription is already adjusted in payout!")
    ### unpack json body

    _json = request.json
    SubscriptionSplitList = _json.get("SubscriptionSplitList")


    Subscriptions.mark_subscriptions_as_splited(subscriptionId=subscriptionId)

    subscription_date=subscription_details['date']
    subscription_frequency=subscription_details['frequency']
    remarks = f" Splitted against Date: {subscription_date} "
    for item in SubscriptionSplitList:
      insert_into_subscription_table_by_split(merchantId, item['splitDate'], item['splitAmount'], 0,
                                     subscription_frequency, 0, 1,remarks=remarks)
    return success()
  except Exception as e:
    print("Error: ", str(e))
    return unhandled()



@validate_token_middleware
def get_subscriptions_for_merchant(merchantId):
  try:
    userId = g.userId
    if not validateAdminUser(userId):
      return unauthorised("user is not authorized")

    records = Subscriptions.get_subscription_records(merchantId)

    return success(jsonify({
      "status": 200,
      "message": "success",
      "data": records
    }))
  except Exception as e:
    print("Error: ", str(e))
    return unhandled()

