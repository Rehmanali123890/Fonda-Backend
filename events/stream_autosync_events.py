import config
from app import app
import json
import datetime

from models.Items import Items
# local imports
from models.Platforms import Platforms
from models.Stream import Stream
from utilities.helpers import get_db_connection, create_log_data, publish_sns_message
from models.Merchants import Merchants
from models.VirtualMerchants import VirtualMerchants
from models.ItemServiceAvailability import ItemServiceAvailability
from models.Addons import Addons
from datetime import datetime, timezone
from dateutil.tz import gettz
import calendar

def merchant_autosync_event(event, context):
  with app.app_context():
    connection, cursor = get_db_connection()
    for record in event['Records']:
      subject = record.get("Sns").get("Subject")
      message = eval(record.get("Sns").get("Message"))

      merchantId = message.get("body").get("merchantId")
      marketStatus = message.get("body").get("marketStatus")
      ip_address = message.get("body").get("ipAddr")
      merchant_details = Stream.get_merchant_by_id_for_stream(merchantId)
      if not merchant_details:
        create_log_data(level='[ERROR]',
                        Message="Stream is not connected with merchant",
                        merchantID=merchantId, functionName="merchant_autosync_event")
        print(f"Stream is not connected with merchant: {merchantId}")
        continue
      platform = Platforms.get_platform_by_merchantid_and_platformtype(merchantId, platformtype=8)
      if not platform:
        create_log_data(level='[ERROR]',
                            Message=f"Platform connection is not found: {merchantId} ,IP address: {ip_address}",
                            functionName="merchant_autosync_event", merchantID=merchantId)
        print(f"Platform connection is not found:: {merchantId}")
        continue

      if platform["synctype"] == 1:
        if subject == "merchant.status_change":
          print("---------------------- merchant_autosync_event is triggered for stream -------------------------")
          print(event)
          # get merchant + all-virtual-merchant ids list
          vms = VirtualMerchants.get_virtual_merchant(merchantId=merchantId)
          merchants_list = [{
            "syncMerchantId": merchantId,
            "isVirtual": 0,
            "mainMerchantId": merchantId
          }]
          for vm in vms:
            merchants_list.append({
              "syncMerchantId": vm["id"],
              "isVirtual": 1,
              "mainMerchantId": merchantId
            })
          virtualmerchantList = VirtualMerchants.get_virtual_merchant(None,merchantId , stream=1)
          stream , stream_status_code , msg = Stream.update_location_status_stream(merchantId,marketStatus,ip_address)
          if stream:
              create_log_data(level='[INFO]',
                  Message=f"Update location status to stream successfully,IP address: {ip_address}",
                  functionName="merchant_autosync_event",merchantID=merchantId, messagebody=message)
          else:
              create_log_data(level='[ERROR]',
                  Message=f"Error in Update location status to stream,IP address: {ip_address}",
                  functionName="merchant_autosync_event",merchantID=merchantId,messagebody=message)
              sns_msg = {
                "event": "error_logs.entry",
                "body": {
                  "userId": None,
                  "merchantId": merchantId,
                  "errorName": "Error occurred while updating the merchant location status on Stream.",
                  "errorSource": "dashboard",
                  "errorStatus": stream_status_code,
                  "errorDetails": msg
                }
              }
              error_logs_sns_resp = publish_sns_message(topic=config.sns_error_logs, message=str(sns_msg),
                                                        subject="error_logs.entry")
          
          if virtualmerchantList:
              for merchant in virtualmerchantList:
                  platform = Platforms.get_platform_by_merchantid_and_platformtype(merchant['id'], platformtype=8)
                  if not platform:
                    create_log_data(level='[ERROR]',
                            Message=f"Stream is not connected with merchant: {merchant['id']} ,IP address: {ip_address}",
                            functionName="merchant_autosync_event", merchantID=merchant['id'])
                    print(f"Stream is not connected with merchant: {merchant['id']}")
                    continue

                  if platform["synctype"] == 1:
                  
                    stream , stream_status_code , msg = Stream.update_location_status_stream(merchant['id'],marketStatus,ip_address)
                    if stream:
                        create_log_data(level='[INFO]',
                            Message=f"Update location status to stream successfully,IP address: {ip_address}",
                            functionName="merchant_autosync_event",merchantID=merchantId,messagebody={
                              "merchantid": merchant['id'],
                              "marketStatus": marketStatus
                            })
                    else:
                        create_log_data(level='[ERROR]',
                            Message=f"Error in Update location status to stream,IP address: {ip_address}",
                            functionName="merchant_autosync_event",merchantID=merchantId,messagebody={
                              "merchantid": merchant['id'],
                              "marketStatus": marketStatus
                            })
                        sns_msg = {
                          "event": "error_logs.entry",
                          "body": {
                            "userId": None,
                            "merchantId": merchantId,
                            "errorName": "Error occurred while updating the virtual merchant location status on Stream.",
                            "errorSource": "dashboard",
                            "errorStatus": stream_status_code,
                            "errorDetails": msg
                          }
                        }
                        error_logs_sns_resp = publish_sns_message(topic=config.sns_error_logs, message=str(sns_msg),
                                                                  subject="error_logs.entry")
                    updatevmarketStatus = 1 if marketStatus else 0
                    virtualData = (updatevmarketStatus,merchant['id'])
                    cursor.execute(
                        "UPDATE virtualmerchants SET marketstatus=%s WHERE id=%s",
                        virtualData)
                  else:
                    create_log_data(level='[ERROR]',
                            Message=f"Stream auto sync is off for merchant: {merchantId} ,IP address: {ip_address}",
                            functionName="merchant_autosync_event", merchantID=merchantId)
                    print("Stream Auto sync is off" + merchant['id'])
            
          connection.commit()
      else:
          create_log_data(level='[ERROR]',
                            Message=f"Stream auto sync is off for merchant: {merchantId} ,IP address: {ip_address}",
                            functionName="merchant_autosync_event", merchantID=merchantId)
          print(f"Stream Auto sync is off" + merchantId)
    
              
def item_autosync_event(event, context):
  with app.app_context():
    
    for record in event['Records']:
      subject = record.get("Sns").get("Subject")
      message = eval(record.get("Sns").get("Message"))
      merchantId = message.get("body").get("merchantId")
      itemId = message.get("body").get("itemId")
      ip_address = message.get("body").get("ipAddr")
      merchant_details = Stream.get_merchant_by_id_for_stream(merchantId)
      if not merchant_details:
        create_log_data(level='[ERROR]',
                        Message="Stream is not connected with merchant",
                        merchantID=merchantId, functionName="merchant_autosync_event")
        print(f"Stream is not connected with merchant: {merchantId}")
        continue
      platform = Platforms.get_platform_by_merchantid_and_platformtype(merchantId, platformtype=8)
      if not platform:
        create_log_data(level='[ERROR]',
                            Message=f"Platform connection is not found:: {merchantId} ,IP address: {ip_address}",
                            functionName="item_autosync_event", merchantID=merchantId)
        print(f"Platform connection is not found:"+ merchantId)
        continue

      if platform["synctype"] == 1:
        if subject == "item.status_change":
            print("---------------------- item_autosync_event is triggered for stream -------------------------")
            print(event)
            item_details = Items.get_item_by_id(itemId)
            create_log_data(level='[INFO]',
                            Message=f"Item detail for stream ,IP address: {ip_address}",messagebody=item_details,
                            functionName="item_autosync_event", merchantID=merchantId)
            print('item_detail ' , item_details)
            status = True if item_details['itemStatus'] == 1 else False
            type="modifier" if item_details.get('itemType')==2 else 'item_family'
            stream, stream_status_code, msg  = Stream.update_stream_menu_item_status(merchantId,itemId,status ,type=type, ip_address=ip_address)
            if stream:
                create_log_data(level='[INFO]',
                    Message=f"Update item status to stream successfully,IP address: {ip_address}",
                    functionName="item_autosync_event",merchantID=merchantId,messagebody=message)
                print('Item type is ', type)
                if type == 'item_family':
                  create_log_data(level='[INFO]',
                                  Message=f"The item type is item_family, so send another event with the suffix _item to itemid,IP address: {ip_address}",
                                  functionName="item_autosync_event", merchantID=merchantId)
                  print('The item type is item_family, so send another event with the suffix _item to itemid')
                  stream, stream_status_code, msg = Stream.update_stream_menu_item_status(merchantId, f"{itemId}_item", status,
                                                                                          type=type,
                                                                                          ip_address=ip_address)
                  if stream:
                    create_log_data(level='[INFO]',
                                    Message=f"Update item status to stream successfully for item_family with the suffix _item to itemid,IP address: {ip_address}",
                                    functionName="item_autosync_event", merchantID=merchantId, messagebody=message)
                  else:
                    create_log_data(level='[ERROR]',
                                    Message=f"Error in Update item status to stream for item_family with the suffix _item to itemid,IP address: {ip_address}",
                                    functionName="item_autosync_event", merchantID=merchantId, messagebody=message)
                    sns_msg = {
                      "event": "error_logs.entry",
                      "body": {
                        "userId": None,
                        "merchantId": merchantId,
                        "errorName": "Error occurred while updating the item status on Stream for item_family with the suffix _item to itemid.",
                        "errorSource": "dashboard",
                        "errorStatus": stream_status_code,
                        "errorDetails": msg
                      }
                    }
                    error_logs_sns_resp = publish_sns_message(topic=config.sns_error_logs, message=str(sns_msg),
                                                              subject="error_logs.entry")
            else:
                create_log_data(level='[ERROR]',
                    Message=f"Error in Update item status to stream,IP address: {ip_address}",
                    functionName="item_autosync_event",merchantID=merchantId,messagebody=message)
                sns_msg = {
                  "event": "error_logs.entry",
                  "body": {
                    "userId": None,
                    "merchantId": merchantId,
                    "errorName": "Error occurred while updating the item status on Stream.",
                    "errorSource": "dashboard",
                    "errorStatus": stream_status_code,
                    "errorDetails": msg
                  }
                }
                error_logs_sns_resp = publish_sns_message(topic=config.sns_error_logs, message=str(sns_msg),
                                                          subject="error_logs.entry")
        elif subject == "addon.update":            
            addonId = message.get("body").get("addonId")
            old_addon_details = message.get("body").get("old_addon_details")
            addon_details = Addons.get_addon_by_id_str(addonId)
            checkaddonStatus = old_addon_details.get('status') != addon_details.get('status') 
            if checkaddonStatus:    
              print("---------------------- item_autosync_event is triggered for stream -------------------------")
              print(event)           
              create_log_data(level='[INFO]',
                              Message=f"Item Addon detail for stream ,IP address: {ip_address}",messagebody=addon_details,
                              functionName="item_autosync_event", merchantID=merchantId)
              status = True if addon_details['status'] == 1 else False
              type="modifier_group"
              stream, stream_status_code, msg  = Stream.update_stream_menu_item_status(merchantId,addonId,status ,type=type, ip_address=ip_address)
              if stream:
                  create_log_data(level='[INFO]',
                      Message=f"Update item status to stream successfully,IP address: {ip_address}",
                      functionName="item_autosync_event",merchantID=merchantId,messagebody=message)
              else:
                  create_log_data(level='[ERROR]',
                      Message=f"Error in Update item status to stream,IP address: {ip_address}",
                      functionName="item_autosync_event",merchantID=merchantId,messagebody=message)
                  sns_msg = {
                    "event": "error_logs.entry",
                    "body": {
                      "userId": None,
                      "merchantId": merchantId,
                      "errorName": "Error occurred while updating the modifier status on Stream.",
                      "errorSource": "dashboard",
                      "errorStatus": stream_status_code,
                      "errorDetails": msg
                    }
                  }
                  error_logs_sns_resp = publish_sns_message(topic=config.sns_error_logs, message=str(sns_msg),
                                                            subject="error_logs.entry")
      else:
        create_log_data(level='[ERROR]',
                            Message=f"Stream auto sync is off for merchant: {merchantId} ,IP address: {ip_address}",
                            functionName="item_autosync_event", merchantID=merchantId)
        print("Stream Auto sync is off")


def inactive_stream_items(event, context):

  print("----------------------- inactive_stream_items triggered --------------------------")
  from datetime import datetime
  import pytz
  utc_time = datetime.now(pytz.utc)
  print("----------------------- trigger at  " , utc_time)
  with app.app_context():
    print(event)
    print('length of records ', len(event['Records']))
    for record in event['Records']:
        print('message body: ' + record["body"])
        message = eval(record["body"])
        try:
          merchantId = message.get("merchantId")
          itemId = message.get("itemId")
          item_details = Items.get_item_by_id(itemId)
          create_log_data(level='[INFO]',
                          Message=f"Item detail for stream", messagebody=item_details,
                          functionName="inactive_stream_items", merchantID=merchantId)
          status = True if item_details['itemStatus'] == 1 else False
          type = "modifier" if item_details.get('itemType') == 2 else 'item_family'
          stream, status_code, msg = Stream.update_stream_menu_item_status(merchantId, itemId, status, type=type)
          if stream:
            create_log_data(level='[INFO]',
                            Message=f"Update item status to stream successfully , status: {status_code} , errorMessage : {msg}",
                            functionName="inactive_stream_items", merchantID=merchantId, messagebody=message)
          else:
            create_log_data(level='[ERROR]',
                            Message=f"Error in Update item status to stream , status: {status_code} , errorMessage : {msg} ",
                            functionName="inactive_stream_items", merchantID=merchantId, messagebody=message)
            sns_msg = {
              "event": "error_logs.entry",
              "body": {
                "userId": None,
                "merchantId": merchantId,
                "errorName": "Error occurred while updating the item status on Stream.",
                "errorSource": "dashboard",
                "errorStatus": status_code,
                "errorDetails": msg
              }
            }
            error_logs_sns_resp = publish_sns_message(topic=config.sns_error_logs, message=str(sns_msg),
                                                      subject="error_logs.entry")


        except Exception as e:
          print("Error: ", str(e))
          create_log_data(level='[ERROR]',
                          Message=f"Exception occurred while updating the item status on Stream., errorMessage : {str(e)} ",
                          functionName="inactive_stream_items",  messagebody=message)
          sns_msg = {
            "event": "error_logs.entry",
            "body": {
              "userId": None,
              "merchantId": merchantId,
              "errorName": "Exception occurred while updating the item status on Stream.",
              "errorSource": "dashboard",
              "errorStatus": 500,
              "errorDetails": str(e)
            }
          }
          error_logs_sns_resp = publish_sns_message(topic=config.sns_error_logs, message=str(sns_msg),
                                                    subject="error_logs.entry")

          