import uuid
import json

from models.MenuMappings import MenuMappings
from models.doordash.DoorDash import Doordash
# local imports
from models.flipdish.Flipdish import Flipdish
from models.ubereats.UberEats import UberEats
from models.clover.Clover import Clover
from models.square.Square import Square
from models.Stream import Stream
from utilities.errors import not_found, unhandled, unauthorised, invalid
from utilities.helpers import get_db_connection, success, publish_sns_message, create_log_data
import config



class Sync():

  @classmethod
  def trigger_manualSync(cls, merchantId, platformId, userId , request=None , ip_address=None):
    try:
      connection, cursor = get_db_connection()
      print("Triggering...")

      # get platform details by id
      cursor.execute("SELECT * FROM platforms WHERE id=%s", (platformId))
      row = cursor.fetchone()
      if row:
        platformType = row["platformtype"]
        
        if platformType == 2:
          print("Platform Type is 2. So posting flipdish menu...")
          return Flipdish.post_completeMenu(platformId)
        
        elif platformType == 3:
          print("Platform Type is 3. So posting Ubereats menu...")
          resp, status, msg = UberEats.post_complete_menu_ubereats(platformId)
          if not resp:

            # Triggering SNS -> error_logs.entry
            sns_msg = {
                "event": "error_logs.entry",
                "body": {
                    "userId": userId,
                    "merchantId": row['merchantid'],
                    "errorName": "Ubereats Manual Sync",
                    "errorSource": "dashboard",
                    "errorStatus": status,
                    "errorDetails": msg 
                }
            }
            error_logs_sns_resp = publish_sns_message(topic=config.sns_error_logs, message=str(sns_msg), subject="error_logs.entry")
          return success()
        elif platformType == 6:
          print("Platform Type is 6. So posting Doordash menu...")
          resp, status, msg = Doordash.post_complete_menu_doordash(platformId)
          if not resp:

            # Triggering SNS -> error_logs.entry
            sns_msg = {
                "event": "error_logs.entry",
                "body": {
                    "userId": userId,
                    "merchantId": row['merchantid'],
                    "errorName": "DoorDash Manual Sync",
                    "errorSource": "dashboard",
                    "errorStatus": status,
                    "errorDetails": msg
                }
            }
            error_logs_sns_resp = publish_sns_message(topic=config.sns_error_logs, message=str(sns_msg), subject="error_logs.entry")
          return success()

        elif platformType == 4:
          print("Platform Type is 4. So posting Clover menu...")
          return Clover.post_complete_menu_clover(platformId)

        elif platformType == 11:
          print("Platform Type is 11. So posting Square menu...")
          resp, status, msg = Square.post_complete_menu_square(platformId)
          if not resp:
            print(msg)
            sns_msg = {
                "event": "error_logs.entry",
                "body": {
                    "userId": userId,
                    "merchantId": row['merchantid'],
                    "errorName": "Square Manual Sync",
                    "errorSource": "dashboard",
                    "errorStatus": status,
                    "errorDetails": msg 
                }
            }
            error_logs_sns_resp = publish_sns_message(topic=config.sns_error_logs, message=str(sns_msg), subject="error_logs.entry")
          return success()
        
        elif platformType == 8:
          print("Platform Type is 8. So posting Stream menu...")
          menumappings = MenuMappings.get_menumappings(merchantId=merchantId, platformType=8)
          create_log_data(level='[INFO]',
                          Message=f"Getting menu mappings,IP address: {ip_address}", messagebody=menumappings,
                          functionName="trigger_manualSync", request=request)
          if len(menumappings) == 0:
            create_log_data(level='[INFO]',
                            Message=f"NO menu attached to this platform,IP address: {ip_address}",
                            messagebody=menumappings,
                            functionName="trigger_manualSync", request=request)
            return invalid("No menu attached to stream.")
          stream, status, msg = Stream.post_complete_menu_stream(merchantId , ip_address=ip_address)
          if not stream: 
            sns_msg = {
                "event": "error_logs.entry",
                "body": {
                    "merchantId": row['merchantid'],
                    "errorName": "Stream Manual Sync",
                    "errorSource": "dashboard",
                    "errorStatus": status,
                    "errorDetails": msg 
                }
            }
            error_logs_sns_resp = publish_sns_message(topic=config.sns_error_logs, message=str(sns_msg), subject="error_logs.entry")
          else:
            sns_msg = {
              "event": "merchant.stream_manual_menu_sync",
              "body": {
                "merchantId": merchantId,
                "userId": userId,
                "eventDetails": f"Menu sync to stream, IP address: {ip_address}"
              }
            }
            logs_sns_resp = publish_sns_message(topic=config.sns_activity_logs, message=str(sns_msg),
                                                subject="merchant.stream_manual_menu_sync")
          return success()

        else:
          return unauthorised("Platform Sync is not yet registered")
      else:
        return not_found("platformId is incorrect!")
    except Exception as e:
      print(str(e))
      return unhandled()
  

  @classmethod
  def trigger_downloadMenu(cls, merchantId, platformId, userId):
    try:
      connection, cursor = get_db_connection()

      cursor.execute("SELECT * FROM platforms WHERE id=%s", (platformId))
      platform_details = cursor.fetchone()

      if not platform_details:
        return not_found("platformId is incorrect!")
      
      platformType = platform_details["platformtype"]

      if platformType == 4:
        # clover
        platform_details["accesstoken"],msg,is_error=Clover.generate_clover_access_token(platform_details)
        if not is_error:
          resp, status, msg = Clover.download_complete_menu_clover(platform_details, userId)
        else:
          return invalid(msg)
        errorName = "Clover Menu Download"

      elif platformType == 11:
        # square
        resp, status, msg = Square.download_complete_menu_square(platform_details, userId)
        if msg=="error: no menu assigned to square!":
          return invalid("No menu assigned to square")
        errorName = "Square Menu Download"
      
      else:
        resp, status, msg, errorName = True, 200, "success", ""

      # if response was not success then do entry in error_logs
      if not resp:
        print(msg)
        sns_msg = {
          "event": "error_logs.entry",
          "body": {
            "userId": userId,
            "merchantId": platform_details['merchantid'],
            "errorName": errorName,
            "errorSource": "dashboard",
            "errorStatus": status,
            "errorDetails": msg 
          }
        }
        resp = publish_sns_message(topic=config.sns_error_logs, message=str(sns_msg), subject="error_logs.entry")

      return success()
    except Exception as e:
      return unhandled(str(e))