import config
from app import app
import json
import datetime

from models.Items import Items
from models.Categories import Categories
# local imports
from utilities.helpers import get_db_connection, create_log_data, publish_sns_message
from models.ItemServiceAvailability import ItemServiceAvailability
from models.Addons import Addons
from datetime import datetime, timezone
from dateutil.tz import gettz

from controllers.ItemsController import *
from controllers.CategoriesController import *
import requests

apiUrl = config.api_url

# if config.env == 'development':
#   apiUrl = "https://api-dev.mifonda.io/"
# elif config.env == 'test':
#   apiUrl = "https://api-test.mifonda.io/"
# elif config.env == 'production':
#   apiUrl = "https://c9l5y8ajra.execute-api.us-west-1.amazonaws.com/prod/"

# def get_minutes_from_time(time: str) -> int:
#   hours, minutes = map(int, time.split(':'))
#   return hours * 60 + minutes

def item_menu_hours(event, context): 
 with app.app_context():
  message = event
  itemId = message.get('itemId')
  connection, cursor = get_db_connection()
  # weekDay = message.get('weekDay')
  itemStatus = message.get('status')
  print(itemStatus)
  item = Items.get_item_by_id(itemId)
  if item:
    userId = '8c1e409d-5c17-4def-8f86-6c092aceb2b4'
    cursor.execute("SELECT token FROM userslogin WHERE userid=%s", userId)
    userLoginrow = cursor.fetchone()
    token = ''
    if userLoginrow:
      token = userLoginrow['token']
    merchantId = message.get('merchantId')
    # mer_timezone = message.get('timezone')
        
    itemStatus = 1 if itemStatus == 'Active' else 0
    print(itemStatus)
    print(item['itemStatus'])
    if itemStatus == 1 :
      if item['itemStatus'] == 1:
          print("Item already active")
          create_log_data(level='[INFO]',
          Message=f"Item already active ",messagebody=item,
          functionName="item_menu_hours")
      else:
         api_url = "{apiUrl}merchant/{merchantId}/item/{itemId}/status"

         # Set up headers and JSON payload
         headers = {
             "Authorization": f"Bearer {token}",  # Replace with an actual token
             "content-type": "application/json",
             "correlationid": "1743050505",
             "x-api-key": f"{config.api_key}"
         }
         
         data = {
             "itemStatus": itemStatus
         }
         
         response = requests.put(api_url.format(merchantId=merchantId, itemId=itemId,apiUrl=apiUrl), json=data, headers=headers)

         print(response.text)
    else :
      if item['itemStatus'] == 0:
          print("Item already inactive")
          create_log_data(level='[INFO]',
          Message=f"Item already inactive",messagebody=item,
          functionName="item_menu_hours")
      else:
         api_url = "{apiUrl}merchant/{merchantId}/item/{itemId}/status"

         # Set up headers and JSON payload
         headers = {
             "Authorization": f"Bearer {token}",  # Replace with an actual token
             "content-type": "application/json",
             "correlationid": "1743050505",
             "x-api-key": f"{config.api_key}"
         }
         
         data = {
             "itemStatus": itemStatus
         }
         
         response = requests.put(api_url.format(merchantId=merchantId, itemId=itemId,apiUrl=apiUrl), json=data, headers=headers)

         print(response.text)

    # today = datetime.utcnow().replace(tzinfo=timezone.utc).astimezone(gettz(mer_timezone))
    # opening_hours = ItemServiceAvailability.get_serviceAvailabilityByitemIdWeekday(itemId=itemId, weekDay=weekDay)
    
    # if opening_hours and "startTime" in opening_hours and "endTime" in opening_hours:
    #   start_time = opening_hours['startTime']
    #   end_time = opening_hours['endTime']
    #   requestobj = {
    #       itemStatus:itemStatus
    #   }

    #   # Convert to minutes
    #   open_time = get_minutes_from_time(start_time)
    #   close_time = get_minutes_from_time(end_time)
    #   current_time = today.hour * 60 + today.minute
      
    #   itemStatus = 1 if itemStatus == 'Active' else 0
    #   if open_time <= current_time <= close_time: 
    #     if itemStatus == 1 :
    #       if item['itemStatus'] == 1:
    #           print("Item already active")
    #           create_log_data(level='[INFO]',
    #           Message=f"Item already active ",messagebody=item,
    #           functionName="item_menu_hours")
    #       else:
    #         updateMerchantItemStatus(merchantId=merchantId,itemId=itemId,_json=requestobj)            
    #     else :
    #       if item['itemStatus'] == 0:
    #           print("Item already inactive")
    #           create_log_data(level='[INFO]',
    #           Message=f"Item already inactive",messagebody=item,
    #           functionName="item_menu_hours")
    #       else:
    #           updateMerchantItemStatus(merchantId=merchantId,itemId=itemId,_json=requestobj)
    #   else:
    #     if item['itemStatus'] == 0:
    #         print("Item already inactive")
    #         create_log_data(level='[INFO]',
    #         Message=f"Item already inactive",messagebody=item,
    #         functionName="item_menu_hours")
    #     else:
    #         updateMerchantItemStatus(merchantId=merchantId,itemId=itemId,_json=requestobj)


def category_menu_hours(event, context): 
 with app.app_context():
  message = event
  print('message ' , message)
  categoryId = message.get('categoryId')
  connection, cursor = get_db_connection()
  # weekDay = message.get('weekDay')
  categoryStatus = message.get('status')
  print(categoryStatus)
  category = Categories.get_category_by_id_str(categoryId)
  if category:

    userId = '8c1e409d-5c17-4def-8f86-6c092aceb2b4'
    cursor.execute("SELECT token FROM userslogin WHERE userid=%s", userId)
    userLoginrow = cursor.fetchone()
    token = ''
    if userLoginrow:
      token = userLoginrow['token']
    merchantId = message.get('merchantId')
    # mer_timezone = message.get('timezone')
        
    categoryStatus = 1 if categoryStatus == 'Active' else 0
    print(categoryStatus)
    print(category['categoryStatus'])
    if categoryStatus == 1 :

        api_url = "{apiUrl}merchant/{merchantId}/category/{categoryId}/status"

         # Set up headers and JSON payload
        headers = {
            "Authorization": f"Bearer {token}",  # Replace with an actual token
            "content-type": "application/json",
            "correlationid": "1743050505",
            "x-api-key": f"{config.api_key}"
        }
        
        data = {
            "categoryStatus": categoryStatus
        }
        print('Updating category to active')
        response = requests.put(api_url.format(merchantId=merchantId, categoryId=categoryId,apiUrl=apiUrl), json=data, headers=headers)

        print(response.text)           
    else :
      if category['categoryStatus'] == 0:
        print("Category already inactive")
        create_log_data(level='[INFO]',
        Message=f"Category already inactive",messagebody=category,
        functionName="category_menu_hours")
      else:
        api_url = "{apiUrl}merchant/{merchantId}/category/{categoryId}/status"

         # Set up headers and JSON payload
        headers = {
            "Authorization": f"Bearer {token}",  # Replace with an actual token
            "content-type": "application/json",
            "correlationid": "1743050505",
            "x-api-key": f"{config.api_key}"
        }
        
        data = {
            "categoryStatus": categoryStatus
        }
        
        response = requests.put(api_url.format(merchantId=merchantId, categoryId=categoryId,apiUrl=apiUrl), json=data, headers=headers)

        print(response.text)


def auto_resume_merchant_store(event, context):
  with app.app_context():
    message = event
    create_log_data(level='[INFO]',
                    Message=f"-------------------------  In the start of auto_resume_merchant_store scheduler   --------------------"
                    , messagebody=message,
                    functionName="auto_resume_merchant_store")
    merchantId = message.get('merchantId')
    scheduleName = message.get('scheduleName')
    connection, cursor = get_db_connection()
    merchant_detail = Merchants.get_merchant_by_id(merchantId)
    userId = '8c1e409d-5c17-4def-8f86-6c092aceb2b4'
    if merchant_detail and merchant_detail.get('marketstatus') == 0:
      cursor.execute("SELECT token FROM userslogin WHERE userid=%s", userId)
      userLoginrow = cursor.fetchone()
      token = ''
      if userLoginrow:
        token = userLoginrow['token']
        api_url = "{apiUrl}merchant/{merchantId}/market-status"

        # Set up headers and JSON payload
        headers = {
          "Authorization": f"Bearer {token}",  # Replace with an actual token
          "content-type": "application/json",
          "correlationid": "1743050505",
          "x-api-key": f"{config.api_key}"
        }

        data = {
          "marketStatus":True,
          "caller":"dashboard"
        }
        response = requests.put(api_url.format(merchantId=merchantId, apiUrl=apiUrl), json=data,
                                headers=headers)

        create_log_data(level='[INFO]',
                        Message=f"Response on updating merchant store status to active."
                        , messagebody=f"Status Code : {response.status_code} , Response : {response.text}",
                        functionName="auto_resume_merchant_store", merchantID=merchantId)
      else:
        create_log_data(level='[ERROR]',
                        Message=f"User token not found",
                        functionName="auto_resume_merchant_store", merchantID=merchantId)
    else:
      create_log_data(level='[INFO]',
                      Message=f"Merchant market status is already active, so skipping the API call."
                      , messagebody=merchant_detail,
                      functionName="auto_resume_merchant_store", merchantID=merchantId)
    #Deleting scheduler
    is_deleted , msg =Merchants.delete_merchant_scheduler(scheduleName)
    if is_deleted:
      create_log_data(level='[INFO]',
                      Message=f"Scheduler Deleted Successfully."
                      , messagebody=f" Scheduler name: {scheduleName}",
                      functionName="auto_resume_merchant_store", merchantID=merchantId)
      sns_msg = {
        "event": "merchant.auto_resume_merchant_scheduler",
        "body": {
          "merchantId": merchantId,
          "userId": userId,
          "schedulerDetail": f"Scheduler Deleted Successfully. Scheduler name: {scheduleName}"
        }
      }
      logs_sns_resp = publish_sns_message(topic=config.sns_activity_logs, message=str(sns_msg),
                                          subject="merchant.auto_resume_merchant_scheduler")

    else:
      create_log_data(level='[ERROR]',
                      Message=f"Error on deleting auto resume merchant store scheduler"
                      , messagebody=f'Error:  {msg} , name : {scheduleName}',
                      functionName="auto_resume_merchant_store", merchantID=merchantId)
      sns_msg = {
        "event": "error_logs.entry",
        "body": {
          "userId": userId,
          "merchantId": merchantId,
          "errorName": 'Error on deleting auto resume merchant store scheduler',
          "errorSource": 'dashboard',
          "errorStatus": 400,
          "errorDetails": f' {msg} , name : {scheduleName}'
        }
      }
      error_logs_sns_resp = publish_sns_message(topic=config.sns_error_logs, message=str(sns_msg),
                                                subject="error_logs.entry")