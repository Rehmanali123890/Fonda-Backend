import json
import uuid
import boto3
import requests
from flask import jsonify, request, g, Flask

from models.MenuMappings import MenuMappings
from models.Platforms import Platforms
from utilities.helpers import publish_sns_message, openDbconnection, get_db_connection
import config
from utilities.helpers import success
import math

app = Flask(__name__)


def upload_menu_image_to_google(localURL, account, location,category, access_token):
  if localURL:
    url = config.google_business_review_base + "v4/" + account + "/" + location + "/media"
    print(" ------------ upload_menu_image_to_google ------- " , url)
    print("------------ upload_menu_image_to_google-------------")
    import json
    payload = json.dumps({
      "mediaFormat": "PHOTO",
      "locationAssociation": {
        "category": category
      },
      "sourceUrl": localURL
    })
    headers = {
      'Authorization': 'Bearer ' + access_token,
      '1': 'application/json'
    }
    import requests
    response = requests.request("POST", url, headers=headers, data=payload)
    print("response " , response)
    if category == "MENU":
      image_key = json.loads(response.text)["name"].split("/")[-1]
      return [image_key]
    return response

  else:
    return []

  # menu.upload_google


def uploadMenuToGoogle(event, context):
  with app.app_context():
    print("---------------------- menu_google_upload_event is triggered -------------------------")
    print(event)
    ipAddr = message.get("body").get("ipAddr")
    for record in event['Records']:
      subject = record.get("Sns").get("Subject")
      message = eval(record.get("Sns").get("Message"))
      print("Subject: ", subject)
      if subject == "menu.upload_google":
        print(" if subject == menu.upload_google: true ")
        location = message.get("body").get("locationId")
        account = message.get("body").get("accountId")
        merchantId = message.get("body").get("merchantId")
        menu_id = message.get("body").get("menu_id")
        token = Platforms.getGoogleToken(merchantId)
        connection, cursor = openDbconnection()
        print("after db connection ")

        cursor.execute("""SELECT * FROM menus 
                 WHERE id = %s """, (menu_id))
        menu = cursor.fetchone()
        print("menu  is ", menu)
        payload_menu = []
        payload_items = []
        category_payload = []

        cursor.execute("""SELECT categories.id id, categories.categoryname categoryName, categories.posname posName, categories.categorydescription categoryDescription, categories.status status 
                    FROM menucategories, categories
                    WHERE menucategories.categoryid=categories.id AND menucategories.menuid=%s  order by sortid asc """, (menu["id"]))
        categories = cursor.fetchall()
        print("categories ", categories)
        category_payload = []
        for category in categories:
          cursor.execute(
            "SELECT productscategories.productid id, items.itemname itemName, items.itemdescription itemDescription, items.itemsku itemSKU, convert(items.itemprice, CHAR) itemPrice, imageurl imageUrl, items.status itemStatus FROM productscategories, items WHERE items.id = productscategories.productid AND productscategories.categoryid = %s   order by sortid asc",
            (category['id']))
          allItems = cursor.fetchall()
          for item in allItems:
            item_dict = {}
            item_dict = {
              "labels": {
                "displayName": item["itemName"],
                "description": item["itemDescription"],
                "languageCode": "en"
              },
              "attributes": {
                "price": {
                  "currencyCode": "USD",
                  "units": int(math.modf(float(item["itemPrice"]))[1]),
                  "nanos": int(math.modf(float(item["itemPrice"]))[0] * 1000000000)
                }
              },
              # "options": payload_options
            }
            if item["imageUrl"] is not None:
              item_dict["attributes"]["mediaKeys"] = upload_menu_image_to_google(item["imageUrl"], account, location,
                                                                                 'MENU', token)
            payload_items.append(item_dict)
            payload_options = []
          if len(payload_items) != 0:
            category_payload.append({
              "labels": {
                "displayName": category["categoryName"],
                "languageCode": "en"
              },
              "items": payload_items
            })
          payload_items = []

        payload_menu.append({
          # "cuisines": [
          #   "AMERICAN"
          # ],
          "labels": [
            {
              "displayName": menu["name"],
              "description": menu["description"],
              "languageCode": "en"
            }
          ],
          "sections": category_payload
        })
        # payload_menu=[]

        url = config.google_business_review_base + "v4/" + account + "/" + location + "/foodMenus"
        #
        payload = json.dumps({
          "menus": payload_menu
        })
        headers = {
          'Authorization': 'Bearer ' + token,
          'Content-Type': 'application/json'
        }
        print("payload ", payload)
        response = requests.request("PATCH", url, headers=headers, data=payload)
        print("response" , response)

        sns_msg = {
          "event": "merchant.sync_gmb_business_hours",
          "body": {
            "merchantId": merchantId,
            "userId": message.get("body").get("user_id"),
            "eventDetails": f"Menu synced to GMB, IP adress:{ipAddr}",
            "eventType": "activity",
            "eventName": "GMB.Menu_Sync"
          }
        }

        publish_sns_message(topic=config.sns_activity_logs, message=str(sns_msg),
                            subject="GMB_activity_logs")

        return True



def update_verification_status(event, context):
    try:
        connection, cursor = openDbconnection()
        cursor.execute("SELECT * FROM googlelocations")
        rows = cursor.fetchall()

        # init sqs client
        sqs_client = boto3.resource('sqs')
        queue = sqs_client.get_queue_by_name(QueueName=config.sqs_gmb_verification)
        messageGroupId = str(uuid.uuid4())


        for location in rows:

          dataObj = {
            "event": "gmb.verification_status",
            "location": location
          }

          response = queue.send_message(
            MessageBody=json.dumps(dataObj),
            MessageGroupId=messageGroupId,
            MessageDeduplicationId=str(uuid.uuid4())
          )
          print(response)


    except Exception as e:
      print("Error: ", str(e))



def Gmb_verification_status_event(event, context):
  print("----------------------- GMB Verification status update Queue --------------------------")

  with app.app_context():
    print(event)

    for record in event['Records']:
      try:
        connection, cursor = openDbconnection()
        print('message body: ' + record["body"])
        message = json.loads(record["body"])

        subject = message.get("event")
        print(subject)
        location = message.get("location")

        token = Platforms.getGoogleToken(location['merchantid'])
        url = config.google_business_verification_base + "v1/" + location['locationid'] + "/VoiceOfMerchantState"

        payload = {}
        headers = {
          'Authorization': 'Bearer ' + token
        }

        response = requests.request("GET", url, headers=headers, data=payload)
        response = response.json()
        # print(response)

        status = 0

        if 'verify' in response:
          status = 1
        elif 'complyWithGuidelines' in response:
          status = 2
        elif 'resolveOwnershipConflict' in response:
          status = 4
        elif 'hasVoiceOfMerchant' in response and 'hasBusinessAuthority' in response:
          status = 3

        cursor.execute("UPDATE googlelocations SET status = %s WHERE locationid = %s",
                       (status, location['locationid']))
        connection.commit()


      except Exception as e:
        print("Error: ", str(e))

