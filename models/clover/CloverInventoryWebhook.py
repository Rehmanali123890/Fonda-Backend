import config
from models.Items import Items
# local imports
from utilities.helpers import get_db_connection, publish_sns_message
from utilities.sns_triggers import trigger_item_update_sns_topic
from models.clover.Clover import Clover



class CloverInventoryWebhook():

  #########################################################
  #########################################################
  #########################################################

  @classmethod
  def trigger_inventory_event(cls, platform, event):
    try:
      print('----------trigger_inventory_event ----------')
      connection, cursor = get_db_connection()

      if event['objectId'].startswith("I:"):
        objectId = event['objectId'].split("I:")[1]
      else:
        objectId = event['objectId'].split("IM:")[1]
      item_id=''
      
      print("objectId" , objectId)
      platform["accesstoken"], msg, is_error = Clover.generate_clover_access_token(platform)
      if is_error:
        print(msg)
        return False
      # search for object by clover assigned id in itemmappings and addonmappings table
      cursor.execute("""SELECT * FROM itemmappings WHERE platformitemid=%s and merchantid=%s""", (objectId , platform['merchantid']))
      item_mapping = cursor.fetchone()
      if item_mapping:
        item_id=item_mapping['itemid']
      else:
        cursor.execute("""SELECT addonmappings.addonoptionid ,items.status  FROM addonmappings join items on addonmappings.addonoptionid= items.id WHERE addonmappings.platformaddonid=%s and addonmappings.merchantid=%s""", (objectId, platform['merchantid']))
        addon_mapping = cursor.fetchone()
        if addon_mapping:
          item_id=addon_mapping['addonoptionid']
        else:
          print('item not found')
          return True
      c_item_status=0
      c_item_price=0
      print('item_id' , item_id)
      if event['type'] == "UPDATE":
        if event['objectId'].startswith("I:"):
          clover_item = Clover.clover_get_item(cMid=platform['storeid'], cItemId=objectId, accessToken=platform['accesstoken'])
          if not clover_item:
            return False
          c_item_price = float(clover_item['priceWithoutVat'] / 100)
          c_item_status = 1 if clover_item['available'] is True else 0
        else:
          clover_item= event['object']
          c_item_price = float(clover_item['price'] / 100)
          c_item_status=addon_mapping['status']

        print(' c_item_price :' , c_item_price)
        print(' c_item_status :', c_item_status)
        # in order to avoid loophole, chk if status or price is changed
        row = Items.get_item_by_id(item_id)
        if row and float(row['itemUnitPrice']) == c_item_price and int(row['itemStatus']) == c_item_status:
          return True

        updResp = Items.put_itemById( merchantId=row['merchantid'], itemId=row['id'],
                                     itemPrice=c_item_price,
                                     itemStatus=c_item_status , frompos=True)
        if updResp:
          if float(row['itemUnitPrice']) != c_item_price:
            print('item price update')
            sns_msg = {
              "event": "item.update",
              "body": {
                "merchantId": row['merchantid'],
                "itemId": row['id'],
                "userId": '',
                "unchanged": None,
                "oldItemStatus": row['itemStatus'],
                "old_item_details": row,
                "source": "Clover",
                "ipAddr": ''
              }
            }
            sns_resp = publish_sns_message(topic=config.sns_item_notification, message=str(sns_msg),
                                           subject="item.update")
          if c_item_status != row['itemStatus']:
            print('item status update')
            # Triggering SNS
            eventName = "item.status_change"
            print(f"Triggering sns - {eventName} ...")
            sns_msg = {
              "event": eventName,
              "body": {
                "merchantId": row['merchantid'],
                "itemId": row['id'],
                "userId": '',
                "itemStatus": c_item_status,
                "ipAddr": ''
              }
            }
            sns_resp = publish_sns_message(topic=config.sns_item_notification, message=str(sns_msg), subject=eventName)
      
      return True
    except Exception as e:
        print("Error: ", str(e))
        return False

  
  @classmethod
  def update_item(cls):
    try:
      pass
    except Exception as e:
      print("Error: ", str(e))
      return False


# https://sandbox.dev.clover.com/oauth/token?client_id=25CCGWSAP52DE&client_secret=b9b2cd61-6a53-c7a2-4a5b-12626b79e49f&code=3f4b4e92-45c9-b417-c5f0-c81f549d115a