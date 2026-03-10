from flask import jsonify
import json

# local imports
import config
from utilities.errors import unhandled
from utilities.helpers import success, get_db_connection
from models.ubereats.UberEats import UberEats
from models.Items import Items



class UberEatsItemSync():

  @classmethod
  def change_item_status(cls, merchant_obj, itemId, platform=None):
    try:
      print("item status_change -> ubereats...")

      connection, cursor = get_db_connection()
      storeId = platform.get('storeid')

      # check and refresh access token
      accessToken = UberEats.ubereats_check_and_get_access_token()
      if not accessToken:
        return unhandled("unhandled exception while checking ubereats access token")
      print(accessToken)

      # check if item is assigned to any menu in ubereats
      cursor.execute("""SELECT * FROM itemmappings WHERE merchantid=%s AND platformtype=%s""", (merchant_obj["syncMerchantId"], platform["platformtype"]))
      itemmapping_row = cursor.fetchone()
      if not itemmapping_row:
        return success()
      all_items_ids = json.loads(itemmapping_row["metadata"])
      if itemId not in all_items_ids:
        print("this item is not assigend to ubereats menu. skipped")
        return success()

      # get item by id
      item_details = Items.get_item_by_id(itemId)
      itemPrice = None
      if type(item_details["itemPriceMappings"]) is list:
        for r in item_details["itemPriceMappings"]:
          if r.get("platformType") == platform['platformtype']:
            itemPrice = r.get("platformItemPrice")
            break
      if itemPrice is None:
        itemPrice = item_details["itemUnitPrice"]
      
      # check item status
      if item_details['itemStatus'] == 0:
        payload = json.dumps({
          'price_info': {
            'price': int(float(itemPrice) * 100),
            'overrides': []
          },
          "suspension_info": {
            "suspension": {
              "suspend_until": 2147483647,
              "reason": "sold out"
            }
          }
        })
      else:
        payload = json.dumps({
          'price_info': {
            'price': int(float(itemPrice) * 100),
            'overrides': []
          },
          "suspension_info": {
            "suspension": {
              "suspend_until": 0,
              "reason": "sold out"
            }
          }
        })

      upd_resp, status_code, msg = UberEats.ubereats_update_item(
        storeId=storeId,
        itemId=itemId,
        accessToken=accessToken,
        payload=payload
      )

      print(status_code)
      print(msg)

      if not upd_resp:
        return unhandled()

      return success()
    except Exception as e:
      return unhandled(f"error: {e}")