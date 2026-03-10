import json

# local imports
from utilities.helpers import get_db_connection
from models.Items import Items
from models.MenuMappings import MenuMappings
from models.Platforms import Platforms
from models.square.Square import Square
from utilities.sns_triggers import trigger_item_update_sns_topic


class SquareCatalogWebhook():

  #########################################################

  @classmethod
  def handle_catalog_webhook_event(cls, event):
    try:
      connection, cursor = get_db_connection()

      # get platform details
      squareMerchantId = event.get("merchant_id")
      platform = Platforms.get_platform_by_storeid(squareMerchantId)

      # get menumappings details
      menu_mapping = MenuMappings.get_menumappings(merchantId=platform["merchantid"],
                                                   platformType=platform["platformtype"])
      if len(menu_mapping) != 1:
        print("error: none or more than 1 menu is assigned to square!")
        return False
      menu_mapping = menu_mapping[0]

      metadata = json.loads(menu_mapping.get("metadata")) if menu_mapping.get("metadata") else {}
      catalog_last_update_time = metadata.get("catalog_last_update_time")
      if not catalog_last_update_time:
        print("catalog_last_update_time not update in menumapping metadata yet")
        return False
      print("catalog_last_update_time: ", catalog_last_update_time)

      # search for catalog objects - square
      payload = {
        "limit": 1000,
        "begin_time": catalog_last_update_time,
        "object_types": [
          "ITEM"
        ],
        "include_deleted_objects": True
      }
      # "object_types": ["MODIFIER","ITEM"],

      res, data = Square.square_search_catalog_objects(platform["accesstoken"], payload)
      if not res:
        print("error: while searching for catalog objects on square!")
        return False
      print("data is " , data)

      ### update the latest time in menumappings table
      metadata = json.dumps({
        "catalog_last_update_time": data.get("latest_time")
      })


      cursor.execute("""UPDATE menumappings SET metadata=%s WHERE id=%s""", (metadata, menu_mapping['id']))
      connection.commit()

      ### loop over catalogs
      if not data.get("objects"):
        return True
      for catalog in data.get("objects"):
        send_sns = False
        catalogType = catalog.get("type")
        if catalogType == "ITEM" or catalogType == "MODIFIER":

          # if catalogType == "ITEM":
          # ITEM TYPE
          catalogId = catalog.get("id")
          print('catalogId ', catalogId)
          newItemStatus = 0 if catalog.get("absent_at_location_ids") and platform.get('accountid') in catalog.get("absent_at_location_ids") else 1
          print('newItemStatus ', newItemStatus)
          query = """ 
            SELECT items.* FROM itemmappings
              LEFT JOIN items ON itemmappings.itemid = items.id
              WHERE itemmappings.platformitemid = %s AND itemmappings.platformtype = 11
            """
          # else:
          #   # MODIFIER type
          #   catalogId = catalog.get("id")
          #   newItemStatus = 0 if catalog.get("is_deleted") is not None and catalog.get("is_deleted") == True else 1
          #   query = """
          #     SELECT items.* FROM addonmappings
          #       LEFT JOIN items ON addonmappings.addonoptionid = items.id
          #       WHERE addonmappings.platformaddonid = %s AND addonmappings.platformtype = 11
          #     """

          cursor.execute(query, (catalogId))
          item_details = cursor.fetchone()
          print('item_details' , item_details)
          if not item_details:
            continue
          newItemPrice = (catalog['item_data']['variations'][0]['item_variation_data']['price_money']['amount']) / 100
          oldItemPrice = float(item_details['itemprice'])
          if newItemPrice != oldItemPrice:
            payload = (newItemPrice, item_details['id'])
            cursor.execute("""UPDATE items SET itemprice= %s, updated_datetime=CURRENT_TIMESTAMP WHERE id=%s""",
                           payload)
            connection.commit()
            send_sns = True
          #################  In future we wil publish the sns for  notification for item price update

          oldItemStatus = int(item_details.get("status"))

          # print(item_details)
          print("old item status: ", str(oldItemStatus))
          print("new item status: ", str(newItemStatus))

          if oldItemStatus != newItemStatus:
            payload = (newItemStatus, item_details['id'])
            cursor.execute("""UPDATE items SET status= %s, updated_datetime=CURRENT_TIMESTAMP WHERE id=%s""", payload)
            connection.commit()
            send_sns = True

          # Triggering Item SNS - item.update
          if send_sns == True:
            old_item_details = {
              "itemUnitPrice": oldItemPrice,
              "itemType": item_details.get("itemtype"),
              "itemDescription": item_details.get("itemdescription"),
              "shortName": item_details.get("shortname"),
              "posName": item_details.get("posname"),
              "itemName": item_details.get("itemname")
            }

            sns_resp = trigger_item_update_sns_topic(
              merchantId=platform['merchantid'],
              itemId=item_details['id'],
              userId=None,
              unchanged=['pos'],
              oldItemStatus=oldItemStatus,
              old_item_details=old_item_details,
              source="Square"

            )

          # # delete from addon_mappings if type is MODIFIER and newItemStatus is 0
          # if catalogType == "MODIFIER" and newItemStatus == 0:
          #   cursor.execute("""DELETE FROM addonmappings WHERE platformaddonid = %s""", (catalogId))
          #   connection.commit()

      return True
    except Exception as e:
      print("error: ", str(e))
      return False

# https://developer.squareup.com/docs/catalog-api/webhooks