from flask import jsonify
import json
import uuid
import time

# local imports
import config
from utilities.errors import invalid, unhandled
from utilities.helpers import success, get_db_connection
from models.square.Square import Square
from models.Items import Items
from models.MenuMappings import MenuMappings



class SquareItemSync():

  @classmethod
  def update_item(cls, merchantId, itemId, platform=None):
    try:
      print("Start update_item -> square...")

      connection, cursor = get_db_connection()
      
      platformType = platform.get("platformtype")
      storeId = platform.get("storeid")
      accessToken = platform.get("accesstoken")

      ### get menumappings
      menu_mapping = MenuMappings.get_menumappings(merchantId=merchantId, platformType=platformType)
      if len(menu_mapping) != 1:
        return invalid("error: none or more than 1 menu is assigned to square!")
      menu_mapping = menu_mapping[0]

      ### get item details
      item_details = Items.get_item_by_id(itemId)
      if not item_details:
        return invalid("error: item not found!!!")

      newItemPrice = int(float(item_details["itemUnitPrice"]) * 100)
      newItemStatus = False if int(item_details["itemStatus"]) == 0 else True
      newItemName = item_details["posName"] if item_details["posName"] else item_details["itemName"]

      ### remove catalog_last_update_time from metadata of menu_mapping
      # if menu_mapping.get("metadata") != None:
      #   cursor.execute("""UPDATE menumappings SET metadata=%s WHERE id=%s""", (None, menu_mapping['id']))
      #   connection.commit()


      '''
        TODO: Product Update On Square
      '''
      print("look for itemmappings...")

      ### get platformitemid from itemmappings
      cursor.execute("""SELECT * FROM itemmappings WHERE itemid=%s AND platformtype=%s""", (itemId, platformType))
      item_mappings = cursor.fetchall()
      print(len(item_mappings))

      # loop over item mappings
      for item_mapping in item_mappings:
        
        # get square item-variation details in order to get itemid
        squareItemVariationId = item_mapping["platformitemid"]
        res, data = Square.square_retrieve_catalog_object(accessToken, squareItemVariationId)
        if not res:
          print("error: while getting item varaition details from square!")
          continue
        
        # get square item details
        squareItemId = squareItemId = data.get("object").get("item_variation_data").get("item_id")
        res, data = Square.square_retrieve_catalog_object(accessToken, squareItemId)
        if not res:
          print("error: while getting item details from square!")
          continue


        # item status update
        data["object"]["present_at_all_locations"] = newItemStatus
        data["object"].pop("absent_at_location_ids", None)

        # item variation price and status update
        for variation in data["object"]["item_data"]["variations"]:
          if variation["id"] == squareItemVariationId:
            variation["item_variation_data"]["price_money"]["amount"] = newItemPrice
            variation["present_at_all_locations"] = newItemStatus
            variation.pop("absent_at_location_ids", None)
        
        # update product...
        print("update product...")
        payload = {
          "idempotency_key": str(uuid.uuid4()),
          "object": data.get("object")
        }
        res, data = Square.square_upsert_catalog_objects(accessToken, payload)
        if not res:
          print("error: while updating product price and status on square!")
          continue


      '''
        TODO: Addon Option Update OR Delete From Square
      '''
      print("look for addonmappings...")

      ### get square addons in whom the particular item exists
      cursor.execute("""
        SELECT * FROM addonmappings WHERE addonid IN (
          SELECT addonid FROM addonsoptions WHERE itemid=%s
        ) AND platformtype=%s AND addonoptionid IS NULL;
      """, (itemId, platformType))
      addons_mappings = cursor.fetchall()
      print(f"item found in <{len(addons_mappings)}> addons on square")

      for addon_mapping in addons_mappings:

        # check if square_addon_option_id exists in addonmappings table
        cursor.execute("""SELECT * FROM addonmappings WHERE addonid=%s AND addonoptionid=%s AND platformtype=%s""", (addon_mapping["addonid"], itemId, platformType))
        addon_option = cursor.fetchone()
        
        if addon_option:
          # if addon_option is already available on square then;
          # a. check if status of addon option is changed to disable then delete addon option from square
          # b. else update price on square for addon_option
          print("addon_option exist on square")
          if newItemStatus == False:
            # delete addon_option from square and delete addon_option entry from addonmappings table
            print("addon_option status is DISABLE. deleting addon option from square addon...")
            catalog_ids_list = {
              "object_ids": [
                addon_option["platformaddonid"]
              ]
            }

            res, data = Square.square_batch_delete_catalog_objects(accessToken, payload=catalog_ids_list)
            if not res:
              print("error: while deleting addon_option from square")
              continue
            cursor.execute("""DELETE FROM addonmappings WHERE id=%s""", addon_option["id"])
            connection.commit()

          else:
            # get addon details from square and loop over addon_options and update specific addon_option price
            print("updating addon_option price on square...")
            res, data = Square.square_retrieve_catalog_object(accessToken, addon_mapping["platformaddonid"])
            if not res:
              print("error: while retrieving addon data from square!")
              continue
            for square_addon in data["object"]["modifier_list_data"]["modifiers"]:
              if square_addon["id"] == addon_option["platformaddonid"]:
                square_addon["modifier_data"]["price_money"]["amount"] = newItemPrice
                break
            payload = {
              "idempotency_key": str(uuid.uuid4()),
              "object": data.get("object")
            }
            res, data = Square.square_upsert_catalog_objects(accessToken, payload)
            if not res:
              print("error: while updating addon option price on square!")
              continue

        else:
          # if addon_option does not exists on square
          # a. check if status of addon_option is changed to enable then create addon_option on square
          # and store its id in addonmappings table
          print("addon_option does not exists in square addon")
          if newItemStatus == True: 
            # create addon_option for that addon on square and store its id in addonmappings table
            print("creating addon_option in square addon...")

            # get modifier details from square
            res, data = Square.square_retrieve_catalog_object(accessToken, addon_mapping["platformaddonid"])
            if not res:
              print("error: while getting addon details from square!")
              continue

            # append new modifier
            data["object"]["modifier_list_data"]["modifiers"].append({
              "type": "MODIFIER",
              "id": "#newmodifier",
              "present_at_all_locations": True,
              "modifier_data": {
                "name": newItemName,
                "price_money": {
                "amount": newItemPrice,
                "currency": "USD"
                },
              "modifier_list_id": addon_mapping["platformaddonid"]
              }
            })

            data = {
              "idempotency_key": str(uuid.uuid4()),
              "object": data["object"]
            }

            # post updated data to square
            res, data = Square.square_upsert_catalog_objects(accessToken, data)
            if not res:
              print("error: while updating addon with new addon option to square!")
              continue

            # insert row into addonmappings table
            newAddonSquareId = None
            for ii in data.get("id_mappings"):
              if ii["client_object_id"] == "#newmodifier":
                newAddonSquareId = ii["object_id"]

            if not newAddonSquareId:
              print("error: new square addon id cannot be extracted from response payload")
              continue

            new_row = (str(uuid.uuid4()), merchantId, addon_mapping['menuid'], addon_mapping['addonid'], itemId, platformType, newAddonSquareId)
            cursor.execute("""
            INSERT INTO addonmappings
              (id, merchantid, menuid, addonid, addonoptionid, platformtype, platformaddonid)
              VALUES (%s,%s,%s,%s,%s,%s,%s)""", new_row)
            connection.commit()


      ### get and store the catalog_last_update_time in menu_mappings table
      print("searching for catalog objects to get last update time of catalogs...")
      payload = {
        "limit": 1
      }
      res, data = Square.square_search_catalog_objects(accessToken, payload)
      if not res:
        return invalid("error: occured while searching for latest_time (last update time) of catalog objects")   
      latest_update_time = data.get("latest_time")

      metadata = json.dumps({
          "catalog_last_update_time": latest_update_time
        })
      
      # store the returned menuId and taxRateId in menumappings table
      time.sleep(1)
      cursor.execute("""UPDATE menumappings SET metadata=%s WHERE id=%s""", (metadata, menu_mapping['id']))
      connection.commit()

      return success()
    except Exception as e:
      return unhandled(f"error: {str(e)}")