
import uuid
import json

from config import sns_item_notification
from utilities.errors import invalid, unhandled

# local imports
from utilities.helpers import get_db_connection, success, publish_sns_message
from models.AddonsOptions import AddonsOptions



class ProductsAddons():

  ############################################### GET

  @classmethod
  def get_productAddonsWithOptions(cls, itemId , freeitem=0,storefront=None , fromtab=None , merchantId=None):
    try:
      connection, cursor = get_db_connection()
      
      # get product addons
      query = """
                    SELECT 
                        addons.id id, 
                        addonname addonName, 
                        addondescription addonDescription,
                        minpermitted minPermitted,
                        maxpermitted maxPermitted,
                        productsaddons.sortid sortId
                    FROM productsaddons, addons 
                    WHERE productsaddons.addonid = addons.id 
                    AND productsaddons.productid = %s
                """

      # Conditionally add the status filter only if storefront is not None
      if storefront is not None:
        query += " AND addons.status = 1"

      # Add the ORDER BY clause
      query += " ORDER BY productsaddons.sortid ASC"

      cursor.execute(query, (itemId,))
      addons = cursor.fetchall()
      if freeitem ==1:
        return addons
      for addon in addons:
        addon["addonOptions"] = []
        addonId = addon["id"]
        addonOptions = AddonsOptions.get_addonOptions(addonId=addonId ,fromtab=fromtab,merchantId=merchantId)
        if addonOptions:
          addon["addonOptions"] = addonOptions

      return addons
    except Exception as e:
      print(str(e))
      return False
  

  @classmethod
  def get_item_addon(cls, itemId=None, addonId=None):
    try:
      connection, cursor = get_db_connection()
      if itemId is not None:
        cursor.execute("SELECT * FROM productsaddons WHERE productid = %s", (itemId))
      elif addonId is not None:
        cursor.execute("SELECT * FROM productsaddons WHERE addonid = %s", (addonId))      
      rows = cursor.fetchall()
      return rows
    except Exception as e:
      print(str(e))
      return False
      
  
  ############################################### POST
  
  @classmethod
  def post_item_addon(cls, itemId, addonId, userId=None , merchantId=None):
    try:
      connection, cursor = get_db_connection()
      if isinstance(itemId, str):
        cursor.execute("SELECT COALESCE(MAX(sortid), 0) as maxSortId FROM productsaddons WHERE productid = %s", (itemId))
        row = cursor.fetchone()
        sortId = row.get("maxSortId") + 1
      if isinstance(addonId, list):
        for addon in addonId:
          guid = uuid.uuid4()
          data = (guid, itemId, addon, sortId, userId)
          cursor.execute(
            "INSERT INTO productsaddons (id, productid, addonid, sortid, created_by) VALUES (%s,%s,%s,%s,%s)", data)
          connection.commit()
          sortId=sortId+1
          print("Triggering item sns...")
          sns_msg = {
            "event": "item.assign_addon",
            "body": {
              "merchantId": merchantId,
              "itemId": itemId,
              "addonId": addon,
              "userId": userId
            }
          }
          sns_resp = publish_sns_message(topic=sns_item_notification, message=str(sns_msg),
                                         subject="item.assign_addon")
      elif isinstance(itemId, list):
        for item in itemId:
          cursor.execute("SELECT COALESCE(MAX(sortid), 0) as maxSortId FROM productsaddons WHERE productid = %s",
                         (item))
          row = cursor.fetchone()
          sortId = row.get("maxSortId") + 1
          guid = uuid.uuid4()
          data = (guid, item, addonId, sortId, userId)
          cursor.execute(
            "INSERT INTO productsaddons (id, productid, addonid, sortid, created_by) VALUES (%s,%s,%s,%s,%s)", data)
          connection.commit()
          print("Triggering item sns...")
          sns_msg = {
            "event": "item.assign_addon",
            "body": {
              "merchantId": merchantId,
              "itemId": item,
              "addonId": addonId,
              "userId": userId
            }
          }
          sns_resp = publish_sns_message(topic=sns_item_notification, message=str(sns_msg),
                                         subject="item.assign_addon")
      else:
        guid = uuid.uuid4()
        data = (guid, itemId, addonId, sortId, userId)
        cursor.execute("INSERT INTO productsaddons (id, productid, addonid, sortid, created_by) VALUES (%s,%s,%s,%s,%s)", data)
        connection.commit()
        print("Triggering item sns...")
        sns_msg = {
          "event": "item.assign_addon",
          "body": {
            "merchantId": merchantId,
            "itemId": itemId,
            "addonId": addonId,
            "userId": userId
          }
        }
        sns_resp = publish_sns_message(topic=sns_item_notification, message=str(sns_msg),
                                       subject="item.assign_addon")

      return True
    except Exception as e:
      print("Error: ", str(e))
      return False

  ############################################### DELETE

  @classmethod
  def delete_item_addon(cls, id=None, itemId=None, addonId=None):
    try:
      connection, cursor = get_db_connection()
      if itemId and addonId:
        cursor.execute("DELETE FROM productsaddons WHERE productid=%s AND addonid=%s", (itemId, addonId))
      connection.commit()
      return True
    except Exception as e:
      print("Error: ", str(e))
      return False
  
  ############################################### OTHER

  @classmethod
  def sort_item_addons(cls, merchantId, itemId, addons):
    try:
      connection, cursor = get_db_connection()

      if not len(addons):
        return invalid("invalid request")

      cursor.execute("SELECT * FROM items WHERE id = %s", (itemId))
      row = cursor.fetchone()
      if not row or row["merchantid"] != merchantId:
        return invalid("invalid request")

      data = list(tuple())
      for row in addons:
        data.append((row["sortId"], itemId, row["addonId"]))

      cursor.executemany("""
        UPDATE productsaddons 
          SET sortid = %s
          WHERE productid = %s AND addonid = %s
      """, (data))

      connection.commit()

      return success()
    except Exception as e:
      return unhandled(f"error: {e}")

  