import uuid
import json

from config import sns_addon_notification
# local imports
from utilities.helpers import get_db_connection, success, publish_sns_message
from models.ItemPriceMappings import ItemPriceMappings
from utilities.errors import invalid, unhandled
import config


class AddonsOptions():

    ############################################### GET

    @classmethod
    def get_addonOptions(cls, addonId , fromtab=None,merchantId=None):
        try:
            connection, cursor = get_db_connection()

            cursor.execute("""SELECT 
          addonsoptions.itemid id,
          addonsoptions.sortid sortId, 
          items.itemname addonOptionName,
          items.posname posName,
          items.shortname shortName,
          items.itemdescription addonOptionDescription,
          itemsku addonOptionSKU, 
          convert(itemprice, CHAR) addonOptionPrice,
          items.status addonOptionStatus, 
          items.pause_type addonPauseType, 
          items.pause_time addonPauseTime, 
          items.resume_time addonResumeTime
        FROM addonsoptions, items
        WHERE items.id = addonsoptions.itemid and addonsoptions.addonid = %s
        ORDER BY addonsoptions.sortid ASC
        """, addonId)

            options = cursor.fetchall()

            for option in options:

                # get addon option price mappings
                priceMappings = ItemPriceMappings.get_itemPriceMappings(itemId=option["id"],merchantId=merchantId, fromtab=fromtab)
                if priceMappings and type(priceMappings) is list:
                    option["addonOptionPriceMappings"] = priceMappings
                else:
                    option["addonOptionPriceMappings"] = []

            return options

        except Exception as e:
            print(str(e))
            return False

    @classmethod
    def get_addonItems(cls, addonId):
        try:
            connection, cursor = get_db_connection()

            cursor.execute("""SELECT 
           productsaddons.productid id,
           productsaddons.sortid sortId, 
           items.itemname addonItemName,
           items.posname posName,
           items.shortname shortName,
           items.itemdescription addonItemDescription,
           itemsku addonOptionSKU, 
           convert(itemprice, CHAR) addonItemPrice,
           items.status addonItemStatus, 
           items.pause_type addonPauseType, 
           items.pause_time addonPauseTime, 
           items.resume_time addonResumeTime
         FROM productsaddons, items
         WHERE items.id = productsaddons.productid and productsaddons.addonid = %s
         ORDER BY productsaddons.sortid ASC
         """, addonId)

            items = cursor.fetchall()

            for item in items:

                # get addon option price mappings
                priceMappings = ItemPriceMappings.get_itemPriceMappings(itemId=item["id"])
                if priceMappings and type(priceMappings) is list:
                    item["addonItemPriceMappings"] = priceMappings
                else:
                    item["addonItemPriceMappings"] = []

            return items

        except Exception as e:
            print(str(e))
            return False
    ############################################### POST

    @classmethod
    def post_addon_option(cls, itemId, addonId, userId=None , merchantId=None , ip_address= None):
        try:
            connection, cursor = get_db_connection()

            if isinstance(addonId, str):
                cursor.execute("SELECT COALESCE(MAX(sortid), 0) as maxSortId FROM addonsoptions WHERE addonid = %s",
                               (addonId))
                row = cursor.fetchone()
                sortId = row.get("maxSortId") + 1

            if isinstance(itemId, list):
                for item in itemId:
                    guid = uuid.uuid4()
                    data = (guid, item, addonId, sortId, userId)
                    cursor.execute(
                        "INSERT INTO addonsoptions (id, itemid, addonid, sortid, created_by) VALUES (%s,%s,%s,%s,%s)",
                        data)
                    connection.commit()
                    sortId = sortId + 1
                    # Triggering Addon SNS - addon.assign_option
                    print("Triggering item sns - addon.assign_option ...")
                    sns_msg = {
                        "event": "addon.assign_option",
                        "body": {
                            "merchantId": merchantId,
                            "addonId": addonId,
                            "itemId": item,
                            "userId": userId,
                            "ipAddr": ip_address,
                        }
                    }
                    sns_resp = publish_sns_message(topic=sns_addon_notification, message=str(sns_msg),
                                                   subject="addon.assign_option")
                    
                    publish_sns_message(topic=config.sns_activity_logs, message=str(sns_msg),
                          subject="addon.assign_option")

            elif isinstance(addonId, list):
                for addon in addonId:
                    cursor.execute("SELECT COALESCE(MAX(sortid), 0) as maxSortId FROM addonsoptions WHERE addonid = %s",
                                   (addon))
                    row = cursor.fetchone()
                    sortId = row.get("maxSortId") + 1
                    guid = uuid.uuid4()
                    data = (guid, itemId, addon, sortId, userId)
                    cursor.execute(
                        "INSERT INTO addonsoptions (id, itemid, addonid, sortid, created_by) VALUES (%s,%s,%s,%s,%s)",
                        data)
                    connection.commit()
                    sortId = sortId + 1
                    # Triggering Addon SNS - addon.assign_option
                    print("Triggering item sns - addon.assign_option ...")
                    sns_msg = {
                        "event": "addon.assign_option",
                        "body": {
                            "merchantId": merchantId,
                            "addonId": addon,
                            "itemId": itemId,
                            "userId": userId,
                            "ipAddr": ip_address
                        }
                    }
                    sns_resp = publish_sns_message(topic=sns_addon_notification, message=str(sns_msg),
                                                   subject="addon.assign_option")
                    publish_sns_message(topic=config.sns_activity_logs, message=str(sns_msg),
                          subject="addon.assign_option")

            else:
                guid = uuid.uuid4()
                data = (guid, itemId, addonId, sortId, userId)
                cursor.execute(
                    "INSERT INTO addonsoptions (id, itemid, addonid, sortid, created_by) VALUES (%s,%s,%s,%s,%s)", data)
                connection.commit()
                # Triggering Addon SNS - addon.assign_option
                print("Triggering item sns - addon.assign_option ...")
                sns_msg = {
                    "event": "addon.assign_option",
                    "body": {
                        "merchantId": merchantId,
                        "addonId": addonId,
                        "itemId": itemId,
                        "userId": userId,
                        "ipAddr": ip_address
                    }
                }
                sns_resp = publish_sns_message(topic=sns_addon_notification, message=str(sns_msg),
                                               subject="addon.assign_option")
                publish_sns_message(topic=config.sns_activity_logs, message=str(sns_msg),
                          subject="addon.assign_option")
            return True
        except Exception as e:
            print("Error: ", str(e))
            return False

    @classmethod
    def Get_used_addons_items(cls, merchantId, addonId, userId=None):
        try:
            connection, cursor = get_db_connection()

            cursor.execute(f"""SELECT B.itemname FROM dashboard.productsaddons A
 inner join dashboard.items B on A.productid=B.id
 where A.addonid=%s and B.merchantid=%s""", (addonId, merchantId))
            rows = cursor.fetchall()
            return rows
        except Exception as e:
            print("Error: ", str(e))
            return False

    ############################################### DELETE

    @classmethod
    def Get_option_addonss(cls, optionId):
        try:
            connection, cursor = get_db_connection()

            cursor.execute("""SELECT addonid as id
                                  FROM addonsoptions
                                  WHERE itemid= %s""",
                           [optionId])

            options = cursor.fetchall()
            print(options)
            addons_ids=[]
            for option in options:
                addons_ids.append(option['id'])

            return addons_ids
        except Exception as e:
            print("Error: ", str(e))
            return False

    ############################################### DELETE

    @classmethod
    def delete_addon_option(cls, id=None, itemId=None, addonId=None):
        try:
            connection, cursor = get_db_connection()
            if itemId and addonId:
                cursor.execute("DELETE FROM addonsoptions WHERE itemid=%s AND addonid=%s", (itemId, addonId))
            connection.commit()
            return True
        except Exception as e:
            print("Error: ", str(e))
            return False

    ############################################### OTHER

    @classmethod
    def sort_addon_options(cls, merchantId, addonId, options):
        try:
            connection, cursor = get_db_connection()

            if not len(options):
                return invalid("invalid request")

            cursor.execute("SELECT * FROM addons WHERE id = %s", (addonId))
            row = cursor.fetchone()
            if not row or row["merchantid"] != merchantId:
                return invalid("invalid request")

            data = list(tuple())
            for row in options:
                data.append((row["sortId"], addonId, row["optionId"]))

            cursor.executemany("""
        UPDATE addonsoptions 
          SET sortid = %s
          WHERE addonid = %s AND itemid = %s
      """, (data))

            connection.commit()

            return success()
        except Exception as e:
            return unhandled(f"error: {e}")