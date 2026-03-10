import uuid
import json

from models.ItemPriceMappings import ItemPriceMappings
# local imports
from utilities.helpers import get_db_connection, success
from utilities.errors import invalid, unhandled
import config
from controllers.Middleware import validate_token_middleware
from utilities.helpers import validateLoginToken, validateMerchantUser, success, publish_sns_message
from utilities.errors import invalid, not_found, unhandled, unauthorised

# config
sns_category_notification = config.sns_category_notification

class ProductsCategories():

    @classmethod
    def get_productscategories(cls, categoryId=None, itemId=None):
        try:
            connection, cursor = get_db_connection()
            if categoryId:
                cursor.execute("""SELECT * FROM productscategories WHERE categoryid=%s""", (categoryId))
            elif itemId:
                cursor.execute("""SELECT * FROM productscategories WHERE productid=%s""", (itemId))
            rows = cursor.fetchall()
            return rows
        except Exception as e:
            print(str(e))
            return False

    @classmethod
    def get_item_categories(cls, itemId=None):
        try:
            connection, cursor = get_db_connection()

            catList = []
            if itemId:
                # get product categories by item id
                cursor.execute("""SELECT categories.id id, 
          categoryname, 
          categorydescription, 
          status
          FROM productscategories, categories 
          WHERE productscategories.categoryid = categories.id and productscategories.productid = %s""", (itemId))
                categories = cursor.fetchall()
                for row in categories:
                    catList.append({
                        "id": row["id"],
                        "categoryName": row["categoryname"],
                        "categoryDescription": row["categorydescription"],
                        "categoryStatus": row["status"]
                    })

            return catList
        except Exception as e:
            print("ProductsCategories Error")
            print(str(e))
            return False

    @classmethod
    def get_category_items(cls, categoryId):
        try:
            connection, cursor = get_db_connection()

            itemsList = []
            cursor.execute("""SELECT items.id id, 
        itemname, 
        itemdescription,
        status,
        productscategories.sortid sortid
        FROM productscategories, items 
        WHERE productscategories.productid = items.id and productscategories.categoryid = %s
        ORDER BY productscategories.sortid ASC""", (categoryId))
            items = cursor.fetchall()
            for row in items:
                itemsList.append({
                    "id": row["id"],
                    "itemName": row["itemname"],
                    "itemDescription": row["itemdescription"],
                    "itemStatus": row["status"],
                    "sortId": row["sortid"]
                })

            return itemsList
        except Exception as e:
            print("ProductsCategories Error")
            print(str(e))
            return False

    @classmethod
    def get_categories_with_items(cls, merchantId, limit="25", offset="0", categoryName=None, menuid = None, categoryid=None):
        try:
            connection, cursor = get_db_connection()

            conditions = []

            if categoryName:
                conditions.append(f'`categories`.`categoryname` LIKE "%%{categoryName}%%"')
            if categoryid:
                conditions.append(f'`categories`.`id` = {categoryid}')

            category_ids = []
            get_categorries=True
            if menuid:
                cursor.execute("""SELECT categoryid FROM menucategories WHERE menuid = %s """, (menuid,))
                result = cursor.fetchall()
                category_ids = [row['categoryid'] for row in result]
                if len(category_ids) > 1:
                    category_ids_tuple = tuple(category_ids)
                    conditions.append(f"categories.id IN {category_ids_tuple}")
                else:
                    category_ids_tuple = (category_ids[0] if category_ids else ())
                    conditions.append(f"categories.id = '{category_ids_tuple}'")
                if len(category_ids)==0:
                    get_categorries=False


            where = ' AND '.join(conditions)
            if not where:
                where = "1"
            all_categories = list()
            if get_categorries==False:
                return all_categories[int(offset):int(limit) + int(offset)]
            cursor.execute(f"""
                SELECT
                  categories.id categoryId, categories.categoryname categoryName, categories.posname categoryPosName, categories.categorydescription categoryDescription, categories.status categoryStatus,
                  productscategories.sortid sortId,
                  items.id itemId, items.itemname itemName, items.posname itemPosName, items.shortname itemShortName, 
                  items.itemdescription itemDescription, itemsku itemSKU, convert(itemprice, CHAR) itemPrice, items.imageurl itemImageUrl,
                  items.status itemStatus, items.pause_type itemPauseType, items.pause_time itemPauseTime, items.resume_time itemResumeTime,
                  items.itemtype itemType

                FROM categories
                LEFT JOIN productscategories ON categories.id = productscategories.categoryid
                LEFT JOIN items ON productscategories.productid = items.id
                WHERE {where} AND categories.merchantid=%s
                ORDER BY categories.categoryname ASC, productscategories.sortid ASC
            """, (merchantId,))
            rows = cursor.fetchall()

            # Getting platforms images
            images=cls.get_platform_images_url()

            for row in rows:

                exists = False
                for category in all_categories:
                    if category["id"] == row["categoryId"]:
                        category["items"].append({
                            "id": row["itemId"],
                            "itemName": row["itemName"],
                            "posName": row["itemPosName"],
                            "shortName": row["itemShortName"],
                            "itemDescription": row["itemDescription"],
                            "itemSKU": row["itemSKU"],
                            "itemPrice": row["itemPrice"],
                            "imageUrl": row["itemImageUrl"] if row.get("itemImageUrl") else config.default_item_imageurl,
                            "itemStatus": row["itemStatus"],
                            "itemPauseType": row["itemPauseType"],
                            "itemPauseTime": row["itemPauseTime"],
                            "itemResumeTime": row["itemResumeTime"],
                            "sortId": row["sortId"],
                            "itemType":row['itemType'],
                            "itemPriceMappings": ItemPriceMappings.get_itemPriceMappings( itemId=row["itemId"],merchantId=merchantId, fromtab=True) or []
                        })
                        exists = True
                        break

                if not exists:
                    menu_count = cursor.execute(f"""select distinct menuid from menucategories where categoryid =%s """,
                                                (row["categoryId"]))
                    cursor.execute(f"""select * from menucategories where categoryid =%s """,
                                   (row["categoryId"]))
                    sorting=cursor.fetchone()
                    sortId=0
                    if sorting:
                        sortId=sorting['sortid']
                    all_categories.append({
                        "id": row["categoryId"],
                        "categoryName": row["categoryName"],
                        "posName": row["categoryPosName"],
                        "categoryDescription": row["categoryDescription"],
                        "categoryStatus": row["categoryStatus"],
                        "menuAssign": menu_count,
                        "sortId": sortId,
                        "DD_image": images.get('dd_image_url'),
                        "GH_image": images.get('gh_image_url'),
                        "UBER_image": images.get('ue_image_url'),
                        "Fonda_image": images.get('fonda_image_url'),
                        "items": [
                            {
                                "id": row["itemId"],
                                "itemName": row["itemName"],
                                "posName": row["itemPosName"],
                                "shortName": row["itemShortName"],
                                "itemDescription": row["itemDescription"],
                                "itemSKU": row["itemSKU"],
                                "itemPrice": row["itemPrice"],
                                "imageUrl": row["itemImageUrl"],
                                "itemStatus": row["itemStatus"],
                                "itemPauseType": row["itemPauseType"],
                                "itemPauseTime": row["itemPauseTime"],
                                "itemResumeTime": row["itemResumeTime"],
                                "sortId": row["sortId"],
                                "itemType":row["itemType"],
                                "itemPriceMappings": ItemPriceMappings.get_itemPriceMappings(merchantId=merchantId, itemId=row["itemId"],  fromtab=True) or []
                            }
                        ] if row["itemId"] is not None else []
                    })
            all_categories = sorted(all_categories, key=lambda x: x['sortId'])
            return all_categories[int(offset):int(limit) + int(offset)]

        except Exception as e:
            print("error: ", str(e))
            return False

    ############################################### POST
    @classmethod
    def get_platform_images_url(cls):
        try:
            connection, cursor = get_db_connection()
            type_list = ('fonda_image_url', 'ue_image_url', 'gh_image_url', 'dd_image_url')

            query = "SELECT * FROM config_master WHERE config_type IN %s"
            cursor.execute(query, (type_list,))  # <-- notice the extra comma
            images = cursor.fetchall()
            image_urls = {row['config_type']: row['config_value'] for row in images}
            return image_urls
        except Exception as e:
            print("Error: ", str(e))
            return False

    @classmethod
    def update_category_items(cls, categoryId, items, userId=None):
        try:
            connection, cursor = get_db_connection()
            cursor.execute("DELETE FROM productscategories WHERE categoryid = %s", categoryId)
            connection.commit()
            for item in items:
                if ('itemID' in item):
                    itemid = item['itemID']
                    prodCatGUID = uuid.uuid4()
                    data = (prodCatGUID, itemid, categoryId, userId)
                    cursor.execute(
                        "INSERT INTO productscategories (id, productid, categoryid, created_by) VALUES (%s,%s,%s,%s)",
                        data)
                    connection.commit()
            return True
        except Exception as e:
            print("Error: ", str(e))
            return False

    @classmethod
    def post_category_item(cls, categoryId, itemId, userId=None,merchantId=None, ip_address = None):
        try:
            connection, cursor = get_db_connection()


            if isinstance(itemId, list):
                cursor.execute(
                    "SELECT COALESCE(MAX(sortid), 0) as maxSortId FROM productscategories WHERE categoryid = %s",
                    (categoryId))
                row = cursor.fetchone()
                sortId = row.get("maxSortId") + 1

                for item_id in itemId:
                    guid = uuid.uuid4()
                    data = (guid, item_id, categoryId, sortId, userId)
                    cursor.execute(
                        "INSERT INTO productscategories (id, productid, categoryid, sortid, created_by) VALUES (%s,%s,%s,%s,%s)",
                        data)
                    sortId += 1
                    
                    print("Triggering category sns...")
                    sns_msg = {
                    "event": "category.assign_item",
                    "body": {
                        "merchantId": merchantId,
                        "categoryId": categoryId,
                        "itemId": itemId,
                        "ipAddr": ip_address,
                        "userId": userId,
                        
                    }
                    }
                    sns_resp = publish_sns_message(topic=sns_category_notification, message=str(sns_msg), subject="category.assign_item")
                    publish_sns_message(topic=config.sns_activity_logs, message=str(sns_msg),
                                    subject="category.assign_item")
            elif isinstance(categoryId, list):
                cursor.execute(
                    "SELECT COALESCE(MAX(sortid), 0) as maxSortId FROM productscategories WHERE productid = %s",
                    (itemId))
                row = cursor.fetchone()
                sortId = row.get("maxSortId") + 1

                for catid in categoryId:
                    guid = uuid.uuid4()
                    data = (guid, itemId, catid, sortId, userId)
                    cursor.execute(
                        "INSERT INTO productscategories (id, productid, categoryid, sortid, created_by) VALUES (%s,%s,%s,%s,%s)",
                        data)
                    sortId += 1
                    print("Triggering category sns...")
                    sns_msg = {
                    "event": "category.assign_item",
                    "body": {
                        "merchantId": merchantId,
                        "categoryId": categoryId,
                        "itemId": itemId,
                        "ipAddr": ip_address,
                        "userId": userId,
                        
                    }
                    }
                    sns_resp = publish_sns_message(topic=sns_category_notification, message=str(sns_msg), subject="category.assign_item")
                    publish_sns_message(topic=config.sns_activity_logs, message=str(sns_msg),
                                    subject="category.assign_item")
            else:
                cursor.execute(
                    "SELECT COALESCE(MAX(sortid), 0) as maxSortId FROM productscategories WHERE categoryid = %s",
                    (categoryId))
                row = cursor.fetchone()
                sortId = row.get("maxSortId") + 1

                guid = uuid.uuid4()
                data = (guid, itemId, categoryId, sortId, userId)
                cursor.execute(
                    "INSERT INTO productscategories (id, productid, categoryid, sortid, created_by) VALUES (%s,%s,%s,%s,%s)",
                    data)
                print("Triggering category sns...")
                sns_msg = {
                "event": "category.assign_item",
                "body": {
                    "merchantId": merchantId,
                    "categoryId": categoryId,
                    "itemId": itemId,
                    "ipAddr": ip_address,
                    "userId": userId,
                    
                }
                }
                sns_resp = publish_sns_message(topic=sns_category_notification, message=str(sns_msg), subject="category.assign_item")
                publish_sns_message(topic=config.sns_activity_logs, message=str(sns_msg),
                                subject="category.assign_item")
            connection.commit()
            
            return True
        except Exception as e:
            print("Error: ", str(e))
            return False

    ############################################### DELETE

    @classmethod
    def delete_category_item(cls, categoryId=None, itemId=None):
        try:
            connection, cursor = get_db_connection()

            if categoryId and itemId:
                cursor.execute("DELETE FROM productscategories WHERE categoryid=%s AND productid=%s",
                               (categoryId, itemId))
            elif categoryId:
                cursor.execute("DELETE from productscategories where categoryid=%s", categoryId)
            elif itemId:
                cursor.execute("DELETE from productscategories where productid=%s", itemId)
            connection.commit()
            return True
        except Exception as e:
            print("Error: ", str(e))
            return False

    @classmethod
    def sort_category_items(cls, merchantId, categoryId, items):
        try:
            connection, cursor = get_db_connection()

            if not len(items):
                return invalid("invalid request")

            cursor.execute("SELECT * FROM categories WHERE id = %s", (categoryId))
            row = cursor.fetchone()
            if not row or row["merchantid"] != merchantId:
                return invalid("invalid request")

            data = list(tuple())
            for row in items:
                data.append((row["sortId"], categoryId, row["itemId"]))

            cursor.executemany("""
        UPDATE productscategories 
          SET sortid = %s
          WHERE categoryid = %s AND productid = %s
      """, (data))

            connection.commit()

            return success()
        except Exception as e:
            return unhandled(f"error: {e}")




