from flask.json import jsonify
import uuid
import json
import boto3
from utilities.errors import invalid, unhandled
import csv
import io
from datetime import datetime, timedelta , timezone
from dateutil.tz import gettz
import pytz
# local imports
from utilities.helpers import get_db_connection, success, publish_sns_message,create_log_data
import config
from models.ProductsCategories import ProductsCategories
from models.ProductsAddons import ProductsAddons
from models.ItemPriceMappings import ItemPriceMappings
from models.Merchants import Merchants
from models.Stream import Stream
from models.ItemServiceAvailability import ItemServiceAvailability

# s3 config
# images_bucket = config.s3_images_bucket
s3_apptopus_bucket = config.s3_apptopus_bucket
images_folder = config.s3_images_folder


class Items():

    ############################################### GET

    @classmethod
    def get_item_by_id_fk(cls, itemId):
        connection, cursor = get_db_connection()
        cursor.execute("SELECT * FROM items WHERE id = %s", (itemId))
        row = cursor.fetchone()
        return row

    @classmethod
    def get_items(cls, merchantId, limit=None, offset=None, itemName=None, productId=None):
        try:
            connection, cursor = get_db_connection()

            if productId is not None and itemName is not None:
                data = (productId, merchantId, f"%{itemName}%", int(limit), int(offset))
                cursor.execute("""
          SELECT id, itemname itemName, posname posName, shortname shortName, itemdescription itemDescription, status itemStatus, convert(itemprice,CHAR) itemUnitPrice, itemsku itemSKU, imageurl imageUrl, itemtype itemType, CONVERT(taxrate, CHAR) taxRate, (select count(*) from productsaddons where productid = items.id) addONCount 
            FROM items WHERE id IN (    
              SELECT itemid FROM addonsoptions
	              WHERE addonid IN ( SELECT addonid FROM productsaddons WHERE productid = %s )
            )
            AND merchantid = %s AND itemname LIKE %s ORDER BY itemtype ASC LIMIT %s OFFSET %s""", data)

            elif productId is not None:
                data = (productId, merchantId, int(limit), int(offset))
                cursor.execute("""
          SELECT id, itemname itemName, posname posName, shortname shortName, itemdescription itemDescription, status itemStatus, convert(itemprice,CHAR) itemUnitPrice, itemsku itemSKU, imageurl imageUrl, itemtype itemType, CONVERT(taxrate, CHAR) taxRate, (select count(*) from productsaddons where productid = items.id) addONCount 
            FROM items WHERE id IN (    
              SELECT itemid FROM addonsoptions
                WHERE addonid IN ( SELECT addonid FROM productsaddons WHERE productid = %s )
            )
            AND merchantid = %s ORDER BY itemtype ASC LIMIT %s OFFSET %s""", data)

            else:

                conditions = []

                if itemName:
                    conditions.append(f'`items`.`itemname` LIKE "%%{itemName}%%"')

                where = ' AND '.join(conditions)
                if not where:
                    where = "1"

                cursor.execute(f"""
          SELECT items.id id, items.itemname itemName, items.posname posName, items.shortname shortName, 
            items.itemdescription itemDescription, 
            items.status itemStatus, convert(items.itemprice,CHAR) itemUnitPrice, items.itemsku itemSKU, items.imageurl imageUrl, 
            items.itemtype itemType, CONVERT(items.taxrate, CHAR) taxRate, 
            (select count(*) from productsaddons where productid = items.id) addONCount,
            (select count(*) from productscategories where productid = items.id) categoryCount,
            (select count(*) from addonsoptions where itemid = items.id) addonoptionCount,
            itempricemappings.platformtype platformType, itempricemappings.platformitemprice platformItemPrice

            FROM items
            LEFT JOIN itempricemappings ON items.id = itempricemappings.itemid
            WHERE {where} AND items.merchantid = %s
            ORDER by items.itemtype ASC, items.itemname ASC""", (merchantId))

            rows = cursor.fetchall()

            if not productId:

                all_items = list()

                for row in rows:

                    exists = False
                    for item in all_items:
                        if row['id'] == item['id']:
                            item['itemPriceMappings'].append({
                                'platformType': row['platformType'],
                                'platformItemPrice': format(row['platformItemPrice'])
                            })

                            exists = True
                            break

                    if not exists:
                        all_items.append({
                            'id': row['id'],
                            'itemName': row['itemName'],
                            'posName': row['posName'],
                            'shortName': row['shortName'],
                            'itemDescription': row['itemDescription'],
                            'itemStatus': row['itemStatus'],
                            'itemUnitPrice': row['itemUnitPrice'],
                            'itemSKU': row['itemSKU'],
                            'imageUrl': row['imageUrl'] if row.get('imageUrl') else config.default_item_imageurl,
                            'itemType': row['itemType'],
                            'taxRate': row['taxRate'],
                            'addONCount': row['addONCount'],
                            'categoryCount': row['categoryCount'],
                            'addonOptionCount': row['addonoptionCount'],
                            'itemPriceMappings': [
                                {
                                    'platformType': row['platformType'],
                                    'platformItemPrice': format(row['platformItemPrice'])
                                }
                            ] if row['platformType'] is not None else []
                        })

                return all_items[0:int(limit)]

            return rows
        except Exception as e:
            print(str(e))
            return False

    @classmethod
    def get_item_by_id(cls, itemId, include=1 , isnotdefaultimage=None,  fromtab=None):
        try:
            connection, cursor = get_db_connection()

            where = ''
            if include == 0:
                where = ' and status=1'

            cursor.execute(f"""SELECT * FROM items WHERE id=%s {where}""", (itemId))
            row = cursor.fetchone()
            if not row:
                return False
            
            if row.get('imageurl'):
                row['imageurl'] = row.get('imageurl').replace(config.amazonaws_s3_url,config.cloud_front_url)
            elif isnotdefaultimage is None:
                row['imageurl'] = config.default_item_imageurl

            #getting platform images
            images=ProductsCategories.get_platform_images_url()
            item = {
                "id": row["id"],
                "merchantid":row['merchantid'],
                "itemSKU": row["itemsku"],
                "itemName": row["itemname"],
                "posName": row["posname"],
                "shortName": row["shortname"],
                "itemDescription": row["itemdescription"],
                "itemUnitPrice": format(row["itemprice"]),
                "imageUrl": row["imageurl"],
                "itemType": row["itemtype"],
                "taxRate": format(row["taxrate"]),
                "itemStatus": row["status"],
                "itemPauseType": row["pause_type"],
                "itemPauseTime": row["pause_time"].strftime("%Y-%m-%dT%H:%M:%S") if row["pause_time"] else None,
                "itemResumeTime": row["resume_time"].strftime("%Y-%m-%dT%H:%M:%S") if row["resume_time"] else None,

                "metadata": json.loads(row["metadata"]) if row["metadata"] else None,
                "itemPriceMappings": [],
                "DD_image": images.get('dd_image_url'),
                "GH_image": images.get('gh_image_url'),
                "UBER_image": images.get('ue_image_url'),
                "Fonda_image": images.get('fonda_image_url')
            }

            priceMappings = ItemPriceMappings.get_itemPriceMappings(itemId=itemId,  merchantId=item['merchantid'], fromtab=fromtab)
            if priceMappings and type(priceMappings) is list:
                item["itemPriceMappings"] = priceMappings

            return item
        except Exception as e:
            print(str(e))
            return False

    @classmethod
    def generating_top_items(cls):
        from globals import app
        with app.app_context():
            try:
                print("in the start of generating_top_items")
                connection, cursor = get_db_connection()
                while not connection or not cursor:
                    try:
                        connection, cursor = get_db_connection()
                    except:
                        pass


                cursor.execute("SELECT * FROM merchants")
                rows = cursor.fetchall()
                print("length of merchants list" , len(rows))

                for row in rows:
                    merchantId= row["id"]

                    cursor.execute("""
                                    SELECT productid, productname, price, SUM(salesRevenue) salesRevenue, SUM(quantitySold) quantitySold FROM 
                                    (
                                        (
                                        SELECT productid, productname, price, SUM(totalprice) as salesRevenue, SUM(quantity) as quantitySold
                                            FROM orderproducts 
                                            WHERE orderid IN (SELECT id FROM orders WHERE merchantid = %s )
                                            GROUP BY productname
                                            ORDER BY salesRevenue DESC
        
                                        )
                                        UNION
                                        (
                                        SELECT productid, productname, price, SUM(totalprice) as salesRevenue, SUM(quantity) as quantitySold
                                            FROM orderproductshistory WHERE orderid IN 
                                            (SELECT id FROM ordershistory WHERE merchantid = %s )
                                            GROUP BY productname
                                            ORDER BY salesRevenue DESC
        
                                        )
                                    ) AS result
                                    GROUP BY productname order by quantitySold desc limit 10;
                                    """, (merchantId,  merchantId))
                    topItems = cursor.fetchall()
                    data_list = list(dict())
                    cursor.execute("""DELETE FROM topitems WHERE merchantid=%s""", (merchantId))
                    connection.commit()
                    for row in topItems:
                        id=str(uuid.uuid4())
                        p_data = (id, row.get('productid'), merchantId , row.get('quantitySold'))
                        cursor.execute("""INSERT INTO topitems (id, itemid, merchantid , quantitysold)
                                   VALUES (%s,%s,%s,%s)""", p_data)
                    connection.commit()
                return True
            except Exception as e:
                print(str(e))
                return False
    @classmethod
    def get_itemDetailsByIdFromDb(cls,itemId, include=1,storefront=None , isnotdefaultimage=None, fromtab=None,merchantId=None):
        try:
            item = cls.get_item_by_id(itemId, include , isnotdefaultimage=isnotdefaultimage, fromtab=fromtab)
            if not item:
                return False

            if include == 1:
                # get product categories
                categories = ProductsCategories.get_item_categories(itemId=itemId)
                if categories:
                    item["categories"] = categories
                else:
                    item["categories"] = []

            # get product_addons with options
            addonsWithOptions = ProductsAddons.get_productAddonsWithOptions(itemId=itemId,storefront=storefront, fromtab=fromtab,merchantId=merchantId)
            if addonsWithOptions:
                item["addons"] = addonsWithOptions
            else:
                item["addons"] = addonsWithOptions

            hoursList = ItemServiceAvailability.get_serviceAvailabilityByitemId(itemId=itemId)
            item['hoursList'] = hoursList
            return item
        except Exception as e:
            print(str(e))
            return False

    @classmethod
    def get_topitemsFromDb(cls, merchantId):
        try:
            connection, cursor = get_db_connection()
            cursor.execute("SELECT itemid , merchantid  FROM topitems WHERE merchantid=%s order by quantitysold desc limit 10", (merchantId))
            topitems = cursor.fetchall()
            return topitems

        except Exception as e:
            print(str(e))
            return False
    ###################################################### PUT

    @classmethod
    def upload_itemImage(cls, userId, merchantId, itemId, imageFile):
        try:
            connection, cursor = get_db_connection()

            # init boto3
            client = boto3.client("s3")

            # generate image name for s3
            ext = imageFile.filename.split(".")[-1]
            imageName = str(uuid.uuid4()) + "." + ext

            # check if item already have image in s3, then delete it
            cursor.execute("SELECT imageurl FROM items WHERE id=%s", (itemId))
            row = cursor.fetchone()
            if not row:
                return False

            oldImageUrl = row["imageurl"]
            if oldImageUrl is not None:
                print("Deleting old image...")
                oldImageName = oldImageUrl.split("/")[-1]
                client.delete_object(Bucket=s3_apptopus_bucket, Key=f"{images_folder}/{oldImageName}")
                print("Old image delete from s3")

            # boto3 upload image to s3
            print("Creating New Image...")
            client.upload_fileobj(
                imageFile,
                s3_apptopus_bucket,
                f"{images_folder}/{imageName}",
                ExtraArgs={
                    "ACL": "public-read",
                    "ContentType": imageFile.content_type
                }
            )

            newImageUrl = client.generate_presigned_url(
                ClientMethod='get_object',
                Params={
                    'Bucket': s3_apptopus_bucket,
                    'Key': f"{images_folder}/{imageName}"
                }
            )
            newImageUrl = newImageUrl.split("?")[0]
            print(newImageUrl)

            # put new image url to the items table
            cursor.execute("""UPDATE items SET imageurl=%s WHERE id=%s""", (newImageUrl, itemId))
            connection.commit()

            return newImageUrl

        except Exception as e:
            print(str(e))
            return False

    @classmethod
    def put_itemById(cls, userId=None, merchantId=None, itemId=None, itemSku=None, itemName=None, posName=None, shortName=None, itemDescription=None, itemPrice=None,
                     itemType=None, taxRate=None, itemStatus=None, metadata=None,
                     itemPriceMappings=None , fromcsv=False , frompos=None):
        try:
            print("Call the update item function ")
            connection, cursor = get_db_connection()
            if fromcsv:
                data = ( itemName, posName, itemDescription, itemPrice, itemStatus,userId, itemId)
                cursor.execute("""UPDATE items 
                SET 
                  itemname= %s,
                  posname=%s,
                  itemdescription= %s, 
                  itemprice= %s, 
                  status= %s, 
                  updated_by= %s,
                  updated_datetime=CURRENT_TIMESTAMP
                WHERE id=%s
                """, data)
            elif frompos:
                data = ( itemPrice,itemStatus, itemId)
                cursor.execute("""UPDATE items 
                SET 
                  itemprice= %s, 
                  status= %s, 
                  updated_datetime=CURRENT_TIMESTAMP
                WHERE id=%s
                """, data)
            else:
                data = (itemSku, itemName, posName, shortName, itemDescription, itemPrice, itemType, taxRate, itemStatus,
                        json.dumps(metadata), userId, itemId)
                cursor.execute("""UPDATE items 
                SET 
                  itemsku= %s, 
                  itemname= %s,
                  posname=%s,
                  shortname=%s,
                  itemdescription= %s, 
                  itemprice= %s, 
                  itemtype = %s, 
                  taxrate = %s, 
                  status= %s, 
                  metadata= %s, 
                  updated_by= %s,
                  updated_datetime=CURRENT_TIMESTAMP
                WHERE id=%s
                """, data)

            if type(itemPriceMappings) is list:
                # delete old price-mappings
                cursor.execute("""DELETE FROM itempricemappings WHERE itemid=%s""", (itemId))

                # insert new price-mappings
                for row in itemPriceMappings:
                    platformType = row.get("platformType")
                    platformItemPrice = row.get("platformItemPrice")
                    p_data = (itemId, platformType, platformItemPrice)
                    cursor.execute("""INSERT INTO itempricemappings (itemid, platformtype, platformitemprice)
                    VALUES (%s,%s,%s)""", p_data)

            if int(itemStatus) == 1:
                categories_to_update = cursor.execute("""UPDATE categories 
                  SET status= 1, updated_by= %s, updated_datetime=CURRENT_TIMESTAMP
                  WHERE id IN ( SELECT categoryid FROM productscategories where productid= %s)""", (userId, itemId))
                connection.commit()

            connection.commit()
            return True

        except Exception as e:
            print(str(e))
            return False

    @classmethod
    def update_category_status(cls, category_id, category_status, userId=None):
        # try:
        connection, cursor = get_db_connection()
        data = (category_status, userId, category_id)
        cursor.execute("""UPDATE categories 
          SET status= %s, updated_by= %s, updated_datetime=CURRENT_TIMESTAMP
          WHERE id=%s""", data)
        connection.commit()
        return True

    @classmethod
    def update_category_and_item_status(cls, merchant_id, category_id, category_status, userId=None):
        # try:
        connection, cursor = get_db_connection()
        data = (category_status, userId, category_id)
        cursor.execute("""UPDATE categories 
          SET status= %s, updated_by= %s, updated_datetime=CURRENT_TIMESTAMP
          WHERE id=%s""", data)
        connection.commit()
        items_to_update = cursor.execute(
            """ select * from items  where id IN (SELECT productid FROM productscategories where categoryId= %s) and status != %s """,
            (category_id, category_status))
        items_to_update = cursor.fetchall()

        data = (category_status, userId, category_id)
        my_cur = cursor.execute(
            """ update items set status= %s, updated_by= %s  where id IN (SELECT productid FROM productscategories where categoryId= %s)""",
            data)
        data = connection.commit()

        return items_to_update

    # @classmethod
    # def update_item_status(cls, merchantId, itemId, itemStatus, userId=None):
    #   try:
    #     connection, cursor = get_db_connection()
    #     data = (itemStatus, userId, itemId)
    #     cursor.execute("""UPDATE items
    #       SET status= %s, updated_by= %s, updated_datetime=CURRENT_TIMESTAMP
    #       WHERE id=%s""", data)
    #     connection.commit()

    #     if int(itemStatus) == 1:
    #       categories_to_update = cursor.execute("""UPDATE categories
    #         SET status= 1, updated_by= %s, updated_datetime=CURRENT_TIMESTAMP
    #         WHERE id IN ( SELECT categoryid FROM productscategories where productid= %s)""", (userId, itemId))
    #       connection.commit()

    #     return True
    #   except Exception as e:
    #     print(str(e))
    #     return False

    @classmethod
    def update_item_status(cls, merchantId, itemId, itemStatus, itemPauseType=None, itemResumeTime=None, userId=None):
        try:
            connection, cursor = get_db_connection()

            if int(itemStatus) == 0:
                if itemPauseType is None:
                    data = (itemStatus, userId, itemId)
                    cursor.execute("""UPDATE items 
            SET status= %s, updated_by= %s, updated_datetime=CURRENT_TIMESTAMP
            WHERE id=%s""", data)
                    connection.commit()
                else:
                    data = (itemStatus, itemPauseType, itemResumeTime, userId, itemId)
                    cursor.execute("""UPDATE items 
            SET status= %s, pause_type=%s, pause_time=CURRENT_TIMESTAMP, resume_time=%s, updated_by= %s, updated_datetime=CURRENT_TIMESTAMP
            WHERE id=%s""", data)
                    connection.commit()
            else:
                data = (itemStatus, userId, itemId)
                cursor.execute("""UPDATE items 
          SET status= %s, updated_by= %s, updated_datetime=CURRENT_TIMESTAMP
          WHERE id=%s""", data)
                connection.commit()

            if int(itemStatus) == 1:
                categories_to_update = cursor.execute("""UPDATE categories 
          SET status= 1, updated_by= %s, updated_datetime=CURRENT_TIMESTAMP
          WHERE id IN ( SELECT categoryid FROM productscategories where productid= %s)""", (userId, itemId))
                connection.commit()
            
            return True
        except Exception as e:
            print(str(e))
            return False

    ###################################################### POST

    @classmethod
    def post_item(cls, userId, merchantId, itemSku, itemName, posName, shortName, itemDescription, itemPrice, itemType,
                  taxRate, itemStatus, metadata, itemPriceMappings=None):
        try:
            connection, cursor = get_db_connection()

            itemId = uuid.uuid4()
            data = (
            itemId, merchantId, itemSku, itemName, posName, shortName, itemDescription, itemPrice, itemType, taxRate,
            itemStatus, json.dumps(metadata), userId)
            cursor.execute("""INSERT INTO items 
        (id, merchantid, itemsku, itemname, posname, shortname, itemdescription, itemprice, itemtype, taxrate, status, metadata, created_by)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""", data)

            if itemPriceMappings and type(itemPriceMappings) is list:
                for row in itemPriceMappings:
                    platformType = row.get("platformType")
                    platformItemPrice = row.get("platformItemPrice")
                    p_data = (itemId, platformType, platformItemPrice)
                    cursor.execute("""INSERT INTO itempricemappings (itemid, platformtype, platformitemprice)
            VALUES (%s,%s,%s)""", p_data)

            connection.commit()
            return str(itemId)
        except Exception as e:
            print(str(e))
            return False

    ############################################### DELETE

    @classmethod
    def delete_itemImage(cls, itemId):
        try:
            connection, cursor = get_db_connection()

            # init boto3
            client = boto3.client("s3")

            # check if item have image in s3
            cursor.execute("SELECT imageurl FROM items WHERE id=%s", (itemId))
            row = cursor.fetchone()
            if not row:
                return False

            imageUrl = row["imageurl"]
            if imageUrl is not None:
                print("Deleting image...")
                imageName = imageUrl.split("/")[-1]
                client.delete_object(Bucket=s3_apptopus_bucket, Key=f"{images_folder}/{imageName}")
                print("Image delete from s3")

            cursor.execute("""UPDATE items SET imageurl=NULL WHERE id=%s""", (itemId))
            connection.commit()
            return True
        except Exception as e:
            print(str(e))
            return False

    @classmethod
    def delete_item(cls, itemId):
        try:
            connection, cursor = get_db_connection()

            print("Deleting item image...")
            delImgResp = Items.delete_itemImage(itemId=itemId)
            if not delImgResp:
                return False

            print("Deleting item-categories...")
            cursor.execute("DELETE from productscategories where productid=%s", itemId)

            print("Deleting item-addons...")
            cursor.execute("DELETE from productsaddons WHERE productid=%s", itemId)

            print("Deleting item addon-options...")
            cursor.execute("DELETE FROM addonsoptions WHERE itemid=%s", itemId)

            print("Delete item-price-mappings")
            cursor.execute("DELETE FROM itempricemappings WHERE itemid=%s", itemId)

            print("Deleting item...")
            cursor.execute("DELETE from items WHERE id=%s", itemId)

            connection.commit()

            print("Successfully deleted")
            return True
        except Exception as e:
            print(str(e))
            return False

    ############################################### CSV

    @classmethod

    def generate_items_csv(cls, merchantId , itemType = None):
        try:
            connection, cursor = get_db_connection()
            # get merchant details
            merchant_details = Merchants.get_merchant_by_id(merchantId)

            temp_file_name = merchant_details['merchantname'] + "_items.csv"
            temp_file_path = "/tmp/" + temp_file_name

            query = f"""
                                   SELECT items.id id, items.merchantid merchantid, items.itemname itemname,items.posname posname ,  items.itemdescription itemdescription,
                                   CONVERT(items.itemprice, CHAR) itemprice,
                                   CASE WHEN items.status = 1 THEN 'Active' ELSE 'Inactive' END AS itemstatus,
                                   itemtype.type itemtype 
                                   FROM items, itemtype
                                   WHERE items.merchantid = "{merchantId}" AND itemtype.id = items.itemtype
                         """
            if itemType:
                query += f" AND items.itemtype = '{itemType}'"
            cursor.execute(query)
            items = cursor.fetchall()

            cursor.execute("""SELECT * FROM platformtype WHERE id != 1""")
            platforms = cursor.fetchall()
            platforms_list = [f"{platform['type']}_price" for platform in platforms]

            for item in items:
                item['merchantname'] = merchant_details['merchantname']

                cursor.execute("""
          SELECT itempricemappings.platformitemprice, platformtype.type
          FROM itempricemappings, platformtype
          WHERE itempricemappings.platformtype = platformtype.id AND itempricemappings.itemid=%s
        """, (item['id']))
                price_mappings = cursor.fetchall()

                for row in price_mappings:
                    item[f"{row['type']}_price"] = format(row['platformitemprice'])

            with open(temp_file_path, mode='w', newline='') as temp_csv:
                fieldnames = ["id", "merchantid", "merchantname", "itemname", "posname", "itemdescription", "itemprice",
                              "itemstatus", "itemtype", *platforms_list]
                writer = csv.DictWriter(temp_csv, fieldnames=fieldnames, delimiter=',', quotechar='"',
                                        quoting=csv.QUOTE_MINIMAL)
                writer.writeheader()
                for row in items:
                    writer.writerow(row)

            s3 = boto3.client('s3')
            s3.upload_file(
                temp_file_path,
                s3_apptopus_bucket,
                f"{config.s3_reports_folder}/{temp_file_name}",
                ExtraArgs={
                    "ACL": "public-read"
                }
            )

            # get s3 url of csv
            download_url = s3.generate_presigned_url(
                ClientMethod='get_object',
                Params={
                    'Bucket': s3_apptopus_bucket,
                    'Key': f"{config.s3_reports_folder}/{temp_file_name}"
                }
            )
            download_url = download_url.split("?")[0]
            print(download_url)

            return success(jsonify({
                "message": "success",
                "status": 200,
                "data": {
                    "download_url": download_url
                }
            }))

        except Exception as e:
            print(str(e))
            return unhandled()

    @classmethod
    def upload_items_price_mappings(cls, csvFile, platformTypes, merchantId, userId , ip_address=None):
        try:
            
            
            
            connection, cursor = get_db_connection()
            basePriceUpdate = False

            platforms_ids_list = platformTypes.split(",")
            platforms_ids_list = [int(i) for i in platforms_ids_list]

            # check if base item price update value is passed
            if 1 in platforms_ids_list:  # 1 = apptopus
                basePriceUpdate = True
                platforms_ids_list.remove(1)
            platformTypes = ",".join(map(str, platforms_ids_list))

            if platformTypes:
                cursor.execute(
                    "SELECT id, CONCAT(type, '_price') as type FROM platformtype WHERE id IN (" + platformTypes + ")")
                platforms = cursor.fetchall()
                print(platforms)
            else:
                platforms = list()

            stream = io.StringIO(csvFile.stream.read().decode("UTF8"), newline=None)
            csv_data = csv.DictReader(stream)

            for item in csv_data:

                # check if merchantid of each item in csv file matched with merchantid passed through api
                if merchantId != item['merchantid']:
                    return invalid("csv file does not belongs to the selected merchant!")

                #create new item price mappings
                print("check platfroms")
                itemPriceMappings=list()
                if platforms:

                    for platform in platforms:
                        platform_price = item.get(platform['type'])

                        if platform_price is not None and platform_price != "" and float(platform_price) > 0:
                            itemPriceMappings.append(
                                {
                                    "platformItemPrice": platform_price,
                                    "platformType":platform['id']
                                }
                            )
                # update item detail like  name , description , posname and status
                old_item_details = Items.get_item_by_id(item['id'])
                if not old_item_details:
                    print(f"item id is invalid! itemid: {item['id']}")
                    continue
                itemstatus = 1 if item['itemstatus'] == 'Active' else 0
                updResp = Items.put_itemById(userId=userId, merchantId=merchantId, itemId=item['id'],
                                             itemName=item['itemname'], posName=item['posname'],
                                            itemDescription=item['itemdescription'],
                                             itemPrice=item['itemprice'],
                                             itemStatus=itemstatus,
                                             itemPriceMappings=itemPriceMappings , fromcsv=True)
                if updResp:
                    create_log_data(level='[INFO]',
                                    Message=f"Successfully updateMerchantItem: itemid: {item['id']}",
                                    functionName="updateMerchantItem")
                    sns_msg = {
                        "event": "item.update",
                        "body": {
                            "merchantId": merchantId,
                            "itemId": item['id'],
                            "userId": userId,
                            "unchanged": None,
                            "oldItemStatus": old_item_details['itemStatus'],
                            "old_item_details": old_item_details,
                            "source": "Fonda",
                            "ipAddr": ip_address
                        }
                    }
                    sns_resp = publish_sns_message(topic=config.sns_item_notification, message=str(sns_msg),
                                                   subject="item.update")

            connection.commit()

            # Triggering SNS
            sns_msg = {
                "event": "item.price_upload_csv",
                "body": {
                    "merchantId": merchantId,
                    "userId": userId,
                    "ipAddr": ip_address
                }
            }
            logs_sns_resp = publish_sns_message(topic=config.sns_activity_logs, message=str(sns_msg),
                                                subject="item.price_upload_csv")

            return success()
        except Exception as e:
            print("Error: ", str(e))
            return unhandled()
