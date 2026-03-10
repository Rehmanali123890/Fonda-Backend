from flask import g
import uuid
import json
from models.ProductsAddons import ProductsAddons
# local imports
import config
import requests

from flask import jsonify, request

from models.MenuMappings import *
from models.Merchants import Merchants
from models.Items import Items
from models.Platforms import Platforms
from utilities.helpers import success, get_db_connection, publish_sns_message,create_log_data
from utilities.errors import invalid, unhandled
from dateutil.tz import gettz
from datetime import timedelta
import datetime
import calendar
import jwt.utils
import time
import math


class Storefront():

    @classmethod
    def get_storefront_by_slug(cls, slug):
        try:
            connection, cursor = get_db_connection()
            cursor.execute("""SELECT * FROM merchants WHERE slug=%s""", (slug))
            row = cursor.fetchone()
            return row
        except Exception as e:
            print(str(e))
            return False

    @classmethod
    def get_static_delivery_fee_configuration(cls):
        try:
            connection, cursor = get_db_connection()
            Static_delivery_fees_amount=0
            Static_delivery_fees_flag=0
            cursor.execute("select * from config_master where config_type in ('Static_delivery_fees_amount','Static_delivery_fees_flag')")
            delivery_fee_configurations = cursor.fetchall()
            if delivery_fee_configurations:
                for row in delivery_fee_configurations:
                    if row.get('config_type')=='Static_delivery_fees_flag':
                        Static_delivery_fees_flag=int(row.get('config_value'))
                    elif row.get('config_type')=='Static_delivery_fees_amount':
                        Static_delivery_fees_amount=float(row.get('config_value'))
            return Static_delivery_fees_flag , Static_delivery_fees_amount
        except Exception as e:
            print(str(e))
            return False


    @classmethod
    def get_storefront_by_id(cls, id):
        try:
            connection, cursor = get_db_connection()
            cursor.execute("""SELECT * FROM merchants WHERE id=%s""", (id))
            row = cursor.fetchone()
            return row
        except Exception as e:
            print(str(e))
            return False


    @staticmethod
    def time_to_minutes(tstr: str) -> int:
        """Convert 'HH:MM AM/PM' string into minutes since midnight."""
        t = datetime.datetime.strptime(tstr, "%I:%M %p")
        return t.hour * 60 + t.minute

    @classmethod
    def get_storefront_hours(cls, merchantid, day, current_time):
        try:
            connection, cursor = get_db_connection()

            # ---- Get today's record ----
            cursor.execute("""SELECT * FROM merchantopeninghrs WHERE merchantid=%s AND day=%s""", (merchantid, day))
            today_rows = cursor.fetchall()

            # Step 1: Current Day Comparison
            current_minutes = current_time.hour * 60 + current_time.minute

            for today_row in today_rows:

                opening = cls.time_to_minutes(today_row['opentime'])
                closing = cls.time_to_minutes(today_row['closetime'])
                if closing < opening:  # overnight adjustment
                    closing += 1440

                # Check current day range
                if opening <= current_minutes <= closing:
                    return today_row, day

            # Step 2: Previous Day Comparison
            # current_time + 24h in minutes
            current_minutes += 1440

            # Get previous day name (assuming DB stores "Monday, Tuesday...")
            prev_day = (current_time - timedelta(days=1)).strftime("%A")
            cursor.execute("""SELECT * FROM merchantopeninghrs WHERE merchantid=%s AND day=%s""",
                           (merchantid, prev_day))
            prev_rows = cursor.fetchall()

            if not prev_rows:
                return today_rows[0], day
            
            for prev_row in prev_rows:

                prev_open = cls.time_to_minutes(prev_row['opentime'])
                prev_close = cls.time_to_minutes(prev_row['closetime'])
                if prev_close < prev_open:
                    prev_close += 1440

                if prev_open <= current_minutes <= prev_close:
                    return prev_row, prev_day

            return today_rows[0], day

        except Exception as e:
            print(str(e))
            return False, False


    @classmethod
    def get_storefront_details(cls, slug):
        try:
            connection, cursor = get_db_connection()
            store_front = cls.get_storefront_by_slug(slug)

            if not store_front:
                return invalid("Storefront not exists.")
                
            cusines = cls.get_cusines(store_front['id'])
            merchantid = store_front['id']
            if store_front and store_front['storefrontstatus'] == 1:
                menuName = ''
                opening_hours_list = []       
                if cusines.get('menuid'):
                    cursor.execute("select * from menus where id=%s",cusines['menuid'])
                    menuData = cursor.fetchone()
                    menuName = menuData['name']
                    connection.commit()
                reviews = []

                if config.env == "production":
                    token = Platforms.getGoogleToken(merchantid)
                    if token:
                        gmb = Platforms.getLinkedLication(store_front['id'])
                        if gmb:
                            url = config.google_business_account_base + "v4/" + gmb['accountid'] + "/" + gmb['locationid']+ "/reviews"
                            payload = {}
                            headers = {
                                'Authorization': 'Bearer ' + token
                            }

                            response = requests.request("GET", url, headers=headers, data=payload)
                            response=response.json()
                            if response:
                                reviews = response


                # today = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc).astimezone(gettz("US/Pacific"))
                today = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc).astimezone(gettz(store_front['timezone']))
                today_dayname = calendar.day_name[today.weekday()]
                opening_hours, day = cls.get_storefront_hours(store_front['id'], today_dayname,today)
                closeforbusinessflag = opening_hours['closeforbusinessflag']
                if opening_hours:
                    opening_hours = opening_hours['opentime'] + " - " + opening_hours['closetime']
                else:
                    opening_hours = ''

                if store_front['busymode'] == 1:
                    pickUptime = int(store_front['preparationtime']) + int(store_front['orderdelaytime'])
                    print("busy mode")
                else:
                    pickUptime = int(store_front['preparationtime'])

                if not day:
                    day = today_dayname

                promo = Merchants.get_default_promo(store_front['id'])

                if store_front.get('banner'):
                    store_front['banner'] = store_front.get('banner').replace(config.amazonaws_s3_url,config.cloud_front_url)
                        
                if store_front.get('logo'):
                    store_front['logo'] = store_front.get('logo').replace(config.amazonaws_s3_url,config.cloud_front_url)
                
                cursor.execute("""SELECT * FROM merchantopeninghrs WHERE merchantid=%s""", (merchantid))
                opening_hours_list = cursor.fetchall()
                connection.commit()

                merchant = {
                    "id": store_front['id'],
                    "merchantName": store_front['merchantname'],
                    "logo": store_front['logo'],
                    "banner": store_front['banner'],
                    "cusines": cusines['cusine'] if cusines else None,
                    "address": store_front['address'],
                    "phone": store_front['businessnumber'],
                    "hours": opening_hours,
                    "day":day,
                    "gmb": reviews,
                    "merchantTaxRate": store_front['taxrate'],
                    "marketStatus": store_front['marketstatus'],
                    "storefrontStatus": store_front['storefrontstatus'],
                    "specialInstructions": store_front['acceptspecialinstructions'],
                    "processingfeeFixed": str(store_front['processingfeefixed']),
                    "processingfeeRate": str(store_front['processingfeerate']),
                    "cardfeeType": store_front['cardfeetype'],
                    "pickUptime": pickUptime,
                    "promo": promo['promo'] if promo else None,
                    "promoDiscount": float(promo['discount']) if promo else 0,
                    'has_address_error': int(store_front["has_address_error"]),
                    'businessAddressLine': store_front['businessaddressline'],
                    'businessAddressCity': store_front['businessaddresscity'],
                    'businessAddressState': store_front['businessaddressstate'],
                    'zip': store_front['zipcode'],
                    "openingHours": opening_hours_list,
                    "menuName" : menuName,
                    "timezone" : store_front['timezone'],
                    "closeforbusinessflag" : closeforbusinessflag
                }

                return success(jsonify({
                    "message": "success",
                    "status": 200,
                    "storefront": merchant
                }))

            else:
                return invalid("Storefront is disabled for the merchant")

        except Exception as e:
            return invalid(str(e))

    @classmethod
    def get_categories(cls, merchantId):
        try:
            connection, cursor = get_db_connection()

            cursor.execute("""SELECT *
                        from menumappings 
                        WHERE merchantid = %s and platformtype=50 """, (merchantId))
            mapping = cursor.fetchone()
            if not mapping:
                return []

            cursor.execute("""SELECT categories.categoryname categoryname, categories.id id, 
                                categories.categorydescription categorydescription, categories.status status, 
                                menucategories.sortid sortId
                                FROM menucategories 
                                INNER JOIN categories
                                ON menucategories.categoryid=categories.id 
                                WHERE menucategories.menuid=%s
                                AND categories.status = 1
                                group by menucategories.categoryid
                                ORDER BY menucategories.sortid ASC;""", (mapping['menuid']))

            return cursor.fetchall()
        except Exception as e:
            print(str(e))
            return False


    @classmethod
    def get_categories_items(cls, merchantId):
        try:
            connection, cursor = get_db_connection()
            catList = []

            cursor.execute("""SELECT categories.id id, 
            categoryname, 
            categorydescription, 
            status,
            productscategories.productid
            FROM productscategories, categories 
            WHERE productscategories.categoryid = categories.id and categories.merchantid = %s and categories.status=1 order by productscategories.sortid ASC""", (merchantId))
            categories = cursor.fetchall()
            for row in categories:
                catList.append({
                    "id": row["id"],
                    "categoryName": row["categoryname"],
                    "categoryDescription": row["categorydescription"],
                    "categoryStatus": row["status"],
                    "productid": row["productid"]
                })

            print(catList)

            return catList
        except Exception as e:
            print("ProductsCategories Error")
            print(str(e))
            return False

    @classmethod
    def create_menu(cls, merchantId):
        try:
            storefront = True
            create_log_data(level='[INFO]', Message="In the start of method create_menu to create menu for store front",
                            messagebody=merchantId, functionName="create_menu",
                            request=request)
            categories = cls.get_categories(merchantId)
            create_log_data(level='[INFO]', Message=f"Categories retrived by merchantId {merchantId}",
                            messagebody=f"categories: {categories}"  , functionName="create_menu",
                            request=request)
            items = cls.get_categories_items(merchantId)
            create_log_data(level='[INFO]', Message=f"Items retrived by merchantId {merchantId}",
                            messagebody=f"items: {items}", functionName="create_menu",
                            request=request)

            menu = []
            for category in categories:

                all_items = []
                for item in items:
                    if category['id'] == item['id']:
                        item_details = Items.get_itemDetailsByIdFromDb(itemId=item['productid'], include=0, storefront=storefront)
                        if item_details:
                            all_items.append(item_details)
                menu.append({
                    "category": category['categoryname'],
                    "items": all_items

                })

            print(menu)
            return menu
        except Exception as e:
            print(str(e))
            return False

    @classmethod
    def get_cusines(cls, merchantId):
        connection, cursor = get_db_connection()

        cursor.execute("""SELECT mp.*, m.cusine
                    from menumappings mp, menus m 
                    WHERE m.id=mp.menuid and  mp.merchantid = %s and mp.platformtype=50 """, (merchantId))
        return cursor.fetchone()


    @classmethod
    def get_doordash_jwt(cls):

       return jwt.encode(
            {
                "aud": "doordash",
                "iss": config.developer_id,
                "kid": config.key_id,
                "exp": str(math.floor(time.time() + 1800)),
                "iat": str(math.floor(time.time())),
            },
            jwt.utils.base64url_decode(config.signing_secret),
            algorithm="HS256",
            headers={"dd-ver": "DD-JWT-V1"})


    @classmethod
    def check_promo(cls, promo, merchant_id):
        try:
            connection, cursor = get_db_connection()
            cursor.execute("""SELECT * FROM promo WHERE promo=%s and merchantid=%s""", (promo, merchant_id))
            return cursor.fetchone()
        except Exception as e:
            print("Error checking promo:", str(e))
            return None
        
    @classmethod
    def check_sourceqr(cls, source, merchant_id):
        try:
            connection, cursor = get_db_connection()
            cursor.execute("""SELECT * FROM storefrontqr WHERE source=%s and merchantid=%s""", (source, merchant_id))
            return cursor.fetchone()
        except Exception as e:
            print("Error checking qr:", str(e))
            return None
    @classmethod
    def check_sourceqr_byId(cls, sourceqrid):
        try:
            connection, cursor = get_db_connection()
            cursor.execute("""SELECT * FROM storefrontqr WHERE id=%s""", (sourceqrid))
            return cursor.fetchone()
        except Exception as e:
            print("Error checking qr:", str(e))
            return None

    # Promo Type
    # 1 : Free Purchase item Promo
    # 2 : Buy X Get Y Free
    # 3 : Buy X% Off on your total order
    @classmethod
    def change_date_format(cls,date_str):
        # Parse the input date string in "MM-DD-YYYY" format
        date_obj = datetime.datetime.strptime(date_str, "%m-%d-%Y")
        # Format the date object into "YYYY-MM-DD" format and convert it to string
        formatted_date = date_obj.strftime("%Y-%m-%d")
        return formatted_date

    @classmethod
    def add_promo(cls, merchentid):
        try:
            create_log_data(level='[INFO]', Message="Inside Storefront model to create a new promo code",
                        merchantID=merchentid, functionName="add_promo",request=request)
            req = request.json

            check_promo = cls.check_promo(req['promo'], merchentid)
            if check_promo:
                create_log_data(level='[ERROR]', Message="Promo code already exist",
                                merchantID=merchentid, functionName="add_promo", request=request)
                return "Promo already exist"

            if "print" in req and req['print'] == 1:
                create_log_data(level='[INFO]', Message="Requesting to update default Promo to specify the print status",
                        merchantID=merchentid, functionName="add_promo",request=request)
                cls.update_default_promo(merchentid)

            connection, cursor = get_db_connection()
            ppuid = uuid.uuid4()
            freeitem = req['freeitem'] if req['promoType'] == 1 or req['promoType'] == 2 else ''
            minPurchaseAmount = req['minPurchaseAmount'] if req['promoType'] == 1 or req['promoType'] == 3 else 0
            primaryitem = req['primaryitem'] if req['promoType'] == 2 else ''
            primaryitemquantity = req['primaryitemquantity'] if req['promoType'] == 2 else 0
            freeitemquantity = req['freeitemquantity'] if req['promoType'] == 2 else 0
            maxDiscount = req['maxDiscount'] if req['promoType'] == 3 else 0
            promoDiscount = req['promoDiscount'] if req['promoType'] == 3 else 0
            happyhourstarttime = req['happyhourstarttime'] if req['ishappyhourenabled'] == 1 else ''
            happyhourendtime = req['happyhourendtime'] if req['ishappyhourenabled'] == 1 else ''
            days = ",".join(req['days']) if req['ishappyhourenabled'] == 1 else ''
            data = (
                ppuid,
                req['promo'],
                req['description'],
                req['status'],
                req['promoText'],
                req.get('print', 0),
                freeitem,
                minPurchaseAmount,
                req['ishappyhourenabled'],
                req['promoType'],
                req['promostartdate'],
                req['promoenddate'],
                primaryitem,
                primaryitemquantity,
                freeitemquantity,
                maxDiscount,
                promoDiscount,
                happyhourstarttime,
                happyhourendtime,
                days,
                req['redirecturl'],
                req['source'],
                merchentid
            )
            cursor.execute(
                "INSERT INTO promo (id, promo, description, status, promotext, printinreceipt, freeitem, minpurchaseamount, ishappyhourenabled, PromoType, promostartdate, promoenddate, primaryitem, primaryitemquantity, freeitemquantity, maxdiscount, discount, happyhourstarttime, happyhourendtime, days,redirecturl, source, merchantid) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",

                data
            )
            connection.commit()
            create_log_data(level='[INFO]', Message="Promo get created Successfully",
                            merchantID=merchentid, functionName="add_promo", request=request)
            return False

        except Exception as e:
            create_log_data(level='[ERROR]', Message="Failed to create promo",
                            messagebody=f"An error occured while creating promo: {e}",
                            merchantID=merchentid, functionName="add_promo", request=request)
            return "Error adding promo:", str(e)


    @classmethod
    def add_qrCode(cls, merchentid):
        try:
            create_log_data(level='[INFO]', Message="Inside Storefront model to create a new qr code",
                        merchantID=merchentid, functionName="add_qrCode",request=request)
            req = request.json

            check_promo = cls.check_sourceqr(req['source'], merchentid)
            if check_promo:
                create_log_data(level='[ERROR]', Message="QR code already exist",
                                merchantID=merchentid, functionName="add_qrCode", request=request)
                return "QR already exist"

            if "print" in req and req['print'] == 1:
                create_log_data(level='[INFO]', Message="Requesting to update default qr to specify the print status",
                        merchantID=merchentid, functionName="add_qrCode",request=request)

            connection, cursor = get_db_connection()
            data = (
                merchentid,
                req['source']
            )
            cursor.execute(
                "INSERT INTO storefrontqr ( merchantid,source) VALUES (%s,%s)",
                data
            )
            connection.commit()
            create_log_data(level='[INFO]', Message="QR get created Successfully",
                            merchantID=merchentid, functionName="add_qrCode", request=request)
            return False

        except Exception as e:
            create_log_data(level='[ERROR]', Message="Failed to create qr",
                            messagebody=f"An error occured while creating qr: {e}",
                            merchantID=merchentid, functionName="add_qrCode", request=request)
            return "Error adding qr:", str(e)
        

    @classmethod
    def delete_qrCode(cls, sourceqrid):
        try:
            create_log_data(level='[INFO]', Message="Inside Storefront model to create a new qr code",
                        sourceqrId=sourceqrid, functionName="delete_qrCode",request=request)
            
    
            check_promo = cls.check_sourceqr_byId(sourceqrid)
            if check_promo:
                connection, cursor = get_db_connection()
                data = (
                    sourceqrid
                )
                cursor.execute(
                    "DELETE from storefrontqr where id = %s",
                    data
                )
                connection.commit()
                return False
            else:
                create_log_data(level='[INFO]', Message="QR code not exist",
                            sourceqrId=sourceqrid, functionName="delete_qrCode", request=request)
                return "QR code does not exist"

        except Exception as e:
            create_log_data(level='[ERROR]', Message="Failed to create qr",
                            messagebody=f"An error occured while creating qr: {e}",
                            sourceqrId=sourceqrid, functionName="delete_qrCode", request=request)
            return "Error deleting qr:", str(e)

    @classmethod
    def get_item_name_for_edit_promo(cls,itmeid):
        connection, cursor = get_db_connection()

        cursor.execute("""SELECT itemname
                            from items
                            WHERE id=%s""", (itmeid))
        row = cursor.fetchone()
        if row:
            return row['itemname']
        return ''

    @classmethod
    def map_promotype(cls, promotype):
        mapping = {
            '1': 'Free Purchase item Promo',
            '2': 'Buy X Get Y Free',
            '3': 'Buy X% Off on your total order'
        }
        return mapping[promotype]


    @classmethod
    def check_updated_promo_field(cls, old_promo, new_promo):

        updated_fields = []
        fields_to_compare = {
            "promo": "promo",
            "description": "description",
            "status": "status",
            "promotext": "promoText",
            "printinreceipt": "print",
            "freeitem": "freeitem",
            "minpurchaseamount": "minPurchaseAmount",
            "ishappyhourenabled": "ishappyhourenabled",
            "PromoType": "promoType",
            "promostartdate": "promostartdate",
            "promoenddate": "promoenddate",
            "primaryitem": "primaryitem",
            "primaryitemquantity": "primaryitemquantity",
            "freeitemquantity": "freeitemquantity",
            "maxdiscount": "maxDiscount",
            "discount": "promoDiscount",
            "happyhourstarttime": "happyhourstarttime",
            "happyhourendtime": "happyhourendtime",
            "days": "days",
            "source": "source",
        }

        for old_field_data, new_field_data in fields_to_compare.items():
            old_value = None
            new_value = None
            if type(old_promo[old_field_data]) == datetime.date:
                old_value, new_value = (old_promo.get(old_field_data), new_promo.get(new_field_data)) if str(
                    old_promo[old_field_data]) != str(new_promo[new_field_data]) else (None, None)
            elif type(old_promo[old_field_data]) == datetime.timedelta:
                if new_promo['ishappyhourenabled'] == 1:
                    if str(old_promo[old_field_data])[1] == ":":
                        if len(str(new_promo[new_field_data]))>5:
                            old_value, new_value = (
                                "0"+str(
                                    +old_promo[old_field_data])[:-3], str(new_promo[new_field_data])[:-3]) if "0" + str(
                                old_promo[old_field_data])[:-3] != str(new_promo[new_field_data])[:-3] else (None, None)
                        else:
                            old_value, new_value = (
                                "0" + str(
                                    +old_promo[old_field_data])[:-3], str(new_promo[new_field_data])) if "0" + str(
                                old_promo[old_field_data])[:-3] != str(new_promo[new_field_data]) else (None, None)

                    else:
                        if len(str(new_promo[new_field_data])) > 5:
                            old_value, new_value = (
                                str(
                                    old_promo[old_field_data])[:-3], str(new_promo[new_field_data])[:-3])  if str(
                                old_promo[old_field_data])[:-3] != str(new_promo[new_field_data])[:-3] else (None, None)
                        else:
                            old_value, new_value = (
                               str(
                                    +old_promo[old_field_data])[:-3], str(new_promo[new_field_data])) if str(
                                old_promo[old_field_data])[:-3] != str(new_promo[new_field_data]) else (None, None)
            elif type(new_promo[new_field_data]) == list:
                if new_promo['ishappyhourenabled'] == 1:
                    old_value, new_value = (old_promo.get(old_field_data).split(','), new_promo.get(new_field_data)) if \
                    old_promo[old_field_data] != ','.join(new_promo[new_field_data]) else (None, None)
            elif new_field_data == 'status':
                old_fields = 'Active' if old_promo[old_field_data] == 1 else 'Inactive'
                new_field = 'Active' if new_promo[new_field_data] else 'Inactive'
                if old_fields != new_field:
                    old_value, new_value = old_fields, new_field
            elif new_field_data in ['print', 'ishappyhourenabled']:
                old_fields = 'Enabled' if old_promo[old_field_data] == 1 else 'Disabled'
                new_field = 'Enabled' if new_promo[new_field_data] == 1 else 'Disabled'
                if old_fields != new_field:
                    old_value, new_value = old_fields, new_field
            elif new_field_data in ['freeitem', 'primaryitem']:
                old_value, new_value = (cls.get_item_name_for_edit_promo(old_promo[old_field_data]),
                                        cls.get_item_name_for_edit_promo(new_promo[new_field_data])) if old_promo[
                                                                                                            old_field_data] != \
                                                                                                        new_promo[
                                                                                                            new_field_data] else (
                None, None)
            elif new_field_data == 'promoType':
                old_value, new_value = (cls.map_promotype(str(old_promo[old_field_data])), cls.map_promotype(str(new_promo[new_field_data]))) if old_promo[
                                                                                                            old_field_data] != \
                                                                                                        new_promo[
                                                                                                            new_field_data] else (
                None, None)
            else:
                old_value, new_value = (old_promo[old_field_data], new_promo[new_field_data]) if old_promo[old_field_data] != new_promo[new_field_data] else (None, None)
            if old_value or new_value:
                format_fields = f'{old_field_data}: <{old_value}> to <{new_value}>'
                updated_fields.append(format_fields)
        return ','.join(updated_fields)

    @classmethod
    def edit_promo(cls, merchentid, user,ipAddress=None):
        try:
            create_log_data(level='[INFO]', Message=f"Inside Storefront model to edit a promo code, IP address: {ipAddress}",
                            merchantID=merchentid, functionName="edit_promo", request=request)
            req = request.json

            if req['print'] == 1:
                create_log_data(level='[INFO]', Message=f"Requesting to update default promo code, IP address: {ipAddress}",
                                merchantID=merchentid, functionName="edit_promo", request=request)
                cls.update_default_promo(merchentid)

            connection, cursor = get_db_connection()
            cursor.execute("""SELECT * FROM promo WHERE id = %s """, (req['promoId']))
            db_promo = cursor.fetchone()

            freeitem = req['freeitem'] if req['promoType'] == 1 or req['promoType'] == 2 else ''
            minPurchaseAmount = req['minPurchaseAmount'] if req['promoType'] == 1 or req['promoType'] == 3 else 0
            primaryitem = req['primaryitem'] if req['promoType'] == 2 else ''
            primaryitemquantity = req['primaryitemquantity'] if req['promoType'] == 2 else 0
            freeitemquantity = req['freeitemquantity'] if req['promoType'] == 2 else 0
            maxDiscount = req['maxDiscount'] if req['promoType'] == 3 else 0
            promoDiscount = req['promoDiscount'] if req['promoType'] == 3 else 0
            happyhourstarttime = req['happyhourstarttime'] if req['ishappyhourenabled'] == 1 else ''
            happyhourendtime = req['happyhourendtime'] if req['ishappyhourenabled'] == 1 else ''
            days = ",".join(req['days']) if req['ishappyhourenabled'] == 1 else ''
            data = (
                req['promo'],
                req['description'],
                req['status'],
                req['promoText'],
                req.get('print', 0),
                freeitem,
                minPurchaseAmount,
                req['ishappyhourenabled'],
                req['promoType'],
                req['promostartdate'],
                req['promoenddate'],
                primaryitem,
                primaryitemquantity,
                freeitemquantity,
                maxDiscount,
                promoDiscount,
                happyhourstarttime,
                happyhourendtime,
                days,
                req['redirecturl'],
                req['source'],
                req['promoId']
            )
            cursor.execute(

                "UPDATE promo SET promo=%s, description=%s, status=%s, promotext=%s, printinreceipt=%s, freeitem=%s, minpurchaseamount=%s, ishappyhourenabled=%s, PromoType=%s, promostartdate=%s, promoenddate=%s,primaryitem=%s,primaryitemquantity=%s, freeitemquantity=%s, maxdiscount=%s,discount=%s, happyhourstarttime=%s, happyhourendtime=%s, days=%s, redirecturl=%s, source=%s WHERE id=%s",

                data
            )
            connection.commit()
            changed_fields = cls.check_updated_promo_field(db_promo, req)
            if changed_fields:
                sns_msg = {
                    "event": "merchant.editPromo",
                    "body": {
                        "userId": user['id'],
                        "merchantId": merchentid,
                        "eventType": "activity",
                        "eventName": "promo.edit",
                        "eventDetails": f"Promo {req['promo']} updated ({changed_fields}), IP address: {ipAddress}"
                    }
                }
                publish_sns_message(topic=config.sns_activity_logs, message=str(sns_msg),
                                    subject="Promo_activity_logs")

            create_log_data(level='[INFO]', Message=f"Promo get updated Successfully, IP address: {ipAddress}",
                            merchantID=merchentid, functionName="edit_promo", request=request)
            return False
        except Exception as e:
            # print("Error adding free purchase item promo:", str(e))
            create_log_data(level='[ERROR]', Message=f"Failed to edit promo, IP address: {ipAddress}",
                            messagebody=f"An error occured while editing promo: {e}, IP address: {ipAddress}",
                            merchantID=merchentid, functionName="edit_promo", request=request)
            return "Error editing promo:", str(e)


    @classmethod
    def get_menu_item(cls,merchantid,freeitem=0):
        try:
            create_log_data(level='[INFO]', Message="Inside Storefront model to get all menu items for promos.",
                            merchantID=merchantid, functionName="get_menu_item", request=request)
            connection, cursor = get_db_connection()
            menumappings = MenuMappings.get_menumappings(merchantId=merchantid, platformType=50)
            print("menumappings length ", len(menumappings))
            if len(menumappings) == 0:

                create_log_data(level='[INFO]',
                                Message=f"Unable to fetch menu against merchant {merchantid} with platform type storefront, (Menu Mapping not found)",
                                merchantID=merchantid, functionName="get_menu_item", request=request)
                return []
            if len(menumappings) > 1:

                create_log_data(level='[INFO]',
                                Message=f"More than one menu is found against merchant {merchantid} with platform type storefront ",
                                merchantID=merchantid, functionName="get_menu_item", request=request)
                return []

            menu_id = menumappings[0]['menuid']
            cursor.execute("""SELECT * FROM menus WHERE id = %s """, (menu_id))
            menu = cursor.fetchone()

            if not menu:
                create_log_data(level='[INFO]',
                                Message=f"No menu is found against merchant {merchantid} ",
                                merchantID=merchantid, functionName="get_menu_item", request=request)
                return []
            cursor.execute("""SELECT categories.id id FROM menucategories, categories WHERE menucategories.categoryid=categories.id AND menucategories.menuid=%s  order by sortid asc """,
                           (menu["id"]))
            categories = cursor.fetchall()
            items = []
            for category in categories:
                if freeitem == 1:
                    cursor.execute(
                        "SELECT productscategories.productid id, items.itemname itemName FROM productscategories, items WHERE items.id = productscategories.productid AND productscategories.categoryid = %s  and  items.status=1 and  items.itemtype=1 order by sortid asc",
                        (category['id']))
                else:
                    cursor.execute(
                        "SELECT productscategories.productid id, items.itemname itemName FROM productscategories, items WHERE items.id = productscategories.productid AND productscategories.categoryid = %s  and  items.status=1 order by sortid asc",
                        (category['id']))
                allItems = cursor.fetchall()
                for item in allItems:
                    if freeitem == 1:
                        addonsWithOptions = ProductsAddons.get_productAddonsWithOptions(itemId=item['id'] , freeitem=freeitem)
                        if len(addonsWithOptions) == 0:
                            items.append({"id": item['id'], "itemName": item['itemName']})
                    else:
                        items.append({"id": item['id'], "itemName": item['itemName']})
            return items
        except Exception as e:
            create_log_data(level='[ERROR]',
                            Message=f"Failed to retrieve menu items against merchant {merchantid}", messagebody=f"An error occured {e}",
                            merchantID=merchantid, functionName="get_menu_item", request=request)
            return e

    @classmethod
    def get_all_promo(cls, merchantid, startdate, enddate, status, limit):
        try:
            create_log_data(level='[INFO]', Message="Inside Storefront model to get all promos code.",
                            merchantID=merchantid, functionName="get_all_promo", request=request)

            connection, cursor = get_db_connection()
            limit_append = '' if limit == 0 else f'LIMIT {limit}'
            if status is None:
                status = 1
            if startdate == "":
                startdate = datetime.datetime.now() - timedelta(hours=87600)
            if enddate == "":
                enddate = datetime.datetime.now()
            
            merchant_details = Merchants.get_merchant_by_id(merchantid)
            merchantTimezone = merchant_details.get("timezone")
            startdate = startdate.replace(tzinfo=gettz(merchantTimezone)).astimezone(datetime.timezone.utc)
            enddate = enddate.replace(tzinfo=gettz(merchantTimezone)).astimezone(
                datetime.timezone.utc) + datetime.timedelta(
                days=1)
            sql_query = f"""SELECT * FROM promo 
                        WHERE merchantid = %s  and status = %s
                        ORDER BY createddatetime DESC {limit_append}"""

            cursor.execute(sql_query, (merchantid, status))
            promos = cursor.fetchall()

            all_promo = []
            
            for promo in promos:
                sql_query = """
                                        SELECT promoid, 
                                               SUM(ordertotal) AS total_ordertotal, 
                                               SUM(promodiscount) AS promo_discount,
                                               COUNT(*)  as count_record
                                        FROM (
                                            SELECT promoid, ordertotal, promodiscount
                                            FROM orders 
                                            WHERE status = %s 
                                                AND merchantid = %s 
                                                AND orderdatetime BETWEEN %s AND %s
                                                AND promoid = %s
                                            UNION ALL
                                            SELECT promoid, ordertotal, promodiscount
                                            FROM ordershistory 
                                            WHERE status = %s 
                                                AND merchantid = %s 
                                                AND orderdatetime BETWEEN %s AND %s
                                                AND promoid = %s
                                        ) AS promo_orders
                                        GROUP BY promoid;
                                    """

                params = (
                    7, merchantid, startdate, enddate, promo['id'], 7, merchantid, startdate, enddate, promo['id'])

                cursor.execute(sql_query, params)
                rows = cursor.fetchall()
                total_sale = rows[0]['total_ordertotal'] if rows else 0
                total_cost = rows[0]['promo_discount'] if rows else 0

                roi = ((total_sale - total_cost) / total_cost) * 100 if total_sale != 0 and total_cost != 0 else 0

                create_log_data(level='[INFO]',
                                Message=f"Successfully fetched data of orders against promo {promo['promo']}",
                                merchantID=merchantid, functionName="get_all_promo", request=request)
                happyhourstarttime = cls.timedelta_to_24_hour_format(promo['happyhourstarttime']) if promo[
                    'happyhourstarttime'] else ''
                happyhourendtime = cls.timedelta_to_24_hour_format(promo['happyhourendtime']) if promo[
                    'happyhourendtime'] else ''

                all_promo.append({
                    "promoId": promo['id'],
                    "promo": promo['promo'],
                    "description": promo['description'],
                    "primaryitem": promo['primaryitem'] if promo['primaryitem'] else "",
                    "primaryitemquantity": promo['primaryitemquantity'] if promo['primaryitemquantity'] else 0,
                    "freeitemquantity": promo['freeitemquantity'] if promo['freeitemquantity'] else 0,
                    "freeitem": promo['freeitem'] if promo['freeitem'] else '',
                    "promostartdate": promo['promostartdate'].strftime('%Y-%m-%d') if promo['promostartdate'] else '',
                    "promoenddate": promo['promoenddate'].strftime('%Y-%m-%d') if promo['promoenddate'] else '',
                    "promoDiscount": float(promo['discount'] if promo['discount'] else 0),
                    "status": promo['status'],
                    "promoText": promo['promotext'],
                    "source": promo['source'],
                    "print": promo['printinreceipt'],
                    "promoType": promo['PromoType'],
                    "source": promo['source'],
                    "minPurchaseAmount": float(promo['minpurchaseamount'] if promo['minpurchaseamount'] else 0),
                    "maxDiscount": float(promo['maxdiscount'] if promo['maxdiscount'] else 0),
                    "ishappyhourenabled": promo['ishappyhourenabled'],
                    "happyhourstarttime": happyhourstarttime,
                    "happyhourendtime": happyhourendtime,
                    "days": promo['days'].split(',') if promo['days'] else [],
                    "sale": float(total_sale),
                    "cost": float(total_cost),
                    "roi": "{:.2f}".format(roi),
                    "redirecturl": promo['redirecturl'] if promo['redirecturl'] else '',
                    "count": rows[0]['count_record'] if rows else 0,

                })
            create_log_data(level='[INFO]',
                            Message=f"Retrieved all promos against merchant {merchantid}",
                            merchantID=merchantid, functionName="get_all_promo", request=request)
            return all_promo
        except Exception as e:
            create_log_data(level='[ERROR]',
                            Message=f"Failed to retrieve promos against merchant {merchantid}",
                            messagebody=f"An error occured {e}",
                            merchantID=merchantid, functionName="get_all_promo", request=request)
            return e

    @classmethod
    def get_all_source_qr(cls, merchantid):
        try:
            create_log_data(level='[INFO]', Message="Inside Storefront model to get all qrs code.",
                            merchantID=merchantid, functionName="get_all_source_qr", request=request)

            connection, cursor = get_db_connection()

        

            sql_query = f"""SELECT * FROM storefrontqr 
                        WHERE merchantid = %s 
                        ORDER BY createddatetime """

            cursor.execute(sql_query, (merchantid))
            storefrontqrs = cursor.fetchall()

            all_qr = []
            for qr in storefrontqrs:
            

                all_qr.append({
                    "qrId": qr['id'],
                    "merchantid": qr['merchantid'],
                    "source": qr['source'],
                })
            create_log_data(level='[INFO]',
                            Message=f"Retrieved all qrs against merchant {merchantid}",
                            merchantID=merchantid, functionName="get_all_source_qr", request=request)
            return all_qr
        except Exception as e:
            create_log_data(level='[ERROR]',
                            Message=f"Failed to retrieve qrs against merchant {merchantid}",
                            messagebody=f"An error occured {e}",
                            merchantID=merchantid, functionName="get_all_source_qr", request=request)
            return e

    @classmethod
    def timedelta_to_24_hour_format(cls,td):
        total_seconds = td.total_seconds()
        hours = int(total_seconds // 3600)
        minutes = int((total_seconds % 3600) // 60)
        seconds = int(total_seconds % 60)

        return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


    @classmethod
    def get_all_storefront_promo(cls,merchantid):

        try:
            create_log_data(level='[INFO]',
                            Message=f"Inside Storefront model to get all storefront promos code.",
                            merchantID=merchantid, functionName="get_all_storefront_promo", request=request)
            connection, cursor = get_db_connection()
            cursor.execute("SELECT timezone FROM merchants WHERE id=%s", (merchantid,))
            merchant = cursor.fetchone()  # Fetch one row

            current_datetime_all = datetime.datetime.now().astimezone(gettz(merchant['timezone']))
            current_date = current_datetime_all.strftime("%Y-%m-%d")
            current_time = current_datetime_all.strftime("%H:%M:%S")
            current_day = current_datetime_all.strftime("%a").upper()
            create_log_data(level='[INFO]',
                            Message=f"Changed timezone according to merchant specific timezone",
                            merchantID=merchantid, functionName="get_all_storefront_promo", request=request)
            cursor.execute(
                "SELECT * FROM promo WHERE merchantid =%s AND status=1 and promostartdate <= %s AND promoenddate >= %s",
                (merchantid, current_date, current_date))
            query_result = cursor.fetchall()
            all_promo = []

            for promo in query_result:
                if promo['ishappyhourenabled'] == 1:
                    happyhourendtime = cls.timedelta_to_24_hour_format(promo['happyhourendtime'])
                    happyhourstarttime = cls.timedelta_to_24_hour_format(promo['happyhourstarttime'])
                    if happyhourstarttime <= current_time <= happyhourendtime and current_day in promo['days']:
                        promo_instance_enabled = {
                            "promoId": promo['id'],
                            "promo": promo['promo'],
                            "description": promo['description'],
                            "status": promo['status'],
                            "promoText": promo['promotext'],
                            "source": promo['source'],
                            "print": promo['printinreceipt'],
                            "minPurchaseAmount": float(promo['minpurchaseamount'] if promo['minpurchaseamount'] else 0),
                            "freeitem": promo['freeitem'],
                            "primaryitem": promo['primaryitem'],
                            "primaryitemquantity": promo['primaryitemquantity'],
                            "freeitemquantity": promo['freeitemquantity'],
                            "promostartdate": promo['promostartdate'].strftime('%m-%d-%Y'),
                            "promoenddate": promo['promoenddate'].strftime('%m-%d-%Y'),
                            "promoType": promo['PromoType'],
                            "maxDiscount": float(promo['maxdiscount'] if promo['maxdiscount'] else 0),
                            "promoDiscount": float(promo['discount'] if promo['discount'] else 0),
                            "ishappyhourenabled": 1,
                            "happyhourstarttime": happyhourstarttime,
                            "happyhourendtime": happyhourendtime,
                            "days": promo['days'].split(',') if promo['days'] else [],
                        }
                        all_promo.append(promo_instance_enabled)
                    else:
                        continue
                elif promo['ishappyhourenabled'] == 0:
                    promo_instance_not_enabled = {
                        "promoId": promo['id'],
                        "promo": promo['promo'],
                        "description": promo['description'],
                        "status": promo['status'],
                        "source": promo['source'],
                        "promoText": promo['promotext'],
                        "print": promo['printinreceipt'],
                        "promostartdate": promo['promostartdate'].strftime('%m-%d-%Y'),
                        "promoenddate": promo['promoenddate'].strftime('%m-%d-%Y'),
                        "promoType": promo['PromoType'],
                        "minPurchaseAmount": float(promo['minpurchaseamount'] if promo['minpurchaseamount'] else 0),
                        "freeitem": promo['freeitem'],
                        "primaryitem": promo['primaryitem'],
                        "primaryitemquantity": promo['primaryitemquantity'],
                        "freeitemquantity": promo['freeitemquantity'],
                        "maxDiscount": float(promo['maxdiscount']if promo['maxdiscount'] else 0),
                        "promoDiscount": float(promo['discount'] if promo['discount'] else 0),
                        "ishappyhourenabled": 0,
                    }
                    all_promo.append(promo_instance_not_enabled)

            create_log_data(level='[INFO]',
                            Message=f"Retrieved all storefornt promos against merchant {merchantid}",
                            merchantID=merchantid, functionName="get_all_storefront_promo", request=request)
            return all_promo

        except Exception as e:
            # print("Error adding free purchase item promo:", str(e))
            create_log_data(level='[ERROR]',
                            Message=f"Failed to retrieve storefront promos against merchant {merchantid}",
                            messagebody=f"An error occured {e}",
                            merchantID=merchantid, functionName="get_all_storefront_promo", request=request)
            return e

    @classmethod
    def validate_promo(cls, merchantid,promo):
        create_log_data(level='[INFO]',
                        Message=f"Inside Storefront model to validate promo.",
                        merchantID=merchantid, functionName="validate_promo", request=request)

        connection, cursor = get_db_connection()
        cursor.execute("SELECT timezone FROM merchants WHERE id=%s", (merchantid,))
        merchant = cursor.fetchone()
        if merchant:
            current_datetime_all = datetime.datetime.now().astimezone(gettz(merchant['timezone']))
            current_date = current_datetime_all.strftime("%Y-%m-%d")
            current_time = current_datetime_all.strftime("%H:%M:%S")
            current_day = current_datetime_all.strftime("%a").upper()
            create_log_data(level='[INFO]',
                            Message=f"Changed timezone according to merchant specific timezone",
                            merchantID=merchantid, functionName="validate_promo", request=request)
        else:
            create_log_data(level='[ERROR]',
                            Message=f"Record not found against merchantid {merchantid}",
                            merchantID=merchantid, functionName="validate_promo", request=request)
            return None

        cursor.execute("""SELECT * FROM promo WHERE merchantid=%s and promo=%s and status=1 and promoenddate>=%s""",
                       (merchantid, promo,current_date))
        promo_query = cursor.fetchone()
        print(promo_query)
        if promo_query:
            if promo_query['ishappyhourenabled'] == 1:
                happyhourendtime = cls.timedelta_to_24_hour_format(promo_query['happyhourendtime'])
                happyhourstarttime = cls.timedelta_to_24_hour_format(promo_query['happyhourstarttime'])
                if happyhourstarttime <= current_time <= happyhourendtime and current_day in promo_query['days']:
                    print(promo_query)
                    return promo_query
                else:
                    return None

        return promo_query

        # return cursor.fetchone()


    @classmethod
    def update_default_promo(cls, merchantid):
        connection, cursor = get_db_connection()
        data = (merchantid)
        cursor.execute(
            "UPDATE promo SET printinreceipt=0  WHERE merchantid=%s", data)
        connection.commit()

        return True


    @classmethod
    def get_storefront_promo_by_id(cls, merchantid, promoid):
        try:
            connection, cursor = get_db_connection()
            cursor.execute("""SELECT * FROM promo WHERE merchantid=%s and id=%s""", (merchantid, promoid))
            return cursor.fetchone()

        except Exception as e:
            print(str(e))
            return False
