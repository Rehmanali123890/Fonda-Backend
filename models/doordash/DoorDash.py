from flask import jsonify
import json
import requests
from datetime import datetime
import uuid
import calendar
import jwt.utils
import time
import math
# local imports
import config
from models.Addons import Addons
import models.Platforms  # we have to avoid circular imports
from utilities.helpers import get_db_connection
from models.MenuCategories import MenuCategories
from models.Items import Items
from models.MenuMappings import MenuMappings
from models.Categories import Categories
from models.Merchants import Merchants
from models.Metadata import Metadata


class Doordash():


    ############################################### Doordash MENU

    @classmethod
    def doordash_post_menu(cls, jwtToken, payload):
        try:
            # url = f"https://openapi.doordash.com/marketplace/api/v1/menus"
            url = f"https://openapi.doordash.com/marketplace/api/v1/menus/f4577bf7-9fbc-42a4-a3ea-33bb42c7886e"

            headers = {
                'Authorization': f'Bearer {jwtToken}',
                'Content-Type': 'application/json',
                'auth-version':'v2',
                'User-Agent':'fonda_inc_sandbox/1.0'
            }
            response = requests.request("PATCH", url, headers=headers, data=payload)
            print(response.text)
            if response and response.status_code >= 200 and response.status_code < 300:
                return True, response.status_code, "success"
            else:
                return False, response.status_code, response.json()
        except Exception as e:
            print("Error UberEats: ", str(e))
            return False, 500, str(e)

    ############################################### JWT Token
    @classmethod
    def doordash_jwtToken(cls):
        try:

            token=jwt.encode(
              {
                "aud": "doordash",
                "iss": "67c3170e-716f-4367-8622-2ef9ba92cac7",
                "kid": "e278a8ba-8c55-4512-af79-a1ecc21811fc",
                "exp": str(math.floor(time.time() + 1800)),
                "iat": str(math.floor(time.time())),
              },
              jwt.utils.base64url_decode('IO8b3O2H0G7fv3DNj-JtBLJFVovuOnQ7pOgNVNwIDsI'),
              algorithm="HS256",
              headers={"dd-ver": "DD-JWT-V1", "typ": "JWT"})
            return token
        except Exception as e:
            print("Error UberEats: ", str(e))
            return False, 500, str(e)


    ############################################### POST COMPLETE MENU


    @classmethod
    def post_complete_menu_doordash(cls, platformId):
        try:
            print('-------------------------------------------------------------------')
            print('-------------------- Doordash MENU MANUAL SYNC --------------------')

            connection, cursor = get_db_connection()

            print("Get required details from platforms table...")
            row = models.Platforms.Platforms.get_platform_by_id(
                platformId)  # we do import like this because we have to avoid circular imports
            storeId = row["storeid"]
            platformType = row["platformtype"]
            syncMerchantId = row["merchantid"]

            # get main/sync merchant details
            merchant_details = Merchants.get_merchant_or_virtual_merchant(syncMerchantId)
            if merchant_details.get("isVirtual") == 1:
                mainMerchantId = merchant_details.get("merchantid")
                isVirtualMerchant = 1
            else:
                mainMerchantId = syncMerchantId
                isVirtualMerchant = 0

            print("Store id: ", storeId)
            print("Sync Merchant id: ", syncMerchantId)
            print("Main Merchant id: ", mainMerchantId)
            print("Platform Type: ", platformType)

            print('check if uber eats provision is done...')
            if not (storeId and int(platformType) == 6):
                return False, 400, "Ubereats is not provisioned for the merchant!"

            # check and refresh access token
            jwtToken = cls.doordash_jwtToken()
            if not jwtToken:
                return False, 500, "Unhandled exception occured while checking for ubereats access token"
            print(jwtToken)

            # form the payload
            payload, msg = cls.create_menu_payload_doordash(mainMerchantId, syncMerchantId, isVirtualMerchant,
                                                                     platformType,storeId)
            if not payload:
                print(msg)
                return False, 500, msg
            print(payload)
            # post menu to ubereats
            resp, status_code, msg = cls.doordash_post_menu(
                jwtToken=jwtToken,
                payload=payload
            )
            print("UberEats Post Menu Message: ", msg)
            print("Status: ", status_code)

            if not resp:
                return False, 500, msg.get("message") if msg.get("message") else msg

            # add items_ids to itemmappings table metadata field
        #     print("storing all items ids into itemmappings table for future changes in items...")
        #     cursor.execute("""DELETE FROM itemmappings WHERE merchantid=%s AND platformtype=%s""",
        #                    (syncMerchantId, platformType))
        #     connection.commit()
        #     cursor.execute("""INSERT INTO itemmappings (id, merchantid, platformtype, metadata)
        # VALUES (%s,%s,%s,%s)""", (uuid.uuid4(), syncMerchantId, platformType, json.dumps(all_ids)))
        #     connection.commit()
        #     print("successfully stored")

            return True, 200, "success"
        except Exception as e:
            print("Error: ", str(e))
            return False, 500, str(e)

    @classmethod
    def create_menu_payload_doordash(cls, mainMerchantId, syncMerchantId, isVirtualMerchant, platformType,storeId):
        try:

            connection, cursor = get_db_connection()



            # get merchant details
            merchant_details = Merchants.get_merchant_by_id(mainMerchantId)
            menu_detail=cls.get_menuID(mainMerchantId)
            if menu_detail:

                # json data
                data = {
                    "store": {
                        "merchant_supplied_id": storeId,
                        "provider_type": "fonda_inc_sandbox"
                    },
                    "menu": {
                        "name": menu_detail['name'],
                        "subtitle": menu_detail['name'],
                        "merchant_supplied_id":menu_detail['id'],
                        "active": True,
                        "categories": []
                    },
                    "store_open_hours": []
                }

                category_no=0




                # get menu categories
                menu_categories = MenuCategories.get_menucategories_fk(menuId=menu_detail['id'], platformType=1,
                                                                       order_by=["sortid ASC"])  # 1=apptopus
                for categorry in menu_categories:
                    category_no=category_no+1
                    print("category_no " , category_no)
                    cursor.execute("""SELECT * FROM categories WHERE id=%s and merchantid=%s""",
                                   (categorry['categoryid'],mainMerchantId))
                    categorry_row = cursor.fetchone()
                    category_data={
                        "name": categorry_row['categoryname'],
                        "subtitle": categorry_row['posname'],
                        "sort_id": categorry['sortid'],
                        "active": True,
                        "merchant_supplied_id": categorry_row['id'],
                        "items":[]
                    }
                    cursor.execute("""SELECT * FROM productscategories WHERE categoryid=%s""",
                                   (categorry_row['id']))
                    items = cursor.fetchall()
                    item_no = 0
                    for item in items:
                        item_no = item_no + 1
                        print("item_no ", item_no , "category_no" , category_no)
                        cursor.execute("""SELECT * FROM items WHERE id=%s and merchantid=%s""",
                                       (item['productid'], mainMerchantId))
                        item_row = cursor.fetchone()
                        item_data={

                                "name":item_row['itemname'] ,
                                "description": item_row['itemdescription'],
                                "price": int(item_row['itemprice']*100),
                                "merchant_supplied_id": item_row['id'],
                                "active": True,
                                "sort_id": item['sortid'],
                                "extras": []
                        }
                        cursor.execute("""SELECT * FROM productsaddons WHERE productid=%s""",
                                       (item_row['id']))
                        Addons = cursor.fetchall()
                        addon_no = 0
                        for addon in Addons:
                            addon_no = addon_no + 1
                            print("addon_no ", addon_no , "item no " , item_no, "category_no" , category_no)
                            cursor.execute("""SELECT * FROM addons WHERE id=%s and merchantid=%s""",
                                           (addon['addonid'], mainMerchantId))
                            addon_row = cursor.fetchone()
                            addon_data={
                               "name":addon_row['addonname'],
                               "active": True,
                               "sort_id": addon['sortid'],
                               "merchant_supplied_id": addon_row['id'],
                                "min_num_options": addon_row['minpermitted'],
                                "max_num_options": addon_row['maxpermitted'],
                                # "min_option_choice_quantity": 1,
                                # "max_option_choice_quantity": 2,
                                # "min_aggregate_options_quantity": 1,
                                # "max_aggregate_options_quantity": 4,
                                "options": []
                            }

                            cursor.execute("""SELECT * FROM addonsoptions WHERE addonid=%s""",
                                           (addon_row['id']))
                            options = cursor.fetchall()
                            option_no = 0
                            for option in options:
                                option_no = option_no + 1
                                print("option_no ", option_no, "addon_no no ", addon_no , "item_no " , item_no , "category_no" , category_no)
                                print("option")
                                cursor.execute("""SELECT * FROM items WHERE id=%s and merchantid=%s""",
                                               (option['itemid'], mainMerchantId))
                                option_row = cursor.fetchone()
                                option_data={
                                    "name": option_row['itemname'],
                                    "active": True,
                                    "price": int(option_row['itemprice']*100),
                                    "merchant_supplied_id": option_row['id'],
                                    "sort_id": option['sortid'],
                                    "default": True,
                                    "extras": [],
                                    "dish_info": {
                                        "nutritional_info": {
                                            "calorific_info": {
                                                "lower_range": 200
                                            }
                                        }
                                    }

                                }
                                addon_data['options'].append(option_data)
                            item_data['extras'].append(addon_data)
                        category_data['items'].append(item_data)
                    data['menu']['categories'].append(category_data)
                print("data is ", data)
                cursor.execute("""SELECT * FROM serviceavailability WHERE menuid=%s""",
                               (menu_detail['id']))
                menu_hours = cursor.fetchall()

                for day_hours in menu_hours:
                    day_hour_data = {
                        "start_time": str(day_hours['starttime']),
                        "end_time": str(day_hours['endtime']),
                        "day_index": calendar.day_abbr[day_hours['weekday'] - 1].upper()
                    }
                    data['store_open_hours'].append(day_hour_data)

                print("data is ", data)


                print("\nUberEats Menu payload: ")
                payload = json.dumps(data)

                return payload, "success"
            else:
                return '','error'

        except Exception as e:
            return False, f"Create Menu Payload Error: {e}", ""

    @classmethod
    def get_menuID(cls,mainMerchantId):
        try:
            connection, cursor = get_db_connection()
            cursor.execute("""SELECT * FROM menus WHERE merchantid=%s""",
                           (mainMerchantId))
            rows = cursor.fetchall()
            if rows:
                return rows[0]
            else:
                return False

        except Exception as e:
            return False, f"Create Menu Payload Error: {e}", ""