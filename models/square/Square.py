import time
from flask import jsonify
import json
import requests
import uuid
import random
import string
import datetime

# local imports
import config
from models.Addons import Addons
from models.Items import Items
from utilities.helpers import get_db_connection, success, is_float
from utilities.errors import invalid, unhandled, not_found
from models.Platforms import Platforms
from models.MenuMappings import MenuMappings
from models.MenuCategories import MenuCategories
from models.ProductsCategories import ProductsCategories
from models.ItemMappings import ItemMappings
from models.AddonMappings import AddonMappings


class Square():

  ###############################################

  @classmethod
  def square_get_request_headers(cls, accessToken):
    return {
      'Square-Version': '2022-01-20',
      'Authorization': f'Bearer {accessToken}',
      'Content-Type': 'application/json'
    }

  ############################################### CATALOG

  @classmethod
  def square_list_catalog(cls, accessToken, cursor=None, types=None):
    try:
      url = f"{config.square_base_url}/v2/catalog/list?cursor={cursor if cursor else ''}&types={types if types else ''}"
      headers = cls.square_get_request_headers(accessToken)
      payload = {}
      response = requests.request("GET", url, headers=headers, data=payload)
      data = response.json() if response and response.status_code >= 200 and response.status_code < 300 else False
      if not data:
        print(response.text)
      return data
    except Exception as e:
      print("error: ", str(e))
      return False

  @classmethod
  def square_retrieve_catalog_object(cls, accessToken, objectId):
    try:
      url = f"{config.square_base_url}/v2/catalog/object/{objectId}"
      headers = cls.square_get_request_headers(accessToken)
      payload = {}
      response = requests.request("GET", url, headers=headers, data=payload)
      res = True if response and response.status_code >= 200 and response.status_code < 300 else False
      if res:
        return res, response.json()
      else:
        print(response.text)
        return res, response.text
    except Exception as e:
      print("error: ", str(e))
      return False, f"error: {str(e)}"

  @classmethod
  def square_search_catalog_objects(cls, accessToken, payload):
    try:
      # https://developer.squareup.com/explorer/square/catalog-api/search-catalog-objects
      url = f"{config.square_base_url}/v2/catalog/search"
      headers = cls.square_get_request_headers(accessToken)
      response = requests.request("POST", url, headers=headers, data=json.dumps(payload))
      res = True if response and response.status_code >= 200 and response.status_code < 300 else False
      if res:
        return res, response.json()
      else:
        print(response.text)
        return res, response.text
    except Exception as e:
      print("error: ", str(e))
      return False, f"error: {str(e)}"

  @classmethod
  def square_upsert_catalog_objects(cls, accessToken, payload):
    try:
      url = f"{config.square_base_url}/v2/catalog/object"
      headers = cls.square_get_request_headers(accessToken)
      response = requests.request("POST", url, headers=headers, data=json.dumps(payload))
      res = True if response and response.status_code >= 200 and response.status_code < 300 else False
      if res:
        return res, response.json()
      else:
        print(response.text)
        return res, response.text
    except Exception as e:
      print("error: ", str(e))
      return False, f"error: {str(e)}"

  @classmethod
  def square_batch_upsert_catalog_objects(cls, accessToken, payload):
    try:
      url = f"{config.square_base_url}/v2/catalog/batch-upsert"
      headers = cls.square_get_request_headers(accessToken)
      response = requests.request("POST", url, headers=headers, data=json.dumps(payload))
      res = True if response and response.status_code >= 200 and response.status_code < 300 else False
      print(res)
      if res:
        return res, response.json()
      else:
        print(response.text)
        return res, response.text
    except Exception as e:
      print("error: ", str(e))
      return False, f"error: {str(e)}"

  @classmethod
  def square_batch_delete_catalog_objects(cls, accessToken, payload):
    try:
      url = f"{config.square_base_url}/v2/catalog/batch-delete"
      headers = cls.square_get_request_headers(accessToken)
      response = requests.request("POST", url, headers=headers, data=json.dumps(payload))
      res = True if response and response.status_code >= 200 and response.status_code < 300 else False
      if res:
        return res, response.json()
      else:
        print(response.text)
        return res, response.text
    except Exception as e:
      print("error: ", str(e))
      return False, f"error: {str(e)}"

  ##################################################################
  ############################################### POST COMPLETE MENU

  @classmethod
  def post_complete_menu_square(cls, platformId):
    try:
      print('-------------------------------------------------------------------')
      print('--------------------- SQUARE MENU MANUAL SYNC ---------------------')

      connection, cursor = get_db_connection()

      print("get required details from platforms table...")
      platform_details = Platforms.get_platform_by_id(platformId)
      storeId = platform_details["storeid"]
      merchantId = platform_details["merchantid"]
      platformType = platform_details["platformtype"]
      accessToken = platform_details["accesstoken"]

      print("Store id: ", storeId)
      print("Merchant id: ", merchantId)
      print("Platform Type: ", platformType)

      if not (storeId and int(platformType) == 11):
        return False, 500, "error: square is not provisioned for the merchant!"

      print("get menu assigned to square...")
      mappings = MenuMappings.get_menumappings(merchantId=merchantId, platformType=platformType)
      if len(mappings) == 0:
        return False, 500, "error: no menu is assigned to square!"
      if len(mappings) > 1:
        return False, 500, "error: more than 1 menu have been assigned to square!"
      menu_mapping = mappings[0]

      # delete last_catalog_update_time from menumappings in order to avoid issues
      if menu_mapping.get("metadata") != None:
        cursor.execute("""
          UPDATE menumappings 
            SET metadata=%s 
            WHERE id=%s""",
                       (None, menu_mapping['id'])
                       )
        connection.commit()



      '''
      TODO: Getting square catalog object ids from our data base and 
      Deleting old menu data from the square pos
        menucategories, itemmappings, addonmappings
      '''

      # Execute the combined query
      cursor.execute("""
          SELECT menucategories.platformcategoryid AS id
          FROM menucategories
          WHERE menucategories.merchantid = %s
            AND menucategories.platformtype = %s
            AND menucategories.menuid = %s

          UNION

          SELECT itemmappings.platformitemid AS id
          FROM itemmappings
          WHERE itemmappings.merchantid = %s
            AND itemmappings.platformtype = %s
            AND itemmappings.menuid = %s

          UNION

          SELECT addonmappings.platformaddonid AS id
          FROM addonmappings
          WHERE addonmappings.merchantid = %s
            AND addonmappings.platformtype = %s
            AND addonmappings.menuid = %s
      """, (merchantId, platformType, menu_mapping['menuid'],
            merchantId, platformType, menu_mapping['menuid'],
            merchantId, platformType, menu_mapping['menuid']))

      # Fetch all results

      # get all catalogs ids from square and store ids in a list
      ids = cursor.fetchall()
      all_catalgos_ids= [ id['id'] for id in ids]

      print("total catalogs ids: ", str(len(all_catalgos_ids)))
      if len(all_catalgos_ids):
        # Splitting the list into chunks of 200 elements each
        chunk_size = 190  # max allowed value is 200
        chunks = [all_catalgos_ids[i:i + chunk_size] for i in range(0, len(all_catalgos_ids), chunk_size)]

        # Iterating over the chunks and calling the function for each chunk
        for chunk in chunks:
          # Creating payload for batch delete
          payload = {"object_ids": chunk}

          # Calling batch delete function
          del_catalogs_resp, del_catalogs_resp_data = cls.square_batch_delete_catalog_objects(accessToken,
                                                                                              payload=payload)

          if not del_catalogs_resp:
            return False, 500, del_catalogs_resp_data

      '''
          TODO: Deleting old menu data from our database
            menucategories, itemmappings, addonmappings
          '''
      print("deleting old menu data from our database and also from the square...")
      resp = MenuCategories.delete_menucategories(merchantId=merchantId, menuId=menu_mapping['menuid'], platformType=platformType )
      if not resp:
        print("error: while deleting old data from menu-categories!!!")
      resp = ItemMappings.delete_itemmappings(merchantId=merchantId, menuId=menu_mapping['menuid'], platformType=platformType)
      if not resp:
        print("error: while deleting old data from item-mappings!!!")
      resp = AddonMappings.delete_addonmappings(merchantId=merchantId, menuId=menu_mapping['menuid'], platformType=platformType)
      if not resp:
        print("error: while deleting old data from addon-mappings!!!")

      '''
        <- 1 -> TODO: Send all addons with options to square and store square ids in database.
          Create a dictionary where dashboardaddons_squareaddons_ids mappings is stored
      '''

      print("send all addons with options to square and store square ids in database....")

      # get all addons with options
      all_addons = Addons.get_all_addons_with_options_str(merchantId=merchantId, platformType=platformType)

      # some lists and dicts
      temp_addons_mappings_dict = dict()  # to store apptopus addonid and addonoptonid temporarily
      all_addons_mappings = list(tuple())  # to create tuple for bulk insert to the addonmappings table

      # create addons payload for square
      square_addons_payload = {
        "idempotency_key": str(uuid.uuid4()),
        "batches": [
          {
            "objects": []
          }
        ]
      }

      for addon in all_addons:

        addonId = addon["id"]
        tempAddonId = '#addon' + ''.join(random.choices(string.ascii_letters + string.digits, k=7))
        addonName = addon["posName"] if addon["posName"] else addon["addonName"]
        minPermitted = addon["minPermitted"]
        maxPermitted = addon["maxPermitted"]

        if not len(
            addon.get("addonOptions")):  # (addon must have addon-options assign in order to be sent by square api)
          continue

        temp_addons_mappings_dict[tempAddonId] = {
          "apptopus_addon_id": addonId,
          "apptopus_addon_option_id": None
        }

        addon_payload = {
          "id": tempAddonId,
          "type": "MODIFIER_LIST",
          "modifier_list_data": {
            "name": addonName,
            "selection_type": "SINGLE" if maxPermitted <= 1 else "MULTIPLE",
            "modifiers": []
          }
        }

        for option in addon.get("addonOptions"):

          addonOptionId = option["id"]
          tempAddonOptionId = '#option' + ''.join(random.choices(string.ascii_letters + string.digits, k=7))
          addonOptionName = option["posName"] if option["posName"] else option["addonOptionName"]
          addonOptionPrice = int(float(option["addonOptionPrice"]) * 100) if is_float(option["addonOptionPrice"]) else 0
          addonOptionStatus = option["addonOptionStatus"]

          temp_addons_mappings_dict[tempAddonOptionId] = {
            "apptopus_addon_id": addonId,
            "apptopus_addon_option_id": addonOptionId
          }

          if addonOptionStatus == 1:
            addon_payload["modifier_list_data"]["modifiers"].append({
              "id": tempAddonOptionId,
              "type": "MODIFIER",
              "modifier_data": {
                "modifier_list_id": f"{tempAddonId}",
                "name": addonOptionName,
                "price_money": {
                  "amount": addonOptionPrice,
                  "currency": "USD"
                }
              }
            })

        # append addon-with-options to square_addons_payload
        if len(addon_payload["modifier_list_data"]["modifiers"]):
          square_addons_payload["batches"][0]["objects"].append(addon_payload)

          # upload addons to square
      square_addons_res, square_addons_res_data = cls.square_batch_upsert_catalog_objects(accessToken,
                                                                                          payload=square_addons_payload)
      if not square_addons_res:
        return False, 500, f"error: occured while posting all addons to square! {square_addons_res_data}"

      # loop over returned ids from square response and form a payload for addonsmappings
      for key, value in temp_addons_mappings_dict.items():
        for row in square_addons_res_data.get("id_mappings"):
          if row["client_object_id"] == key:
            all_addons_mappings.append((str(uuid.uuid4()), merchantId, menu_mapping['menuid'],
                                        value["apptopus_addon_id"], value["apptopus_addon_option_id"], platformType,
                                        row["object_id"]))
            break

      # insert data into addonmappoings table
      print("insert data into addonmappings table...")
      cursor.executemany("""
        INSERT INTO addonmappings
        (id, merchantid, menuid, addonid, addonoptionid, platformtype, platformaddonid)
        VALUES (%s,%s,%s,%s,%s,%s,%s)""", (all_addons_mappings))
      connection.commit()

      '''
        <- 2 -> TODO: Get all menu-categories and upload them to the square.
          Store the square category ids into menucategories table
      '''

      print("Get all menu-categories...")
      categories = MenuCategories.get_menucategories(menuId=menu_mapping['menuid'], platformType=1)  # 1=apptopus

      # some lists and dicts for categories
      temp_category_mapping_dict = dict()  # temporarily storing tempCategoryId and apptopusCategoryId mapping
      all_categories_mappings_list = list()  # for storing apptopus and square categories ids mapping
      all_categories_mappings_tuple = list(
        tuple())  # id, merchantid, menuid, categoryid, platformtype, platformcategoryid

      # create square category payload
      square_category_payload = {
        "idempotency_key": str(uuid.uuid4()),
        "batches": [
          {
            "objects": []
          }
        ]
      }

      # loop over categories
      for category in categories:
        categoryId = category["id"]
        tempCategoryId = "#cat" + ''.join(random.choices(string.ascii_letters + string.digits, k=7))
        categoryName = category["posName"] if category["posName"] else category["categoryName"]

        temp_category_mapping_dict[tempCategoryId] = {
          "apptopus_category_id": categoryId
        }

        square_category_payload["batches"][0]["objects"].append({
          "id": tempCategoryId,
          "type": "CATEGORY",
          "category_data": {
            "name": categoryName
          }
        })

      # post categories to the square
      square_categories_res, square_categories_res_data = cls.square_batch_upsert_catalog_objects(accessToken,
                                                                                                  square_category_payload)
      if not square_categories_res:
        return False, 500, f"error: occured while posting categories to square! {square_categories_res_data}"

      # form categories mappings
      for key, value in temp_category_mapping_dict.items():
        for row in square_categories_res_data.get("id_mappings"):
          if row["client_object_id"] == key:
            all_categories_mappings_list.append({
              "apptopus_category_id": value["apptopus_category_id"],
              "square_category_id": row["object_id"]
            })
            all_categories_mappings_tuple.append((uuid.uuid4(), merchantId, menu_mapping["menuid"],
                                                  value["apptopus_category_id"], platformType, row["object_id"]))
            break

      # insert into menucategories table
      cursor.executemany("""
        INSERT INTO menucategories 
          (id, merchantid, menuid, categoryid, platformtype, platformcategoryid)
          VALUES (%s,%s,%s,%s,%s,%s)
        """, (all_categories_mappings_tuple))
      connection.commit()

      '''
        <- 3 -> TODO: Get all categories' products and upload them to square
          Assign Category and Addons to each product
          Store products ids in itemmappings table
      '''

      # some contants
      temp_item_mapping_dict = dict()
      all_items_mappings_tuple = list(tuple())
      temp_item_ids = list()

      # create square item payload for bulk insert
      square_items_payload = {
        "idempotency_key": str(uuid.uuid4()),
        "batches": [
          {
            "objects": []
          }
        ]
      }

      # loop over category mappings
      print("looping over all menu categories")
      for category_mapping in all_categories_mappings_list:

        # get each category's items
        print("getting category items...")
        category_items = ProductsCategories.get_productscategories(categoryId=category_mapping['apptopus_category_id'])

        # loop over items of a category
        for row in category_items:

          itemId = row['productid']

          if itemId in temp_item_ids:
            continue
          else:
            temp_item_ids.append(itemId)

          item_details = Items.get_item_by_id(itemId)
          if not item_details:
            print("item details not found")
            continue

          # get item addons square-ids
          square_item_addons_list = list()
          cursor.execute("""
            SELECT addonmappings.addonid addonid, addonmappings.platformaddonid platformaddonid
            FROM productsaddons
            LEFT JOIN addonmappings ON productsaddons.addonid = addonmappings.addonid
            WHERE 
              productsaddons.productid = %s 
              AND addonmappings.platformtype = %s
              AND addonmappings.addonoptionid IS NULL
          """, (itemId, 11))
          addons = cursor.fetchall()

          for addon in addons:
            square_item_addons_list.append({
              "modifier_list_id": addon["platformaddonid"]
            })

          # append temporary item-variation id to dict
          tempItemVariationId = "#variation" + "".join(random.choices(string.ascii_letters + string.digits, k=8))
          temp_item_mapping_dict[tempItemVariationId] = {
            "apptopus_item_id": item_details["id"]
          }

          itemObject = {
            "id": "#" + item_details["id"],
            "type": "ITEM",
            "present_at_all_locations": True if item_details["itemStatus"] == 1 else False,
            "item_data": {
              "name": item_details["posName"] if item_details["posName"] else item_details["itemName"],
              "description": item_details["itemDescription"] if item_details["itemDescription"] else "",
              "abbreviation": item_details["shortName"],
              "category_id": category_mapping["square_category_id"],
              "modifier_list_info": square_item_addons_list,
              "product_type": "REGULAR",
              "variations": [
                {
                  "type": "ITEM_VARIATION",
                  "id": tempItemVariationId,
                  "present_at_all_locations": True if item_details["itemStatus"] == 1 else False,
                  "item_variation_data": {
                    "item_id": "#" + item_details["id"],
                    "pricing_type": "FIXED_PRICING",
                    "price_money": {
                      "amount": int(float(item_details['itemUnitPrice']) * 100),
                      "currency": "USD"
                    }
                  }
                },

              ]

            }
          }
          if item_details['imageUrl'] is not None:
            img_url = "https://square-catalog-sandbox.s3.amazonaws.com/files/a232c534692eaecb8a244cf28704ba604f607ccc/original.png"
            res_src = requests.get(img_url)

            headers = {
              'Square-Version': '2022-09-21',
              'Authorization': 'Bearer ' + accessToken,
              'Accept': 'application/json',
            }

            files = {
              'file': ('image_name.png', res_src.content, 'image/png'),
              'request': (None,
                          '{\n "idempotency_key": "' + str(uuid.uuid4()) + '" ,\n    "image": {\n      "id": "#' + str(
                            uuid.uuid4()) + '" ,\n      "type": "IMAGE",\n      "image_data": {\n        "caption": "image"\n      }\n    }\n  }'),
            }

            responseImage = requests.post(config.square_base_url + '/v2/catalog/images', headers=headers,
                                          files=files)

            print(responseImage.text)
            print("gfdsahgfdsanhbgvfcdxszb vcxzgvfcdxsza")

            responseImage = responseImage.json()
            responseImage = responseImage['image']['id']
            print(responseImage)
            itemObject['item_data']['image_ids'] = [
              responseImage
            ]

          # create square item payload
          square_items_payload["batches"][0]["objects"].append(itemObject)

      square_items_res, square_items_res_data = cls.square_batch_upsert_catalog_objects(accessToken,
                                                                                        square_items_payload)
      print(square_items_payload)
      if not square_items_res:
        return False, 500, f"error: occured while posting items to square! {square_items_res_data}"

      # form itemmappings table payload
      for key, value in temp_item_mapping_dict.items():
        for row in square_items_res_data.get("id_mappings"):
          if row["client_object_id"] == f'#{value.get("apptopus_item_id")}':
            # id, merchantid, menuid, itemid, itemtype, platformtype, platformitemid
            all_items_mappings_tuple.append((
              uuid.uuid4(), merchantId, menu_mapping["menuid"], value["apptopus_item_id"],
              1, platformType, row["object_id"]))
            break

      # insert into itemmappings table
      cursor.executemany("""
        INSERT INTO itemmappings 
          (id, merchantid, menuid, itemid, itemtype, platformtype, platformitemid)
          VALUES (%s,%s,%s,%s,%s,%s,%s)
        """, (all_items_mappings_tuple))
      connection.commit()

      '''
        <- 4 -> TODO: Search Catalog Objects and Store the latet_time (last updation time) of catalogs
          for future auto_sync use
      '''

      payload = {
        "limit": 1
      }

      print("searching for catalog objects to get last update time of catalogs...")
      res, data = cls.square_search_catalog_objects(accessToken, payload)
      if not res:
        return False, 500, f"error: occured while searching for latest_time (last update time) of catalog objects"

      latest_update_time = data.get("latest_time")

      metadata = json.dumps({
        "catalog_last_update_time": latest_update_time
      })

      time.sleep(5)  # avoid catalog_last_update_time to be used by webhook for next 5 seconds

      # store the returned menuId and taxRateId in menumappings table
      cursor.execute("""
        UPDATE menumappings 
          SET metadata=%s 
          WHERE id=%s""",
                     (metadata, menu_mapping['id'])
                     )
      connection.commit()

      return True, 200, "success"
    except Exception as e:
      return False, 500, str(e)

  @classmethod
  def download_complete_menu_square(cls, platform_details, userId):
    try:
      print("---------- SQUARE MENU DOWNLOAD ----------")
      connection, cursor = get_db_connection()

      storeId = platform_details["storeid"]
      merchantId = platform_details["merchantid"]
      platformType = platform_details["platformtype"]
      accessToken = platform_details["accesstoken"]
      accountId = platform_details["accountid"]

      merchantdetail = Platforms.get_platform_by_merchantid_and_platformtype(merchantId, 11)

      print("Store id: ", storeId)
      print("Account id: ", accountId)
      print("Merchant id: ", merchantId)
      print("Platform Type: ", platformType)

      if not (storeId and int(platformType) == 11):
        return False, 500, "error: square is not provisioned for the merchant!"

      ### init database tables data lists
      mappings = MenuMappings.get_menumappings(merchantId=merchantId, platformType=platformType)
      new_menu = False
      if len(mappings)!=0:
        menu_mapping = mappings[0]
        menuId = menu_mapping['menuid']
      else:
        menuId = str(uuid.uuid4())
        new_menu = True
      menu_payload = (menuId, merchantId, f"square-{datetime.date.today().isoformat()}", "", 1,
                      userId)  # (id, merchantid, name, description, status, created_by)
      categories_payload = list(tuple())  # (id, merchantid, categoryname, categorydescription, status, created_by)
      menu_categories_payload = list(tuple())  # (id, merchantid, menuid, categoryid, platformtype, platformcategoryid)
      items_payload = list(
        tuple())  # (id, merchantid, itemsku, itemname, itemdescription, itemprice, itemtype, status, taxrate, created_by)
      items_categories_payload = list(tuple())  # (id, productid, categoryid, created_by)
      item_mappings_payload = list(tuple())  # (id, merchantid, menuid, itemid, itemtype, platformtype, platformitemid)
      addons_payload = list(
        tuple())  # (id, merchantid, addonname, addondescription, minpermitted, maxpermitted, multiselect, status, created_by)
      items_addons_payload = list(tuple())  # (id, productid, addonid, created_by)
      addon_mappings_payload = list(
        tuple())  # (id, merchantid, menuid, addonid, addonoptionid, platformtype, platformaddonid)
      addons_options_payload = list(tuple())  # (id, itemid, addonid, created_by)

      """
        get square categories
        for each square_category:
          - generate and store fondaCatId:uuid4() for further processing
          - append category details to categories_payload
          - append 2 rows to menu_categories_payload
            - one for apptopus mappings
            - second for square mappings
      """
      # get all categories from square
      square_categories = list()
      next_page = None
      while True:
        resp = cls.square_list_catalog(accessToken, cursor=next_page, types="CATEGORY")
        if not (resp and resp.get("objects")):
          # no catalog available in square menu
          break

        # extend categories to the list
        square_categories.extend(resp.get("objects"))

        next_page = resp.get("cursor")
        print(next_page)
        if not next_page:
          break

      for sc in square_categories:
        cursor.execute(
          """SELECT * FROM menucategories WHERE merchantid = %s AND platformtype = %s AND menuid = %s AND platformcategoryid=%s""",
          (merchantId, platformType, menuId, sc['id']))
        getcat = cursor.fetchone()
        if getcat:
          catname = sc['category_data']['name']
          cursor.execute("""
                           UPDATE categories 
                             SET categoryname=%s 
                             WHERE id=%s""",
                         (catname,getcat['categoryid'])
                         )

          fondaCatId = getcat['categoryid']
          sc["fondaCatId"] = fondaCatId
          continue
        fondaCatId = str(uuid.uuid4())
        sc["fondaCatId"] = fondaCatId
        categories_payload.append((fondaCatId, merchantId, sc["category_data"].get("name") or "", "", 1, userId))
        menu_categories_payload.append((str(uuid.uuid4()), merchantId, menuId, fondaCatId, 1, None))
        menu_categories_payload.append((str(uuid.uuid4()), merchantId, menuId, fondaCatId, platformType, sc["id"]))

      """
      
      """
      # get all items from square
      square_items = list()
      item_without_category = False
      dummy_category_id = str(uuid.uuid4())
      next_page = None
      while True:
        resp = cls.square_list_catalog(accessToken, cursor=next_page, types="ITEM")
        if not (resp and resp.get("objects")):
          # no catalog available in square menu
          break

        # extend categories to the list
        square_items.extend(resp.get("objects"))

        next_page = resp.get("cursor")
        print(next_page)
        if not next_page:
          break

      print(square_items)
      for sitem in square_items:
        sItemCategoryId = sitem.get("item_data").get("category_id")
        itemStatus = 1
        if sitem.get("absent_at_location_ids") and len(sitem.get("absent_at_location_ids")):
          if accountId in sitem.get("absent_at_location_ids"):
            itemStatus = 0
        else:
          if sitem.get("present_at_all_locations") == False:
            itemStatus = 0
        if itemStatus == 1 and sitem.get("item_data").get("variations") and len(
            sitem.get("item_data").get("variations")):
          variation_status = sitem.get("item_data").get("variations")[0]
          locations_overrides = variation_status.get("item_variation_data").get('location_overrides')
          if locations_overrides and locations_overrides[0].get('sold_out'):
            itemStatus = 0
        cursor.execute("""SELECT * FROM itemmappings WHERE merchantid = %s AND platformtype = %s AND menuid = %s AND platformitemid=%s""",
                       (merchantId, platformType, menuId ,sitem['id'] ))
        getitem= cursor.fetchone()
        if getitem:
          itemname=sitem['item_data']['name']
          sItemVariationId = None
          itemprice = 0
          if sitem.get("item_data").get("variations") and len(sitem.get("item_data").get("variations")):
            variation = sitem.get("item_data").get("variations")[0]  # we consider the first variation only
            sItemVariationId = variation.get("id")
            itemprice = variation.get("item_variation_data").get("price_money").get("amount") / 100

          if sItemVariationId is None:
            print(f"item <{sitem['id']}> have no variations")
            continue
          cursor.execute("""
                   UPDATE items 
                     SET itemname=%s , itemprice = %s , status= %s
                     WHERE id=%s""",
                         (itemname, itemprice ,itemStatus, getitem['itemid'])
                         )
          fondaItemId = getitem['itemid']
          sitem["fondaItemId"] = fondaItemId

        else:
          imageurl = None
          if sitem.get("item_data").get("image_ids"):
            imageurl = sitem.get("item_data").get("image_ids")[0]

            url = config.square_base_url + "/v2/catalog/object/" + imageurl

            payload = {}
            headers = {
              'Square-Version': '2022-09-21',
              'Authorization': 'Bearer ' + merchantdetail['accesstoken'],
              'Content-Type': 'application/json'
            }

            imageurl = requests.request("GET", url, headers=headers, data=payload)
            imageurl = imageurl.json()
            print(imageurl)
            if imageurl:
              imageurl = imageurl['object']['image_data']['url']

          fondaItemId = str(uuid.uuid4())

          sitem["fondaItemId"] = fondaItemId

          itemName = sitem.get("item_data").get("name")
          itemDescription = sitem.get("item_data").get("description")
          sItemVariationId = None

          itemPrice = 0
          if sitem.get("item_data").get("variations") and len(sitem.get("item_data").get("variations")):
            variation = sitem.get("item_data").get("variations")[0]  # we consider the first variation only
            sItemVariationId = variation.get("id")
            itemPrice = variation.get("item_variation_data").get("price_money").get("amount") / 100



          # if not item variation id, then skip the item
          if sItemVariationId is None:
            print(f"item <{sitem['id']}> have no variations")
            continue

          items_payload.append(
            (fondaItemId, merchantId, "", itemName, itemDescription or "", itemPrice, 1, itemStatus, 0, userId, imageurl))
          item_mappings_payload.append(
            (str(uuid.uuid4()), merchantId, menuId, fondaItemId, 1, platformType, sitem['id']))

        if sItemCategoryId:
          for sc in square_categories:
            if sc["id"] == sItemCategoryId:
              cursor.execute(
                """SELECT * FROM productscategories WHERE productid = %s AND categoryid = %s """,
                (fondaItemId, sc["fondaCatId"]))
              getitemcatnmapping = cursor.fetchone()
              if not getitemcatnmapping:
                items_categories_payload.append((str(uuid.uuid4()), fondaItemId, sc["fondaCatId"], userId))
              break
        else:
          print("item is not assigned to any category")
          items_categories_payload.append((str(uuid.uuid4()), fondaItemId, dummy_category_id, userId))
          item_without_category = True

      # if item_without_category is True then create and store dummy category
      if item_without_category:
        categories_payload.append((dummy_category_id, merchantId, "Unassigned", "", 1, userId))
        menu_categories_payload.append((str(uuid.uuid4()), merchantId, menuId, dummy_category_id, 1, None))

      """
      
      """
      # get all addons from square
      square_addons = list()
      next_page = None
      while True:
        resp = cls.square_list_catalog(accessToken, cursor=next_page, types="MODIFIER_LIST")
        if not (resp and resp.get("objects")):
          # no catalog available in square menu
          break

        # extend categories to the list
        square_addons.extend(resp.get("objects"))

        next_page = resp.get("cursor")
        print(next_page)
        if not next_page:
          break

      for saddon in square_addons:
        cursor.execute(
          """SELECT * FROM addonmappings WHERE merchantid = %s AND platformtype = %s AND menuid = %s AND platformaddonid=%s""",
          (merchantId, platformType, menuId, saddon['id']))
        getaddon = cursor.fetchone()
        if getaddon:
          addonname = saddon['modifier_list_data']['name']
          cursor.execute("""
                                   UPDATE addons 
                                     SET addonname=%s 
                                     WHERE id=%s""",
                         (addonname, getaddon['addonid'])
                         )
          fondaAddonId=getaddon['addonid']
        else:
          fondaAddonId = str(uuid.uuid4())

        addon_part_of_any_item = False

        for sitem in square_items:

          item_modifiers = sitem.get("item_data").get("modifier_list_info")
          if item_modifiers and len(item_modifiers):

            for im in item_modifiers:
              if (
                  im.get("modifier_list_id") == saddon["id"] and im.get("enabled") == True
              ):
                cursor.execute(
                  """SELECT * FROM productsaddons WHERE addonid = %s AND productid = %s """,
                  (fondaAddonId, sitem["fondaItemId"]))
                getitemaddonmapping = cursor.fetchone()
                if not getitemaddonmapping:
                  items_addons_payload.append((str(uuid.uuid4()), sitem["fondaItemId"], fondaAddonId, userId))
                addon_part_of_any_item = True

        # if addon is not part of any item then ignore it
        if not addon_part_of_any_item:
          continue


        if not getaddon:
          addonName = saddon.get("modifier_list_data").get("name")
          minPermitted = 0
          maxPermitted = 1
          if saddon.get("modifier_list_data").get("selection_type") == "MULTIPLE":
            maxPermitted = len(saddon.get("modifier_list_data").get("modifiers"))

          addons_payload.append((fondaAddonId, merchantId, addonName or "", "", minPermitted, maxPermitted, 1, 1, userId))
          addon_mappings_payload.append(
            (str(uuid.uuid4()), merchantId, menuId, fondaAddonId, None, platformType, saddon["id"]))

        for soption in saddon.get("modifier_list_data").get("modifiers"):
          cursor.execute(
            """SELECT * FROM addonmappings WHERE merchantid = %s AND platformtype = %s AND menuid = %s AND platformaddonid=%s""",
            (merchantId, platformType, menuId, soption['id']))
          getaddonoption = cursor.fetchone()
          if getaddonoption:
            optionName = soption.get("modifier_data").get("name")
            optionPrice = soption.get("modifier_data").get("price_money").get("amount") / 100
            cursor.execute("""
                                            UPDATE items 
                                              SET itemname=%s ,itemprice=%s 
                                              WHERE id=%s""",
                           (optionName,optionPrice, getaddonoption['addonoptionid'])
                           )
            continue
          fondaOptionId = str(uuid.uuid4())

          optionName = soption.get("modifier_data").get("name")
          optionPrice = soption.get("modifier_data").get("price_money").get("amount") / 100
          sortId = soption.get("modifier_data").get("ordinal") if isinstance(
            soption.get("modifier_data").get("ordinal"), int) else 0
          optionStatus = 1

          items_payload.append(
            (fondaOptionId, merchantId, "", optionName, "", optionPrice, 2, optionStatus, 0, userId, ''))
          addons_options_payload.append((str(uuid.uuid4()), fondaOptionId, fondaAddonId, sortId, userId))
          addon_mappings_payload.append(
            (str(uuid.uuid4()), merchantId, menuId, fondaAddonId, fondaOptionId, platformType, soption["id"]))

      """
        store all the payloads in database """
      if new_menu:
        print("menus insert...")
        cursor.execute("""
          INSERT INTO menus (id, merchantid, name, description, status, created_by)
          VALUES (%s,%s,%s,%s,%s,%s)
        """, menu_payload)
        print(cursor.rowcount)

        # insertin menu mapping with Square platfrom
        data = (str(uuid.uuid4()), menuId, merchantId, 11, '', 0, None, userId)
        cursor.execute("""INSERT INTO menumappings 
                  (id, menuid, merchantid, platformtype, platformmenuid, mappingstatus, metadata, created_by)
                  VALUES(%s,%s,%s,%s,%s,%s,%s,%s)""", data)


      print("categories insert...")
      cursor.executemany("""
        INSERT INTO categories (id, merchantid, categoryname, categorydescription, status, created_by)
        VALUES (%s,%s,%s,%s,%s,%s)
      """, (categories_payload))
      print(cursor.rowcount)

      print("menucategories insert...")
      cursor.executemany("""
        INSERT INTO menucategories (id, merchantid, menuid, categoryid, platformtype, platformcategoryid)
        VALUES (%s,%s,%s,%s,%s,%s)
      """, (menu_categories_payload))
      print(cursor.rowcount)

      print("items insert...")
      print(items_payload)
      cursor.executemany("""
        INSERT INTO items (id, merchantid, itemsku, itemname, itemdescription, itemprice, itemtype, status, taxrate, created_by, imageurl)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
      """, (items_payload))
      print(cursor.rowcount)

      print("productscategories insert...")
      cursor.executemany("""
        INSERT IGNORE INTO productscategories (id, productid, categoryid, created_by)
        VALUES (%s,%s,%s,%s)
      """, (items_categories_payload))
      print(cursor.rowcount)

      print("itemmappings insert...")
      cursor.executemany("""
        INSERT INTO itemmappings (id, merchantid, menuid, itemid, itemtype, platformtype, platformitemid)
        VALUES (%s,%s,%s,%s,%s,%s,%s)
      """, (item_mappings_payload))
      print(cursor.rowcount)

      print("addons insert...")
      cursor.executemany("""
        INSERT INTO addons (id, merchantid, addonname, addondescription, minpermitted, maxpermitted, multiselect, status, created_by)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
      """, (addons_payload))
      print(cursor.rowcount)

      print("productsaddons insert...")
      cursor.executemany("""
        INSERT IGNORE INTO productsaddons (id, productid, addonid, created_by)
        VALUES (%s,%s,%s,%s)
      """, (items_addons_payload))
      print(cursor.rowcount)

      print("addonmappings insert...")
      cursor.executemany("""
        INSERT INTO addonmappings (id, merchantid, menuid, addonid, addonoptionid, platformtype, platformaddonid)
        VALUES (%s,%s,%s,%s,%s,%s,%s)
      """, (addon_mappings_payload))
      print(cursor.rowcount)

      print("addonsoptions insert...")
      cursor.executemany("""
        INSERT IGNORE INTO addonsoptions (id, itemid, addonid, sortid, created_by)
        VALUES (%s,%s,%s,%s,%s)
      """, (addons_options_payload))
      print(cursor.rowcount)

      print("committing inserts...")
      connection.commit()

      return True, 200, "success"
    except Exception as e:
      print("Error: ", str(e))
      return False, 500, str(e)


"""  
<- 1 -> TODO: ADDONS AND OPTIONS ACCESSING, INSERTING AND STOING-IDS STRUCTURE

--> get all addons and addon-options from the database for a merchant

--> form a temp_addons_mapping_dict
{
  "addon---1": {
    "apptopus_addon_id": "",
    "apptopus_addon_option_id": None
  },
  "addon---2": {
    "apptopus_addon_id": "",
    "apptopus_addon_option_id": None
  },
  "option---1": {
    "apptopus_addon_id": "",
    "apptopus_addon_option_id": "",
  },
  "option---2": {
    "apptopus_addon_id": "",
    "apptopus_addon_option_id": "",
  }
}

--> form addons catalogs payload and send payload to the square
--> loop over above dictionary
--> create new list of tuple

[
  (id, merchantid, menuid, addonid, addonoptionid, platformtype, platformaddonid),
  (id, merchantid, menuid, addonid, addonoptionid, platformtype, platformaddonid)
]

--> Insert data into addonmappings table
"""

""" 
<- 2 -> TODO: for uploading categories to square and storing their ids

--> temp_category_mapping_dict for temporarily storing tempCategoryId and apptopusCategoryId mapping
temp_category_mapping_dict["tempCategoryId"] = {
  "apptopus_category_id": ""
}

--> all_categories_mappings_list for storing apptopus and square categories ids mapping
{
  "apptopus_category_id": "",
  "square_category_id": "square_category_id"
}


--> all_categories_mappings_tuple for storing apptopus and square data in form of tuple to be bulk inserted into menucategories table
all_categories_mappings_tuple.append(( uuid.uuid4(), merchantId, menuId, apptopus_category_id, platformType, square_category_id ))
"""

""" 
<- 3 -> TODO: upload items to square and store item-variation ids in itemmappings table

--> loop over all_categories_mappings_list formed in part-2
--> for each categoryId, get all category items
--> for each item, get all square_addon_ids
--> form square item payload and append to square_item_paylod object

--> store the temporarily assigned item-variation-id in temp_items_mappings_dict along with apptopus item id
temp_items_mapping_dict[tempItemVariationId] = {
  "apptopus_item_id": ""
}
we will later store item-variation-id in itemmappings table, because we require square-item-variation-id instead of square-item-id during order_creation

--> all_items_mappings_tuple [(id, merchantid, menuid, itemid, itemtype, platformtype, platformitemid)]
we will bulk insert all_items_mappings_tuple in itemmappings table
"""

"""
<- 4 -> TODO: Store last update datetime of catalogs objects (latest_time) in metadata field in menumappings table

--> we store it becasue we will use it in auto sync later
"""
