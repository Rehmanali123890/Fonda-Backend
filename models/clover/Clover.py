from flask import jsonify
import json
import requests
import datetime
import uuid
import boto3
import time
# local imports
import config
from models.Addons import Addons
from models.Items import Items
from models.ProductsAddons import ProductsAddons
from utilities.helpers import get_db_connection, success
from utilities.errors import invalid, unhandled, not_found
import models.Platforms
from models.MenuMappings import MenuMappings
from models.MenuCategories import MenuCategories
from models.ProductsCategories import ProductsCategories
from models.ItemMappings import ItemMappings
from models.AddonMappings import AddonMappings


class Clover():

  ############################################### ORDERS

  @classmethod
  def clover_get_order_by_id(cls, cMid, cOrderId, accessToken, refunds=False):
    try:
      url = f"{config.clover_base_url}/v3/merchants/{cMid}/orders/{cOrderId}"
      if refunds:
        url += "?expand=refunds"
      headers = {
          "Content-Type": "application/json",
          "Authorization": f"Bearer {accessToken}"
      }
      response = requests.request("GET", url, headers=headers)
      # print(response.text)
      data = response.json() if response and response.status_code >= 200 and response.status_code < 300 else False
      return data
    except Exception as e:
      print("Error: ", str(e))
      return False
  
  ############################################### CATEGORIES

  @classmethod
  def clover_create_category(cls, cMid, accessToken, name):
    try:
      url = f"{config.clover_base_url}/v3/merchants/{cMid}/categories"
      headers = {
          "Content-Type": "application/json",
          "Authorization": f"Bearer {accessToken}"
      }
      payload = {
        "name": name
      }
      response = requests.request("POST", url, json=payload, headers=headers)
      print(response.text)
      data = response.json() if response and response.status_code >= 200 and response.status_code < 300 else False
      return data
    except Exception as e:
      print("Error: ", str(e))
      return False
  
  @classmethod
  def clover_update_category(cls, cMid, cCid, accessToken, name):
    try:
      url = f"{config.clover_base_url}/v3/merchants/{cMid}/categories/{cCid}"
      headers = {
          "Content-Type": "application/json",
          "Authorization": f"Bearer {accessToken}"
      }
      payload = {
        "name": name
      }
      response = requests.request("POST", url, json=payload, headers=headers)
      print(response.text)
      data = response.json() if response and response.status_code >= 200 and response.status_code < 300 else False
      return data
    except Exception as e:
      print("Error: ", str(e))
      return False
  
  @classmethod
  def clover_get_all_categories(cls, cMid, accessToken):
    try:
      url = f"{config.clover_base_url}/v3/merchants/{cMid}/categories?limit=1000&offset=0"
      headers = {
          "Content-Type": "application/json",
          "Authorization": f"Bearer {accessToken}"
      }
      response = requests.request("GET", url, headers=headers)
      # print(response.text)
      data = response.json() if response and response.status_code >= 200 and response.status_code < 300 else False
      return data
    except Exception as e:
      print("Error: ", str(e))
      return False
  
  @classmethod
  def clover_delete_categories(cls, cMid, accessToken, categoryIds=None):
    try:
      url = f"{config.clover_base_url}/v3/merchants/{cMid}/categories?categoryIds={categoryIds}"
      headers = {
          "Content-Type": "application/json",
          "Authorization": f"Bearer {accessToken}"
      }
      response = requests.request("DELETE", url, headers=headers)
      print(response.text)
      data = True if response and response.status_code >= 200 and response.status_code < 300 else False
      return data
    except Exception as e:
      print("Error: ", str(e))
      return False
  
  ############################################### ITEMS

  @classmethod
  def clover_create_item(cls, cMid, accessToken, payload):
    try:
      print('------ Trigger Clover Create Item --------')
      url = f"{config.clover_base_url}/v3/merchants/{cMid}/items"
      headers = {
          "Content-Type": "application/json",
          "Authorization": f"Bearer {accessToken}"
      }
      print(' CLover API url for creating item ', url)
      print(' Payload  ' , payload)
      response = requests.request("POST", url, data=json.dumps(payload), headers=headers)
      print(response.text)
      data = response.json() if response and response.status_code >= 200 and response.status_code < 300 else False
      return data
    except Exception as e:
      print("Error: ", str(e))
      return False
  
  @classmethod
  def clover_update_item(cls, cMid, cItemId, accessToken, payload):
    try:
      url = f"{config.clover_base_url}/v3/merchants/{cMid}/items/{cItemId}"
      headers = {
          "Content-Type": "application/json",
          "Authorization": f"Bearer {accessToken}"
      }

      response = requests.request("POST", url, data=json.dumps(payload), headers=headers)
      print(response.text)
      data = response.json() if response and response.status_code >= 200 and response.status_code < 300 else False
      return data
    except Exception as e:
      print("Error: ", str(e))
      return False

  @classmethod
  def clover_get_all_items(cls, cMid, accessToken, limit=1000, offset=0, expand=""):
    try:
      url = f"{config.clover_base_url}/v3/merchants/{cMid}/items?limit={limit}&offset={offset}&expand={expand}"
      headers = {
          "Content-Type": "application/json",
          "Authorization": f"Bearer {accessToken}"
      }
      response = requests.request("GET", url, headers=headers)
      # print(response.text)
      data = response.json() if response and response.status_code >= 200 and response.status_code < 300 else False
      return data
    except Exception as e:
      print("Error: ", str(e))
      return False
  
  @classmethod
  def clover_get_item(cls, cMid, cItemId, accessToken):
    try:
      print('--------- Clover get item -------')
      url = f"{config.clover_base_url}/v3/merchants/{cMid}/items/{cItemId}"
      headers = {
          "Content-Type": "application/json",
          "Authorization": f"Bearer {accessToken}"
      }

      response = requests.request("GET", url, headers=headers)
      print("response.text: " ,response.text )
      data = response.json() if response and response.status_code >= 200 and response.status_code < 300 else False
      print('Clover get item api url ' , url , ' Response : ' , data)
      return data
    except Exception as e:
      print("Error: ", str(e))
      return False

  @classmethod
  def clover_delete_items(cls, cMid, accessToken, itemIds=None):
    try:
      url = f"{config.clover_base_url}/v3/merchants/{cMid}/items?itemIds={itemIds}"
      headers = {
          "Content-Type": "application/json",
          "Authorization": f"Bearer {accessToken}"
      }
      response = requests.request("DELETE", url, headers=headers)
      print(response.text)
      data = True if response and response.status_code >= 200 and response.status_code < 300 else False
      return data
    except Exception as e:
      print("Error: ", str(e))
      return False
  
  ############################################### CATEGORIES-ITEMS MAPPING

  @classmethod
  def clover_items_categories_association(cls, cMid, accessToken, object, delete=False):
    try:
      url = f"{config.clover_base_url}/v3/merchants/{cMid}/category_items"
      if delete:
        url = url + "?delete=true"
      headers = {
          "Content-Type": "application/json",
          "Authorization": f"Bearer {accessToken}"
      }
      payload = json.dumps(object)
      response = requests.request("POST", url, headers=headers, data=payload)
      print(response.text)
      data = True if response and response.status_code >= 200 and response.status_code < 300 else False
      return data
    except Exception as e:
      print("Error: ", str(e))
      return False

  
  ############################################### ADDONS

  @classmethod
  def clover_create_modifier_group(cls, cMid, accessToken, payload):
    try:
      url = f"{config.clover_base_url}/v3/merchants/{cMid}/modifier_groups"
      headers = {
          "Content-Type": "application/json",
          "Authorization": f"Bearer {accessToken}"
      }

      response = requests.request("POST", url, data=json.dumps(payload), headers=headers)
      print(response.text)
      data = response.json() if response and response.status_code >= 200 and response.status_code < 300 else False
      return data
    except Exception as e:
      print("Error: ", str(e))
      return False


  @classmethod
  def clover_update_modifier_group(cls, cMid, cAddonId, accessToken, payload):
    try:
      url = f"{config.clover_base_url}/v3/merchants/{cMid}/modifier_groups/{cAddonId}"
      headers = {
          "Content-Type": "application/json",
          "Authorization": f"Bearer {accessToken}"
      }

      response = requests.request("POST", url, data=json.dumps(payload), headers=headers)
      print(response.text)
      data = response.json() if response and response.status_code >= 200 and response.status_code < 300 else False
      return data
    except Exception as e:
      print("Error: ", str(e))
      return False
  

  @classmethod
  def clover_get_all_modifier_groups(cls, cMid, accessToken, limit=1000, offset=0, expand=""):
    try:
      url = f"{config.clover_base_url}/v3/merchants/{cMid}/modifier_groups?limit={limit}&offset={offset}&expand={expand}"
      headers = {
          "Content-Type": "application/json",
          "Authorization": f"Bearer {accessToken}"
      }
      response = requests.request("GET", url, headers=headers)
      # print(response.text)
      data = response.json() if response and response.status_code >= 200 and response.status_code < 300 else False
      return data
    except Exception as e:
      print("Error: ", str(e))
      return False
  

  @classmethod
  def clover_delete_modifier_groups(cls, cMid, accessToken, addonIds=None):
    try:
      url = f"{config.clover_base_url}/v3/merchants/{cMid}/modifier_groups?modifierGroupIds={addonIds}"
      headers = {
          "Content-Type": "application/json",
          "Authorization": f"Bearer {accessToken}"
      }
      response = requests.request("DELETE", url, headers=headers)
      print(response.text)
      data = True if response and response.status_code >= 200 and response.status_code < 300 else False
      return data
    except Exception as e:
      print("Error: ", str(e))
      return False

  ############################################### ADDONS-OPTIONS

  @classmethod
  def clover_create_modifier(cls, cMid, cAddonId, accessToken, payload):
    try:
      url = f"{config.clover_base_url}/v3/merchants/{cMid}/modifier_groups/{cAddonId}/modifiers"
      headers = {
          "Content-Type": "application/json",
          "Authorization": f"Bearer {accessToken}"
      }

      response = requests.request("POST", url, data=json.dumps(payload), headers=headers)
      # print(response.text)
      data = response.json() if response and response.status_code >= 200 and response.status_code < 300 else False
      return data
    except Exception as e:
      print("Error: ", str(e))
      return False
  
  @classmethod
  def clover_update_modifier(cls, cMid, cAddonId, cOptionId, accessToken, payload):
    try:
      url = f"{config.clover_base_url}/v3/merchants/{cMid}/modifier_groups/{cAddonId}/modifiers/{cOptionId}"
      headers = {
          "Content-Type": "application/json",
          "Authorization": f"Bearer {accessToken}"
      }

      response = requests.request("POST", url, data=json.dumps(payload), headers=headers)
      # print(response.text)
      data = response.json() if response and response.status_code >= 200 and response.status_code < 300 else False
      return data
    except Exception as e:
      print("Error: ", str(e))
      return False

  @classmethod
  def clover_delete_modifier(cls, cMid, cAddonId, cOptionId, accessToken):
    try:
      url = f"{config.clover_base_url}/v3/merchants/{cMid}/modifier_groups/{cAddonId}/modifiers/{cOptionId}"
      headers = {
          "Content-Type": "application/json",
          "Authorization": f"Bearer {accessToken}"
      }
      response = requests.request("DELETE", url, headers=headers)
      data = True if response and response.status_code >= 200 and response.status_code < 300 else False
      return data
    except Exception as e:
      print("Error: ", str(e))
      return False
  

  ############################################### ITEMS-ADDONS MAPPING

  @classmethod
  def clover_items_modifiergroups_association(cls, cMid, accessToken, object, delete=False):
    try:
      url = f"{config.clover_base_url}/v3/merchants/{cMid}/item_modifier_groups"
      if delete:
        url = url + "?delete=true"
      headers = {
          "Content-Type": "application/json",
          "Authorization": f"Bearer {accessToken}"
      }
      payload = json.dumps(object)
      response = requests.request("POST", url, headers=headers, data=payload)
      print(response.text)
      data = True if response and response.status_code >= 200 and response.status_code < 300 else False
      return data
    except Exception as e:
      print("Error: ", str(e))
      return False


  ##################################################################
  ############################################### POST COMPLETE MENU

  @classmethod
  def post_complete_menu_clover(cls, platformId):
    try:
      print('-------------------------------------------------------------------')
      print('-------------------- CLOVER MENU MANUAL SYNC --------------------')
      
      connection, cursor = get_db_connection()

      print("Get required details from platforms table...")
      platform_details = models.Platforms.Platforms.get_platform_by_id(platformId)
      platform_details["accesstoken"], msg, is_error = Clover.generate_clover_access_token(platform_details)
      if is_error:
        return invalid(msg)
      # we do import like this because we have to avoid circular imports
      storeId = platform_details["storeid"]
      merchantId = platform_details["merchantid"]
      platformType = platform_details["platformtype"]
      accessToken = platform_details["accesstoken"]

      print("Store id: ", storeId)
      print("Merchant id: ", merchantId)
      print("Platform Type: ", platformType)

      if not (storeId and int(platformType) == 4):
        return invalid("error: clover is not provisioned for the merchant!")
      
      mappings = MenuMappings.get_menumappings(merchantId=merchantId, platformType=platformType)
      if len(mappings) == 0:
        return unhandled("error: no menu is assigned to clover!")
      if len(mappings) > 1:
        return unhandled("error: more than 1 menu have been assigned to clover!")
      menu_mapping = mappings[0]
      '''
           TODO: getting old menu mapping  data from our database for deleting specific
             menucategories, itemmappings, addonmappings
           '''

      menu_mapping_resp = cls.get_clover_menu_mapping(merchantId=merchantId, platformType=platformType , menuId=menu_mapping['menuid'])
      if not menu_mapping_resp:
        print("error: while getting old data from db!!!")
      '''
      TODO: Deleting old menu data from our database and also from the clover pos
        menucategories, itemmappings, addonmappings
      '''

      resp = MenuCategories.delete_menucategories(merchantId=merchantId, platformType=platformType, menuId=menu_mapping['menuid'])
      if not resp:
        print("error: while deleting old data from menu-categories!!!")
      resp = ItemMappings.delete_itemmappings(merchantId=merchantId, platformType=platformType, menuId=menu_mapping['menuid'])
      if not resp:
        print("error: while deleting old data from item-mappings!!!")
      resp = AddonMappings.delete_addonmappings(merchantId=merchantId, platformType=platformType, menuId=menu_mapping['menuid'])
      if not resp:
        print("error: while deleting old data from addon-mappings!!!")
      

      # get all clover categories and delete them
      temp_cat_list = ','.join(menu_mapping_resp['catPlatformIds'])
      deleted = cls.clover_delete_categories(cMid=storeId, accessToken=accessToken, categoryIds=temp_cat_list)
      if not deleted:
        print("error: while deleting categories from clover!!!")

      # get all clover products and delete them
      temp_items_list = ','.join(menu_mapping_resp['itemPlatformIds'])
      deleted = cls.clover_delete_items(cMid=storeId, accessToken=accessToken, itemIds=temp_items_list)
      if not deleted:
        print("error: while deleting items from clover!!!")


      # get all clover addons and delete them
      temp_mods_list = ','.join(menu_mapping_resp['addonPlatformIds'])
      deleted = cls.clover_delete_modifier_groups(cMid=storeId, accessToken=accessToken, addonIds=temp_mods_list)
      if not deleted:
        print("error: while deleting addons from clover!!!")


      ''' TODO: END ###
      '''

      ### EVERY THING ABOUT CATEGORIES
      print("Get all menu-categories...")
      categories = MenuCategories.get_menucategories(menuId=menu_mapping['menuid'], platformType=1) #1=apptopus
      categories_ids_dict = list(dict())

      for category in categories:
        
        categoryName = category.get('posName') if category.get('posName') else category.get('categoryName')
        clover_cat_resp = cls.clover_create_category(cMid=storeId, accessToken=accessToken, name=categoryName)
        if not clover_cat_resp:
          print("error: while creating clover category!")
          continue

        categories_ids_dict.append({
          'id': category['id'],
          'clover_id': clover_cat_resp['id']
        })

        # store clover category data in menucategories table
        mc_resp = MenuCategories.post_menucategory(
          merchantId=merchantId, 
          menuId=menu_mapping['menuid'], 
          categoryId=category['id'],
          platformType=platformType, 
          platformCategoryId=clover_cat_resp['id'])
        if not mc_resp:
          print("error: while storing platform category id in menucategories table")
      

      ### ALL ABOUT CATEGORIES AND ITEMS NOW
      items_ids_list = list()
      items_ids_dict = list(dict())
      clover_categories_items_mapping_json = list(dict())

      for category in categories_ids_dict:
        
        # get each category items
        print("getting category items...")
        category_items = ProductsCategories.get_productscategories(categoryId=category['id'])
        
        for row in category_items:
          if row['productid'] not in items_ids_list:

            item_details = Items.get_item_by_id(row['productid'])
            if not item_details:
              continue

            # get product details
            # send item to the clover
            # store clover_item_id and db_item_id in a dict
            # store clover_item_id in into itemmappings table
            # also form json for sending all mapping between categories and items in a single request api of clover  

            item_payload = {
              "hidden": False,
              "available": True if int(item_details['itemStatus']) == 1 else False,
              "autoManage": False,
              "defaultTaxRates": True,
              "isRevenue": False,
              "name": item_details['posName'] if item_details['posName'] else item_details['itemName'],
              "alternateName": item_details['shortName'],
              "price": int(float(item_details['itemUnitPrice']) * 100),
              "priceType": "FIXED",
              "priceWithoutVat": int(float(item_details['itemUnitPrice']) * 100)
            }

            clover_item_resp = cls.clover_create_item(
              cMid=storeId,
              accessToken=accessToken,
              payload=item_payload
            )
            if not clover_item_resp:
              print("error: while posting item to clover!")
              continue

            items_ids_list.append(row['productid'])
            items_ids_dict.append({
              'id': row['productid'],
              'clover_id': clover_item_resp['id']
            })

            clover_categories_items_mapping_json.append({
              "category": {"id": category["clover_id"]},
              "item": {"id": clover_item_resp["id"]}
            })

            # store item-mappings...
            resp = ItemMappings.post_itemmappings(
              merchantId=merchantId, 
              menuId=menu_mapping['menuid'],
              itemId=row['productid'],
              itemType=1,  # 1 for product itemType
              platformType=platformType,
              platformItemId=clover_item_resp['id'])
            if not resp:
              print("error: while storing clover productId in item-mappings!!!")
            print("")
          
          else:
            # append the menu-category mapping to clover_menu_categories_mappings_json
            for ritem in items_ids_dict:
              if ritem['id'] == row['productid']:
                clover_categories_items_mapping_json.append({
                  "category": {"id": category["clover_id"]},
                  "item": {"id": ritem["clover_id"]}
                })
                break
              #
            #
          #
        #
      #

      # create association between categories and items on clover
      print("create association between categories and items on clover...")
      clover_categories_items_mapping_json = {
        "elements": clover_categories_items_mapping_json
      }
      clover_cat_item_mapping_resp = cls.clover_items_categories_association(
        cMid=storeId, 
        accessToken=accessToken, 
        object=clover_categories_items_mapping_json
      )
      if not clover_cat_item_mapping_resp:
        print("error: while creating association between categories and items!!!")


      '''
      TODO: POST TO SQS FOR FURTHER PROCESSING OF ITEMS-ADDONS
      '''
      print("Triggering CloverMenuSyncQueue - SQS...")
      sqs = boto3.resource('sqs')
      queue = sqs.get_queue_by_name(QueueName=config.clover_manual_sync_queue)
      dataObj = {
          "platformId":platformId,
          "menuId": menu_mapping['menuid'],
          "items_ids_dict": items_ids_dict
      }
      response = queue.send_message(MessageBody=json.dumps(dataObj))
      print("Message successfully sent to SQS")

      ''' 
      END TODO '''

      # return cls.post_items_addons_to_clover(platformId=platformId, menuId=menu_mapping['menuid'], items_ids_dict=items_ids_dict)
      return success()
    except Exception as e:
      print("Error: ", str(e))
      return unhandled()
  

  

  @classmethod
  def post_items_addons_to_clover(cls, platformId, menuId, items_ids_dict=None, auto_sync=False):
    try:
      connection, cursor = get_db_connection()

      print("Get required details from platforms table...")
      platform_details = models.Platforms.Platforms.get_platform_by_id(platformId)
      platform_details["accesstoken"], msg, is_error = Clover.generate_clover_access_token(platform_details)
      if is_error:
        print(msg)
        return False
      # we do import like this because we have to avoid circular imports
      storeId = platform_details["storeid"]
      merchantId = platform_details["merchantid"]
      platformType = platform_details["platformtype"]
      accessToken = platform_details["accesstoken"]


      ### ALL ABOUT ITEMS AND ADDONS NOW
      print("\nALL ABOUT ITEMS + ADDONS NOW...")

      addons_ids_list = list()
      addons_ids_dict = list(dict())
      clover_items_addons_mapping_json = list(dict())

      for item in items_ids_dict:

        # get each item addons
        # send item to the clover
        # store clover_item_id and db_item_id in a dict
        # store clover_item_id in into itemmappings table
        # also form json for sending all mapping between categories and items in a single request api of clover

        item_addons = ProductsAddons.get_item_addon(item['id'])

        for row in item_addons:
          
          if row['addonid'] not in addons_ids_list:

            ### check if addon is already available on clover
            cursor.execute("""SELECT * FROM addonmappings WHERE addonid=%s AND platformtype=%s AND addonoptionid IS NULL""", (row['addonid'], platformType))
            addon_mapping = cursor.fetchone()

            if addon_mapping and addon_mapping['platformaddonid']:
              addons_ids_list.append(row['addonid'])
              clover_items_addons_mapping_json.append({
                "item": {"id": item["clover_id"]},
                "modifierGroup": {"id": addon_mapping['platformaddonid']}
              })

            else:
            
              addon_details = Addons.get_addon_by_id_with_options_str(row['addonid'])
              if not addon_details:
                print("error: while getting addon details!!!")
                continue

              # create modifier group payload
              payload = {
                "showByDefault": "true",
                "name": addon_details['posName'] if addon_details['posName'] else addon_details['addonName'],
                "alternateName": addon_details['addonName'],
                "minRequired": 0,
                "maxAllowed": 2
              }
              clover_addon_resp = cls.clover_create_modifier_group(cMid=storeId, accessToken=accessToken, payload=payload)
              if not clover_addon_resp:
                print("error: while posting addon with options to clover!!!")
                continue
            
              # appending to multiple lists
              addons_ids_list.append(row['addonid'])
              addons_ids_dict.append({
                'id': row['addonid'],
                'clover_id': clover_addon_resp['id']
              })
              clover_items_addons_mapping_json.append({
                "item": {"id": item["clover_id"]},
                "modifierGroup": {"id": clover_addon_resp['id']}
              })

              # post addon-mapping to our database
              addon_map_resp = AddonMappings.post_addonmappings(
                merchantId=merchantId,
                menuId=menuId,
                addonId=row['addonid'],
                platformType=platformType,
                platformAddonId=clover_addon_resp['id']
              )

              # post addon options to clover
              print("posting addon options to clover addon...")
              for option in addon_details['addonOptions']:
                opt_payload = {
                  "available": "true",
                  "price": int(float(option['addonOptionPrice']) * 100),
                  "name": option['posName'] if option['posName'] else option['addonOptionName'],
                  "alternateName": option['shortName']
                }
                clover_options_resp = cls.clover_create_modifier(
                  cMid=storeId, 
                  cAddonId=clover_addon_resp['id'],
                  accessToken=accessToken,
                  payload=opt_payload
                )
                if not clover_options_resp:
                  print("error: while posting addon_options to clover!!!")
                  continue

                # post addon-mapping to our database
                addonopt_map_resp = AddonMappings.post_addonmappings(
                  merchantId=merchantId,
                  menuId=menuId,
                  addonId=row['addonid'],
                  addonOptionId=option['id'],
                  platformType=platformType,
                  platformAddonId=clover_options_resp['id']
                )
       
          else:
            # append the items-addons mapping to clover_items_addons_mapping_json
            for raddon in addons_ids_dict:
              if raddon['id'] == row['addonid']:
                clover_items_addons_mapping_json.append({
                  "item": {"id": item["clover_id"]},
                  "modifierGroup": {"id": raddon['clover_id']}
                })
                break
              #
            #
          #
        #
      #

      # create association between items and addons
      print("create association between items and addons...")
      clover_items_addons_mapping_json = {
        "elements": clover_items_addons_mapping_json
      }
      clover_item_addons_mapping_resp = cls.clover_items_modifiergroups_association(
        cMid=storeId, 
        accessToken=accessToken, 
        object=clover_items_addons_mapping_json
      )
      if not clover_item_addons_mapping_resp:
        print("error: while creating association between items and addons!!!")
      
      print("so whole menu is uploaded to clover. Worker is done")

      return success()
    except Exception as e:
      return unhandled(str(e))
  

  ###############################################
  ########################## DOWNLOAD COMPLETE MENU

  @classmethod
  def download_complete_menu_clover(cls, platform_details, userId):
    try:
      print("---------- CLOVER MENU DOWNLOAD ----------")
      connection, cursor = get_db_connection()

      storeId = platform_details["storeid"]
      merchantId = platform_details["merchantid"]
      platformType = platform_details["platformtype"]
      accessToken = platform_details["accesstoken"]

      print("Store id: ", storeId)
      print("Merchant id: ", merchantId)
      print("Platform Type: ", platformType)

      if not (storeId and int(platformType) == 4):
        return False, 500, "error: clover is not provisioned for the merchant!"

      mappings = MenuMappings.get_menumappings(merchantId=merchantId, platformType=platformType)
      new_menu=False
      if len(mappings) != 0:
        menu_mapping = mappings[0]
        ### init database tables data lists
        menuId = menu_mapping['menuid']
      else:
        menuId=str(uuid.uuid4())
        new_menu=True
      menu_payload = (menuId, merchantId, f"clover-{datetime.date.today().isoformat()}", "", 1, userId) # (id, merchantid, name, description, status, created_by)
      categories_payload = list(tuple())  # (id, merchantid, categoryname, categorydescription, status, created_by)
      menu_categories_payload = list(tuple()) # (id, merchantid, menuid, categoryid, platformtype, platformcategoryid)
      items_payload = list(tuple()) # (id, merchantid, itemsku, itemname, itemdescription, itemprice, itemtype, status, taxrate, created_by)
      items_categories_payload = list(tuple()) # (id, productid, categoryid, created_by)
      item_mappings_payload = list(tuple()) # (id, merchantid, menuid, itemid, itemtype, platformtype, platformitemid)
      addons_payload = list(tuple()) # (id, merchantid, addonname, addondescription, minpermitted, maxpermitted, multiselect, status, created_by)
      items_addons_payload = list(tuple()) # (id, productid, addonid, created_by)
      addon_mappings_payload = list(tuple()) # (id, merchantid, menuid, addonid, addonoptionid, platformtype, platformaddonid)
      addons_options_payload = list(tuple()) # (id, itemid, addonid, created_by)


      """
        get clover categories
        for each clover_category:
          - generate and store fondaCatId:uuid4() for further processing
          - append category details to categories_payload
          - append 2 rows to menu_categories_payload
            - one for apptopus mappings
            - second for clover mappings
      """
      clover_categories = cls.clover_get_all_categories(cMid=storeId, accessToken=accessToken)
      clover_categories = clover_categories["elements"]

      for cc in clover_categories:
        cursor.execute(
          """SELECT * FROM menucategories WHERE merchantid = %s AND platformtype = %s AND menuid = %s AND platformcategoryid=%s""",
          (merchantId, platformType, menuId, cc['id']))
        getcat = cursor.fetchone()
        if getcat:
          catname = cc["name"]
          cursor.execute("""
                                   UPDATE categories 
                                     SET posname=%s 
                                     WHERE id=%s""",
                         (catname, getcat['categoryid'])
                         )
          fondaCatId = getcat['categoryid']
          cc["fondaCatId"] = fondaCatId
          continue
        fondaCatId = str(uuid.uuid4())
        cc["fondaCatId"] = fondaCatId
        categories_payload.append(( fondaCatId, merchantId, cc["name"], "", 1, userId ))
        menu_categories_payload.append(( str(uuid.uuid4()), merchantId, menuId, fondaCatId, 1, None, cc["sortOrder"] ))
        menu_categories_payload.append(( str(uuid.uuid4()), merchantId, menuId, fondaCatId, platformType, cc["id"], 0 ))



      """
        get all items from clover
        for citem in clover_items:
          - generate and store fondaItemId:uuid4() in citem for further processing
          - append item details to items_payload
          - append item_mapping to item_mappings_payload
          - check if item is assigned to any category on clover:
            True:
              - loop over clover_item categories:
                append productscategories to items_categories_payload
            False:
              - create a dummy category named (Unassigned) and assign item to that category
      """
      clover_items = list(dict())
      item_without_category = False
      dummy_category_id = str(uuid.uuid4())
      base_limit, limit, offset = 1000, 1000, 0

      while True:
        c_items = cls.clover_get_all_items(cMid=storeId, accessToken=accessToken, limit=limit, offset=offset, expand="categories")
        if not c_items:
          print("api error")
          break
        clover_items.extend(c_items["elements"])
        offset = limit
        limit += base_limit
        if len(c_items["elements"]) == 0 or len(c_items["elements"]) < base_limit:
          break

      for citem in clover_items:

        cursor.execute(
          """SELECT * FROM itemmappings WHERE merchantid = %s AND platformtype = %s AND menuid = %s AND platformitemid=%s""",
          (merchantId, platformType, menuId, citem['id']))
        getitem = cursor.fetchone()
        if getitem:
          itemname = citem["name"]
          itemPrice = citem["price"] / 100
          itemStatus = 1 if citem["available"] == True else 0
          cursor.execute("""
                             UPDATE items 
                               SET  itemprice = %s , posname=%s , status = %s 
                               WHERE id=%s""",
                         (itemPrice,itemname , itemStatus ,  getitem['itemid'])
                         )
          fondaItemId = getitem['itemid']
          citem["fondaItemId"] = fondaItemId
          citem["inside_category"]=True
        else:
          fondaItemId = str(uuid.uuid4())
          citem["fondaItemId"] = fondaItemId
          citem["inside_category"] = True

          itemPrice = citem["price"] / 100
          itemStatus = 1 if citem["available"] == True else 0
          items_payload.append(( fondaItemId, merchantId, "", citem["name"],citem["name"], citem.get("alternateName") or "", itemPrice, 1, itemStatus, 0, userId ))

          item_mappings_payload.append(( str(uuid.uuid4()), merchantId, menuId, fondaItemId, 1, platformType, citem["id"] ))

        if len(citem["categories"]["elements"]) > 0:

          for itemCat in citem["categories"]["elements"]:
            for cc in clover_categories:
              if cc["id"] == itemCat["id"]:
                cursor.execute(
                  """SELECT * FROM productscategories WHERE productid = %s AND categoryid = %s """,
                  (fondaItemId, cc["fondaCatId"]))
                getitemcatnmapping = cursor.fetchone()
                if not getitemcatnmapping:
                  items_categories_payload.append(( str(uuid.uuid4()), fondaItemId, cc["fondaCatId"], userId ))
                  break

        else:
          print("item is not assigned to any category")
          items_categories_payload.append(( str(uuid.uuid4()), fondaItemId, dummy_category_id, userId ))
          item_without_category = True

      # if item_without_category is True then create and store dummy category
      if item_without_category:
        categories_payload.append(( dummy_category_id, merchantId, "Unassigned", "", 1, userId ))
        menu_categories_payload.append(( str(uuid.uuid4()), merchantId, menuId, dummy_category_id, 1, None, 0 ))


      """
        get all addons with options from clover
        for caddon in clover_addons:
          - generate and store fondaItemId:uuid4() for further processing
          - check if addon is assigned to any item on clover:
            True:
              - loop over caddon's items and form items_addons_payload for productsaddons db table
              - append to addons_payload
              - append to addon_mappings_payload
              - loop: for coption in caddon:
                - append option details to items_payload
                - append to addons_options_payload for addonsoptions db table
                - append to addon_mappings_payload
            
            False:
              do nothing

      """
      clover_addons = list(dict())
      limit, offset = 1000, 0
      while True:
        c_addons = cls.clover_get_all_modifier_groups(cMid=storeId, accessToken=accessToken, limit=limit, offset=offset, expand="modifiers,items")
        if not c_addons:
          print("api error")
          break
        clover_addons.extend(c_addons["elements"])
        offset = limit
        limit += base_limit
        if len(c_addons["elements"]) == 0 or len(c_addons["elements"]) < base_limit:
          break
      
      for caddon in clover_addons:
        cursor.execute(
          """SELECT * FROM addonmappings WHERE merchantid = %s AND platformtype = %s AND menuid = %s AND platformaddonid=%s""",
          (merchantId, platformType, menuId, caddon["id"]))
        getaddon = cursor.fetchone()

        if getaddon:
          addonname = caddon["name"]
          cursor.execute("""
                                           UPDATE addons 
                                             SET posname=%s 
                                             WHERE id=%s""",
                         (addonname, getaddon['addonid'])
                         )
          fondaAddonId = getaddon['addonid']
        else:
          fondaAddonId = str(uuid.uuid4())

        if len(caddon["items"]["elements"]) > 0:
          
          for addonProd in caddon["items"]["elements"]:
            for citem in clover_items:
              if citem["id"] == addonProd["id"] and citem["inside_category"] == True:
                cursor.execute(
                  """SELECT * FROM productsaddons WHERE addonid = %s AND productid = %s """,
                  (fondaAddonId, citem["fondaItemId"]))
                getitemaddonmapping = cursor.fetchone()
                if not getitemaddonmapping:
                  items_addons_payload.append(( str(uuid.uuid4()), citem["fondaItemId"], fondaAddonId, userId ))
                  break
          
          if not getaddon:
            addons_payload.append(( fondaAddonId, merchantId, caddon["name"],  "", caddon.get("minRequired",0), caddon.get("maxAllowed",0), 1, 1, userId))

            addon_mappings_payload.append(( str(uuid.uuid4()), merchantId, menuId, fondaAddonId, None, platformType, caddon["id"] ))

          for coption in caddon["modifiers"]["elements"]:
            cursor.execute(
              """SELECT * FROM addonmappings WHERE merchantid = %s AND platformtype = %s AND menuid = %s AND platformaddonid=%s""",
              (merchantId, platformType, menuId, coption['id']))
            getaddonoption = cursor.fetchone()
            if getaddonoption:
              optionName = coption["name"]
              optionPrice = coption["price"] / 100
              cursor.execute("""
                                                        UPDATE items 
                                                          SET itemprice=%s , posname=%s 
                                                          WHERE id=%s""",
                             ( optionPrice,optionName ,  getaddonoption['addonoptionid'])
                             )
              continue
            fondaOptionId = str(uuid.uuid4())

            optionPrice = coption["price"] / 100
            # optionStatus = 1 if coption.get("available") is None or coption.get("available") == True else 0
            optionStatus = 1
            items_payload.append(( fondaOptionId, merchantId, "", coption["name"],coption["name"],  "", optionPrice, 2, optionStatus, 0, userId ))

            addons_options_payload.append(( str(uuid.uuid4()), fondaOptionId, fondaAddonId, userId))

            addon_mappings_payload.append(( str(uuid.uuid4()), merchantId, menuId, fondaAddonId, fondaOptionId, platformType, coption["id"] ))

        else:
          print("addon is not assigned to any item. skip...")



      """
        store all the payloads in database """

      if new_menu:
        print("menus insert...")
        #  Inserting new menu
        cursor.execute("""
          INSERT INTO menus (id, merchantid, name, description, status, created_by)
          VALUES (%s,%s,%s,%s,%s,%s)
        """, menu_payload)

        # insertin menu mapping with Clover platfrom
        data = (str(uuid.uuid4()), menuId, merchantId, 4, '', 0, None, userId)
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
        INSERT INTO menucategories (id, merchantid, menuid, categoryid, platformtype, platformcategoryid, sortid)
        VALUES (%s,%s,%s,%s,%s,%s,%s)
      """, (menu_categories_payload))
      print(cursor.rowcount)

      print("items insert...")
      cursor.executemany("""
        INSERT INTO items (id, merchantid, itemsku, itemname,posname, itemdescription, itemprice, itemtype, status, taxrate, created_by)
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
        INSERT IGNORE INTO addonsoptions (id, itemid, addonid, created_by)
        VALUES (%s,%s,%s,%s)
      """, (addons_options_payload))
      print(cursor.rowcount)

      print("committing inserts...")
      connection.commit()

      return True, 200, "success"
    except Exception as e:
      return False, 500, str(e)


  ############################################### ORDER TYPES

  @classmethod
  def clover_get_order_type_id(cls, cMid, accessToken, orderType):
    try:
      ## getting ordertype if it is already in clover
      url = f"{config.clover_base_url}/v3/merchants/{cMid}/order_types"
      payload = ""
      headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {accessToken}"
      }
      response = requests.request("GET", url, headers=headers, data=payload)
      data = response.json() if response and response.status_code >= 200 and response.status_code < 300 else False

      if data:
        for cloverOrderType in data['elements']:
          if cloverOrderType['label'] == orderType.upper():
            return cloverOrderType['id']

      ## creating ordertype if it is not already in clover
      payload = json.dumps({
        "labelKey": orderType.upper(),
        "label": orderType.upper()
      })
      headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {accessToken}"
      }
      response = requests.request("POST", url, headers=headers, data=payload)
      data = response.json() if response and response.status_code >= 200 and response.status_code < 300 else False
      if data:
        return data['id']
      else:
        return data

    except Exception as e:
      print("Error: ", str(e))
      return False


  ############################################### TENDERS

  @classmethod
  def verify_tenders(cls, cMid, accessToken, tenders=['Fonda-DD', 'Fonda-UE', 'Fonda-GH', 'Fonda-SF']):
    try:
      ## getting tenders if already in clover
      headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/json',
        'Authorization': 'Bearer ' + accessToken
      }

      url = config.clover_base_url + "/v3/merchants/" + cMid + "/tenders"
      payload = {}
      tender_response = requests.request("GET", url, headers=headers, data=payload)
      tender_response = tender_response.json()

      tenders_names = [tender['label'] for tender in tender_response['elements']]
      missing_tenders = [tender for tender in tenders if tender not in tenders_names]

      ## adding tenders if already not in clover
      for tender in missing_tenders:
        url = config.clover_base_url + "/v3/merchants/" + cMid + "/tenders"
        payload = json.dumps({
          "editable": True,
          "label": tender,
          "opensCashDrawer": True,
          "supportsTipping": False,
          "enabled": True,
          "visible": True
        })

        response = requests.request("POST", url, headers=headers, data=payload)
        if not (response and response.status_code >= 200 and response.status_code < 300):
          return False

      return True
    except Exception as e:
      print("Error: ", str(e))
      return False

  @classmethod
  def get_tender_id(cls, cMid, accessToken, tender):
    try:
      ## getting tenders if already in clover
      headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/json',
        'Authorization': 'Bearer ' + accessToken
      }

      url = config.clover_base_url + "/v3/merchants/" + cMid + "/tenders"
      payload = {}
      tender_response = requests.request("GET", url, headers=headers, data=payload)
      tender_response = tender_response.json()

      tenders = {tender['label']: tender['id'] for tender in tender_response['elements']}

      if tender in tenders:
        return tenders[tender]
      else:
        return False
    except Exception as e:
      print("Error: ", str(e))
      return False

  @classmethod
  def get_clover_menu_mapping(cls, merchantId, platformType, menuId):
    # Getting menu categories
    connection, cursor = get_db_connection()
    cursor.execute("""SELECT platformcategoryid FROM menucategories WHERE merchantid=%s AND menuid=%s AND platformtype=%s """,
                   (merchantId,menuId, platformType))
    categories_mapping = cursor.fetchall()

    # Getting menu categories
    connection, cursor = get_db_connection()
    cursor.execute("""SELECT platformitemid FROM itemmappings WHERE merchantid=%s AND menuid=%s AND platformtype=%s """,
                   (merchantId, menuId, platformType))
    items_mapping = cursor.fetchall()

    # Getting menu categories
    connection, cursor = get_db_connection()
    cursor.execute("""SELECT platformaddonid FROM addonmappings WHERE merchantid=%s AND menuid=%s AND platformtype=%s AND addonoptionid is null""",
                   (merchantId, menuId, platformType))
    addon_mapping = cursor.fetchall()

    return { 'catPlatformIds':[ catPlatformId['platformcategoryid'] for catPlatformId in categories_mapping],
              'itemPlatformIds':[ itemPlatformId['platformitemid'] for itemPlatformId in items_mapping],
              'addonPlatformIds':[ addonPlatformId['platformaddonid'] for addonPlatformId in addon_mapping]
    }

  @classmethod
  def generate_clover_access_token(cls, platform_details):
    try:
      print('-----------------------  Calling the generate clover access token api --------------------')
      print('Platform detail: ' ,platform_details )
      url = f"{config.clover_base_url}/oauth/v2/refresh"
      if platform_details.get('token_metadata') is None:
        return '','Refresh token not found', True
      # Prepare the payload with necessary parameters
      json_metatdata=json.loads(platform_details.get('token_metadata'))
      current_time = int(time.time())  # Get current Unix timestamp
      remaining_time = json_metatdata["access_token_expiration"] - current_time
      if remaining_time > 300:
        print('Token not expire so return previous token')
        return json_metatdata.get('access_token'), 'success', False
      payload = {
        'client_id': config.clover_client_id,
        'refresh_token': json_metatdata.get('refresh_token')
      }

      # Set the headers with the correct Content-Type
      headers = {
        'Content-Type': 'application/json'
      }

      response = requests.post(url, headers=headers, data=json.dumps(payload))
      print("response ", response)

      json_data = response.json() if response and response.status_code == 200 else None
      print("json_data ", json_data)
      if json_data:
        token_metadata = json.dumps(json_data)
        token = json_data['access_token']
        print('token_metadata ' , token_metadata)
        connection, cursor = get_db_connection()
        cursor.execute("""UPDATE platforms SET accesstoken=%s , token_metadata=%s where id=%s""",
                       (token, token_metadata ,platform_details['id'] ))
        connection.commit()
        return token ,'success' ,  False
      return '','error: while generating clover access token!', True
    except Exception as e:
      print("Error: ", str(e))
      return '',str(e) , True
