from flask import jsonify
import uuid
import json
import requests
import boto3

# local imports
import config
from utilities.helpers import get_db_connection, success
from models.clover.Clover import Clover
from models.MenuMappings import MenuMappings
from models.Categories import Categories
from models.MenuCategories import MenuCategories
from models.ProductsCategories import ProductsCategories
from models.Items import Items
from models.ItemMappings import ItemMappings
from models.ProductsAddons import ProductsAddons

# import config


class CloverMenuSync():


  @classmethod
  def assign_category(cls, merchantId, menuId, categoryId, platform=None):
    try:
      print("Start assigning category - clover...")
      connection, cursor = get_db_connection()
      platform["accesstoken"], msg, is_error = Clover.generate_clover_access_token(platform)
      if is_error:
        print(msg)
        return False
      platformType = platform['platformtype']
      storeId = platform['storeid']
      accessToken = platform['accesstoken']

      # check if menu is assigned to clover...
      mappings = MenuMappings.get_menumappings(menuId=menuId, platformType=platformType)
      if not len(mappings):
        # menu is not assigned to clover, exiting...
        return True
      menu_mapping = mappings[0]

      # get category details...
      category_details = Categories.get_category_by_id(categoryId)
      if not category_details:
        return False
      
      print("post category to clover...")
      categoryName = category_details['posname'] if category_details['posname'] else category_details['categoryname']
      clover_cat_resp = Clover.clover_create_category(cMid=storeId, accessToken=accessToken, name=categoryName)
      if not clover_cat_resp:
        print("error: while creating clover category!!!")
        return False
      
      # store clover category data in menucategories table...
      mc_resp = MenuCategories.post_menucategory(
        merchantId=merchantId, 
        menuId=menu_mapping['menuid'], 
        categoryId=categoryId,
        platformType=platformType, 
        platformCategoryId=clover_cat_resp['id'])
      if not mc_resp:
        print("error: while storing platform category id in menucategories table")
        return False
      
      ### ALL ABOUT CATEGORIES AND ITEMS NOW
      items_ids_list = list()
      items_ids_dict = list(dict())
      clover_categories_items_mapping_json = list(dict())
        
      # getting category items...
      category_items = ProductsCategories.get_productscategories(categoryId=categoryId)
      
      # loop over category_items
      for row in category_items:
        if row['productid'] not in items_ids_list:

          ### check if item is already available on clover
          cursor.execute("""SELECT * FROM itemmappings WHERE itemid=%s AND platformtype=%s""", (row['productid'], platformType))
          item_mapping = cursor.fetchone()

          if item_mapping and item_mapping['platformitemid']:
            # if itemmapping is already available then do not sent item again to clover, only store the category_item mapping
            items_ids_list.append(row['productid'])
            clover_categories_items_mapping_json.append({
              "category": {"id": clover_cat_resp["id"]},
              "item": {"id": item_mapping["platformitemid"]}
            })

          else:
            # else item is new and send item details to clover and also store its category_item mapping
            item_details = Items.get_item_by_id(row['productid'])
            if not item_details:
              continue

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

            clover_item_resp = Clover.clover_create_item(
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
              "category": {"id": clover_cat_resp["id"]},
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
                "category": {"id": clover_cat_resp["id"]},
                "item": {"id": ritem["clover_id"]}
              })
              break
            #
          #
        #
      #

      # create association between categories and items on clover
      print("create association between categories and items on clover...")
      clover_categories_items_mapping_json = {
        "elements": clover_categories_items_mapping_json
      }
      clover_cat_item_mapping_resp = Clover.clover_items_categories_association(
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
          "platformId":platform['id'],
          "menuId": menu_mapping['menuid'],
          "items_ids_dict": items_ids_dict
      }
      response = queue.send_message(MessageBody=json.dumps(dataObj))
      print("Message successfully sent to SQS")

      ''' END TODO '''
      
      return True
    except Exception as e:
      print("Error: ", str(e))
      return False


  #########################################################
  #########################################################
  #########################################################


  @classmethod
  def unassign_category(cls, merchantId, menuId, categoryId, platform=None):
    try:  
      print("Start un-assigning category - clover...")
      connection, cursor = get_db_connection()
      platform["accesstoken"], msg, is_error = Clover.generate_clover_access_token(platform)
      if is_error:
        print(msg)
        return False
      platformType = platform['platformtype']
      storeId = platform['storeid']
      accessToken = platform['accesstoken']

      print("check if menu is assigned to clover...")
      mappings = MenuMappings.get_menumappings(menuId=menuId, platformType=platformType)
      if not len(mappings):
        # menu is not assigned to clover, exiting...
        return True
      menu_mapping = mappings[0]

      print("get menu_category mapping...")
      rows = MenuCategories.get_menucategories_fk(menuId=menuId, categoryId=categoryId, platformType=platformType)
      if not len(rows):
        print("error: no mapping entry in menucategories table")
        return False
      menu_category = rows[0]

      print("delete category from clover...")
      clover_cat_del_resp = Clover.clover_delete_categories(
        cMid=storeId, 
        accessToken=accessToken,
        categoryIds=menu_category['platformcategoryid']
      )
      if not clover_cat_del_resp:
        print("error: while deleting category from clover!!!")
        return False
      
      # delete menu_cateogry mapping...
      resp = MenuCategories.delete_menucategories(menuId=menuId, categoryId=categoryId, platformType=platformType)


      '''
        TODO: DELETE CATEGORY ITEMS IF THEY ARE NOT ATTACHED TO ANY OTHER CATEGORY IN CLOVER
      '''

      print("getting category_items and menu_categories...")
      category_items = ProductsCategories.get_productscategories(categoryId=categoryId)

      items_ids_list = list()
      clover_items_ids_list = list()

      # loop over category_items
      for row in category_items:

        cursor.execute("""
          SELECT * FROM productscategories WHERE productid=%s AND categoryid IN (
            SELECT categoryid FROM menucategories WHERE menuid=%s AND platformtype=%s
          )  
          """, (row['productid'], menuId, 1))
        pc_rows = cursor.fetchall()
        if not pc_rows:

          cursor.execute("""SELECT * FROM itemmappings WHERE itemid=%s AND platformtype=%s""", (row['productid'], platformType))
          item_mapping = cursor.fetchone()

          if item_mapping:
            items_ids_list.append(row['productid'])
            clover_items_ids_list.append(item_mapping['platformitemid'])
      
      if len(clover_items_ids_list): 
        clover_items_ids_list = ','.join(clover_items_ids_list)

        clover_item_del_resp = Clover.clover_delete_items(cMid=storeId, accessToken=accessToken, itemIds=clover_items_ids_list)
        if not clover_item_del_resp:
          print("error: while deleting item from clover!!!")
        
        cursor.execute("""DELETE FROM itemmappings WHERE platformtype=%s AND itemid IN %s""", (platformType, tuple(items_ids_list)))
        connection.commit()
      
      ''' END TODO '''
      

      ''' 
        TODO: DELETE EACH ITEM ADDONS FROM CLOVER IF THEY ARE NOT CONNECTED TO ANY OTHER ITEM IN CLOVER
      '''
      addons_ids_list = list()
      clover_addons_ids_list = list()

      for item_id in items_ids_list:

        item_addons = ProductsAddons.get_item_addon(itemId=item_id)

        for item_addon in item_addons:
          
          cursor.execute("""
            SELECT * FROM productsaddons WHERE addonid=%s AND productid IN (
              SELECT itemid FROM itemmappings WHERE menuid=%s AND platformtype=%s
            )
            """, (item_addon['addonid'], menuId, platformType))
          data_rows = cursor.fetchall()
          if not len(data_rows):

            cursor.execute("""SELECT * FROM addonmappings WHERE addonid=%s AND platformtype=%s AND addonoptionid is NULL""", (item_addon['addonid'], platformType))
            addon_mapping = cursor.fetchone()
            
            if addon_mapping:
              addons_ids_list.append(item_addon['addonid'])
              clover_addons_ids_list.append(addon_mapping['platformaddonid'])
   
      if len(clover_addons_ids_list):
        clover_addons_ids_list = ','.join(clover_addons_ids_list)

        clover_addons_del_resp = Clover.clover_delete_modifier_groups(cMid=storeId, accessToken=accessToken, addonIds=clover_addons_ids_list)
        if not clover_addons_del_resp:
          print("error: while deleting addons from clover!!!")

        cursor.execute("""DELETE FROM addonmappings WHERE platformtype=%s AND addonid IN %s""", (platformType, tuple(addons_ids_list)))
        connection.commit()

      ''' END TODO '''

      return True
    except Exception as e:
      print("Error: ", str(e))
      return False
  

  #########################################################
  #########################################################
  #########################################################


  @classmethod
  def update_menu(cls, merchantId, menuId, platform=None):
    try:
      print("Start updating_menu - clover...")

      
      return True
    except Exception as e:
      print("Error: ", str(e))
      return False
  

  #########################################################
  #########################################################
  #########################################################


  @classmethod
  def delete_menu(cls, merchantId, menuId, mappings, platform=None):
    try:
      print("Start delete_menu - clover...")

      return True
    except Exception as e:
      print("Error: ", str(e))
      return False