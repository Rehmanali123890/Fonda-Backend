import json
import boto3

# local imports
import config
from models.clover.Clover import Clover
from utilities.helpers import get_db_connection
from models.MenuCategories import MenuCategories
from models.Categories import Categories
from models.MenuMappings import MenuMappings
from models.ItemMappings import ItemMappings
from models.Items import Items
from models.ProductsAddons import ProductsAddons


class CloverCategorySync():


  @classmethod
  def assign_item_to_category(cls, merchantId, categoryId, itemId, platform=None):
    try:
      print("Start assigning item to category -> clover...")
      connection, cursor = get_db_connection()

      platformType = platform['platformtype']
      storeId = platform['storeid']
      accessToken = platform['accesstoken']

      print("check if menu is assigned to clover...")
      mappings = MenuMappings.get_menumappings(merchantId=merchantId, platformType=platformType)
      if not len(mappings):
        # menu is not assigned to clover, exiting...
        return True
      menu_mapping = mappings[0]

      print("check if category is assigned to clover menu...")
      rows = MenuCategories.get_menucategories_fk(menuId=menu_mapping['menuid'], categoryId=categoryId, platformType=platformType)
      if not len(rows):
        # the category is not assigned to clover menu
        return True
      menu_category = rows[0]

      items_ids_dict = list(dict())
      clover_categories_items_mapping_json = list(dict())

      ### check if item is already available on clover
      cursor.execute("""SELECT * FROM itemmappings WHERE itemid=%s AND platformtype=%s""", (itemId, platformType))
      item_mapping = cursor.fetchone()

      if item_mapping and item_mapping['platformitemid']:
        # if itemmapping is already available then do not sent item again to clover, only store the category_item mapping
        clover_categories_items_mapping_json.append({
          "category": {"id": menu_category["platformcategoryid"]},
          "item": {"id": item_mapping["platformitemid"]}
        })

      else:
        # else item is new and send item details to clover and also store its category_item mapping
        item_details = Items.get_item_by_id(itemId)
        if not item_details:
          return False

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
          return False

        items_ids_dict.append({
          'id': itemId,
          'clover_id': clover_item_resp['id']
        })

        clover_categories_items_mapping_json.append({
          "category": {"id": menu_category["platformcategoryid"]},
          "item": {"id": clover_item_resp["id"]}
        })

        # store item-mappings...
        resp = ItemMappings.post_itemmappings(
          merchantId=merchantId, 
          menuId=menu_mapping['menuid'],
          itemId=itemId,
          itemType=1,  # 1 for product itemType
          platformType=platformType,
          platformItemId=clover_item_resp['id'])
        if not resp:
          print("error: while storing clover productId in item-mappings!!!")
      

      ### create association between category and item on clover
      print("create association between category and item on clover...")
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
      if len(items_ids_dict):
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

      return True
    except Exception as e:
      print("Error: ", str(e))
      return False
  
  #########################################################
  #########################################################
  #########################################################

  @classmethod
  def unassign_item_to_category(cls, merchantId, categoryId, itemId, platform=None):
    try:
      print("Start un-assigning item from category -> clover...")
      connection, cursor = get_db_connection()

      platformType = platform['platformtype']
      storeId = platform['storeid']
      accessToken = platform['accesstoken']

      # check if category is assigned to clover menu
      print("check if category is assigned to clover menu...")
      menu_categoires = MenuCategories.get_menucategories_fk(categoryId=categoryId, platformType=platformType)
      if not menu_categoires:
        # category is not assigned to clover menu
        return True
      menu_category = menu_categoires[0]

      # check if item is only assigned to more than 1 category in clover menu
      cursor.execute("""
        SELECT * FROM productscategories WHERE productid=%s AND categoryid IN (
          SELECT categoryid FROM menucategories WHERE menuid=%s AND platformtype=%s AND categoryid != %s
        )  
        """, (itemId, menu_category['menuid'], 1, categoryId))
      pc_rows = cursor.fetchall()

      # get itemmappings
      cursor.execute("""SELECT * FROM itemmappings WHERE itemid=%s AND platformtype=%s""", (itemId, platformType))
      item_mapping = cursor.fetchone()
      if not item_mapping:
        return False

      if pc_rows:
        # only remove the association between product and category and do not delete the product
        object = {
          "elements": [
            {
              "category": {"id": menu_category["platformcategoryid"]},
              "item": {"id": item_mapping["platformitemid"]}
            }
          ]
        }
        clover_del_association_resp = Clover.clover_items_categories_association(cMid=storeId, accessToken=accessToken, object=object, delete=True)
        if not clover_del_association_resp:
          print("error: while removing association between category and item on clover!!!")
      
      else:
        # remove product from the clover and its addons
        clover_item_del_resp = Clover.clover_delete_items(cMid=storeId, accessToken=accessToken, itemIds=item_mapping['platformitemid'])
        if not clover_item_del_resp:
          print("error: while deleting item from clover!!!")
        
        cursor.execute("""DELETE FROM itemmappings WHERE platformtype=%s AND itemid = %s""", (platformType, itemId))
        connection.commit()

        # DELETE EACH ITEM ADDONS FROM CLOVER IF THEY ARE NOT CONNECTED TO ANY OTHER ITEM IN CLOVER 
        addons_ids_list = list()
        clover_addons_ids_list = list()

        item_addons = ProductsAddons.get_item_addon(itemId=itemId)

        for item_addon in item_addons:
          
          cursor.execute("""
            SELECT * FROM productsaddons WHERE addonid=%s AND productid IN (
              SELECT itemid FROM itemmappings WHERE menuid=%s AND platformtype=%s
            )
            """, (item_addon['addonid'], menu_category['menuid'], platformType))
          data_rows = cursor.fetchall()
          if not len(data_rows):

            cursor.execute("""SELECT * FROM addonmappings WHERE addonid=%s AND platformtype=%s AND addonoptionid is NULL""", (item_addon['addonid'], platformType))
            addon_mapping = cursor.fetchone()
            
            if addon_mapping:
              addons_ids_list.append(item_addon['addonid'])
              clover_addons_ids_list.append(addon_mapping['platformaddonid'])
    
        if len(clover_addons_ids_list):
          
          clover_addons_del_resp = Clover.clover_delete_modifier_groups(cMid=storeId, accessToken=accessToken, addonIds=','.join(clover_addons_ids_list))
          if not clover_addons_del_resp:
            print("error: while deleting addons from clover!!!")

          cursor.execute("""DELETE FROM addonmappings WHERE platformtype=%s AND addonid IN %s""", (platformType, tuple(addons_ids_list)))
          connection.commit()

      return True
    except Exception as e:
      print("Error: ", str(e))
      return False
  
  #########################################################
  #########################################################
  #########################################################

  @classmethod
  def update_category(cls, merchantId, categoryId, platform=None):
    try:
      print("Start update_category -> clover...")
      platformType = platform.get("platformtype")
      storeId = platform.get("storeid")
      accessToken = platform.get("accesstoken")

      # check if menu is assigned to clover...
      mappings = MenuMappings.get_menumappings(merchantId=merchantId, platformType=platformType)
      if not len(mappings):
        return True
      menu_mapping = mappings[0]
      
      # get platform-category-id from menucategories
      menu_categories = MenuCategories.get_menucategories_fk(menuId=menu_mapping['menuid'], categoryId=categoryId, platformType=platformType)
      if not len(menu_categories):
        return False
      menu_category = menu_categories[0]

      # get category details...
      category_details = Categories.get_category_by_id(categoryId)
      if not category_details:
        return False
      
      # update category deatils on clover
      clover_cat_resp = Clover.clover_update_category(
        cMid=storeId, 
        cCid=menu_category['platformcategoryid'], 
        accessToken=accessToken, 
        name=category_details['posname'] if category_details['posname'] else category_details['categoryname']
      )
      if not clover_cat_resp:
        print("error: while updating category details on clover!!!")
        return False
      
      return True
    except Exception as e:
      print("Error: ", str(e))
      return False

  #########################################################
  #########################################################
  #########################################################  

  @classmethod
  def delete_category(cls, merchantId, categoryId, platform=None, products_ids_list=None):
    try:
      print("Start delete_category -> clover...")
      connection, cursor = get_db_connection()
      platformType = platform['platformtype']
      storeId = platform['storeid']
      accessToken = platform['accesstoken']

      print("check if menu is assigned to clover...")
      mappings = MenuMappings.get_menumappings(merchantId=merchantId, platformType=platformType)
      if not len(mappings):
        # menu is not assigned to clover, exiting...
        return True
      menu_mapping = mappings[0]

      print("get menu_category mapping...")
      rows = MenuCategories.get_menucategories_fk(menuId=menu_mapping['menuid'], categoryId=categoryId, platformType=platformType)
      if not len(rows):
        print("error: no mapping entry in menucategories table")
        return False
      menu_category = rows[0]

      # delete menu_cateogry mapping...
      resp = MenuCategories.delete_menucategories(menuId=menu_mapping['menuid'], categoryId=categoryId, platformType=platformType)

      print("delete category from clover...")
      clover_cat_del_resp = Clover.clover_delete_categories(
        cMid=storeId, 
        accessToken=accessToken,
        categoryIds=menu_category['platformcategoryid']
      )
      if not clover_cat_del_resp:
        print("error: while deleting category from clover!!!")
        return False


      '''
        TODO: DELETE CATEGORY ITEMS IF THEY ARE NOT ATTACHED TO ANY OTHER CATEGORY IN CLOVER
      '''

      items_ids_list = list()
      clover_items_ids_list = list()

      # loop over category_items
      for productid in products_ids_list:

        cursor.execute("""
          SELECT * FROM productscategories WHERE productid=%s AND categoryid IN (
            SELECT categoryid FROM menucategories WHERE menuid=%s AND platformtype=%s
          )  
          """, (productid, menu_mapping['menuid'], 1))
        pc_rows = cursor.fetchall()
        if not pc_rows:

          cursor.execute("""SELECT * FROM itemmappings WHERE itemid=%s AND platformtype=%s""", (productid, platformType))
          item_mapping = cursor.fetchone()

          if item_mapping:
            items_ids_list.append(productid)
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
            """, (item_addon['addonid'], menu_mapping['menuid'], platformType))
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
  

