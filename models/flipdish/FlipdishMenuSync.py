from flask import jsonify
import uuid
import json
import requests
import boto3

# local imports
import config
from utilities.helpers import get_db_connection, publish_sqs_message
from models.MenuCategories import MenuCategories
from models.ProductsCategories import ProductsCategories
from models.flipdish.Flipdish import Flipdish
from models.Categories import Categories
from models.Menus import Menus
from models.ItemMappings import ItemMappings


class FlipdishMenuSync():


  @classmethod
  def assign_category(cls, merchant_obj, menuId, categoryId, platform=None):
    try:
      print("assigning category to flipdish...")
      connection, cursor = get_db_connection()

      platformType = platform.get("platformtype")
      accessToken = platform.get("accesstoken")

      print(f"check if menu <{menuId}> is assigned to flipdish")
      mapping, msg = Flipdish.check_and_get_menumappings(menuId=menuId, platformType=platformType)
      if not mapping:
        print(msg)
        return True
      fMenuId = mapping.get("platformmenuid")
      metadata = json.loads(mapping["metadata"]) if mapping['metadata'] else None
      fTaxRateId = metadata.get("flipdish_taxrate_id")

      if merchant_obj.get("isVirtual"):
        print(f"check if menu <{menuId}> is assigned to virtual-merchant-id, then continue processing...")
        cursor.execute("SELECT * FROM vmerchantmenus WHERE vmerchantid = %s AND menuid = %s", (merchant_obj["syncMerchantId"], menuId))
        row = cursor.fetchone()
        if not row:
          return True
      else:
        print(f"check if menu <{menuId}> is assigned to any virtual-merchant, then skip processing...")
        cursor.execute("SELECT * FROM vmerchantmenus WHERE merchantid = %s AND menuid = %s", (merchant_obj["mainMerchantId"], menuId))
        row = cursor.fetchone()
        if row:
          return True  

      print("get category by id from db...")
      category = Categories.get_category_by_id(id=categoryId)
      if not category:
        print("error: category does not exists")
        return False

      print("creating menu-category in flipdish...")
      f_category_data = Flipdish.flipdish_create_menu_category(
        accessToken=accessToken,
        fMenuId=fMenuId,
        categoryName=category["categoryname"],
        categoryDescription=category["categorydescription"]
      )
      if not f_category_data:
        print("error while adding menu-section (category) in flipdish. exiting...")
        return False
      
      fCategoryId = f_category_data.get("Data").get("MenuSectionId")
      fCategoryPublicId = f_category_data.get("Data").get("PublicId")


      print("delete if old category mapping is available in menucategories table...")
      mcdel_resp = MenuCategories.delete_menucategories(
        merchantId=merchant_obj["syncMerchantId"],
        menuId=menuId,
        categoryId=categoryId,
        platformType=platformType
      )

      print("Storing new flipdish category mapping (PublicId) into menucategories table...")
      mc_resp = MenuCategories.post_menucategory(
        merchantId=merchant_obj["syncMerchantId"], 
        menuId=menuId, 
        categoryId=categoryId,
        platformType=platformType, 
        platformCategoryId=fCategoryPublicId
      )
      if not mc_resp:
        print("Error while adding flipdish category publicId into menucategory table!!!")
      
      print("Set menu-section (category) availability hours...")
      response = Flipdish.flipdish_set_category_availability(accessToken=accessToken, fMenuId=fMenuId, fCategoryId=fCategoryId)
      if not response:
        print("Error while setting menu-section (category) availability!!!")

      print("Getting category items...")
      category_items = ProductsCategories.get_category_items(categoryId=category['id'])
      if not type(category_items) is list:
        print("Error while getting category items")
        return False
      
      # init sqs_client
      sqs_client = boto3.resource('sqs')
      queue = sqs_client.get_queue_by_name(QueueName=config.flipdish_create_item_queue)
      messageGroupId = str(uuid.uuid4())
      
      for citem in category_items:
        itemId = citem['id']

        # SQS Send Items
        dataObj = {
          "platformId":platform["id"],
          "menuId": menuId,
          "categoryId": categoryId,
          "itemId": itemId,
          "itemSortId": citem['sortId'],
          "fMenuId": fMenuId,
          "fCategoryId": fCategoryId, 
          "fTaxRateId": fTaxRateId,
          "mainMerchantId": merchant_obj["mainMerchantId"],
          "syncMerchantId": merchant_obj["syncMerchantId"]
        }
        
        response = queue.send_message(
          MessageBody=json.dumps(dataObj),
          MessageGroupId=messageGroupId,
          MessageDeduplicationId=str(uuid.uuid4())
        )
        print(response)
        
      print("---End Category---\n")
      return True
    except Exception as e:
      print("Error: ", str(e))
      return False
  


  @classmethod
  def unassign_category(cls, merchant_obj, menuId, categoryId, platform=None):
    try:  
      print("unassigning category from flipdish...")
      connection, cursor = get_db_connection()

      platformType = platform.get("platformtype")
      accessToken = platform.get("accesstoken")

      print(f"check if menu <{menuId}> is assigned to flipdish")
      mapping, msg = Flipdish.check_and_get_menumappings(menuId=menuId, platformType=platformType)
      if not mapping:
        print(msg)
        return True
      fMenuId = mapping.get("platformmenuid")

      if merchant_obj.get("isVirtual"):
        print(f"check if menu <{menuId}> is assigned to virtual-merchant-id, then continue processing...")
        cursor.execute("SELECT * FROM vmerchantmenus WHERE vmerchantid = %s AND menuid = %s", (merchant_obj["syncMerchantId"], menuId))
        row = cursor.fetchone()
        if not row:
          return True
      else:
        print(f"check if menu <{menuId}> is assigned to any virtual-merchant, then skip processing...")
        cursor.execute("SELECT * FROM vmerchantmenus WHERE merchantid = %s AND menuid = %s", (merchant_obj["mainMerchantId"], menuId))
        row = cursor.fetchone()
        if row:
          return True  

      print("get category by id from db...")
      category = Categories.get_category_by_id(id=categoryId)
      if not category:
        print("error: category does not exists")
        return False

      print("get flipdish public-id from menucategories...")
      rows = MenuCategories.get_menucategories_fk(merchantId=merchant_obj["syncMerchantId"], menuId=menuId, categoryId=categoryId, platformType=platformType)
      if not len(rows):
        print("error. no entry in menucategories table")
        return True
      menu_category = rows[0]
      fCategoryPublicId = menu_category.get("platformcategoryid")
      print(fCategoryPublicId)

      print(f"delete category <{categoryId}> entries from menucategories and itemmappings tables...")
      resp = MenuCategories.delete_menucategories(merchantId=merchant_obj["syncMerchantId"], menuId=menuId, categoryId=categoryId, platformType=platformType)
      resp = ItemMappings.delete_itemmappings(merchantId=merchant_obj["syncMerchantId"], categoryId=categoryId, platformType=platformType)

      print("get all menu-categories from flipdish menu...")
      response = Flipdish.flipdish_get_menu_categories_by_id(accessToken=accessToken, fMenuId=fMenuId)
      if not response:
        print("error while getting menu-categories from flipdish")
        return False

      print("check menu-category by public-id stored in menu-category...")
      fCategoryId = None
      for row in response.get("Data"):
        print(row.get("PublicId"))
        if row.get("PublicId") == fCategoryPublicId:
          fCategoryId = row.get("MenuSectionId")
          break
      if fCategoryId is None:
        print("error. category public id not found in flipdish menu-section")
        return False

      print("delete menu-category from flipdish...")
      response = Flipdish.flipdish_delete_menu_category(accessToken=accessToken, fMenuId=fMenuId, fCategoryId=fCategoryId)
      if not response:
        print("error while deleting menu-category from flipdish")

      return True
    except Exception as e:
      print("Error: ", str(e))
      return False
  

  @classmethod
  def update_menu(cls, merchant_obj, menuId, platform=None):
    try:
      print("updating flipdish menu...")
      connection, cursor = get_db_connection()

      platformType = platform.get("platformtype")
      accessToken = platform.get("accesstoken")

      print(f"check if menu <{menuId}> is assigned to flipdish")
      mapping, msg = Flipdish.check_and_get_menumappings(menuId=menuId, platformType=platformType)
      if not mapping:
        print(msg)
        return True
      fMenuId = mapping.get("platformmenuid")

      if merchant_obj.get("isVirtual"):
        print(f"check if menu <{menuId}> is assigned to virtual-merchant-id, then continue processing...")
        cursor.execute("SELECT * FROM vmerchantmenus WHERE vmerchantid = %s AND menuid = %s", (merchant_obj["syncMerchantId"], menuId))
        row = cursor.fetchone()
        if not row:
          return True
      else:
        print(f"check if menu <{menuId}> is assigned to any virtual-merchant, then skip processing...")
        cursor.execute("SELECT * FROM vmerchantmenus WHERE merchantid = %s AND menuid = %s", (merchant_obj["mainMerchantId"], menuId))
        row = cursor.fetchone()
        if row:
          return True

      print("get menu details by id from db...")
      menu = Menus.get_menu_by_id(menuId=menuId)

      print("updating menu name...")
      response = Flipdish.flipdish_set_menu_name(
        accessToken=accessToken, 
        fMenuId=fMenuId, 
        menuName=menu["name"]
      )
      if not response:
        print("error while updating flipdish menu name")
        return False
      
      return True
    except Exception as e:
      print("Error: ", str(e))
      return False
  

  @classmethod
  def delete_menu(cls, merchant_obj, menuId, mappings, platform=None):
    try:
      print("delete_menu from flipdish...")
      connection, cursor = get_db_connection()

      platformType = platform.get("platformtype")
      fStoreId = platform.get("storeid")
      accessToken = platform.get("accesstoken")

      print(f"check if menu <{menuId}> is assigned to flipdish")
      mapping = None
      for row in mappings:
        if row["platformType"] == platformType:
          mapping = row
          break
      if mapping is None:
        print("menu was not assigned to flipdish. exiting...")
        return True
      
      if merchant_obj.get("isVirtual"):
        print(f"check if menu <{menuId}> is assigned to virtual-merchant-id, then continue processing...")
        cursor.execute("SELECT * FROM vmerchantmenus WHERE vmerchantid = %s AND menuid = %s", (merchant_obj["syncMerchantId"], menuId))
        row = cursor.fetchone()
        if not row:
          return True
      else:
        print(f"check if menu <{menuId}> is assigned to any virtual-merchant, then skip processing...")
        cursor.execute("SELECT * FROM vmerchantmenus WHERE merchantid = %s AND menuid = %s", (merchant_obj["mainMerchantId"], menuId))
        row = cursor.fetchone()
        if row:
          return True
      
      fMenuId = mapping.get("platformMenuId")
      print("fMenuId: ", str(fMenuId))

      print("check if flipdish menuid is stored in menumapping table...")
      if fMenuId is None:
        print("menu was not synced with flipdish. exiting...")
        return True

      print("unassign menu from flipdish store")
      response = Flipdish.flipdish_assign_menu_to_store(accessToken=accessToken, fStoreId=fStoreId, fMenuId=1)
        
      print("delete menu from flipdish...")
      response = Flipdish.flipdish_delete_menu(accessToken=accessToken, fMenuId=fMenuId)
      if not response:
        print("Error while deleting flipdish menu!!")
        return False

      return True
    except Exception as e:
      print("Error: ", str(e))
      return False