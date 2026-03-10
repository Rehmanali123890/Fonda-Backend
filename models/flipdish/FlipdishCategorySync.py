import json
import boto3
import uuid

# local imports
import config
from models.ItemMappings import ItemMappings
from models.MenuMappings import MenuMappings
from utilities.helpers import get_db_connection, publish_sqs_message
from models.MenuCategories import MenuCategories
from models.flipdish.Flipdish import Flipdish
from models.Categories import Categories


class FlipdishCategorySync():


  @classmethod
  def assign_item_to_category(cls, merchant_obj, categoryId, itemId, platform=None):
    try:
      print("assigning item to category -> flipdish...")

      connection, cursor = get_db_connection()

      platformType = platform.get("platformtype")
      fStoreId = platform.get("storeid")
      accessToken = platform.get("accesstoken")

      print(f"check if merchant/vmerchant with id <{merchant_obj['syncMerchantId']}> have a menu assigned to flipdish")
      mappings = MenuMappings.get_menumappings(merchantId=merchant_obj["mainMerchantId"], platformType=platformType)

      menuMappingRow = list()
      if merchant_obj["isVirtual"] == 1:
        for mapping in mappings:
          # check if menu is assigned to specified virtual-merchant-id, then append it to list
          cursor.execute("""SELECT * FROM vmerchantmenus WHERE vmerchantid = %s AND menuid = %s""", (merchant_obj["syncMerchantId"], mapping["menuid"]))
          row = cursor.fetchone()
          if row:
            menuMappingRow.append(mapping)
      else:
        for mapping in mappings:
          # check if menu is assigned to any virtual-merchant, then skip it
          cursor.execute("""SELECT * FROM vmerchantmenus WHERE merchantid = %s AND menuid = %s""", (merchant_obj["mainMerchantId"], mapping["menuid"]))
          row = cursor.fetchone()
          if not row:
            menuMappingRow.append(mapping)

      if len(menuMappingRow) == 0 or len(menuMappingRow) > 1:
        return False
      # else get the one and only one row
      menuMappingRow = menuMappingRow[0]

      fMenuId = menuMappingRow.get("platformmenuid")
      menuId = menuMappingRow.get("menuid")
      metadata = json.loads(menuMappingRow["metadata"]) if menuMappingRow['metadata'] else None
      fTaxRateId = metadata.get("flipdish_taxrate_id")

      # check if category is assigned to flipdish menu...
      rows = MenuCategories.get_menucategories_fk(merchantId=merchant_obj["syncMerchantId"], menuId=menuId, categoryId=categoryId, platformType=platformType)
      if not len(rows):
        print("category is not assigned to flipdish menu. exiting...")
        return True
      menu_category = rows[0]
      fCategoryPublicId = menu_category.get("platformcategoryid")

      # get all menu-categories from flipdish menu...
      response = Flipdish.flipdish_get_menu_categories_by_id(accessToken=accessToken, fMenuId=fMenuId)
      if not response:
        print("error while getting menu-categories from flipdish")
        return False
      
      # compare menu-category public-id with flipdish categories...
      fCategoryId = None
      for row in response.get("Data"):
        if row.get("PublicId") == fCategoryPublicId:
          fCategoryId = row.get("MenuSectionId")
          break
      if fCategoryId is None:
        print("error. category public id not found in flipdish menu-section")
        return False
      
      # SQS Send Item
      dataObj = {
        "platformId":platform["id"],
        "menuId": menuId,
        "categoryId": categoryId,
        "itemId": itemId,
        "itemSortId": 200,
        "fMenuId": fMenuId,
        "fCategoryId": fCategoryId, 
        "fTaxRateId": fTaxRateId,
        "mainMerchantId": merchant_obj["mainMerchantId"],
        "syncMerchantId": merchant_obj["syncMerchantId"]
      }

      sqs_client = boto3.resource('sqs')
      queue = sqs_client.get_queue_by_name(QueueName=config.flipdish_create_item_queue)
      response = queue.send_message(
        MessageBody=json.dumps(dataObj),
        MessageGroupId=str(uuid.uuid4()),
        MessageDeduplicationId=str(uuid.uuid4())
      )
      print(response)
        
      print("--- End ---")
      return True
    except Exception as e:
      print("Error: ", str(e))
      return False
  

  @classmethod
  def unassign_item_to_category(cls, merchant_obj, categoryId, itemId, platform=None):
    try:
      print("unassigning item from category -> flipdish...")
      
      connection, cursor = get_db_connection()

      platformType = platform.get("platformtype")
      fStoreId = platform.get("storeid")
      accessToken = platform.get("accesstoken")

      print(f"check if merchant/vmerchant with id <{merchant_obj['syncMerchantId']}> have a menu assigned to flipdish")
      mappings = MenuMappings.get_menumappings(merchantId=merchant_obj["mainMerchantId"], platformType=platformType)

      menuMappingRow = list()
      if merchant_obj["isVirtual"] == 1:
        for mapping in mappings:
          # check if menu is assigned to specified virtual-merchant-id, then append it to list
          cursor.execute("""SELECT * FROM vmerchantmenus WHERE vmerchantid = %s AND menuid = %s""", (merchant_obj["syncMerchantId"], mapping["menuid"]))
          row = cursor.fetchone()
          if row:
            menuMappingRow.append(mapping)
      else:
        for mapping in mappings:
          # check if menu is assigned to any virtual-merchant, then skip it
          cursor.execute("""SELECT * FROM vmerchantmenus WHERE merchantid = %s AND menuid = %s""", (merchant_obj["mainMerchantId"], mapping["menuid"]))
          row = cursor.fetchone()
          if not row:
            menuMappingRow.append(mapping)

      if len(menuMappingRow) == 0 or len(menuMappingRow) > 1:
        return False
      # else get the one and only one row
      menuMappingRow = menuMappingRow[0]

      fMenuId = menuMappingRow.get("platformmenuid")
      menuId = menuMappingRow.get("menuid")

      # check if category is assigned to flipdish menu...
      rows = MenuCategories.get_menucategories_fk(merchantId=merchant_obj["syncMerchantId"], menuId=menuId, categoryId=categoryId, platformType=platformType)
      if not len(rows):
        print("category is not assigned to flipdish menu. exiting...")
        return True
      menu_category = rows[0]
      fCategoryPublicId = menu_category.get("platformcategoryid")

      # get category-item public-id from itemmappings table...
      cursor.execute("""SELECT * FROM itemmappings 
        WHERE merchantid=%s AND categoryid=%s AND itemid=%s AND platformtype=%s AND addonid is NULL""", (merchant_obj["syncMerchantId"], categoryId, itemId, platformType)
      )
      item = cursor.fetchone()
      if not item:
        print("error: item mappings data is not available. cannot remove the product from flipdish. please perform manual sync...")
        return False
      fItemPublicId = item.get("platformitemid")


      # get flipdish menu by id
      fmenu_data = Flipdish.flipdish_get_menu_by_id(accessToken=accessToken, fMenuId=fMenuId)
      if not fmenu_data:
        print("Error occured while gettig menu from flipdish")
        return False
      fMenuSections = fmenu_data.get("Data").get("MenuSections")
      

      # compare menu-category public-id with flipdish categories...
      fCategoryId = None
      fMenuSectionItems = None
      for section in fMenuSections:
        if section.get("PublicId") == fCategoryPublicId:
          fCategoryId = section.get("MenuSectionId")
          fMenuSectionItems = section.get("MenuItems")
          break
      if fCategoryId is None:
        print("error. category public id not found in flipdish menu-section")
        return False
      

      # compare menu-category-item public-id with flipdish category-items...
      fItemId = None
      for item in fMenuSectionItems:
        if item.get("PublicId") == fItemPublicId:
          fItemId = item.get("MenuItemId")
          break
      if fItemId is None:
        print("error. item public-id not found in flipdish menu-category-item")
        return False
      
      # now we have everything to rock
      response = Flipdish.flipdish_delete_category_product(
        accessToken=accessToken, 
        fMenuId=fMenuId, 
        fCategoryId=fCategoryId, 
        fItemId=fItemId
      )
      if not response:
        print("error: category-item not deleted from flipdish")
        return False
      

      print("delete data from itemmappings...")
      cursor.execute("""DELETE FROM itemmappings 
        WHERE merchantid=%s AND categoryid=%s AND itemid=%s AND platformtype=%s""", (merchant_obj["syncMerchantId"], categoryId, itemId, platformType))
      connection.commit()
      
      return True
    except Exception as e:
      print("Error: ", str(e))
      return False
  

  @classmethod
  def update_category(cls, merchant_obj, categoryId, platform=None):
    try:
      print("update_category -> flipdish...")

      connection, cursor = get_db_connection()

      platformType = platform.get("platformtype")
      fStoreId = platform.get("storeid")
      accessToken = platform.get("accesstoken")

      print(f"check if merchant/vmerchant with id <{merchant_obj['syncMerchantId']}> have a menu assigned to flipdish")
      mappings = MenuMappings.get_menumappings(merchantId=merchant_obj["mainMerchantId"], platformType=platformType)
      print(mappings)

      menuMappingRow = list()
      if merchant_obj["isVirtual"] == 1:
        for mapping in mappings:
          # check if menu is assigned to specified virtual-merchant-id, then append it to list
          cursor.execute("""SELECT * FROM vmerchantmenus WHERE vmerchantid = %s AND menuid = %s""", (merchant_obj["syncMerchantId"], mapping["menuid"]))
          row = cursor.fetchone()
          if row:
            menuMappingRow.append(mapping)
      else:
        for mapping in mappings:
          # check if menu is assigned to any virtual-merchant, then skip it
          cursor.execute("""SELECT * FROM vmerchantmenus WHERE merchantid = %s AND menuid = %s""", (merchant_obj["mainMerchantId"], mapping["menuid"]))
          row = cursor.fetchone()
          if not row:
            menuMappingRow.append(mapping)
      
      print(menuMappingRow)

      if len(menuMappingRow) == 0 or len(menuMappingRow) > 1:
        return False
      # else get the one and only one row
      menuMappingRow = menuMappingRow[0]

      fMenuId = menuMappingRow.get("platformmenuid")
      menuId = menuMappingRow.get("menuid")

      # check if category is assigned to flipdish menu...
      rows = MenuCategories.get_menucategories_fk(merchantId=merchant_obj["syncMerchantId"], menuId=menuId, categoryId=categoryId, platformType=platformType)
      print(rows)
      if not len(rows):
        print("category is not assigned to flipdish menu. exiting...")
        return True
      menu_category = rows[0]
      fCategoryPublicId = menu_category.get("platformcategoryid")

      # get all menu-categories from flipdish menu...
      response = Flipdish.flipdish_get_menu_categories_by_id(accessToken=accessToken, fMenuId=fMenuId)
      if not response:
        print("error while getting menu-categories from flipdish")
        return False
      print(response)

      # compare menu-category public-id with flipdish categories...
      fCategoryId = None
      for row in response.get("Data"):
        if row.get("PublicId") == fCategoryPublicId:
          fCategoryId = row.get("MenuSectionId")
          break
      if fCategoryId is None:
        print("error. category public id not found in flipdish menu-section")
        return False
      
      # get category details by id...
      category = Categories.get_category_by_id(id=categoryId)

      # update menu-category details on flipdish...
      response = Flipdish.flipdish_update_menu_category(
        accessToken=accessToken,
        fMenuId=fMenuId,
        fCategoryId=fCategoryId,
        categoryName=category["categoryname"],
        categoryDescription=category["categorydescription"]
      )
      if not response:
        return False

      print("---updated---")
      return True
    except Exception as e:
      print("Error: ", str(e))
      return False
  

  @classmethod
  def delete_category(cls, merchant_obj, categoryId, platform=None):
    try:
      print("delete_category -> flipdish...")

      connection, cursor = get_db_connection()

      platformType = platform.get("platformtype")
      fStoreId = platform.get("storeid")
      accessToken = platform.get("accesstoken")

      print(f"check if merchant/vmerchant with id <{merchant_obj['syncMerchantId']}> have a menu assigned to flipdish")
      mappings = MenuMappings.get_menumappings(merchantId=merchant_obj["mainMerchantId"], platformType=platformType)

      menuMappingRow = list()
      if merchant_obj["isVirtual"] == 1:
        for mapping in mappings:
          # check if menu is assigned to specified virtual-merchant-id, then append it to list
          cursor.execute("""SELECT * FROM vmerchantmenus WHERE vmerchantid = %s AND menuid = %s""", (merchant_obj["syncMerchantId"], mapping["menuid"]))
          row = cursor.fetchone()
          if row:
            menuMappingRow.append(mapping)
      else:
        for mapping in mappings:
          # check if menu is assigned to any virtual-merchant, then skip it
          cursor.execute("""SELECT * FROM vmerchantmenus WHERE merchantid = %s AND menuid = %s""", (merchant_obj["mainMerchantId"], mapping["menuid"]))
          row = cursor.fetchone()
          if not row:
            menuMappingRow.append(mapping)

      if len(menuMappingRow) == 0 or len(menuMappingRow) > 1:
        return False
      # else get the one and only one row
      menuMappingRow = menuMappingRow[0]

      fMenuId = menuMappingRow.get("platformmenuid")
      menuId = menuMappingRow.get("menuid")

      # check if category is assigned to flipdish menu...
      rows = MenuCategories.get_menucategories_fk(merchantId=merchant_obj["syncMerchantId"], menuId=menuId, categoryId=categoryId, platformType=platformType)
      if not len(rows):
        print("category is not assigned to flipdish menu. exiting...")
        return True
      menu_category = rows[0]
      fCategoryPublicId = menu_category.get("platformcategoryid")
      print(fCategoryPublicId)

      # get all menu-categories from flipdish menu...
      response = Flipdish.flipdish_get_menu_categories_by_id(accessToken=accessToken, fMenuId=fMenuId)
      if not response:
        print("error while getting menu-categories from flipdish")
        return False

      # compare menu-category public-id with flipdish categories...
      fCategoryId = None
      for row in response.get("Data"):
        if row.get("PublicId") == fCategoryPublicId:
          fCategoryId = row.get("MenuSectionId")
          break
      if fCategoryId is None:
        print("error. category public id not found in flipdish menu-section")
        return False
      
      # delete menu-category from flipdish...
      response = Flipdish.flipdish_delete_menu_category(
        accessToken=accessToken,
        fMenuId=fMenuId,
        fCategoryId=fCategoryId
      )
      if not response:
        print("error while deleting menu-category from flipdish")
        return False
      
      # delete data from menucategories and itemmappings...
      mc_resp = MenuCategories.delete_menucategories(merchantId=merchant_obj["syncMerchantId"], menuId=menuId, categoryId=categoryId, platformType=platformType)
      im_resp = ItemMappings.delete_itemmappings(merchantId=merchant_obj["syncMerchantId"], menuId=menuId, categoryId=categoryId, platformType=platformType)

      return True
    except Exception as e:
      print("Error: ", str(e))
      return False
  

