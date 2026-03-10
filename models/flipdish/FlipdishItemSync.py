

# local imports
import config
from models.Items import Items
from models.ItemMappings import ItemMappings
from utilities.helpers import get_db_connection
from models.flipdish.Flipdish import Flipdish
from models.Addons import Addons
from models.MenuMappings import MenuMappings


class FlipdishItemSync():


  @classmethod
  def assign_addon_to_item(cls, merchant_obj, itemId, addonId, platform=None):
    try:
      print("assigning_addon_to_item -> flipdish...")

      connection, cursor = get_db_connection()

      platformType = platform.get("platformtype")
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

      # Get Flipdish-Categories that contains the specified item
      # multiple categories can be returned
      print(f"get flipdish categories which is contains the item <{itemId}>...")
      cursor.execute("""SELECT * FROM menucategories 
        WHERE merchantid=%s AND menuid=%s AND platformtype=%s AND categoryid IN (
	        SELECT categoryid FROM itemmappings WHERE addonid is NULL AND merchantid=%s AND menuid=%s AND itemid=%s AND platformtype=%s
        )""", (merchant_obj["syncMerchantId"], menuId, platformType, merchant_obj["syncMerchantId"], menuId, itemId, platformType)
      )
      menucategories = cursor.fetchall()
      print(menucategories)
      if not len(menucategories):
        print("product is not assigned to flipdish categories. exiting...")
        return True


      # get flipdish menu by id
      fmenu_data = Flipdish.flipdish_get_menu_by_id(accessToken=accessToken, fMenuId=fMenuId)
      if not fmenu_data:
        print("Error occured while gettig menu from flipdish")
        return False
      fMenuSections = fmenu_data.get("Data").get("MenuSections")


      # loop over the categories
      for menucategory in menucategories:
        categoryId = menucategory.get("categoryid")
        fCategoryPublicId = menucategory.get("platformcategoryid")

        # get item public-id from itemmappings table...
        cursor.execute("""SELECT * FROM itemmappings 
          WHERE merchantid=%s AND categoryid=%s AND itemid=%s AND platformtype=%s AND addonid is NULL""", (merchant_obj["syncMerchantId"], categoryId, itemId, platformType)
        )
        item = cursor.fetchone()
        if not item:
          print("error: item mappings data is not available. cannot add item to flipdish category!")
          continue
        fItemPublicId = item.get("platformitemid")
        
        # get fCategoryId from Flipdish Menu
        fCategoryId = None
        fMenuSectionItems = None
        for section in fMenuSections:
          if section.get("PublicId") == fCategoryPublicId:
            fCategoryId = section.get("MenuSectionId")
            fMenuSectionItems = section.get("MenuItems")
            break
        if fCategoryId is None:
          print("error. category public id not found in flipdish menu-section")
          continue

        # compare menu-category-item public-id with flipdish category-items...
        fItemId = None
        for item in fMenuSectionItems:
          if item.get("PublicId") == fItemPublicId:
            fItemId = item.get("MenuItemId")
            break
        if fItemId is None:
          print("error, item public-id not found in flipdish menu-category-item")
          return False
        
        # get addon details with options
        # assign addon to item
        # store addon public-id in itemmappings table
        # assign addon-options to item
        # store addon-options details in itemmappings table
        print("get addon by id with options from db...")
        addon = Addons.get_addon_by_id_with_options_str(addonId=addonId)
        if not addon:
          print(f"Addon <{addonId}> not found in database!")
          continue

        addon_resp = Flipdish.flipdish_post_complete_addon(
          merchantId=merchant_obj["syncMerchantId"], menuId=menuId, categoryId=categoryId,
          itemId=itemId, addon=addon, platformType=platformType, 
          fMenuId=fMenuId, fCategoryId=fCategoryId, fItemId=fItemId,
          accessToken=accessToken
        )
        if not addon_resp:
          print("--- Addon Adding Failed ---\n")
        print("End category-item")

      return True
    except Exception as e:
      print("Error: ", str(e))
      return False


  #########################################################
  #########################################################
  #########################################################

  
  @classmethod
  def unassign_addon_to_item(cls, merchant_obj, itemId, addonId, platform=None):
    try:
      print("Start un_assigning_addon_to_item -> flipdish...")

      connection, cursor = get_db_connection()

      platformType = platform.get("platformtype")
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

      # Get Flipdish-Categories that contains the specified item
      # multiple categories can be returned
      print(f"get flipdish categories which is contains the item <{itemId}>...")
      cursor.execute("""SELECT * FROM menucategories 
        WHERE merchantid=%s AND menuid=%s AND platformtype=%s AND categoryid IN (
	        SELECT categoryid FROM itemmappings WHERE addonid is NULL AND merchantid=%s AND menuid=%s AND itemid=%s AND platformtype=%s
        )""", (merchant_obj["syncMerchantId"], menuId, platformType, merchant_obj["syncMerchantId"], menuId, itemId, platformType)
      )
      menucategories = cursor.fetchall()
      print(menucategories)
      if not len(menucategories):
        print("product is not assigned to flipdish categories. exiting...")
        return True


      # get flipdish menu by id...
      fmenu_data = Flipdish.flipdish_get_menu_by_id(accessToken=accessToken, fMenuId=fMenuId)
      if not fmenu_data:
        print("Error occured while gettig menu from flipdish")
        return False
      fMenuSections = fmenu_data.get("Data").get("MenuSections")


      # loop over the categories
      for menucategory in menucategories:
        categoryId = menucategory.get("categoryid")
        fCategoryPublicId = menucategory.get("platformcategoryid")

        # get item public id from itemmappings table
        # get addon-public-id from itemmappings table
        cursor.execute("""SELECT * FROM itemmappings 
          WHERE merchantid=%s AND categoryid=%s AND itemid=%s AND platformtype=%s AND addonid is NULL""", (merchant_obj["syncMerchantId"], categoryId, itemId, platformType)
        )
        item = cursor.fetchone()
        if not item:
          print("error: item-mappings data is not available in itemmappings table")
          continue
        fItemPublicId = item.get("platformitemid")
        
        cursor.execute("""SELECT * FROM itemmappings 
          WHERE merchantid=%s AND categoryid=%s AND itemid=%s AND addonid=%s AND platformtype=%s AND addonoptionid is NULL""", (merchant_obj["syncMerchantId"], categoryId, itemId, addonId, platformType)
        )
        item = cursor.fetchone()
        if not item:
          print("error: addon mappings data is not available in itemmappings table")
          continue
        fAddonPublicId = item.get("platformitemid")
        print("fAddonPublicId: ", str(fAddonPublicId))
        

        # get fCategoryId from Flipdish Menu
        # get fItemId from Flipdish Menu
        # get fAddonId from Flipdish Menu
        print("compare menu-category public-id with flipdish categories...")
        fCategoryId = None
        fMenuSectionItems = None
        for section in fMenuSections:
          if section.get("PublicId") == fCategoryPublicId:
            fCategoryId = section.get("MenuSectionId")
            fMenuSectionItems = section.get("MenuItems")
            break
        if fCategoryId is None:
          print("error. category public id not found in flipdish menu-section")
          continue
        
        print("compare menu-category-item public-id with flipdish category-items...")
        fItemId = None
        fMenuItemOptionSets = None
        for item in fMenuSectionItems:
          if item.get("PublicId") == fItemPublicId:
            fItemId = item.get("MenuItemId")
            fMenuItemOptionSets = item.get("MenuItemOptionSets")
            break
        if fItemId is None:
          print("error, item public-id not found in flipdish menu-category-item")
          continue

        print("compare item-addons public-id with flipdish item-addons...")
        fAddonId = None
        for addon in fMenuItemOptionSets:
          if addon.get("PublicId") == fAddonPublicId:
            fAddonId = addon.get("MenuItemOptionSetId")
            break
        if fAddonId is None:
          print("error, item-addon public-id not found in flipdish menu-category-item-addons")
          continue
        print("fAddonId: ", str(fAddonId))
        
        # delete item-addon data from itemmappings table
        # delete item-addon from flipdish
        resp = ItemMappings.delete_itemmappings(merchantId=merchant_obj["syncMerchantId"], categoryId=categoryId, itemId=itemId, addonId=addonId, platformType=platformType)

        response = Flipdish.flipdish_delete_product_addon(
          accessToken=accessToken,
          fMenuId=fMenuId,
          fCategoryId=fCategoryId,
          fItemId=fItemId,
          fAddonId=fAddonId
        )
        if not response:
          print("error while deleting item-addon from flipdish!")
          continue
        
        print("End category-item")

      return True
    except Exception as e:
      print("Error: ", str(e))
      return False
  

  #########################################################
  #########################################################
  #########################################################


  @classmethod
  def update_item(cls, merchant_obj, itemId, platform=None):
    try:
      print("update_item -> flipdish...")

      connection, cursor = get_db_connection()

      platformType = platform.get("platformtype")
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

      # Get Flipdish-Categories that contains the specified item
      # multiple categories can be returned
      print(f"get flipdish categories which is contains the item <{itemId}>...")
      cursor.execute("""SELECT * FROM menucategories 
        WHERE merchantid=%s AND menuid=%s AND platformtype=%s AND categoryid IN (
	        SELECT categoryid FROM itemmappings WHERE addonid is NULL AND merchantid=%s AND menuid=%s AND itemid=%s AND platformtype=%s
        )""", (merchant_obj["syncMerchantId"], menuId, platformType, merchant_obj["syncMerchantId"], menuId, itemId, platformType)
      )
      menucategories = cursor.fetchall()
      print(menucategories)
      
      # get all addon-options-mappings having addonoptionid=itemId from itemmappings table...
      cursor.execute("SELECT * FROM itemmappings WHERE merchantid=%s AND addonoptionid=%s AND platformtype=%s", (merchant_obj["syncMerchantId"], itemId, platformType))
      opt_mappings = cursor.fetchall()
      print(opt_mappings)

      # check if item is assigned to flipdish either as product or addon-option
      if not (len(menucategories) or len(opt_mappings)):
        print("item/addon-options is not assigned to flipdish. exiting...")
        return True

      # get item details by id from db
      print("get item details by id from db...")
      item_details = Items.get_item_by_id(itemId=itemId)
      print(item_details)
      itemPrice = None
      if type(item_details["itemPriceMappings"]) is list:
        for r in item_details["itemPriceMappings"]:
          if r.get("platformType") == platformType:
            itemPrice = r.get("platformItemPrice")
            break
      if itemPrice is None:
        itemPrice = item_details["itemUnitPrice"]

      # get flipdish menu by id
      print("Get flipdish menu by id...")
      fmenu_data = Flipdish.flipdish_get_menu_by_id(accessToken=accessToken, fMenuId=fMenuId)
      if not fmenu_data:
        print("Error occured while gettig menu from flipdish")
        return False
      fMenuSections = fmenu_data.get("Data").get("MenuSections")


      # loop over the categories
      for menucategory in menucategories:
        categoryId = menucategory.get("categoryid")
        fCategoryPublicId = menucategory.get("platformcategoryid")

        # get item public id from itemmappings table
        cursor.execute("""SELECT * FROM itemmappings 
          WHERE merchantid=%s AND categoryid=%s AND itemid=%s AND platformtype=%s AND addonid is NULL""", (merchant_obj["syncMerchantId"], categoryId, itemId, platformType)
        )
        item = cursor.fetchone()
        if not item:
          print("error: item-mappings data is not available in itemmappings table")
          continue
        fItemPublicId = item.get("platformitemid")

        # get fCategoryId from Flipdish Menu
        # get fItemId from Flipdish Menu
        print("compare menu-category public-id with flipdish categories...")
        fCategoryId = None
        fMenuSectionItems = None
        for section in fMenuSections:
          if section.get("PublicId") == fCategoryPublicId:
            fCategoryId = section.get("MenuSectionId")
            fMenuSectionItems = section.get("MenuItems")
            break
        if fCategoryId is None:
          print("error. category public id not found in flipdish menu-section")
          continue
        print("fCategoryId: ", str(fCategoryId))
        
        print("compare menu-category-item public-id with flipdish category-items...")
        fItemId = None
        for item in fMenuSectionItems:
          if item.get("PublicId") == fItemPublicId:
            fItemId = item.get("MenuItemId")
            break
        if fItemId is None:
          print("error, item public-id not found in flipdish menu-category-item")
          continue
        print("fItemId: ", str(fItemId))

        # update flipdish-item details
        response = Flipdish.flipdish_update_category_product(
          accessToken=accessToken,
          fMenuId=fMenuId,
          fCategoryId=fCategoryId,
          fItemId=fItemId,
          name=item_details["itemName"],
          description=item_details["itemDescription"],
          price=itemPrice,
          status=item_details["itemStatus"]
        )
        if not response:
          print("error while updating product details on flipdish!")
          continue

        print("End category-item")

      print("\n updating addonoptions...")

      #
      # Now we will update if item is addonOption
      #
      # get addonoptionid=itemId from itemmappings
      # get addonoption-public-id, addon-public-id, item-public-id, category-public-id, menu-public-id
      # 
      # loop over addon-options-mappings
      for opt_mapping in opt_mappings:
        print("get (addonoption, addon, item, category) public-ids from itemmappings and menumappings tables...")

        fOptionPublicId = opt_mapping.get("platformitemid")

        cursor.execute("""SELECT platformitemid FROM itemmappings 
          WHERE merchantid=%s AND menuid=%s AND categoryid=%s AND itemid=%s AND addonid=%s AND addonoptionid is NULL AND platformtype=%s""",
          (merchant_obj["syncMerchantId"], opt_mapping["menuid"], opt_mapping["categoryid"], opt_mapping["itemid"], opt_mapping["addonid"], platformType))
        row = cursor.fetchone()
        fAddonPublicId = row["platformitemid"]

        cursor.execute("""SELECT platformitemid FROM itemmappings 
          WHERE merchantid=%s AND menuid=%s AND categoryid=%s AND itemid=%s AND addonid is NULL AND platformtype=%s""",
          (merchant_obj["syncMerchantId"], opt_mapping["menuid"], opt_mapping["categoryid"], opt_mapping["itemid"], platformType))
        row = cursor.fetchone()
        fItemPublicId = row["platformitemid"]

        cursor.execute("""SELECT platformcategoryid FROM menucategories 
          WHERE merchantid=%s AND menuid=%s AND categoryid=%s AND platformtype=%s""",
          (merchant_obj["syncMerchantId"], opt_mapping["menuid"], opt_mapping["categoryid"], platformType))
        row = cursor.fetchone()
        fCategoryPublicId = row["platformcategoryid"]

        print("fOptionPublicId: ", fOptionPublicId)
        print("fAddonPublicId: ", fAddonPublicId)
        print("fItemPublicId: ", fItemPublicId)
        print("fCategoryPublicId: ", fCategoryPublicId)
        

        # get flipdish ids based on the public ids
        print("get flipdish ids based on the public-ids from db...")

        print("compare menu-category public-id with flipdish categories...")
        fCategoryId = None
        fMenuSectionItems = None
        for section in fMenuSections:
          if section.get("PublicId") == fCategoryPublicId:
            fCategoryId = section.get("MenuSectionId")
            fMenuSectionItems = section.get("MenuItems")
            break
        if fCategoryId is None:
          print("error. category public id not found in flipdish menu-section")
          continue
        print("fCategoryId: ", str(fCategoryId))
        
        print("compare menu-category-item public-id with flipdish category-items...")
        fItemId = None
        fMenuItemOptionSets = None
        for item in fMenuSectionItems:
          if item.get("PublicId") == fItemPublicId:
            fItemId = item.get("MenuItemId")
            fMenuItemOptionSets = item.get("MenuItemOptionSets")
            break
        if fItemId is None:
          print("error, item public-id not found in flipdish menu-category-item")
          continue
        print("fItemId: ", str(fItemId))

        print("compare item-addons public-id with flipdish item-addons...")
        fAddonId = None
        fMenuItemOptionSetItems = None
        for addon in fMenuItemOptionSets:
          if addon.get("PublicId") == fAddonPublicId:
            fAddonId = addon.get("MenuItemOptionSetId")
            fMenuItemOptionSetItems = addon.get("MenuItemOptionSetItems")
            break
        if fAddonId is None:
          print("error, item-addon public-id not found in flipdish menu-category-item-addons")
          continue
        print("fAddonId: ", str(fAddonId))

        print("compare addon-option public-id with flipdish addon-options...")
        fOptionId = None
        for opt in fMenuItemOptionSetItems:
          if opt.get("PublicId") == fOptionPublicId:
            fOptionId = opt.get("MenuItemOptionSetItemId")
            break
        if fOptionId is None:
          print("error, addon-option public-id not found in flipdish menu-category-item-addon-options")
          continue
        print("fOptionId: ", str(fOptionId))

        # update addonoption data on flipdish
        print("update addon option on flipdish...")
        response = Flipdish.flipdish_update_addon_option(
          accessToken=accessToken,
          fMenuId=fMenuId,
          fCategoryId=fCategoryId,
          fItemId=fItemId,
          fAddonId=fAddonId,
          fOptionId=fOptionId,
          name=item_details["itemName"],
          price=itemPrice,
          status=item_details["itemStatus"]
        )
        if not response:
          print("error while updating addon-option details on flipdish!")
          continue
        
        print("--- END Addon-Option ---")

      return True
    except Exception as e:
      print("Error: ", str(e))
      return False
  

  #########################################################
  #########################################################
  #########################################################
  

  @classmethod
  def delete_item(cls, merchant_obj, itemId, platform=None):
    try:
      print("Start delete_item -> flipdish...")
      connection, cursor = get_db_connection()

      platformType = platform.get("platformtype")
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

      # Get Flipdish-Categories that contains the specified item
      # multiple categories can be returned
      print(f"get flipdish categories which is contains the item <{itemId}>...")
      cursor.execute("""SELECT * FROM menucategories 
        WHERE merchantid=%s AND menuid=%s AND platformtype=%s AND categoryid IN (
	        SELECT categoryid FROM itemmappings WHERE addonid is NULL AND merchantid=%s AND menuid=%s AND itemid=%s AND platformtype=%s
        )""", (merchant_obj["syncMerchantId"], menuId, platformType, merchant_obj["syncMerchantId"], menuId, itemId, platformType)
      )
      menucategories = cursor.fetchall()
      print(menucategories)

      print("get all addon-options-mappings having addonoptionid=itemId from itemmappings table...")
      cursor.execute("SELECT * FROM itemmappings WHERE merchantid=%s AND addonoptionid=%s AND platformtype=%s", (merchant_obj["syncMerchantId"], itemId, platformType))
      opt_mappings = cursor.fetchall()

      # check if item is assigned to flipdish either as product or addon-option
      if not (len(menucategories) or len(opt_mappings)):
        print("item/addon-options is not assigned to flipdish. exiting...")
        return True

      # get flipdish menu by id
      print("Get flipdish menu by id...")
      fmenu_data = Flipdish.flipdish_get_menu_by_id(accessToken=accessToken, fMenuId=fMenuId)
      if not fmenu_data:
        print("Error occured while gettig menu from flipdish")
        return False
      fMenuSections = fmenu_data.get("Data").get("MenuSections")

      # loop over the categories
      for menucategory in menucategories:
        categoryId = menucategory.get("categoryid")
        fCategoryPublicId = menucategory.get("platformcategoryid")

        # get item public-id from itemmappings table
        cursor.execute("""SELECT * FROM itemmappings 
          WHERE merchantid=%s AND categoryid=%s AND itemid=%s AND platformtype=%s AND addonid is NULL""", (merchant_obj["syncMerchantId"], categoryId, itemId, platformType)
        )
        item = cursor.fetchone()
        if not item:
          print("error: item-mappings data is not available in itemmappings table")
          continue
        fItemPublicId = item.get("platformitemid")

        # get fCategoryId from Flipdish Menu
        # get fItemId from Flipdish Menu
        print("compare menu-category public-id with flipdish categories...")
        fCategoryId = None
        fMenuSectionItems = None
        for section in fMenuSections:
          if section.get("PublicId") == fCategoryPublicId:
            fCategoryId = section.get("MenuSectionId")
            fMenuSectionItems = section.get("MenuItems")
            break
        if fCategoryId is None:
          print("error. category public id not found in flipdish menu-section")
          continue
        print("fCategoryId: ", str(fCategoryId))
        
        print("compare menu-category-item public-id with flipdish category-items...")
        fItemId = None
        for item in fMenuSectionItems:
          if item.get("PublicId") == fItemPublicId:
            fItemId = item.get("MenuItemId")
            break
        if fItemId is None:
          print("error, item public-id not found in flipdish menu-category-item")
          continue
        print("fItemId: ", str(fItemId))

        # delete item from itemmappings
        # delete item from flipdish
        cursor.execute("""DELETE FROM itemmappings 
          WHERE merchantid=%s AND categoryid=%s AND itemid=%s AND platformtype=%s""", (merchant_obj["syncMerchantId"], categoryId, itemId, platformType))
        connection.commit()

        response = Flipdish.flipdish_delete_category_product(
          accessToken=accessToken,
          fMenuId=fMenuId,
          fCategoryId=fCategoryId,
          fItemId=fItemId
        )
        if not response:
          print("error while deleting category-product from flipdish!")
          continue

        print("End category-item")

      print("\n deleting addonoptions...")

      #
      # Now we will delete item-addon-options
      #
      # get addonoptionid=itemId from itemmappings
      # get addonoption-public-id, addon-public-id, item-public-id, category-public-id, menu-public-id
      #
      # loop over addon-options-mappings
      for opt_mapping in opt_mappings:
        print("get (addonoption, addon, item, category) public-ids from itemmappings and menumappings tables...")

        fOptionPublicId = opt_mapping.get("platformitemid")

        cursor.execute("""SELECT platformitemid FROM itemmappings 
          WHERE merchantid=%s AND menuid=%s AND categoryid=%s AND itemid=%s AND addonid=%s AND addonoptionid is NULL AND platformtype=%s""",
          (merchant_obj["syncMerchantId"], opt_mapping["menuid"], opt_mapping["categoryid"], opt_mapping["itemid"], opt_mapping["addonid"], platformType))
        row = cursor.fetchone()
        fAddonPublicId = row["platformitemid"]

        cursor.execute("""SELECT platformitemid FROM itemmappings 
          WHERE merchantid=%s AND menuid=%s AND categoryid=%s AND itemid=%s AND addonid is NULL AND platformtype=%s""",
          (merchant_obj["syncMerchantId"], opt_mapping["menuid"], opt_mapping["categoryid"], opt_mapping["itemid"], platformType))
        row = cursor.fetchone()
        fItemPublicId = row["platformitemid"]

        cursor.execute("""SELECT platformcategoryid FROM menucategories 
          WHERE merchantid=%s AND menuid=%s AND categoryid=%s AND platformtype=%s""",
          (merchant_obj["syncMerchantId"], opt_mapping["menuid"], opt_mapping["categoryid"], platformType))
        row = cursor.fetchone()
        fCategoryPublicId = row["platformcategoryid"]

        print("fOptionPublicId: ", fOptionPublicId)
        print("fAddonPublicId: ", fAddonPublicId)
        print("fItemPublicId: ", fItemPublicId)
        print("fCategoryPublicId: ", fCategoryPublicId)
        

        # get flipdish ids based on the public ids
        print("get flipdish ids based on the public-ids from db...")

        print("compare menu-category public-id with flipdish categories...")
        fCategoryId = None
        fMenuSectionItems = None
        for section in fMenuSections:
          if section.get("PublicId") == fCategoryPublicId:
            fCategoryId = section.get("MenuSectionId")
            fMenuSectionItems = section.get("MenuItems")
            break
        if fCategoryId is None:
          print("error. category public id not found in flipdish menu-section")
          continue
        print("fCategoryId: ", str(fCategoryId))
        
        print("compare menu-category-item public-id with flipdish category-items...")
        fItemId = None
        fMenuItemOptionSets = None
        for item in fMenuSectionItems:
          if item.get("PublicId") == fItemPublicId:
            fItemId = item.get("MenuItemId")
            fMenuItemOptionSets = item.get("MenuItemOptionSets")
            break
        if fItemId is None:
          print("error, item public-id not found in flipdish menu-category-item")
          continue
        print("fItemId: ", str(fItemId))


        print("compare item-addons public-id with flipdish item-addons...")
        fAddonId = None
        fMenuItemOptionSetItems = None
        for addon in fMenuItemOptionSets:
          if addon.get("PublicId") == fAddonPublicId:
            fAddonId = addon.get("MenuItemOptionSetId")
            fMenuItemOptionSetItems = addon.get("MenuItemOptionSetItems")
            break
        if fAddonId is None:
          print("error, item-addon public-id not found in flipdish menu-category-item-addons")
          continue
        print("fAddonId: ", str(fAddonId))

        print("compare addon-option public-id with flipdish addon-options...")
        fOptionId = None
        for opt in fMenuItemOptionSetItems:
          if opt.get("PublicId") == fOptionPublicId:
            fOptionId = opt.get("MenuItemOptionSetItemId")
            break
        if fOptionId is None:
          print("error, addon-option public-id not found in flipdish menu-category-item-addon-options")
          continue
        print("fOptionId: ", str(fOptionId))

        # delete addonoption from itemmappings
        # delete addonoption from flipdish
        cursor.execute("""DELETE FROM itemmappings 
          WHERE platformitemid=%s""", (fOptionPublicId))
        connection.commit()

        response = Flipdish.flipdish_delete_addon_option(
          accessToken=accessToken,
          fMenuId=fMenuId,
          fCategoryId=fCategoryId,
          fItemId=fItemId,
          fAddonId=fAddonId,
          fOptionId=fOptionId
        )
        if not response:
          print("error while deleting addon-option from flipdish!")
          continue
        
        print("--- END Addon-Option ---")

      print("--- End ---\n")
      return True
    except Exception as e:
      print("Error: ", str(e))
      return False


  #########################################################
  #########################################################
  #########################################################


  @classmethod
  def update_item_image(cls, merchant_obj, itemId, platform=None, action="update"):
    try:
      print("update/delete item_image -> flipdish...")

      connection, cursor = get_db_connection()

      platformType = platform.get("platformtype")
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

      # Get Flipdish-Categories that contains the specified item
      # multiple categories can be returned
      print(f"get flipdish categories which is contains the item <{itemId}>...")
      cursor.execute("""SELECT * FROM menucategories 
        WHERE merchantid=%s AND menuid=%s AND platformtype=%s AND categoryid IN (
	        SELECT categoryid FROM itemmappings WHERE addonid is NULL AND merchantid=%s AND menuid=%s AND itemid=%s AND platformtype=%s
        )""", (merchant_obj["syncMerchantId"], menuId, platformType, merchant_obj["syncMerchantId"], menuId, itemId, platformType)
      )
      menucategories = cursor.fetchall()
      print(menucategories)

      # check if item is assigned to flipdish either as product or addon-option
      if not len(menucategories):
        print("item is not assigned to flipdish. exiting...")
        return True

      # get item details by id from db
      print("get item details by id from db...")
      item_details = Items.get_item_by_id(itemId=itemId)
      print(item_details)
      itemPrice = None
      if type(item_details["itemPriceMappings"]) is list:
        for r in item_details["itemPriceMappings"]:
          if r.get("platformType") == platformType:
            itemPrice = r.get("platformItemPrice")
            break
      if itemPrice is None:
        itemPrice = item_details["itemUnitPrice"]
      print("item price: ", str(itemPrice))


      # get flipdish menu by id
      print("Get flipdish menu by id...")
      fmenu_data = Flipdish.flipdish_get_menu_by_id(accessToken=accessToken, fMenuId=fMenuId)
      if not fmenu_data:
        print("Error occured while gettig menu from flipdish")
        return False
      fMenuSections = fmenu_data.get("Data").get("MenuSections")


      # loop over the categories
      for menucategory in menucategories:
        categoryId = menucategory.get("categoryid")
        fCategoryPublicId = menucategory.get("platformcategoryid")

        # get item public id from itemmappings table
        cursor.execute("""SELECT * FROM itemmappings 
          WHERE merchantid=%s AND categoryid=%s AND itemid=%s AND platformtype=%s AND addonid is NULL""", (merchant_obj["syncMerchantId"], categoryId, itemId, platformType)
        )
        item = cursor.fetchone()
        if not item:
          print("error: item-mappings data is not available in itemmappings table")
          continue
        fItemPublicId = item.get("platformitemid")

        # get fCategoryId from Flipdish Menu
        # get fItemId from Flipdish Menu
        print("compare menu-category public-id with flipdish categories...")
        fCategoryId = None
        fMenuSectionItems = None
        for section in fMenuSections:
          if section.get("PublicId") == fCategoryPublicId:
            fCategoryId = section.get("MenuSectionId")
            fMenuSectionItems = section.get("MenuItems")
            break
        if fCategoryId is None:
          print("error. category public id not found in flipdish menu-section")
          continue
        print("fCategoryId: ", str(fCategoryId))
        
        print("compare menu-category-item public-id with flipdish category-items...")
        fItemId = None
        for item in fMenuSectionItems:
          if item.get("PublicId") == fItemPublicId:
            fItemId = item.get("MenuItemId")
            break
        if fItemId is None:
          print("error, item public-id not found in flipdish menu-category-item")
          continue
        print("fItemId: ", str(fItemId))
        

        # update or delete flipdish-item_image
        if action == "update":
          print("updating flipdish item image...")
          response = Flipdish.flipdish_update_category_product(
            accessToken=accessToken,
            fMenuId=fMenuId,
            fCategoryId=fCategoryId,
            fItemId=fItemId,
            name=item_details["itemName"],
            description=item_details["itemDescription"],
            price=itemPrice,
            status=item_details["itemStatus"],
            imageUrl=item_details["imageUrl"]
          )
          if not response:
            print("error while updating product image on flipdish!")
            continue
        elif action == "delete":
          print("deleting flipdish tem image...")
          response = Flipdish.flipdish_delete_product_image(
            accessToken=accessToken,
            fMenuId=fMenuId,
            fCategoryId=fCategoryId,
            fItemId=fItemId
          )
          if not response:
            print("error while deleting product image on flipdish!")
            continue

        print("End category-item")

      print("--- End ---\n")
      return True
    except Exception as e:
      print("Error: ", str(e))
      return False