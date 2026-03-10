

# local imports
import config
from models.Items import Items
from models.ItemMappings import ItemMappings
from utilities.helpers import get_db_connection
from models.flipdish.Flipdish import Flipdish
from models.Addons import Addons
from models.MenuMappings import MenuMappings


class FlipdishAddonSync():

  #########################################################
  #########################################################
  #########################################################

  @classmethod
  def assign_addonoption_to_addon(cls, merchant_obj, addonId, itemId, platform=None):
    try:
      print("assigning_addon-option_to_addon -> flipdish...")

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

      # get addon by addonid from itemmappings
      cursor.execute("""SELECT * FROM itemmappings 
        WHERE merchantid=%s AND menuid=%s AND addonid=%s AND platformtype=%s and addonoptionid is NULL""", (merchant_obj["syncMerchantId"], menuId, addonId, platformType))
      addon_mappings = cursor.fetchall()
      if not len(addon_mappings):
        print("addon is not assigned to flipdish items. exiting...")
        return True

      # get flipdish menu by id
      print("Get flipdish menu by id...")
      fmenu_data = Flipdish.flipdish_get_menu_by_id(accessToken=accessToken, fMenuId=fMenuId)
      if not fmenu_data:
        print("Error occured while gettig menu from flipdish")
        return False
      fMenuSections = fmenu_data.get("Data").get("MenuSections")

      
      # loop over addon_mappings
      for addon_mapping in addon_mappings:

        # check if addon_mapping menuId is same as menuId that is assigned to flipdish
        if addon_mapping["menuid"] != menuId:
          print("addon data belongs to some other menu. Skipping...")
          continue

        fAddonPublicId = addon_mapping.get("platformitemid")

        print("get product public id...")
        cursor.execute("""SELECT platformitemid FROM itemmappings 
          WHERE merchantid=%s AND menuid=%s AND categoryid=%s AND itemid=%s AND addonid is NULL AND platformtype=%s""",
          (merchant_obj["syncMerchantId"], addon_mapping["menuid"], addon_mapping["categoryid"], addon_mapping["itemid"], platformType))
        row = cursor.fetchone()
        fItemPublicId = row["platformitemid"]

        print("get category public id...")
        cursor.execute("""SELECT platformcategoryid FROM menucategories 
          WHERE merchantid=%s AND menuid=%s AND categoryid=%s AND platformtype=%s""",
          (merchant_obj["syncMerchantId"], addon_mapping["menuid"], addon_mapping["categoryid"], platformType))
        row = cursor.fetchone()
        fCategoryPublicId = row["platformcategoryid"]

        print("fAddonPublicId: ", fAddonPublicId)
        print("fItemPublicId: ", fItemPublicId)
        print("fCategoryPublicId: ", fCategoryPublicId)


        # get flipdish ids based on the public ids

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
        for addon in fMenuItemOptionSets:
          if addon.get("PublicId") == fAddonPublicId:
            fAddonId = addon.get("MenuItemOptionSetId")
            break
        if fAddonId is None:
          print("error, item-addon public-id not found in flipdish menu-category-item-addons")
          continue
        print("fAddonId: ", str(fAddonId))

        
        # get addon-option details from db
        # assign option to addon in flipdish
        # store addon data in itemmappings table
        print("get item(here, addon-option) details by id from db...")
        item_details = Items.get_item_by_id(itemId=itemId)
        itemPrice = None
        if type(item_details["itemPriceMappings"]) is list:
          for r in item_details["itemPriceMappings"]:
            if r.get("platformType") == platformType:
              itemPrice = r.get("platformItemPrice")
              break
        if itemPrice is None:
          itemPrice = item_details["itemUnitPrice"]

        # posting addon-option to flipdish...
        foption_data = Flipdish.flipdish_create_addon_option(
          accessToken=accessToken, 
          fMenuId=fMenuId, 
          fCategoryId=fCategoryId,
          fItemId=fItemId, 
          fAddonId=fAddonId, 
          name=item_details["itemName"], 
          price=itemPrice  
        )
        if not foption_data:
          print("error while posting addon-option to flipdish!")
          continue
        fOptionId = foption_data.get("Data").get("MenuItemOptionSetItemId")
        fOptionPublicId = foption_data.get("Data").get("PublicId")

        print("storing addon-option public-id in itemmappings...")
        resp = ItemMappings.post_itemmappings(
          merchantId=merchant_obj["syncMerchantId"], 
          menuId=menuId,
          itemId=addon_mapping["itemid"],
          itemType=2,  # 2 for addon-option itemType
          platformType=platformType,
          platformItemId=fOptionPublicId,
          categoryId=addon_mapping["categoryid"],
          addonId=addonId,
          addonOptionId=itemId
        )
        if not resp:
          print("error while storing addon-option-public-id in itemmappings!!!")

        print("-End Addon-")

      print("--- End ---\n")
      return True
    except Exception as e:
      print("Error: ", str(e))
      return False
  

  #########################################################
  #########################################################
  #########################################################

  @classmethod
  def unassign_addonoption_to_addon(cls, merchant_obj, addonId, itemId, platform=None):
    try:
      print("unassigning_addon-option_to_addon -> flipdish...")

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

      # get all addon-options-mappings by addonid and addonoptionid from itemmappings table...
      cursor.execute("SELECT * FROM itemmappings WHERE merchantid=%s AND menuid=%s AND addonid=%s AND addonoptionid=%s AND platformtype=%s", (merchant_obj["syncMerchantId"], menuId, addonId, itemId, platformType))
      opt_mappings = cursor.fetchall()
      if not len(opt_mappings):
        print("addon option is not assigned to addons in flipdish. exiting...")
        return True

      # get flipdish menu by id...
      fmenu_data = Flipdish.flipdish_get_menu_by_id(accessToken=accessToken, fMenuId=fMenuId)
      if not fmenu_data:
        print("Error occured while gettig menu from flipdish")
        return False
      fMenuSections = fmenu_data.get("Data").get("MenuSections")

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

      return True
    except Exception as e:
      print("Error: ", str(e))
      return False
  

  #########################################################
  #########################################################
  #########################################################

  @classmethod
  def update_addon(cls, merchant_obj, addonId, platform=None):
    try:
      print("update_addon -> flipdish...")
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

      # get addon by addonid from itemmappings
      cursor.execute("""SELECT * FROM itemmappings 
        WHERE merchantid=%s AND menuid=%s AND addonid=%s AND platformtype=%s and addonoptionid is NULL""", (merchant_obj["syncMerchantId"], menuId, addonId, platformType))
      addon_mappings = cursor.fetchall()
      if not len(addon_mappings):
        print("addon is not assigned to any product in flipdish. exiting...")
        return True

      # get flipdish menu by id
      print("Get flipdish menu by id...")
      fmenu_data = Flipdish.flipdish_get_menu_by_id(accessToken=accessToken, fMenuId=fMenuId)
      if not fmenu_data:
        print("Error occured while gettig menu from flipdish")
        return False
      fMenuSections = fmenu_data.get("Data").get("MenuSections")


      # loop over addon mappings...
      for addon_mapping in addon_mappings:

        # check if addon_mapping menuId is same as menuId that is assigned to flipdish
        if addon_mapping["menuid"] != menuId:
          print("addon data belongs to some other menu. Skipping...")
          continue

        fAddonPublicId = addon_mapping.get("platformitemid")

        # get product public id...
        cursor.execute("""SELECT platformitemid FROM itemmappings 
          WHERE merchantid=%s AND menuid=%s AND categoryid=%s AND itemid=%s AND addonid is NULL AND platformtype=%s""",
          (merchant_obj["syncMerchantId"], addon_mapping["menuid"], addon_mapping["categoryid"], addon_mapping["itemid"], platformType))
        row = cursor.fetchone()
        fItemPublicId = row["platformitemid"]

        # get category public id...
        cursor.execute("""SELECT platformcategoryid FROM menucategories 
          WHERE merchantid=%s AND menuid=%s AND categoryid=%s AND platformtype=%s""",
          (merchant_obj["syncMerchantId"], addon_mapping["menuid"], addon_mapping["categoryid"], platformType))
        row = cursor.fetchone()
        fCategoryPublicId = row["platformcategoryid"]

        print("fAddonPublicId: ", fAddonPublicId)
        print("fItemPublicId: ", fItemPublicId)
        print("fCategoryPublicId: ", fCategoryPublicId)


        # get flipdish ids based on the public ids

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
        for addon in fMenuItemOptionSets:
          if addon.get("PublicId") == fAddonPublicId:
            fAddonId = addon.get("MenuItemOptionSetId")
            break
        if fAddonId is None:
          print("error, item-addon public-id not found in flipdish menu-category-item-addons")
          continue
        print("fAddonId: ", str(fAddonId))

        
        # get addon details by id from db
        # update addon on flipdish
        addon = Addons.get_addon_by_id(addonId)

        print("update addon on flipdish...")
        response = Flipdish.flipdish_update_product_addon(
          accessToken=accessToken,
          fMenuId=fMenuId,
          fCategoryId=fCategoryId,
          fItemId=fItemId,
          fAddonId=fAddonId,
          name=addon["addonname"]
        )
        if not response:
          print("error while updating addon on flipdish!")
          continue

        print("-End Addon Mapping-")

      return True
    except Exception as e:
      print("Error: ", str(e))
      return False

  #########################################################
  #########################################################
  #########################################################
  
  @classmethod
  def delete_addon(cls, merchant_obj, addonId, platform=None):
    try:
      print("delete_addon -> flipdish...")
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

      # get addon by addonid from itemmappings
      cursor.execute("""SELECT * FROM itemmappings 
        WHERE merchantid=%s AND menuid=%s AND addonid=%s AND platformtype=%s and addonoptionid is NULL""", (merchant_obj["syncMerchantId"], menuId, addonId, platformType))
      addon_mappings = cursor.fetchall()
      if not len(addon_mappings):
        print("addon is not assigned to any product in flipdish. exiting...")
        return True

      # get flipdish menu by id
      fmenu_data = Flipdish.flipdish_get_menu_by_id(accessToken=accessToken, fMenuId=fMenuId)
      if not fmenu_data:
        print("Error occured while gettig menu from flipdish")
        return False
      fMenuSections = fmenu_data.get("Data").get("MenuSections")


      # loop over addon mappings...
      for addon_mapping in addon_mappings:

        # check if addon_mapping menuId is same as menuId that is assigned to flipdish
        if addon_mapping["menuid"] != menuId:
          print("addon data belongs to some other menu. Skipping...")
          continue

        fAddonPublicId = addon_mapping.get("platformitemid")

        print("get product public id...")
        cursor.execute("""SELECT platformitemid FROM itemmappings 
          WHERE merchantid=%s AND menuid=%s AND categoryid=%s AND itemid=%s AND addonid is NULL AND platformtype=%s""",
          (merchant_obj["syncMerchantId"], addon_mapping["menuid"], addon_mapping["categoryid"], addon_mapping["itemid"], platformType))
        row = cursor.fetchone()
        fItemPublicId = row["platformitemid"]

        print("get category public id...")
        cursor.execute("""SELECT platformcategoryid FROM menucategories 
          WHERE merchantid=%s AND menuid=%s AND categoryid=%s AND platformtype=%s""",
          (merchant_obj["syncMerchantId"], addon_mapping["menuid"], addon_mapping["categoryid"], platformType))
        row = cursor.fetchone()
        fCategoryPublicId = row["platformcategoryid"]

        print("fAddonPublicId: ", fAddonPublicId)
        print("fItemPublicId: ", fItemPublicId)
        print("fCategoryPublicId: ", fCategoryPublicId)


        # get flipdish ids based on the public ids

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
        for addon in fMenuItemOptionSets:
          if addon.get("PublicId") == fAddonPublicId:
            fAddonId = addon.get("MenuItemOptionSetId")
            break
        if fAddonId is None:
          print("error, item-addon public-id not found in flipdish menu-category-item-addons")
          continue
        print("fAddonId: ", str(fAddonId))

        
        # delete addon details from itemmappings
        # delete addon from flipdish
        resp = ItemMappings.delete_itemmappings(
          merchantId=merchant_obj["syncMerchantId"],
          categoryId=addon_mapping["categoryid"], 
          itemId=addon_mapping["itemid"], 
          addonId=addonId, 
          platformType=platformType
        )

        response = Flipdish.flipdish_delete_product_addon(
          accessToken=accessToken,
          fMenuId=fMenuId,
          fCategoryId=fCategoryId,
          fItemId=fItemId,
          fAddonId=fAddonId
        )
        if not response:
          print("error while deleting addon from flipdish!")
          continue

        print("-End Addon Mapping-")

      print("--- End ---\n")
      return True
    except Exception as e:
      print("Error: ", str(e))
      return False