from app import app
import json
import datetime

# local imports
from models.Platforms import Platforms
from models.flipdish.Flipdish import Flipdish
from models.flipdish.FlipdishMenuSync import FlipdishMenuSync
from models.flipdish.FlipdishCategorySync import FlipdishCategorySync
from models.flipdish.FlipdishItemSync import FlipdishItemSync
from models.flipdish.FlipdishAddonSync import FlipdishAddonSync
from models.MenuCategories import MenuCategories
from models.ItemMappings import ItemMappings
from utilities.helpers import get_db_connection
from models.Merchants import Merchants
from models.VirtualMerchants import VirtualMerchants
from models.MenuMappings import MenuMappings


def menu_autosync_event(event, context):
  with app.app_context():
    print("---------------------- menu_autosync_event -------------------------")

    for record in event['Records']:
      subject = record.get("Sns").get("Subject")
      message = eval(record.get("Sns").get("Message"))
      print("subject: ", subject)

      # extract details from event
      merchantId = message.get("body").get("merchantId")
      menuId = message.get("body").get("menuId")
      categoryId = message.get("body").get("categoryId")
      userId = message.get("body").get("userId")

      # get merchant + all-virtual-merchant ids list
      vms = VirtualMerchants.get_virtual_merchant(merchantId=merchantId)
      merchants_list = [{
        "syncMerchantId": merchantId,
        "isVirtual": 0,
        "mainMerchantId": merchantId
      }]
      for vm in vms:
        merchants_list.append({
          "syncMerchantId": vm["id"],
          "isVirtual": 1,
          "mainMerchantId": merchantId
        })
      
      # loop over merchants + vmerchants list
      for merchant in merchants_list:    

        # check if merchant is connected and auto-synced to flipdish
        platform = Platforms.get_platform_by_merchantid_and_platformtype(merchantid=merchant["syncMerchantId"], platformtype=2)

        if not platform:
          continue

        if platform["synctype"] == 1:
          # autosync

          if subject == "menu.assign_category":
            resp = FlipdishMenuSync.assign_category(
              merchant_obj=merchant, 
              menuId=menuId, 
              categoryId=categoryId,
              platform=platform
            )
          elif subject == "menu.unassign_category":
            resp = FlipdishMenuSync.unassign_category(
              merchant_obj=merchant, 
              menuId=menuId, 
              categoryId=categoryId,
              platform=platform
            )
          elif subject == "menu.update":
            resp = FlipdishMenuSync.update_menu(
              merchant_obj=merchant, 
              menuId=menuId, 
              platform=platform
            )
          elif subject == "menu.delete":
            resp = FlipdishMenuSync.delete_menu(
              merchant_obj=merchant, 
              menuId=menuId,
              mappings=message.get("body").get("mappings"),
              platform=platform
            )
          else:
            print(f"unrecognized sns subject: {subject}")
        else:
          # manual sync
          # we have to delete data from our tables in case of unassign or delete menu
          # even if autosync is OFF, in order to keep tables clean
          print("Merchant synctype preference is manual")
          if subject == "menu.unassign_category":
            print(f"delete category <{categoryId}> entries from menucategories and itemmappings tables...")
            resp = MenuCategories.delete_menucategories(merchantId=merchant["syncMerchantId"], menuId=menuId, categoryId=categoryId, platformType=2)
            resp = ItemMappings.delete_itemmappings(merchantId=merchant["syncMerchantId"], categoryId=categoryId, platformType=2)


def category_autosync_event(event, context):
  with app.app_context():
    print("---------------------- category_autosync_event is triggered -------------------------")
    # print(event)
    for record in event['Records']:
      subject = record.get("Sns").get("Subject")
      message = eval(record.get("Sns").get("Message"))
      print("Subject: ", subject)

      # extract details from event
      merchantId = message.get("body").get("merchantId")
      categoryId = message.get("body").get("categoryId")
      itemId = message.get("body").get("itemId")
      userId = message.get("body").get("userId")

      # get merchant + all-virtual-merchant ids list
      vms = VirtualMerchants.get_virtual_merchant(merchantId=merchantId)
      merchants_list = [{
        "syncMerchantId": merchantId,
        "isVirtual": 0,
        "mainMerchantId": merchantId
      }]
      for vm in vms:
        merchants_list.append({
          "syncMerchantId": vm["id"],
          "isVirtual": 1,
          "mainMerchantId": merchantId
        })

      print(merchants_list)
      
      # loop over merchants + vmerchants list
      for merchant in merchants_list:

        # check if merchant is connected and auto-synced to flipdish
        platform = Platforms.get_platform_by_merchantid_and_platformtype(merchantid=merchant["syncMerchantId"], platformtype=2)

        if not platform:
          continue
          
        if platform["synctype"] == 1:
          # auto-sync
          if subject == "category.assign_item":
            resp = FlipdishCategorySync.assign_item_to_category(
              merchant_obj=merchant, 
              categoryId=categoryId,
              itemId=itemId,
              platform=platform
            )
          elif subject == "category.unassign_item":
            resp = FlipdishCategorySync.unassign_item_to_category(
              merchant_obj=merchant, 
              categoryId=categoryId,
              itemId=itemId,
              platform=platform
            )
          elif subject == "category.update":
            resp = FlipdishCategorySync.update_category(
              merchant_obj=merchant, 
              categoryId=categoryId,
              platform=platform
            )
          elif subject == "category.delete":
            resp = FlipdishCategorySync.delete_category(
              merchant_obj=merchant, 
              categoryId=categoryId,
              platform=platform
            )
          else:
            print(f"unrecognized sns subject: {subject}")
        else:
          # manual sync
          # we have to delete data from our tables in case of unassign or delete category
          # even if autosync is OFF, in order to keep tables clean
          print("Merchant synctype preference is manual")
          if subject == "category.unassign_item" or subject == "category.delete":

            connection, cursor = get_db_connection()
            mappings = MenuMappings.get_menumappings(merchantId=merchant["mainMerchantId"], platformType=2)
            menuMappingRow = list()
            if merchant["isVirtual"] == 1:
              for mapping in mappings:
                # check if menu is assigned to specified virtual-merchant-id, then append it to list
                cursor.execute("""SELECT * FROM vmerchantmenus WHERE vmerchantid = %s AND menuid = %s""", (merchant["syncMerchantId"], mapping["menuid"]))
                row = cursor.fetchone()
                if row:
                  menuMappingRow.append(mapping)
            else:
              for mapping in mappings:
                # check if menu is assigned to any virtual-merchant, then skip it
                cursor.execute("""SELECT * FROM vmerchantmenus WHERE merchantid = %s AND menuid = %s""", (merchant["mainMerchantId"], mapping["menuid"]))
                row = cursor.fetchone()
                if not row:
                  menuMappingRow.append(mapping)

            if len(menuMappingRow) == 1:
              menuMappingRow = menuMappingRow[0]
              menuId = menuMappingRow.get("menuid")
              mc_resp = MenuCategories.delete_menucategories(merchantId=merchant["syncMerchantId"], menuId=menuId, categoryId=categoryId, platformType=2)
              im_resp = ItemMappings.delete_itemmappings(merchantId=merchant["syncMerchantId"], menuId=menuId, categoryId=categoryId, platformType=2)


def item_autosync_event(event, context):
  with app.app_context():
    print("---------------------- item_autosync_event is triggered -------------------------")
    # print(event)
    for record in event['Records']:
      subject = record.get("Sns").get("Subject")
      message = eval(record.get("Sns").get("Message"))
      print("Subject: ", subject)

      # extract details from event
      merchantId = message.get("body").get("merchantId")
      itemId = message.get("body").get("itemId")
      addonId = message.get("body").get("addonId")
      userId = message.get("body").get("userId")

      # get merchant + all-virtual-merchant ids list
      vms = VirtualMerchants.get_virtual_merchant(merchantId=merchantId)
      merchants_list = [{
        "syncMerchantId": merchantId,
        "isVirtual": 0,
        "mainMerchantId": merchantId
      }]
      for vm in vms:
        merchants_list.append({
          "syncMerchantId": vm["id"],
          "isVirtual": 1,
          "mainMerchantId": merchantId
        })

      # loop over merchants + vmerchants list
      for merchant in merchants_list:

        # check if merchant is connected and auto-synced to flipdish
        platform = Platforms.get_platform_by_merchantid_and_platformtype(merchantid=merchant["syncMerchantId"], platformtype=2)

        if not platform:
          continue

        if platform["synctype"] == 1:
          # auto-sync
          if subject == "item.assign_addon":
            resp = FlipdishItemSync.assign_addon_to_item(
              merchant_obj=merchant,
              itemId=itemId,
              addonId=addonId,
              platform=platform
            )
          elif subject == "item.unassign_addon":
            resp = FlipdishItemSync.unassign_addon_to_item(
              merchant_obj=merchant,
              itemId=itemId,
              addonId=addonId,
              platform=platform
            )
          elif subject == "item.update" or subject == "item.status_change":
            resp = FlipdishItemSync.update_item(
              merchant_obj=merchant,
              itemId=itemId,
              platform=platform
            )
          elif subject == "item.delete":
            resp = FlipdishItemSync.delete_item(
              merchant_obj=merchant,
              itemId=itemId,
              platform=platform
            )
          elif subject == "item.image_update":
            resp = FlipdishItemSync.update_item_image(
              merchant_obj=merchant,
              itemId=itemId,
              platform=platform,
              action="update"
            )
          elif subject == "item.image_delete":
            resp = FlipdishItemSync.update_item_image(
              merchant_obj=merchant,
              itemId=itemId,
              platform=platform,
              action="delete"
            )
          else:
            print(f"unrecognized sns subject: {subject}")
        else:
          # manual sync
          print("Merchant synctype preference is manual")
          if subject == "item.unassign_addon":
            # delete data from itemmappings table
            resp = ItemMappings.delete_itemmappings(merchantId=merchant["syncMerchantId"], itemId=itemId, addonId=addonId, platformType=2) # we can delete by itemid and addonid from all menu-categories
          elif subject == "item.delete":
            print("delete item data from itemmappings table...")
            resp = ItemMappings.delete_itemmappings(merchantId=merchant["syncMerchantId"], itemId=itemId, platformType=2)
            resp = ItemMappings.delete_itemmappings(merchantId=merchant["syncMerchantId"], addonOptionId=itemId, platformType=2)


def addon_autosync_event(event, context):
  with app.app_context():
    print("---------------------- addon_autosync_event is triggered -------------------------")
    # print(event)
    for record in event['Records']:
      subject = record.get("Sns").get("Subject")
      message = eval(record.get("Sns").get("Message"))
      print("Subject: ", subject)

      # extract details from event
      merchantId = message.get("body").get("merchantId")
      addonId = message.get("body").get("addonId")
      itemId = message.get("body").get("itemId")
      userId = message.get("body").get("userId")

      # get merchant + all-virtual-merchant ids list
      vms = VirtualMerchants.get_virtual_merchant(merchantId=merchantId)
      merchants_list = [{
        "syncMerchantId": merchantId,
        "isVirtual": 0,
        "mainMerchantId": merchantId
      }]
      for vm in vms:
        merchants_list.append({
          "syncMerchantId": vm["id"],
          "isVirtual": 1,
          "mainMerchantId": merchantId
        })

      # loop over merchants + vmerchants list
      for merchant in merchants_list:

        # check if merchant is connected and auto-synced to flipdish
        platform = Platforms.get_platform_by_merchantid_and_platformtype(merchantid=merchant["syncMerchantId"], platformtype=2)

        if not platform:
          continue

        if platform["synctype"] == 1:
          # auto-sync
          if subject == "addon.assign_option":
            resp = FlipdishAddonSync.assign_addonoption_to_addon(
              merchant_obj=merchant,
              addonId=addonId,
              itemId=itemId,
              platform=platform
            )
          elif subject == "addon.unassign_option":
            resp = FlipdishAddonSync.unassign_addonoption_to_addon(
              merchant_obj=merchant,
              addonId=addonId,
              itemId=itemId,
              platform=platform
            )
          elif subject == "addon.update":
            resp = FlipdishAddonSync.update_addon(
              merchant_obj=merchant,
              addonId=addonId,
              platform=platform
            )
          elif subject == "addon.delete":
            resp = FlipdishAddonSync.delete_addon(
              merchant_obj=merchant,
              addonId=addonId,
              platform=platform
            )
          else:
            print(f"unrecognized sns subject: {subject}")
        else:
          # manual sync
          print("Merchant synctype preference is manual")
          if subject == "addon.unassign_option":
            resp = ItemMappings.delete_itemmappings(addonId=addonId, addonOptionId=itemId, platformType=2)
          elif subject == "addon.delete":
            resp = ItemMappings.delete_itemmappings(addonId=addonId, platformType=2)


def merchant_autosync_event(event, context):
  with app.app_context():
    print("---------------------- merchant_autosync_event is triggered -------------------------")
    connection, cursor = get_db_connection()
    print(event)
    for record in event['Records']:
      subject = record.get("Sns").get("Subject")
      message = eval(record.get("Sns").get("Message"))

      merchantId = message.get("body").get("merchantId")

      if subject == "merchant.status_change":

        # get main merchant details
        merchant_details = Merchants.get_merchant_by_id(merchantId)
        
        # get merchant + all-virtual-merchant ids list
        vms = VirtualMerchants.get_virtual_merchant(merchantId=merchantId)
        merchants_list = [{
          "syncMerchantId": merchantId,
          "isVirtual": 0,
          "mainMerchantId": merchantId
        }]
        for vm in vms:
          merchants_list.append({
            "syncMerchantId": vm["id"],
            "isVirtual": 1,
            "mainMerchantId": merchantId
          })

        # loop over merchants + vmerchants list
        for merchant in merchants_list:

          platform = Platforms.get_platform_by_merchantid_and_platformtype(merchantid=merchant["syncMerchantId"], platformtype=2)#2=flipdish

          if not platform:
            continue
          
          # payload
          if int(merchant_details['marketstatus']) == 0:

            current_datetime = datetime.datetime.utcnow()
            current_datetime_str = current_datetime.strftime("%Y-%m-%dT%H:%M:%SZ")
            tomorrow_datetime_str = (current_datetime + datetime.timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%SZ")

            payload = {
              "DeliveryType": "Delivery",
              "StartTime": current_datetime_str,
              "EndTime": tomorrow_datetime_str,
              "Type": "Closed"
            }
            resp1 = Flipdish.flipdish_create_business_hours_overrides(platform['accesstoken'], platform['storeid'], payload)

            payload["DeliveryType"] = "Pickup"
            resp2 = Flipdish.flipdish_create_business_hours_overrides(platform['accesstoken'], platform['storeid'], payload)

            if not (resp1 and resp2):
              print("error while closing restaurant on flipdish!")
              continue


            # update metadata in platforms table
            data = [
              resp1.get("Data"), 
              resp2.get("Data")
            ]

            metadata = json.loads(platform['metadata']) if platform['metadata'] else dict()
            metadata["hours_overrides"] = data

            cursor.execute("""UPDATE platforms SET metadata=%s WHERE id=%s""", (json.dumps(metadata), platform['id']))
            connection.commit()

          else:

            metadata = json.loads(platform['metadata']) if platform['metadata'] else dict()

            if metadata.get("hours_overrides") and len(metadata.get("hours_overrides")):

              for row in metadata.get("hours_overrides"):
                id = row.get("BusinessHoursOverrideId")
                if not id:
                  continue

                resp = Flipdish.flipdish_delete_business_hours_overrides(platform['accesstoken'], platform['storeid'], id)
                if not resp:
                  print("error: while deleting hours overrides")
            
            # update metadata in platforms table
            metadata["hours_overrides"] = list()
            cursor.execute("""UPDATE platforms SET metadata=%s WHERE id=%s""", (json.dumps(metadata), platform['id']))
            connection.commit()


