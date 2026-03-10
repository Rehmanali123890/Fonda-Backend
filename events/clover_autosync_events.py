from app import app
import json

# local imports
from utilities.helpers import success
import config
from models.clover.Clover import Clover
from models.clover.CloverMenuSync import CloverMenuSync
from models.clover.CloverCategorySync import CloverCategorySync
from models.clover.CloverItemSync import CloverItemSync
from models.clover.CloverAddonSync import CloverAddonSync
from models.Platforms import Platforms
from models.MenuCategories import MenuCategories


def clover_manual_sync_queue_event(event, context):
  with app.app_context():
    print("--------------------- process clover_menu_sync_queue event --------------------------")
    for record in event['Records']:
      dataObj = json.loads(record["body"])
      platformId  = dataObj['platformId']
      menuId = dataObj["menuId"]
      items_ids_dict = dataObj["items_ids_dict"]

      resp = Clover.post_items_addons_to_clover(
        platformId=platformId,
        menuId=menuId,
        items_ids_dict=items_ids_dict
      )
      print("Done")



def menu_autosync_event(event, context):
  with app.app_context():
    print("---------------------- menu_autosync_event is triggered -------------------------")

    for record in event['Records']:
      subject = record.get("Sns").get("Subject")
      message = eval(record.get("Sns").get("Message"))
      print("Subject: ", subject)

      merchantId = message.get("body").get("merchantId")
      menuId = message.get("body").get("menuId")
      categoryId = message.get("body").get("categoryId")

      platform = Platforms.get_platform_by_merchantid_and_platformtype(merchantid=merchantId, platformtype=4)#4=clover

      if platform:
        if platform["synctype"] == 1:

          if subject == "menu.assign_category":
            resp = CloverMenuSync.assign_category(
              merchantId=merchantId, 
              menuId=menuId, 
              categoryId=categoryId,
              platform=platform
            )
          elif subject == "menu.unassign_category":
            resp = CloverMenuSync.unassign_category(
              merchantId=merchantId, 
              menuId=menuId, 
              categoryId=categoryId,
              platform=platform
            )
          elif subject == "menu.update":
            resp = CloverMenuSync.update_menu(
              merchantId=merchantId, 
              menuId=menuId, 
              platform=platform
            )
          elif subject == "menu.delete":
            resp = CloverMenuSync.delete_menu(
              merchantId=merchantId, 
              menuId=menuId,
              mappings=message.get("body").get("mappings"),
              platform=platform
            )
          else:
            print(f"unrecognized sns subject: {subject}")
        else:
          print("Merchant synctype preference is manual")
          pass
      else:
        print("Merchant is not connected to flipdish")
      print("Done")



def category_autosync_event(event, context):
  with app.app_context():
    print("---------------------- category_autosync_event is triggered -------------------------")

    for record in event['Records']:
      subject = record.get("Sns").get("Subject")
      message = eval(record.get("Sns").get("Message"))
      print("Subject: ", subject)

      merchantId = message.get("body").get("merchantId")
      categoryId = message.get("body").get("categoryId")
      itemId = message.get("body").get("itemId")

      platform = Platforms.get_platform_by_merchantid_and_platformtype(merchantid=merchantId, platformtype=4)#4=clover

      if not platform:
        print("Merchant is not connected to clover")
      else:
        platform["accesstoken"], msg, is_error = Clover.generate_clover_access_token(platform)
        if is_error:
          print(msg)
          return False
        if platform["synctype"] == 1:
          # auto-sync
          if subject == "category.assign_item":
            resp = CloverCategorySync.assign_item_to_category(
              merchantId=merchantId, 
              categoryId=categoryId,
              itemId=itemId,
              platform=platform
            )
          elif subject == "category.unassign_item":
            resp = CloverCategorySync.unassign_item_to_category(
              merchantId=merchantId, 
              categoryId=categoryId,
              itemId=itemId,
              platform=platform
            )
          elif subject == "category.update":
            resp = CloverCategorySync.update_category(
              merchantId=merchantId, 
              categoryId=categoryId,
              platform=platform
            )
          elif subject == "category.delete":
            resp = CloverCategorySync.delete_category(
              merchantId=merchantId, 
              categoryId=categoryId,
              platform=platform,
              products_ids_list=message.get("body").get("products_ids_list")
            )
          else:
            print(f"unrecognized sns subject: {subject}")
        else:
          print("Merchant synctype preference is manual (0)")
          if subject == "category.delete":
            resp = MenuCategories.delete_menucategories(categoryId=categoryId, platformType=4)#4=clover
        
      print("Done")


def item_autosync_event(event, context):
  with app.app_context():
    print("---------------------- item_autosync_event is triggered -------------------------")
    print(event)
    for record in event['Records']:
      print(record)
      subject = record.get("Sns").get("Subject")
      message = eval(record.get("Sns").get("Message"))
      print("Subject: ", subject)

      merchantId = message.get("body").get("merchantId")
      itemId = message.get("body").get("itemId")
      addonId = message.get("body").get("addonId")
      unchanged = message.get("body").get("unchanged")

      if unchanged and type(unchanged) is list and 'pos' in unchanged:
        print("clover remains unchanged, handling loop. exiting...")
        continue

      platform = Platforms.get_platform_by_merchantid_and_platformtype(merchantid=merchantId, platformtype=4)# 4=clover

      if platform:
        if platform["synctype"] == 1:
          platform["accesstoken"], msg, is_error = Clover.generate_clover_access_token(platform)
          if is_error:
            print(msg)
            return False
          if subject == "item.assign_addon":
            resp = CloverItemSync.assign_addon_to_item(
              merchantId=merchantId,
              itemId=itemId,
              addonId=addonId,
              platform=platform
            )
          elif subject == "item.unassign_addon":
            resp = CloverItemSync.unassign_addon_to_item(
              merchantId=merchantId,
              itemId=itemId,
              addonId=addonId,
              platform=platform
            )
          elif subject == "item.update" or subject == "item.status_change":
            resp = CloverItemSync.update_item(
              merchantId=merchantId,
              itemId=itemId,
              platform=platform
            )
          elif subject == "item.delete":
            resp = CloverItemSync.delete_item(
              merchantId=merchantId,
              itemId=itemId,
              platform=platform,
              addons_ids_list=message.get("body").get("addons_ids_list")
            )
          else:
            print(f"unrecognized sns subject: {subject}")
        else:
          print("Merchant synctype preference is manual")
          

def addon_autosync_event(event, context):
  with app.app_context():
    print("---------------------- addon_autosync_event is triggered -------------------------")

    for record in event['Records']:
      subject = record.get("Sns").get("Subject")
      message = eval(record.get("Sns").get("Message"))
      print("Subject: ", subject)

      merchantId = message.get("body").get("merchantId")
      addonId = message.get("body").get("addonId")
      itemId = message.get("body").get("itemId")

      platform = Platforms.get_platform_by_merchantid_and_platformtype(merchantid=merchantId, platformtype=4)#4=clover

      if platform:
        if platform["synctype"] == 1:
          platform["accesstoken"], msg, is_error = Clover.generate_clover_access_token(platform)
          if is_error:
            print(msg)
            return False
          if subject == "addon.assign_option":
            resp = CloverAddonSync.assign_addonoption_to_addon(
              merchantId=merchantId,
              addonId=addonId,
              itemId=itemId,
              platform=platform
            )
          elif subject == "addon.unassign_option":
            resp = CloverAddonSync.unassign_addonoption_to_addon(
              merchantId=merchantId,
              addonId=addonId,
              itemId=itemId,
              platform=platform
            )
          elif subject == "addon.update":
            resp = CloverAddonSync.update_addon(
              merchantId=merchantId,
              addonId=addonId,
              platform=platform
            )
          elif subject == "addon.delete":
            resp = CloverAddonSync.delete_addon(
              merchantId=merchantId,
              addonId=addonId,
              platform=platform
            )
          else:
            print(f"unrecognized sns subject: {subject}")
        else:
          print("Merchant synctype preference is manual")