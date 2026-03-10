

# local imports
import config
from utilities.helpers import get_db_connection
from models.clover.Clover import Clover
from models.Items import Items
from models.ItemMappings import ItemMappings
from models.Addons import Addons
from models.MenuMappings import MenuMappings
from models.AddonMappings import AddonMappings

# import config


class CloverItemSync():


  @classmethod
  def assign_addon_to_item(cls, merchantId, itemId, addonId, platform=None):
    try:
      print("Start assigning_addon_to_item -> clover...")
      connection, cursor = get_db_connection()

      platformType = platform['platformtype']
      storeId = platform['storeid']
      accessToken = platform['accesstoken']
      clover_items_addons_mapping_json = list(dict())

      print("check if menu is assigned to clover...")
      mappings = MenuMappings.get_menumappings(merchantId=merchantId, platformType=platformType)
      if not len(mappings):
        return True
      menu_mapping = mappings[0]

      print("check if item is part of clover menu...")
      cursor.execute("""SELECT * FROM itemmappings WHERE menuid=%s AND itemid=%s AND platformtype=%s""", (menu_mapping['menuid'], itemId, platformType))
      item_mapping = cursor.fetchone()
      if not item_mapping:
        return True

      ### check if addon is already available on clover
      cursor.execute("""SELECT * FROM addonmappings WHERE addonid=%s AND platformtype=%s AND addonoptionid IS NULL""", (addonId, platformType))
      addon_mapping = cursor.fetchone()

      if addon_mapping and addon_mapping['platformaddonid']:
        clover_items_addons_mapping_json.append({
          "item": {"id": item_mapping["platformitemid"]},
          "modifierGroup": {"id": addon_mapping['platformaddonid']}
        })

      else:
      
        addon_details = Addons.get_addon_by_id_with_options_str(addonId)
        if not addon_details:
          return False

        # create modifier group payload
        payload = {
          "showByDefault": "true",
          "name": addon_details['posName'] if addon_details['posName'] else addon_details['addonName'],
          "alternateName": addon_details['addonName'],
          "minRequired": 0,
          "maxAllowed": 2
        }
        clover_addon_resp = Clover.clover_create_modifier_group(cMid=storeId, accessToken=accessToken, payload=payload)
        if not clover_addon_resp:
          print("error: while posting addon with options to clover!!!")
          return False
      
        # appending to multiple lists
        clover_items_addons_mapping_json.append({
          "item": {"id": item_mapping['platformitemid']},
          "modifierGroup": {"id": clover_addon_resp['id']}
        })

        # post addon-mapping to our database
        addon_map_resp = AddonMappings.post_addonmappings(
          merchantId=merchantId,
          menuId=menu_mapping['menuid'],
          addonId=addonId,
          platformType=platformType,
          platformAddonId=clover_addon_resp['id']
        )

        # post addon options to clover
        for option in addon_details['addonOptions']:
          opt_payload = {
            "available": "true",
            "price": int(float(option['addonOptionPrice']) * 100),
            "name": option['posName'] if option['posName'] else option['addonOptionName'],
            "alternateName": option['shortName']
          }
          clover_options_resp = Clover.clover_create_modifier(
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
            menuId=menu_mapping['menuid'],
            addonId=addonId,
            addonOptionId=option['id'],
            platformType=platformType,
            platformAddonId=clover_options_resp['id']
          )
      
      ### create association between items and addons ###
      clover_items_addons_mapping_json = {
        "elements": clover_items_addons_mapping_json
      }
      clover_item_addons_mapping_resp = Clover.clover_items_modifiergroups_association(
        cMid=storeId, 
        accessToken=accessToken, 
        object=clover_items_addons_mapping_json
      )
      if not clover_item_addons_mapping_resp:
        print("error: while creating association between items and addons!!!")
      
      return True
    except Exception as e:
      print("Error: ", str(e))
      return False


  #########################################################
  #########################################################
  #########################################################

  
  @classmethod
  def unassign_addon_to_item(cls, merchantId, itemId, addonId, platform=None):
    try:
      print("Start un_assigning_addon_to_item -> clover...")
      connection, cursor = get_db_connection()

      platformType = platform['platformtype']
      storeId = platform['storeid']
      accessToken = platform['accesstoken']

      print("check if menu is assigned to clover...")
      mappings = MenuMappings.get_menumappings(merchantId=merchantId, platformType=platformType)
      if not len(mappings):
        return True
      menu_mapping = mappings[0]

      print("check if item is part of clover menu...")
      cursor.execute("""SELECT * FROM itemmappings WHERE menuid=%s AND itemid=%s AND platformtype=%s""", (menu_mapping['menuid'], itemId, platformType))
      item_mapping = cursor.fetchone()
      if not item_mapping:
        return True

      # get addon mapping
      cursor.execute("""SELECT * FROM addonmappings WHERE addonid=%s AND platformtype=%s AND addonoptionid is NULL""", (addonId, platformType))
      addon_mapping = cursor.fetchone()
      if not addon_mapping:
        return False

      ### check if addon is assigned to 1 or more items
      cursor.execute("""
        SELECT * FROM productsaddons WHERE addonid=%s AND productid IN (
          SELECT itemid FROM itemmappings WHERE menuid=%s AND platformtype=%s AND itemid != %s
        )
        """, (addonId, menu_mapping['menuid'], platformType, itemId))
      data_rows = cursor.fetchall()

      if len(data_rows):  
        # only remove the association between product and addon and do not delete the addon as it is assigned to other products also on clover
        object = {
          "elements": [
            {
              "item": {"id": item_mapping["platformitemid"]},
              "modifierGroup": {"id": addon_mapping['platformaddonid']}
            }
          ]
        }
        clover_del_association_resp = Clover.clover_items_modifiergroups_association(cMid=storeId, accessToken=accessToken, object=object, delete=True)
        if not clover_del_association_resp:
          print("error: while removing association between item and addon on clover!!!")
      
      else:
        clover_addons_del_resp = Clover.clover_delete_modifier_groups(cMid=storeId, accessToken=accessToken, addonIds=addon_mapping['platformaddonid'])
        if not clover_addons_del_resp:
          print("error: while deleting addon from clover!!!")

        cursor.execute("""DELETE FROM addonmappings WHERE platformtype=%s AND addonid=%s""", (platformType, addonId))
        connection.commit()
      
      return True
    except Exception as e:
      print("Error: ", str(e))
      return False
  

  #########################################################
  #########################################################
  #########################################################


  @classmethod
  def update_item(cls, merchantId, itemId, platform=None):
    try:
      print("Start update_item -> clover...")
      connection, cursor = get_db_connection()

      platformType = platform.get("platformtype")
      storeId = platform.get("storeid")
      accessToken = platform.get("accesstoken")
      
      ### get platformitemid from itemmappings
      cursor.execute("""SELECT * FROM itemmappings WHERE itemid=%s AND platformtype=%s""", (itemId, platformType))
      item_mappings = cursor.fetchall()

      # loop over item mappings
      for item_mapping in item_mappings:
        item_details = Items.get_item_by_id(itemId)
        if not item_details:
          print("error: item not found!!!")
          continue

        # update item details on clover
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

        clover_item_resp = Clover.clover_update_item(
          cMid=storeId,
          cItemId=item_mapping['platformitemid'],
          accessToken=accessToken,
          payload=item_payload
        )
        if not clover_item_resp:
          print("error: while updating item details on clover!!!")

      
      ### get (platformaddonid as platformaddonoptionid) from addonmappings
      cursor.execute("""SELECT * FROM addonmappings WHERE addonoptionid=%s AND platformtype=%s""", (itemId, platformType))
      option_mappings = cursor.fetchall()

      for option_mapping in option_mappings:
        option_details = Items.get_item_by_id(itemId)
        if not option_details:
          print("error: item not found!!!")
          continue
        
        # get (platformaddonid) from addonmappings
        cursor.execute("""SELECT * FROM addonmappings WHERE addonid=%s AND platformtype=%s AND addonoptionid is NULL""", (option_mapping['addonid'] ,platformType))
        addon_mapping = cursor.fetchone()
        if not addon_mapping:
          print("error: addon mapping not found")
          continue

        # update addon-option details on clover
        option_payload = {
          "available": True if int(option_details['itemStatus']) == 1 else False,
          "price": int(float(option_details['itemUnitPrice']) * 100),
          "name": option_details['posName'] if option_details['posName'] else option_details['itemName'],
          "alternateName": option_details['shortName']
        }

        clover_option_resp = Clover.clover_update_modifier(
          cMid=storeId,
          cAddonId=addon_mapping['platformaddonid'],
          cOptionId=option_mapping['platformaddonid'],
          accessToken=accessToken,
          payload=option_payload
        )
        if not clover_option_resp:
          print("error: while updating addon option details on clover!!!")

      return True
    except Exception as e:
      print("Error: ", str(e))
      return False
  

  #########################################################
  #########################################################
  #########################################################
  

  @classmethod
  def delete_item(cls, merchantId, itemId, platform=None, addons_ids_list=None):
    try:
      print("Start delete_item -> clover...")
      connection, cursor = get_db_connection()

      platformType = platform.get("platformtype")
      storeId = platform.get("storeid")
      accessToken = platform.get("accesstoken")

      print("check if menu is assigned to clover...")
      mappings = MenuMappings.get_menumappings(merchantId=merchantId, platformType=platformType)
      if not len(mappings):
        return True
      menu_mapping = mappings[0]

      print("get itemmapping...")
      cursor.execute("""SELECT * FROM itemmappings WHERE menuid=%s AND itemid=%s AND platformtype=%s""", (menu_mapping['menuid'], itemId, platformType))
      item_mapping = cursor.fetchone()
      
      if item_mapping:

        # delete itemmapping...
        resp = ItemMappings.delete_itemmappings(itemId=itemId, platformType=platformType)

        # delete item from clover
        clover_item_del_resp = Clover.clover_delete_items(
          cMid=storeId, 
          accessToken=accessToken,
          itemIds=item_mapping['platformitemid']
        )
        if not clover_item_del_resp:
          print("error: while deleting item from clover!!!")
          return False


      ### NOW DELETE ITEM-ADDONS FROM CLOVER
      del_addons_ids_list = list()
      clover_addons_ids_list = list()

      for addon_id in addons_ids_list:

        ### if addon is not assigned to any other item on clover, then delete it
        cursor.execute("""
          SELECT * FROM productsaddons WHERE addonid=%s AND productid IN (
            SELECT itemid FROM itemmappings WHERE menuid=%s AND platformtype=%s
          )
          """, (addon_id, menu_mapping['menuid'], platformType))
        data_rows = cursor.fetchall()

        if not len(data_rows):
          cursor.execute("""SELECT * FROM addonmappings WHERE addonid=%s AND platformtype=%s AND addonoptionid is NULL""", (addon_id, platformType))
          addon_mapping = cursor.fetchone()
          
          if addon_mapping:
            del_addons_ids_list.append(addon_id)
            clover_addons_ids_list.append(addon_mapping['platformaddonid'])
   
      if len(clover_addons_ids_list):
        clover_addons_del_resp = Clover.clover_delete_modifier_groups(cMid=storeId, accessToken=accessToken, addonIds=','.join(clover_addons_ids_list))
        if not clover_addons_del_resp:
          print("error: while deleting addons from clover!!!")

        cursor.execute("""DELETE FROM addonmappings WHERE platformtype=%s AND addonid IN %s""", (platformType, tuple(del_addons_ids_list)))
        connection.commit()


      ### PART 2 -> DELETE ADDON OPTION (itemId) FROM CLOVER ADDONS
      cursor.execute("""SELECT * FROM addonmappings WHERE addonoptionid=%s AND platformtype=%s""", (itemId, platformType))
      addon_opt_mappings = cursor.fetchall()

      if addon_opt_mappings:
        for addon_opt in addon_opt_mappings:

          cursor.execute("""DELETE FROM addonmappings WHERE addonoptionid=%s AND platformtype=%s""", (itemId, platformType))
          connection.commit()

          cursor.execute("""SELECT * FROM addonmappings WHERE addonid=%s AND platformtype=%s AND addonoptionid IS NULL""", (addon_opt['addonid'], platformType))
          addon_map = cursor.fetchone()
          if not addon_map:
            continue

          clover_del_option_resp = Clover.clover_delete_modifier(
            cMid=storeId,
            cAddonId=addon_map['platformaddonid'],
            cOptionId=addon_opt['platformaddonid'],
            accessToken=accessToken
          )
          if not clover_del_option_resp:
            print("error: while deleting addon_option from clover addon!!!")
            continue

      return True
    except Exception as e:
      print("Error: ", str(e))
      return False


  #########################################################
  #########################################################
  #########################################################
