

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


class CloverAddonSync():

  #########################################################
  #########################################################
  #########################################################

  @classmethod
  def assign_addonoption_to_addon(cls, merchantId, addonId, itemId, platform=None):
    try:
      print("Start assigning_addon-option_to_addon -> clover...")
      connection, cursor = get_db_connection()

      platformType = platform.get("platformtype")
      storeId = platform.get("storeid")
      accessToken = platform.get("accesstoken")

      # check if menu is assigned to clover...
      mappings = MenuMappings.get_menumappings(merchantId=merchantId, platformType=platformType)
      if not len(mappings):
        return True
      menu_mapping = mappings[0]

      # get addonmappings...
      cursor.execute("""SELECT * FROM addonmappings WHERE addonid=%s AND platformtype=%s AND addonoptionid IS NULL""", (addonId, platformType))
      addon_mapping = cursor.fetchone()
      if not addon_mapping:
        return True
      
      # check if addonoption is already assigned to addon...
      cursor.execute("""SELECT * FROM addonmappings WHERE addonid=%s AND platformtype=%s AND addonoptionid=%s""", (addonId, platformType, itemId))
      row = cursor.fetchone()
      if row:
        return True
      
      # get item details
      option_details = Items.get_item_by_id(itemId)
      
      # post addon options to clover
      opt_payload = {
        "available": "true",
        "price": int(float(option_details['itemUnitPrice']) * 100),
        "name": option_details['posName'] if option_details['posName'] else option_details['itemName'],
        "alternateName": option_details['shortName']
      }
      clover_options_resp = Clover.clover_create_modifier(
        cMid=storeId, 
        cAddonId=addon_mapping['platformaddonid'],
        accessToken=accessToken,
        payload=opt_payload
      )
      if not clover_options_resp:
        print("error: while posting addon_options to clover!!!")
        return False

      # post addon-mapping to our database
      addonopt_map_resp = AddonMappings.post_addonmappings(
        merchantId=merchantId,
        menuId=menu_mapping['menuid'],
        addonId=addonId,
        addonOptionId=itemId,
        platformType=platformType,
        platformAddonId=clover_options_resp['id']
      )

      return True
    except Exception as e:
      print("Error: ", str(e))
      return False
  

  #########################################################
  #########################################################
  #########################################################

  @classmethod
  def unassign_addonoption_to_addon(cls, merchantId, addonId, itemId, platform=None):
    try:
      print("Start un_assigning_addon-option_to_addon -> clover...")
      connection, cursor = get_db_connection()

      platformType = platform.get("platformtype")
      storeId = platform.get("storeid")
      accessToken = platform.get("accesstoken")

      # check if menu is assigned to clover...
      mappings = MenuMappings.get_menumappings(merchantId=merchantId, platformType=platformType)
      if not len(mappings):
        return True
      menu_mapping = mappings[0]

      # get addonmappings...
      cursor.execute("""SELECT * FROM addonmappings WHERE addonid=%s AND platformtype=%s AND addonoptionid IS NULL""", (addonId, platformType))
      addon_mapping = cursor.fetchone()
      if not addon_mapping:
        return True
      
      # check if addonoption mapping...
      cursor.execute("""SELECT * FROM addonmappings WHERE addonid=%s AND platformtype=%s AND addonoptionid=%s""", (addonId, platformType, itemId))
      option_mapping = cursor.fetchone()
      if not option_mapping:
        return True
      
      cursor.execute("""DELETE FROM addonmappings WHERE addonoptionid=%s AND platformtype=%s""", (itemId, platformType))
      connection.commit()
      
      clover_del_option_resp = Clover.clover_delete_modifier(
        cMid=storeId,
        cAddonId=addon_mapping['platformaddonid'],
        cOptionId=option_mapping['platformaddonid'],
        accessToken=accessToken
      )
      if not clover_del_option_resp:
        print("error: while deleting addon_option from clover addon!!!")
        return False
      
      return True
    except Exception as e:
      print("Error: ", str(e))
      return False
  

  #########################################################
  #########################################################
  #########################################################

  @classmethod
  def update_addon(cls, merchantId, addonId, platform=None):
    try:
      print("Start update_addon -> clover...")
      connection, cursor = get_db_connection()

      platformType = platform.get("platformtype")
      storeId = platform.get("storeid")
      accessToken = platform.get("accesstoken")

      # get addonmappings...
      cursor.execute("""SELECT * FROM addonmappings WHERE addonid=%s AND platformtype=%s AND addonoptionid IS NULL""", (addonId, platformType))
      addon_mapping = cursor.fetchone()
      if not addon_mapping:
        return True
      
      # get addon details...
      addon_details = Addons.get_addon_by_id(addonId)
      if not addon_details:
        return False
      
      # update addon deatils on clover
      payload = {
        "showByDefault": "true",
        "name": addon_details['posname'] if addon_details['posname'] else addon_details['addonname'],
        "alternateName": addon_details['addonname'],
        "minRequired": 0,
        "maxAllowed": 2
      }
      clover_addon_resp = Clover.clover_update_modifier_group(
        cMid=storeId,
        cAddonId=addon_mapping['platformaddonid'],
        accessToken=accessToken,
        payload=payload
      )
      if not clover_addon_resp:
        print("error: while updating addon details on clover!!!")
        return False

      return True
    except Exception as e:
      print("Error: ", str(e))
      return False

  #########################################################
  #########################################################
  #########################################################
  
  @classmethod
  def delete_addon(cls, merchantId, addonId, platform=None):
    try:
      print("Start delete_addon -> clover...")
      connection, cursor = get_db_connection()

      platformType = platform.get("platformtype")
      storeId = platform.get("storeid")
      accessToken = platform.get("accesstoken")

      # get addonmappings...
      cursor.execute("""SELECT * FROM addonmappings WHERE addonid=%s AND platformtype=%s AND addonoptionid IS NULL""", (addonId, platformType))
      addon_mapping = cursor.fetchone()
      if not addon_mapping:
        return True
      
      # delete addonmapping data from database
      cursor.execute("""DELETE FROM addonmappings WHERE addonid=%s AND platformtype=%s""", (addonId, platformType))
      connection.commit()

      # delete addon from clover
      clover_addons_del_resp = Clover.clover_delete_modifier_groups(
        cMid=storeId, 
        accessToken=accessToken, 
        addonIds=addon_mapping['platformaddonid']
      )
      if not clover_addons_del_resp:
        print("error: while deleting addons from clover!!!")

      return True
    except Exception as e:
      print("Error: ", str(e))
      return False