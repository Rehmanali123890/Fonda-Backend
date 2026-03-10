from flask import jsonify
import json
import requests
import boto3
import uuid

# local imports
import config
from utilities.helpers import success, get_db_connection
from utilities.errors import invalid, not_found, unhandled
from models.Menus import Menus
from models.MenuCategories import MenuCategories
from models.ProductsCategories import ProductsCategories
from models.Items import Items
from models.ItemMappings import ItemMappings
from models.MenuMappings import MenuMappings
from models.Merchants import Merchants



class Flipdish():

  @classmethod
  def get_flipdish_url(cls, accessToken):
    flipdish_base_url = config.flipdish_base_url
    accessToken = f"Bearer {accessToken}"
    flipdish_headers = {
      'Accept': 'application/json',
      'Content-Type': 'application/json',
      'Authorization': accessToken
    }
    return flipdish_base_url, flipdish_headers
  
  ############################################### FLIPDISH STORE

  @classmethod
  def flipdish_update_store_by_identifier(cls, accessToken, fStoreId, payload):
    try:
      base_url, headers = cls.get_flipdish_url(accessToken)
      payload = json.dumps(payload)
      response = requests.request(
        "POST", 
        base_url + f"/stores/{fStoreId}", 
        headers=headers, 
        data=payload
      )
      print(response.text)
      resp = True if response and response.status_code >= 200 and response.status_code < 300 else False
      return resp
    except Exception as e:
      print("Error: ", str(e))
      return False
  

  @classmethod
  def flipdish_create_business_hours_overrides(cls, accessToken, fStoreId, payload):
    try:
      base_url, headers = cls.get_flipdish_url(accessToken)
      response = requests.request(
        "POST", 
        base_url + f"/stores/{fStoreId}/businesshoursoverrides", 
        headers=headers, 
        data=json.dumps(payload)
      )
      print(response.text)
      resp = response.json() if response and response.status_code >= 200 and response.status_code < 300 else False
      return resp
    except Exception as e:
      print("Error: ", str(e))
      return False
  

  @classmethod
  def flipdish_delete_business_hours_overrides(cls, accessToken, fStoreId, businessHoursOverrideId):
    try:
      base_url, headers = cls.get_flipdish_url(accessToken)
      response = requests.request(
        "DELETE", 
        base_url + f"/stores/{fStoreId}/businesshoursoverrides/{businessHoursOverrideId}", 
        headers=headers
      )
      print(response.text)
      resp = True if response and response.status_code >= 200 and response.status_code < 300 else False
      return resp
    except Exception as e:
      print("Error: ", str(e))
      return False

  ############################################### FLIPDISH MENU

  @classmethod
  def flipdish_get_menu_by_id(cls, accessToken, fMenuId):
    try:
      base_url, headers = cls.get_flipdish_url(accessToken)  
      response = requests.request(
        "GET", 
        base_url + f"/menus/{fMenuId}",
        headers=headers
      )
      fmenu_data = response.json() if response and response.status_code >= 200 and response.status_code < 300 else None
      return fmenu_data
    except Exception as e:
      print("Exception while creating flipdish product: ", str(e))
      return False


  @classmethod
  def flipdish_create_menu(cls, accessToken, appId, menuName, taxRate):
    try:
      base_url, headers = cls.get_flipdish_url(accessToken)
      payload = json.dumps({
        "Name": menuName,
        "DisplaySectionLinks": True,
        "MenuSectionBehaviour": "ExpandSingle",
        "TaxType": "ExcludedFromBasePrice",
        "TaxRates": [
          {
            "Name": "merchant_tax_rate",
            "Rate": float(taxRate)
          }
        ]
      })
      response = requests.request(
        'POST', 
        base_url+ f"/{appId}/menus", 
        headers=headers, 
        data=payload
      )
      fmenu_resp_data = response.json() if response and response.status_code == 200 else None
      return fmenu_resp_data
    except Exception as e:
      print("Exception while creating flipdish menu: ", str(e))
      return False


  @classmethod
  def flipdish_delete_menu(cls, accessToken, fMenuId):
    try:
      base_url, headers = cls.get_flipdish_url(accessToken)
      response = requests.request(
        'DELETE', 
        base_url+f"/menus/{fMenuId}", 
        headers=headers
      )
      print(response.text)
      resp = True if response and response.status_code >= 200 and response.status_code < 300 else False
      return resp
    except Exception as e:
      print("Exception while deleting flipdish menu: ", str(e))
      return False


  @classmethod
  def flipdish_assign_menu_to_store(cls, accessToken, fStoreId, fMenuId):
    try:
      base_url, headers = cls.get_flipdish_url(accessToken)
      response = requests.request(
        "POST",
        base_url + f"/stores/{fStoreId}/menu/{fMenuId}",
        headers=headers
      )
      if response and response.status_code >= 200 and response.status_code <= 300:
        return True
      else:
        return False
    except Exception as e:
      print("Exception while assigning menu to flipdish store: ", str(e))
      return False
  

  @classmethod
  def flipdish_set_menu_name(cls, accessToken, fMenuId, menuName):
    try:
      base_url, headers = cls.get_flipdish_url(accessToken)
      payload = str(menuName)
      response = requests.request(
        "POST", 
        base_url + f"/menus/{fMenuId}/name",
        json=payload,
        headers=headers
      )
      data = True if response and response.status_code >= 200 and response.status_code < 300 else None
      return data
    except Exception as e:
      print("Flipdish Exception: ", str(e))
      return False

  ############################################### FLIPDISH MENU-CATEGORIES

  @classmethod
  def flipdish_get_menu_categories_by_id(cls, accessToken, fMenuId):
    try:
      base_url, headers = cls.get_flipdish_url(accessToken)  
      response = requests.request(
        "GET", 
        base_url + f"/menus/{fMenuId}/sections",
        headers=headers
      )
      data = response.json() if response and response.status_code >= 200 and response.status_code < 300 else None
      return data
    except Exception as e:
      print("Flipdish Exception: ", str(e))
      return False


  @classmethod
  def flipdish_create_menu_category(cls, accessToken, fMenuId, categoryName, categoryDescription, sortId=None):
    try:
      base_url, headers = cls.get_flipdish_url(accessToken)
      payload = json.dumps({
        "Name": categoryName,
        "Description": categoryDescription,
        "DisplayOrder": sortId,
        "IsAvailable": True,
        "IsHiddenFromCustomers": False
      })
      response = requests.request(
        "POST", 
        base_url + f"/menus/{fMenuId}/sections", 
        headers=headers, 
        data=payload
      )
      fcategory_data = response.json() if response and response.status_code >= 200 and response.status_code < 300 else None
      return fcategory_data
    except Exception as e:
      print("Exception while creating flipdish menu category: ", str(e))
      return False
  

  @classmethod
  def flipdish_update_menu_category(cls, accessToken, fMenuId, fCategoryId, categoryName, categoryDescription):
    try:
      base_url, headers = cls.get_flipdish_url(accessToken)
      payload = json.dumps({
        "Name": categoryName,
        "Description": categoryDescription
      })
      response = requests.request(
        "POST", 
        base_url + f"/menus/{fMenuId}/sections/{fCategoryId}", 
        headers=headers, 
        data=payload
      )
      fcategory_data = True if response and response.status_code >= 200 and response.status_code < 300 else None
      return fcategory_data
    except Exception as e:
      print("Exception while updating flipdish menu-category: ", str(e))
      return False
  

  @classmethod
  def flipdish_set_category_availability(cls, accessToken, fMenuId, fCategoryId):
    try:
      base_url, headers = cls.get_flipdish_url(accessToken)  
      payload = json.dumps({"AvailabilityMode": "DisplayAlways"})
      response = requests.request(
        "POST",
        base_url + f"/menus/{fMenuId}/sections/{fCategoryId}/availability",
        data=payload,
        headers=headers
      )
      resp = True if response and response.status_code >= 200 and response.status_code < 300 else False
      return resp
    except Exception as e:
      print("Exception while updating flipdish category availability: ", str(e))
      return False
  

  @classmethod
  def flipdish_delete_menu_category(cls, accessToken, fMenuId, fCategoryId):
    try:
      base_url, headers = cls.get_flipdish_url(accessToken)  
      response = requests.request(
        "DELETE", 
        base_url + f"/menus/{fMenuId}/sections/{fCategoryId}",
        headers=headers
      )
      data = True if response and response.status_code >= 200 and response.status_code < 300 else None
      return data
    except Exception as e:
      print("Flipdish Exception: ", str(e))
      return False

  ############################################### FLIPDISH CATEGORY-PRODCUTS

  @classmethod
  def flipdish_create_category_product(cls, accessToken, fMenuId, fCategoryId, name, description, price, imageUrl, status, sortId=None):
    try:
      base_url, headers = cls.get_flipdish_url(accessToken)  
      payload = json.dumps({
        "Name": name,
        "Description": description,
        "Price": float(price),
        "IsAvailable": True if int(status) == 1 else False,
        "ImageUrl": imageUrl,
        "DisplayOrder": sortId
      })
      response = requests.request(
        "POST", 
        base_url + f"/menus/{fMenuId}/sections/{fCategoryId}/sectionitems",
        headers=headers, 
        data=payload
      )
      fitem_data = response.json() if response and response.status_code >= 200 and response.status_code < 300 else None
      return fitem_data
    except Exception as e:
      print("Exception while creating flipdish product: ", str(e))
      return False
  

  @classmethod
  def flipdish_set_product_taxrate(cls, accessToken, fMenuId, fCategoryId, fItemId, fTaxRateId):
    try:
      base_url, headers = cls.get_flipdish_url(accessToken)  
      response = requests.request(
        "POST", 
        base_url + f"/menus/{fMenuId}/sections/{fCategoryId}/sectionitems/{fItemId}/taxrate/{fTaxRateId}",
        headers=headers
      )
      resp = True if response and response.status_code >= 200 and response.status_code < 300 else False
      return resp
    except Exception as e:
      print("Exception while creating flipdish product: ", str(e))
      return False
    
  
  @classmethod
  def flipdish_delete_category_product(cls, accessToken, fMenuId, fCategoryId, fItemId):
    try:
      base_url, headers = cls.get_flipdish_url(accessToken)  
      response = requests.request(
        "DELETE", 
        base_url + f"/menus/{fMenuId}/sections/{fCategoryId}/sectionitems/{fItemId}",
        headers=headers
      )
      data = True if response and response.status_code >= 200 and response.status_code < 300 else None
      return data
    except Exception as e:
      print("Flipdish Exception: ", str(e))
      return False
  

  @classmethod
  def flipdish_update_category_product(cls, accessToken, fMenuId, fCategoryId, fItemId, name=None, description=None, price=None, status=None, imageUrl=None):
    try:
      base_url, headers = cls.get_flipdish_url(accessToken)
      if imageUrl is None:
        payload = {
          "Name": name,
          "Description": description,
          "Price": float(price),
          "IsAvailable": True if int(status) == 1 else False
        }
      else:
        payload = {
          "ImageUrl": imageUrl
        }
      response = requests.request(
        "POST", 
        base_url + f"/menus/{fMenuId}/sections/{fCategoryId}/sectionitems/{fItemId}",
        headers=headers, 
        json=payload
      )
      print(response)
      fitem_data = True if response and response.status_code >= 200 and response.status_code < 300 else False
      return fitem_data
    except Exception as e:
      print("Exception while updating flipdish product: ", str(e))
      return False
  

  # @classmethod
  # def flipdish_update_product_image(cls, accessToken, fMenuId, fCategoryId, fItemId):
  #   try:
  #     base_url, headers = cls.get_flipdish_url(accessToken)
  #     payload = {
  #       "Name": name,
  #       "Description": description,
  #       "Price": float(price),
  #       "IsAvailable": True if int(status) == 1 else False
  #     }
  #     response = requests.request(
  #       "POST", 
  #       base_url + f"/menus/{fMenuId}/sections/{fCategoryId}/sectionitems/{fItemId}/image",
  #       headers=headers, 
  #       json=payload
  #     )
  #     fitem_data = True if response and response.status_code >= 200 and response.status_code < 300 else False
  #     return fitem_data
  #   except Exception as e:
  #     print("Exception while updating flipdish product_image: ", str(e))
  #     return False


  @classmethod
  def flipdish_delete_product_image(cls, accessToken, fMenuId, fCategoryId, fItemId):
    try:
      base_url, headers = cls.get_flipdish_url(accessToken)
      response = requests.request(
        "DELETE", 
        base_url + f"/menus/{fMenuId}/sections/{fCategoryId}/sectionitems/{fItemId}/image",
        headers=headers
      )
      fitem_data = True if response and response.status_code >= 200 and response.status_code < 300 else False
      return fitem_data
    except Exception as e:
      print("Exception while deleting flipdish product_image: ", str(e))
      return False

  ############################################### FLIPDISH PRODUCT-ADDONS

  @classmethod
  def flipdish_create_product_addon(cls, accessToken, fMenuId, fCategoryId, fItemId, name, sortId=None):
    try:
      base_url, headers = cls.get_flipdish_url(accessToken)  
      payload = json.dumps({
        "Name": name,
        "IsMasterOptionSet": False,
        "MinSelectCount": 1,
        "MaxSelectCount": 1,
        "DisplayOrder": sortId
      })
      response = requests.request(
        "POST", 
        base_url + f"/menus/{fMenuId}/sections/{fCategoryId}/sectionitems/{fItemId}/optionsets", 
        headers=headers, 
        data=payload
      )
      faddon_data = response.json() if response and response.status_code >= 200 and response.status_code < 300 else None
      return faddon_data
    except Exception as e:
      print("Exception while creating flipdish product-addon: ", str(e))
      return False
  

  @classmethod
  def flipdish_delete_product_addon(cls, accessToken, fMenuId, fCategoryId, fItemId, fAddonId):
    try:
      base_url, headers = cls.get_flipdish_url(accessToken)  
      response = requests.request(
        "DELETE", 
        base_url + f"/menus/{fMenuId}/sections/{fCategoryId}/sectionitems/{fItemId}/optionsets/{fAddonId}", 
        headers=headers
      )
      faddon_data = True if response and response.status_code >= 200 and response.status_code < 300 else False
      return faddon_data
    except Exception as e:
      print("Exception while deleting flipdish product addon: ", str(e))
      return False
  

  @classmethod
  def flipdish_update_product_addon(cls, accessToken, fMenuId, fCategoryId, fItemId, fAddonId, name):
    try:
      base_url, headers = cls.get_flipdish_url(accessToken)  
      payload = json.dumps({
        "Name": name
      })
      response = requests.request(
        "POST", 
        base_url + f"/menus/{fMenuId}/sections/{fCategoryId}/sectionitems/{fItemId}/optionsets/{fAddonId}", 
        headers=headers, 
        data=payload
      )
      faddon_data = True if response and response.status_code >= 200 and response.status_code < 300 else False
      return faddon_data
    except Exception as e:
      print("Exception while updating flipdish product-addon: ", str(e))
      return False

  ############################################### FLIPDISH ADDON-OPTIONS
  
  @classmethod
  def flipdish_create_addon_option(cls, accessToken, fMenuId, fCategoryId, fItemId, fAddonId, name, price, sortId):
    try:
      base_url, headers = cls.get_flipdish_url(accessToken)  
      payload = json.dumps({
        "Name": name,
        "Price": float(price),
        "IsAvailable": True,
        "DisplayOrder": sortId
      })
      response = requests.request(
        "POST", 
        base_url + f"/menus/{fMenuId}/sections/{fCategoryId}/sectionitems/{fItemId}/optionsets/{fAddonId}/optionsetitems",
        headers=headers, 
        data=payload
      )
      foption_data = response.json() if response and response.status_code >= 200 and response.status_code < 300 else None
      return foption_data
    except Exception as e:
      print("Exception while creating flipdish addon-option: ", str(e))
      return False
  

  @classmethod
  def flipdish_update_addon_option(cls, accessToken, fMenuId, fCategoryId, fItemId, fAddonId, fOptionId, name, price, status):
    try:
      base_url, headers = cls.get_flipdish_url(accessToken)  
      payload = json.dumps({
        "Name": name,
        "Price": float(price),
        "IsAvailable": True if int(status) == 1 else False
      })
      response = requests.request(
        "POST", 
        base_url + f"/menus/{fMenuId}/sections/{fCategoryId}/sectionitems/{fItemId}/optionsets/{fAddonId}/optionsetitems/{fOptionId}",
        headers=headers, 
        data=payload
      )
      foption_data = True if response and response.status_code >= 200 and response.status_code < 300 else False
      return foption_data
    except Exception as e:
      print("Exception while updating flipdish addon-option: ", str(e))
      return False
  

  @classmethod
  def flipdish_delete_addon_option(cls, accessToken, fMenuId, fCategoryId, fItemId, fAddonId, fOptionId):
    try:
      base_url, headers = cls.get_flipdish_url(accessToken)  
      response = requests.request(
        "DELETE", 
        base_url + f"/menus/{fMenuId}/sections/{fCategoryId}/sectionitems/{fItemId}/optionsets/{fAddonId}/optionsetitems/{fOptionId}",
        headers=headers
      )
      foption_data = True if response and response.status_code >= 200 and response.status_code < 300 else False
      return foption_data
    except Exception as e:
      print("Exception while deleting flipdish addon-option: ", str(e))
      return False
  
  ############################################### FLIPDISH ORDERS

  @classmethod
  def flipdish_get_order_by_id(cls, accessToken, fOrderId):
    try:
      base_url, headers = cls.get_flipdish_url(accessToken)  
      response = requests.request(
        "GET", 
        base_url + f"/orders/{fOrderId}",
        headers=headers
      )
      order_details = response.json() if response and response.status_code >= 200 and response.status_code < 300 else False
      return order_details
    except Exception as e:
      print(f"error: {e}")
      return False
  
  ############################################### FLIPDISH HELPERS

  @classmethod
  def check_and_get_menumappings(cls, merchantId=None, menuId=None, platformType=None):
    if menuId and platformType is not None:
      response = MenuMappings.get_menumappings(menuId=menuId, platformType=platformType)
    elif merchantId and platformType is not None:
      response = MenuMappings.get_menumappings(merchantId=merchantId, platformType=platformType)
    else:
      return False, "error"
    if not len(response):
      msg = "menu is not assigned to flipdish. exiting..."
      return False, msg
    mapping = response[0]

    if mapping.get("platformmenuid") is None:
      msg = "menu is not synced with flipdish. please perform manual_sync first"
      return False, msg
    return mapping, "success"

  ############################################### FLIPDISH MANUAL-SYNC

  @classmethod
  def post_completeMenu(cls, platformId):
    try:
      connection, cursor = get_db_connection()

      print("Get required details from platforms table...")
      cursor.execute("SELECT * FROM platforms WHERE id=%s", (platformId))
      row = cursor.fetchone()
      appId = row["accountid"]
      fStoreId = row["storeid"]
      accessToken = row["accesstoken"]
      syncMerchantId = row["merchantid"]
      platformType = row["platformtype"]

      print("App id: ", appId)
      print("Store id: ", fStoreId)
      print("Access token: ", accessToken)
      print("Syncing Merchant id: ", syncMerchantId)
      print("Platform Type: ", platformType)

      # get merchant details
      merchant_details = Merchants.get_merchant_or_virtual_merchant(syncMerchantId)
      if not merchant_details:
        return invalid("invalid merchant!")
      
      # get main merchantid iff provided merchantid is of virtual-merchant
      if merchant_details.get("isVirtual"):
        mainMerchantId = merchant_details["merchantid"]
      else:
        mainMerchantId = syncMerchantId

      print("Getting menu-mappings by main-restaurant-merchantId and platformType...")
      mappings = MenuMappings.get_menumappings(merchantId=mainMerchantId, platformType=platformType)
      
      menuMappingRow = list()
      if merchant_details.get("isVirtual"):
        for mapping in mappings:
          # check if menu is assigned to specified virtual-merchant-id, then append it to list
          cursor.execute("""SELECT * FROM vmerchantmenus WHERE vmerchantid = %s AND menuid = %s""", (syncMerchantId, mapping["menuid"]))
          row = cursor.fetchone()
          if row:
            menuMappingRow.append(mapping)
      else:
        for mapping in mappings:
          # check if menu is assigned to any virtual-merchant, then skip it
          cursor.execute("""SELECT * FROM vmerchantmenus WHERE merchantid = %s AND menuid = %s""", (mainMerchantId, mapping["menuid"]))
          row = cursor.fetchone()
          if not row:
            menuMappingRow.append(mapping)

      if len(menuMappingRow) == 0:
        return not_found("No Menu is assigned to flipdish!!!")
      if len(menuMappingRow) > 1:
        return invalid("More than 1 menu have been assigned to flipdish!!!")
      # else get the one and only one row
      menuMappingRow = menuMappingRow[0]

      platformMenuId = menuMappingRow["platformmenuid"]
      menuId = menuMappingRow["menuid"]

      # check if menu is already pushed on flipdish.
      # 1. if menu is on flipdish then delete it from flipdish and delete its
      #    mapping data also from: a. menucategories, b. itemmappings
      # 2. if menu is not on flipdish then delete old mapping data from 
      #    a. menucategories, b. itemmappings
      
      if platformMenuId:
        print("Delete old menu from flipdish...")

        # first unassign old menu from store
        response = cls.flipdish_assign_menu_to_store(accessToken=accessToken, fStoreId=fStoreId, fMenuId=1)
        if not response:
          print("Error while un-assigning old menu from store!!")
        
        # then delete the old menu
        response = cls.flipdish_delete_menu(accessToken=accessToken, fMenuId=platformMenuId)
        if not response:
          print("Error while deleting flipdish menu!!")

      # delete from menucategories
      resp = MenuCategories.delete_menucategories(merchantId=syncMerchantId, platformType=platformType)
      if not resp:
        print("[Non-Critical] Error while deleting old data from menu-categories!!!")

      # delete from itemmappings
      resp = ItemMappings.delete_itemmappings(merchantId=syncMerchantId, platformType=platformType)
      if not resp:
        print("[Non-Critical] Error while deleting old data from item-mappings!!!")

      # get menu by id
      print("Get Menu Details...")
      getMenuResp = Menus.get_menu_by_id(menuId=menuId)
      menuName = getMenuResp["name"]
      print("Menu Name from db: ", menuName)

      # get merchant tax rate from merchants table
      print("Get merchant tax rate...")
      cursor.execute("SELECT taxrate from merchants WHERE id=%s", (mainMerchantId))
      row = cursor.fetchone()
      if row:
        merchantTaxRate = row['taxrate']
      else:
        merchantTaxRate = 0

      # post empty menu to flipdish
      print("Post empty menu to flipdish...")
      fMenuId = cls.flipdish_create_menu(accessToken=accessToken, appId=appId, menuName=menuName, taxRate=merchantTaxRate)
      print("Returned Menu Id: ", str(fMenuId))
      if fMenuId is None:
        return unhandled("Error occured while posting menu to flipdish")
      
      
      # get flipdish menu by id
      print("Get flipdish menu by id...")
      fmenu_data = cls.flipdish_get_menu_by_id(accessToken=accessToken, fMenuId=fMenuId)
      if not fmenu_data:
        return unhandled("Error occured while gettig menu from flipdish")
      fTaxRateArr = fmenu_data.get("Data").get("TaxRates")
      metadata = None
      fTaxRateId = 0
      if len(fTaxRateArr):
        fTaxRateId = fTaxRateArr[0].get("TaxRateId")
        print("flipdish_taxrate_id: ", fTaxRateId)
        metadata = json.dumps({
          "flipdish_taxrate_id": fTaxRateId
        })

      # store the returned menuId and taxRateId in menumappings table
      cursor.execute("""UPDATE menumappings 
        SET platformmenuid=%s, mappingstatus=%s, metadata=%s 
        WHERE id=%s""", 
        (fMenuId, 1, metadata, menuMappingRow['id'])
      )
      connection.commit()

      # assign menu to the store
      response = cls.flipdish_assign_menu_to_store(accessToken=accessToken, fStoreId=fStoreId, fMenuId=fMenuId)
      if not response:
        return unhandled("[Critical] Error occured while assigning menu to the store!!!")
      
      print("-------FIRST PHASE COMPLETED--------- \n\n")
      
      # get all the menu-categories
      print("Get all menu-categories...")
      categories = MenuCategories.get_menucategories(menuId=menuId, platformType=1) #1=apptopus

      # loop over categories
      print("loop over categories...")
      for category in categories:
        
        # create menu-section (category)
        print("Adding Category: <" + category['categoryName'] + ">...")
        fcategory_data = cls.flipdish_create_menu_category(
          accessToken=accessToken,
          fMenuId=fMenuId, 
          categoryName=category['categoryName'],
          categoryDescription=category['categoryDescription'],
          sortId=category['sortId']
        )
        if not fcategory_data:
          print("[Non-Critical] Error while adding menu-section (category) in flipdish!!!")
          continue

        fCategoryId = fcategory_data.get("Data").get("MenuSectionId")
        fCategoryPublicId = fcategory_data.get("Data").get("PublicId")


        # store flipdish category data in menucategories table
        print("Storing flipdish category mapping (PublicId) into menucategories table...")
        mcResp = MenuCategories.post_menucategory( merchantId=syncMerchantId, menuId=menuId, categoryId=category['id'],
          platformType=platformType, platformCategoryId=fCategoryPublicId)
        if not mcResp:
          print("[Non-Critical] Error while adding flipdish category publicId into menucategory table!!!")
      

        # set menu-section (category) availability hours
        print("Set menu-section (category) availability hours...")
        response = cls.flipdish_set_category_availability(accessToken=accessToken, fMenuId=fMenuId, fCategoryId=fCategoryId)
        if not response:
          print("[Non-Critical] Error while setting menu-section (category) availability!!!")
        

        # get category-items
        print("Getting category items...")
        categoryItems = ProductsCategories.get_category_items(categoryId=category['id'])
        if not type(categoryItems) is list:
          continue


        # init sqs client
        sqs_client = boto3.resource('sqs')
        queue = sqs_client.get_queue_by_name(QueueName=config.flipdish_create_item_queue)
        messageGroupId = str(uuid.uuid4())

        
        for citem in categoryItems:

          print("triggering flipdish-create-item-fifo queue...")
          dataObj = {
              "platformId":platformId,
              "menuId": menuId,
              "categoryId": category['id'],
              "itemId": citem['id'],
              "itemSortId": citem['sortId'],
              "fMenuId": fMenuId,
              "fCategoryId": fCategoryId, 
              "fTaxRateId": fTaxRateId,
              "mainMerchantId": mainMerchantId,
              "syncMerchantId": syncMerchantId
          }
          response = queue.send_message(
            MessageBody=json.dumps(dataObj),
            MessageGroupId=messageGroupId,
            MessageDeduplicationId=str(uuid.uuid4())
          )
          print(response)
        
        print("---End Category---\n")

      return success(jsonify({
        "message": "success",
        "status": 200
      }))
    except Exception as e:
      return unhandled(f"unhandled exception: {e}")
  


  @classmethod
  def flipdish_post_complete_product(cls, platformId, menuId, categoryId, itemId, itemSortId, fMenuId, fCategoryId, fTaxRateId, mainMerchantId, syncMerchantId):
    try:
      connection, cursor = get_db_connection()

      print("Get required details from platforms table...")
      cursor.execute("SELECT * FROM platforms WHERE id=%s", (platformId))
      platform = cursor.fetchone()
      appId = platform["accountid"]
      fStoreId = platform["storeid"]
      accessToken = platform["accesstoken"]
      platformType = platform["platformtype"]

      if platform["merchantid"] != syncMerchantId:
        return invalid("invalid merchantId")

      # get each item details now
      print("Get item details by id...")
      itemDetails = Items.get_itemDetailsByIdFromDb(itemId=itemId)
      if not itemDetails:
        print("[Non-Critical] Error while getting item details!!!")
        return not_found("Item details not found in database!!!")

      itemName = itemDetails["itemName"]
      itemDescription = itemDetails["itemDescription"]
      imageUrl = itemDetails["imageUrl"]
      itemStatus = itemDetails["itemStatus"]
      itemPrice = None
      if type(itemDetails["itemPriceMappings"]) is list:
        for r in itemDetails["itemPriceMappings"]:
          if r.get("platformType") == platformType:
            itemPrice = r.get("platformItemPrice")
            break
      if itemPrice is None:
        itemPrice = itemDetails["itemUnitPrice"]
      

      # post item to the flipdish
      print("Post item to flipdish...")
      fitem_data = cls.flipdish_create_category_product(accessToken=accessToken, fMenuId=fMenuId, fCategoryId=fCategoryId,
        name=itemName, description=itemDescription, price=itemPrice, imageUrl=imageUrl, status=itemStatus, sortId=itemSortId
      )
      if not fitem_data:
        print("[Critical] Error while adding product to the flipdish!!!")
        return invalid("[Critical] Error while adding product to the flipdish!!!")
      fItemId = fitem_data.get("Data").get("MenuItemId")
      fItemPublicId = fitem_data.get("Data").get("PublicId")

      # upload product image to flipdish
      if imageUrl:
        fitem_img_resp = cls.flipdish_update_category_product(accessToken=accessToken, 
        fMenuId=fMenuId, fCategoryId=fCategoryId, fItemId=fItemId, imageUrl=imageUrl
        )
        if not fitem_img_resp:
          print("[Non-Critical] Error while updating flipdish product image!!!")

      # store item-mappings
      print("store item-mappings...")
      resp = ItemMappings.post_itemmappings(
        merchantId=syncMerchantId, 
        menuId=menuId,
        itemId=itemId,
        itemType=1,  # 1 for product itemType
        platformType=platformType,
        platformItemId=fItemPublicId,
        categoryId=categoryId)
      if not resp:
        print("[Non-Critical] Error while storing flipdish productId in item-mappings!!!")
      print("")


      # assign taxrate to the item
      print("Assign taxrate to the product...")
      response = cls.flipdish_set_product_taxrate(accessToken=accessToken, fMenuId=fMenuId, 
        fCategoryId=fCategoryId, fItemId=fItemId, fTaxRateId=fTaxRateId
      )
      if not response:
        print("[Non-Critical] Error while assigning taxRateId to the product in flipdish!!!")

      # loop over item-addons
      print("Loop over item-addons...")
      for addon in itemDetails.get("addons"):
        resp = cls.flipdish_post_complete_addon(
          merchantId=syncMerchantId, menuId=menuId, categoryId=categoryId, itemId=itemId,
          addon=addon, platformType=platformType, fMenuId=fMenuId, fCategoryId=fCategoryId, 
          fItemId=fItemId, accessToken=accessToken
        )
        if resp:
          print("--- Addon Added ---\n")
        else:
          print("--- Addon Adding Failed ---\n")

      return success()
    except Exception as e:
      print(str(e))
      return unhandled("Unhandled Exception: " + str(e))
  


  @classmethod
  def flipdish_post_complete_addon(cls, merchantId, menuId, categoryId, itemId, addon, platformType, fMenuId, fCategoryId, fItemId, accessToken):
    try:
      connection, cursor = get_db_connection()

      addonId = addon.get("id")
      addonName = addon.get("addonName")
      addonSortId = addon.get("sortId")

      # send addon to flipdish
      print("Posting addon to flipdish...")
      faddon_data = cls.flipdish_create_product_addon(accessToken=accessToken, fMenuId=fMenuId, 
      fCategoryId=fCategoryId, fItemId=fItemId, name=addonName, sortId=addonSortId
      )
      if not faddon_data:
        print("[Non-Critical] Error while addin addon to the flipdish!!!")
        return False
      fAddonId = faddon_data.get("Data").get("MenuItemOptionSetId")
      fAddonPublicId = faddon_data.get("Data").get("PublicId")
      print("faddonpublicid: ", str(fAddonPublicId))

      # store addon details in item-mappings
      print("store addon public-id details in item-mappings...")
      resp = ItemMappings.post_itemmappings(
        merchantId=merchantId, 
        menuId=menuId,
        itemId=itemId,
        itemType=1,  # 1 for product itemType
        platformType=platformType,
        platformItemId=fAddonPublicId,
        categoryId=categoryId,
        addonId=addonId)
      if not resp:
        print("[Non-Critical] Error while storing addon-public-id in itemmappings!!!")
      

      # loop over addon-options
      print("Loop over addon options")
      for option in addon.get("addonOptions"):
        optionId = option["id"]
        optionName = option["addonOptionName"]
        optionPrice = None
        if type(option["addonOptionPriceMappings"]) is list:
          for r in option["addonOptionPriceMappings"]:
            if r.get("platformType") == platformType:
              optionPrice = r.get("platformItemPrice")
              break
        if optionPrice is None:
          optionPrice = option["addonOptionPrice"]
        optionSortId = option.get("sortId")


        # post addon option to flipdish
        print("Posting addon-option to flipdish...")
        foption_data = cls.flipdish_create_addon_option(accessToken=accessToken, fMenuId=fMenuId, fCategoryId=fCategoryId,
          fItemId=fItemId, fAddonId=fAddonId, name=optionName, price=optionPrice, sortId=optionSortId
        )
        if not foption_data:
          print("[Non-Critical] Error addon-option is not added to the flipdish!!!")
          continue
        fOptionId = foption_data.get("Data").get("MenuItemOptionSetItemId")
        fOptionPublicId = foption_data.get("Data").get("PublicId")

        print("Storing addon-option publicId in itemmappings...")
        resp = ItemMappings.post_itemmappings(
          merchantId=merchantId, 
          menuId=menuId,
          itemId=itemId,
          itemType=2,  # 2 for addon-option itemType
          platformType=platformType,
          platformItemId=fOptionPublicId,
          categoryId=categoryId,
          addonId=addonId,
          addonOptionId=optionId
        )
        if not resp:
          print("[Non-Critical] Error while storing addon-option-public-id in itemmappings!!!")
        print("-End Addon Option-")
      
      return True

    except Exception as e:
      print(str(e))
      return False