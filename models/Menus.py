import os
import uuid
import json
import random
import string
import csv
from flask import jsonify
import zipfile

# local imports
import config
from models.MenuMappings import MenuMappings
from models.MenuCategories import MenuCategories
from models.VMerchantMenus import VMerchantMenus
from utilities.errors import invalid, unhandled
from utilities.helpers import get_db_connection, success, store_file_in_s3
import pdfkit
from flask import render_template
from flask import make_response
from models.ServiceAvailability import ServiceAvailability

# rds config
rds_host  = config.db_host  
username = config.db_username
password = config.db_password
database_name = config.db_name 



def week_days(day_number):
    days = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
    
    if 1 <= day_number <= 7:
        return days[day_number - 1]
    else:
        return "Invalid day number"

class Menus():

  @classmethod
  def post_menus(cls, userId, merchantId, name, description, metadata, menuPlatforms=None, cusine=None):
    try:
      connection, cursor = get_db_connection()
      
      menuId = uuid.uuid4()
      data = (menuId, merchantId, name, description, "1", json.dumps(metadata), cusine, userId)
      cursor.execute("""INSERT INTO menus 
        (id, merchantid, name, description, status, metadata, cusine, created_by)
        VALUES (%s,%s,%s,%s,%s,%s,%s, %s)""", data)
      connection.commit()
      print("menu created")
      
      return str(menuId)
    except Exception as e:
      print("error: ", str(e))
      return False


  @classmethod
  def put_menuById(cls, userId, menuId, merchantId, name, description, status, metadata, menuPlatforms=None, cusine=None):
    try:
      connection, cursor = get_db_connection()

      metadata = json.dumps(metadata) if metadata is not None else None
      data = (name, description, status, metadata, cusine, userId, menuId)
      cursor.execute("""UPDATE menus SET name=%s, description=%s, status=%s, metadata=COALESCE(%s, metadata), cusine=%s, updated_by=%s, updated_datetime=CURRENT_TIMESTAMP
        WHERE id=%s""", data)
      connection.commit()
      print("menu updated")
      return menuId
      
    except Exception as e:
      print("Error: ", str(e))
      return False
      
  @classmethod
  def get_menu_by_id_fk(cls, menuId):
    try:
      connection, cursor = get_db_connection()
      cursor.execute("SELECT * FROM menus WHERE id = %s", (menuId))
      row = cursor.fetchone()
      return row
    except Exception as e:
      print("error: ", str(e))
      return False

  @classmethod
  def get_menu_by_id(cls, menuId):
    try:
      connection, cursor = get_db_connection()
      
      cursor.execute("""SELECT * FROM menus WHERE id = %s """, (menuId))
      row = cursor.fetchone()
      if row:
        menuResp = {
          "id": row.get("id"),
          "merchantId": row.get("merchantid"),
          "name": row.get("name"),
          "description": row.get("description"),
          "status": row.get("status"),
          "metadata": json.loads(row.get("metadata")) if row.get("metadata") else None,
          "createdBy": row.get("created_by"),
          "menuPlatforms": [],
          "categories": [],
          "virtualMerchants": [],
          "cusines": row['cusine']
        }

        # get menu-mappings by menuId
        menuPlatforms = MenuMappings.get_menumappings_str(menuId=menuId)
        if menuPlatforms:
          menuResp["menuPlatforms"] = menuPlatforms
        
        # get menu-categories by menuId
        catResp = MenuCategories.get_menucategories(menuId=menuId, platformType=1) #1=apptopus
        if type(catResp) is list:
          menuResp["categories"] = catResp
        
        # get virtual-merchants associated with the menu
        vmResp = VMerchantMenus.get_virtual_merchants_by_menuid(menuId)
        if type(vmResp) is list:
          menuResp["virtualMerchants"] = vmResp

        return menuResp
      else:
        return False
    except Exception as e:
      print(str(e))
      return False


  @classmethod
  def get_menus(cls, merchantId, limit=None, offset=None):
    try:
      connection, cursor = get_db_connection()
      cursor.execute("""SELECT * FROM menus 
        WHERE merchantid = %s ORDER BY name ASC LIMIT %s OFFSET %s""", (merchantId, limit, offset))
      rows = cursor.fetchall()
      respData = list()
      for row in rows:
        respData.append({
          'id': row['id'],
          'merchantId': row['merchantid'],
          'name': row['name'],
          'description': row['description'],
          'status': row['status'],
          'metadata': json.loads(row['metadata']) if row['metadata'] else None,
          'createdBy': row['created_by'],
          "menuPlatforms": [],
          "categories": [],
          "virtualMerchants": [],
          "cusines": row['cusine']
        })

        # get menu-mappings
        menuPlatforms = MenuMappings.get_menumappings_str(menuId=row['id'])
        if menuPlatforms:
          respData[-1]["menuPlatforms"] = menuPlatforms

        # get menu-categories
        catResp = MenuCategories.get_menucategories(menuId=row['id'], platformType=1)#1=apptopus
        if type(catResp) is list:
          respData[-1]["categories"] = catResp
        
        # get virtual-merchants associated with the menu
        vmResp = VMerchantMenus.get_virtual_merchants_by_menuid(menuId=row['id'])
        if type(vmResp) is list:
          respData[-1]["virtualMerchants"] = vmResp

      return respData
    except Exception as e:
      print(str(e))
      return False


  @classmethod
  def new_get_menus(cls, merchantId, limit=None, offset=None):
    print("New get menus function name")
    try:
      connection, cursor = get_db_connection()
      cursor.execute("""SELECT * FROM menus 
        WHERE merchantid = %s ORDER BY name ASC LIMIT %s OFFSET %s""", (merchantId, limit, offset))
      rows = cursor.fetchall()
      respData = list()
      for row in rows:
        print(row['id'])
        respData.append({
          'id': row['id'],
          'merchantId': row['merchantid'],
          'name': row['name'],
          'status': row['status'],
        })

        # get menu-mappings
        menuPlatforms = MenuMappings.new_get_menumappings_str(menuId=row['id'])

        if menuPlatforms:
          menuPlatforms = ', '.join(menuPlatforms)
        respData[-1]["menuPlatforms"] = menuPlatforms

        service_availlability = ServiceAvailability.get_serviceAvailabilityByMenuId(menuId=row['id'])
        availability_list = []

        for row in service_availlability:
          start_time = row['startTime']
          end_time = row['endTime']
          week_num = row['weekDay']
          week_day = week_days(week_num)

          found = False
          for entry in availability_list:
            if entry['startTime'] == start_time and entry['endTime'] == end_time:
              entry['days'].append(week_day)
              found = True
              break
          if not found:
            availability_list.append({
              "startTime": start_time,
              "endTime": end_time,
              "days": [week_day]  # Start with a list of one day
            })

        for entry in availability_list:
          entry['days'] = ", ".join(entry['days'])

        # Add this list to the response data

        respData[-1]["serviceAvailability"] = availability_list

      return respData
    except Exception as e:
      print(str(e))
      return False



  @classmethod
  def delete_menu_by_id(cls, menuId):
    try:
      connection, cursor = get_db_connection()
      cursor.execute("""DELETE FROM menus WHERE id = %s""", (menuId))
      connection.commit()
      return True
    except Exception as e:
      print(str(e))
      return False

  @classmethod
  def get_menu_details(cls, menuId):
    try:
      connection, cursor = get_db_connection()
      cursor.execute("""SELECT * FROM menus WHERE id = %s """, (menuId))
      row = cursor.fetchone()
      if row:
        cursor.execute("""SELECT * FROM merchants WHERE id = %s """, (row.get("merchantid")))
        merchant = cursor.fetchone()

        data = []
        catResp = MenuCategories.get_menucategories(menuId=menuId, platformType=1)
        if not len(catResp):
          return invalid("No category is assigned")
        catergory = []
        for catRow in catResp:
          cursor.execute(
            "SELECT productscategories.productid id, items.itemname itemName, items.itemdescription itemDescription, items.itemsku itemSKU, convert(items.itemprice, CHAR) itemPrice, imageurl imageUrl, items.status itemStatus FROM productscategories, items WHERE items.id = productscategories.productid AND productscategories.categoryid = %s order by sortid asc",
            (catRow['id']))
          allItems = cursor.fetchall()

          items = []
          for item in allItems:
            cursor.execute("""SELECT addons.id id,
              addonname addonName
              FROM productsaddons, addons
              WHERE productsaddons.addonid = addons.id and productsaddons.productid = %s order by sortid asc""", item['id'])
            addons = cursor.fetchall()
            # print(addons)
            addonopt = []
            for addon in addons:
              cursor.execute("""SELECT addonsoptions.itemid id,
                      items.itemname addonOptionName,
                      convert(itemprice, CHAR) addonOptionPrice
                      FROM addonsoptions, items
                      WHERE items.id = addonsoptions.itemid and addonsoptions.addonid = %s order by sortid asc""", addon['id'])

              options = cursor.fetchall()
              # print(options)
              addonopt.append({
                "name": addon['addonName'],
                "options": options
              })
            print(item['itemDescription'])
            items.append({
              "name": item['itemName'],
              "price": item['itemPrice'],
              "description": item['itemDescription'],
              "addon": addonopt
            })
            addonopt =[]

          catergory.append({
            "name": catRow['categoryName'],
            "items": items
          })
          data.append(catergory)
          catergory = []

        menu = {
          "name": merchant.get("merchantname"),
          "menu": row.get("name"),
          "data": data
        }

        html = render_template(
          "menupdf.html",
          resp=menu)
        path_wkhtmltopdf = os.environ.get("wkhtmltopdf_path")
        config = pdfkit.configuration(wkhtmltopdf=path_wkhtmltopdf)
        pdf = pdfkit.from_string(html, False, configuration=config)
        response = make_response(pdf)
        response.headers["Content-Type"] = "application/pdf"
        response.headers['Access-Control-Allow-Origin'] = '*'
        response.headers['Access-Control-Allow-Headers'] = '*'
        response.headers['Access-Control-Allow-Methods'] = '*'
        response.headers["Content-Disposition"] = "inline; filename=menu.pdf"

        return response
      else:
        return False
    except Exception as e:
      print(str(e))
      return False


  @classmethod
  def download_menu_csv(cls, menuId):
    try:
      connection, cursor = get_db_connection()


      ### constants
      random_str = ''.join(random.choices(string.ascii_letters + string.digits, k=4))
      
      categories_file_name = f"categories-{random_str}.csv"
      items_file_name = f"items-{random_str}.csv"
      addons_file_name = f"addons-{random_str}.csv"
      options_file_name = f"options-{random_str}.csv"
      menu_zip_name = f"menu-{menuId}-{random_str}.zip"

      # categories_file_path = "C://Users//SL//Dashboard-API//models//tmp//"+categories_file_name
      # items_file_path = "C://Users//SL//Dashboard-API//models//tmp//"+items_file_name
      # addons_file_path = "C://Users//SL//Dashboard-API//models//tmp//"+addons_file_name
      # options_file_path = "C://Users//SL//Dashboard-API//models//tmp//"+options_file_name
      # menu_zip_path = "C://Users//SL//Dashboard-API//models//tmp//"+menu_zip_name
      categories_file_path = "/tmp/"+categories_file_name
      items_file_path = "/tmp/"+items_file_name
      addons_file_path = "/tmp/"+addons_file_name
      options_file_path = "/tmp/"+options_file_name
      menu_zip_path = "/tmp/"+menu_zip_name

      
      ### checks
      menu_details = cls.get_menu_by_id_fk(menuId)
      if not menu_details:
        return invalid("menuId is invalid")
      

      ### get categories
      categories = MenuCategories.get_menucategories(menuId=menuId, platformType=1)
      categories_ids = list()

      categories_csv = open(categories_file_path, mode='w', newline='')
      categories_writer = csv.writer(categories_csv, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
      categories_writer.writerow(["Category Id", "Category Name", "Category Description", "Category Status"])

      for category in categories:
        categories_ids.append(category["id"])
        categoryStatus = "inactive" if category["categoryStatus"] == 0 else "active"
        categories_writer.writerow([category["id"], category["categoryName"], category["categoryDescription"], categoryStatus])
      
      categories_csv.close()


      ### get items
      if categories_ids:
        cursor.execute("""
          SELECT 
            categories.id categoryId, categories.categoryname categoryName,
            productscategories.sortid sortId,
            items.id itemId, items.itemname itemName, items.itemdescription itemDescription, 
            items.itemprice itemPrice, items.status itemStatus, items.imageurl itemImageUrl
          FROM items
          INNER JOIN productscategories ON items.id = productscategories.productid
          INNER JOIN categories ON productscategories.categoryid = categories.id
          WHERE categories.id IN %s
          ORDER BY categories.id ASC, productscategories.sortid ASC
        """, (tuple(categories_ids),))
        items = cursor.fetchall()
      else:
        return invalid("No category is assigned")
      items_ids = list()

      items_csv = open(items_file_path, mode='w', newline='')
      items_writer = csv.writer(items_csv, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
      items_writer.writerow(["Category Id", "Category Name", "Item Id", "Item Name", "Item Description", "Item Price", "Item Status", "Item Image Url"])
      
      for item in items:
        items_ids.append(item["itemId"])
        itemStatus = "inactive" if item["itemStatus"] == 0 else "active"
        items_writer.writerow([item["categoryId"], item["categoryName"], item["itemId"], item["itemName"], item["itemDescription"], item["itemPrice"], itemStatus, item["itemImageUrl"]])
      
      items_csv.close()


      ### get addons
      cursor.execute("""
        SELECT 
          items.id itemId, items.itemname itemName,
          productsaddons.sortid sortId,
          addons.id addonId, addons.addonname addonName, addons.addondescription addonDescription, 
          addons.status addonStatus, addons.minpermitted minPermitted, addons.maxpermitted maxPermitted
        FROM addons
        INNER JOIN productsaddons ON addons.id = productsaddons.addonid
        INNER JOIN items ON productsaddons.productid = items.id
        WHERE items.id IN %s
        ORDER BY items.id ASC, productsaddons.sortid ASC
      """, (tuple(items_ids),))
      addons = cursor.fetchall()
      addons_ids = list()

      addons_csv = open(addons_file_path, mode='w', newline='')
      addons_writer = csv.writer(addons_csv, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
      addons_writer.writerow(["Item Id", "Item Name", "Addon Id", "Addon Name", "Addon Description", "Addon Status", "Minimum Quantity", "Maximum Quantity"])

      for addon in addons:
        addons_ids.append(addon["addonId"])
        addonStatus = "inactive" if addon["addonStatus"] == 0 else "active"
        addons_writer.writerow([addon["itemId"], addon["itemName"], addon["addonId"], addon["addonName"], addon["addonDescription"], addonStatus, addon["minPermitted"], addon["maxPermitted"]])

      addons_csv.close()


      ### get addon-options

      if addons_ids:
        cursor.execute("""
          SELECT
            addons.id addonId, addons.addonname addonName,
            addonsoptions.sortid sortId,
            items.id itemId, items.itemname itemName, items.itemdescription itemDescription, 
            items.itemprice itemPrice, items.status itemStatus, items.imageurl itemImageUrl
          FROM items
          LEFT JOIN addonsoptions ON items.id = addonsoptions.itemid
          LEFT JOIN addons ON addonsoptions.addonid = addons.id
          WHERE addons.id IN %s
          ORDER BY addons.id ASC, addonsoptions.sortid ASC
        """, (tuple(addons_ids),))
        options = cursor.fetchall()
      else:
        options = ()

      options_csv = open(options_file_path, mode='w', newline='')
      options_writer = csv.writer(options_csv, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
      options_writer.writerow(["Addon Id", "Addon Name", "Option Id", "Option Name", "Option Description", "Option Price", "Option Status", "Option ImageUrl"])

      for option in options:
        optionStatus = "inactive" if option["itemStatus"] == 0 else "active"
        options_writer.writerow([option["addonId"], option["addonName"], option["itemId"], option["itemName"], option["itemDescription"], option["itemPrice"], optionStatus, option["itemImageUrl"]])

      options_csv.close()


      ### create a zip file
      with zipfile.ZipFile(menu_zip_path, mode="w") as archive:
        archive.write(categories_file_path, arcname=categories_file_name)
        archive.write(items_file_path, arcname=items_file_name)
        archive.write(addons_file_path, arcname=addons_file_name)
        archive.write(options_file_path, arcname=options_file_name)


      ### store file in s3
      download_url = store_file_in_s3(
        s3_bucket=config.s3_apptopus_bucket,
        from_path=menu_zip_path,
        to_path=f"{config.s3_reports_folder}/{menu_zip_name}"
      )

      ### response
      return success(jsonify({
        "message": "success",
        "status": 200,
        "data": {
          "download_url": download_url
        }
      }))
    except Exception as e:
      return unhandled(f"error: {e}")


  @classmethod
  def update_menu_status(cls, menu_id, menu_status, userId=None):
      # try:
      connection, cursor = get_db_connection()
      data = (menu_status, userId, menu_id)
      cursor.execute("""UPDATE menus 
        SET status= %s, updated_by= %s, updated_datetime=CURRENT_TIMESTAMP
        WHERE id=%s""", data)
      connection.commit()
      return True
  
  @classmethod
  def find_config(cls,configValue,configType):
    try:
      connection, cursor = get_db_connection()
      data = (configValue, configType)
      cursor.execute("""SELECT * from config_master WHERE config_value=%s and config_type=%s""", data)
      options = cursor.fetchall()
      connection.commit()

      return options
    except Exception as e:
      return unhandled(f"error: {e}")
    

  @classmethod
  def get_config_list(cls,configType=None):
    try:
      connection, cursor = get_db_connection()
      cursor.execute("""SELECT * from config_master where config_type=%s""",configType)
      options = cursor.fetchall()
      connection.commit()

      return options
    except Exception as e:
      return unhandled(f"error: {e}")
  
  @classmethod
  def add_config(cls,configType,configValue):
    try:
      connection, cursor = get_db_connection()

      data = (configType, configValue )
      cursor.execute("""INSERT INTO config_master (config_type,config_value) values(%s,%s)""", data)
      connection.commit()

      return True
    except Exception as e:
      return unhandled(f"error: {e}")