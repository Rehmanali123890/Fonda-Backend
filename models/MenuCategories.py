import uuid
import json

from flask import jsonify

# local imports
import config
from utilities.errors import invalid, unhandled
from utilities.helpers import get_db_connection, success


class MenuCategories():
  
  ############################################### GET

  @classmethod
  def get_menucategories(cls, menuId=None, merchantId=None, platformType=None):
    try:
      connection, cursor = get_db_connection()
      data = list()

      if menuId and platformType:
        cursor.execute("""SELECT categories.id id, categories.categoryname categoryName, categories.posname posName, categories.categorydescription categoryDescription, categories.status status, menucategories.sortid sortId 
          FROM menucategories, categories
          WHERE menucategories.categoryid=categories.id AND menucategories.menuid=%s AND menucategories.platformtype=%s
          ORDER BY menucategories.sortid ASC
          """, (menuId, platformType))
        rows = cursor.fetchall()
        for row in rows:
          data.append({
            "id": row["id"],
            "categoryName": row["categoryName"],
            "posName": row["posName"],
            "categoryDescription": row["categoryDescription"],
            "categoryStatus": row["status"],
            "sortId": row["sortId"]
          })

      connection.commit()
      return data
    except Exception as e:
      print("Error: ", str(e))
      return False
  

  @classmethod
  def get_menucategories_fk(cls, merchantId=None, menuId=None, categoryId=None,  platformType=None, platformCategoryId=None, order_by: list= []):
    try:
      connection, cursor = get_db_connection()

      conditions = []
      if merchantId: conditions.append(f'merchantid = "{merchantId}"')
      if menuId: conditions.append(f'menuid = "{menuId}"')
      if categoryId: conditions.append(f'categoryid = "{categoryId}"')
      if platformType is not None: conditions.append(f'platformtype = "{platformType}"')
      if platformCategoryId is not None: conditions.append(f'platformcategoryid = "{platformCategoryId}"')

      WHERE = ' AND '.join(conditions)
      if not WHERE:
        print("no condition is specified!")
        return False
      
      ORDER_BY = ""
      if len(order_by):
        ORDER_BY = "ORDER BY " + ", ".join(order_by)
      

      cursor.execute(f"""
        SELECT * FROM menucategories
        WHERE {WHERE}
        {ORDER_BY} 
      """)

      # if merchantId and menuId and categoryId and platformType:
      #   cursor.execute("""SELECT * FROM menucategories 
      #     WHERE merchantid=%s AND menuid=%s AND categoryid=%s AND platformtype=%s""", (merchantId, menuId, categoryId, platformType))

      # elif menuId and categoryId and platformType:
      #   cursor.execute("""SELECT * FROM menucategories 
      #     WHERE menuid=%s AND categoryid=%s AND platformtype=%s""", (menuId, categoryId, platformType))

      # elif menuId and platformType:
      #   cursor.execute("""SELECT * FROM menucategories 
      #     WHERE menuid=%s AND platformtype=%s""", (menuId, platformType))
      
      # elif categoryId and platformType is not None:
      #   cursor.execute("""SELECT * FROM menucategories 
      #     WHERE categoryid=%s AND platformtype=%s""", (categoryId, platformType))
      
      rows = cursor.fetchall()
      return rows
    except Exception as e:
      print("Error: ", str(e))
      return False
  
  ############################################### DELETE

  @classmethod
  def delete_menucategories(cls, merchantId=None, menuId=None, categoryId=None, platformType=None):
    try:
      connection, cursor = get_db_connection()

      conditions = []
      if merchantId: conditions.append(f'merchantid = "{merchantId}"')
      if menuId: conditions.append(f'menuid = "{menuId}"')
      if categoryId: conditions.append(f'categoryid = "{categoryId}"')
      if platformType is not None: conditions.append(f'platformtype = "{platformType}"')

      WHERE = ' AND '.join(conditions)
      if not WHERE:
          print("no condition is specified. threat to complete table data!")
          return False

      cursor.execute(f"""DELETE FROM menucategories WHERE {WHERE}""")

      print("delete menu-categories-mappings rows: ", str(cursor.rowcount))
      connection.commit()
      return True
    except Exception as e:
      print("Error: ", str(e))
      return False
  
  ############################################### PUT

  @classmethod
  def update_menucategories(cls, merchantId, menuId, categoryIds):
    try:
      connection, cursor = get_db_connection()

      if menuId and type(categoryIds) is list:
        
        # select from menucategories by categoryId
        cursor.execute("""SELECT * FROM menucategories 
          WHERE menuid=%s AND platformtype=%s""", (menuId, 1)) #1=apptopus
        rows = cursor.fetchall()

        # compare rows with the menuIds
        for row in rows:
          rowCategoryId = row["categoryid"]
          if rowCategoryId in categoryIds:
            categoryIds.remove(rowCategoryId)
          else:
            cursor.execute("""DELETE FROM menucategories WHERE menuid=%s AND categoryid=%s AND platformtype=%s""", (menuId, rowCategoryId, 1))#1=apptopus
            connection.commit()
        
        for categoryId in categoryIds:
          cursor.execute("""SELECT id FROM categories WHERE id=%s AND merchantid=%s""", (categoryId, merchantId))
          cat = cursor.fetchone()
          if cat:
            mcid = uuid.uuid4()
            data = (mcid, merchantId, menuId, categoryId, 1)
            cursor.execute("""INSERT INTO menucategories (id, merchantid, menuid, categoryid, platformtype)
              VALUES(%s,%s,%s,%s,%s)""", data)

      connection.commit()
      return True
      
    except Exception as e:
      print("Error: ", str(e))
      return False

  ############################################### POST

  @classmethod
  def post_menucategory(cls, merchantId, menuId, categoryId, platformType, platformCategoryId=None, metadata=None):
    try:
      connection, cursor = get_db_connection()

      sortId = 0
      if platformType == 1:
        cursor.execute(
          "SELECT COALESCE(MAX(sortid), 0) as maxSortId FROM menucategories WHERE menuid = %s AND platformtype = %s",
          (menuId, platformType)
        )
        row = cursor.fetchone()
        sortId = row.get("maxSortId") + 1

      if isinstance(categoryId, list):
        # If categoryId is a list, insert all items in the list
        for cat_id in categoryId:
          guid = uuid.uuid4()
          data = (guid, merchantId, menuId, cat_id, platformType, platformCategoryId, metadata, sortId)
          cursor.execute("""
                      INSERT INTO menucategories 
                      (id, merchantid, menuid, categoryid, platformtype, platformcategoryid, metadata, sortid)
                      VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                  """, data)
          sortId += 1  # Increment sortId for each inserted record

      else:
        # If categoryId is a single string, insert it as is
        guid = uuid.uuid4()
        data = (guid, merchantId, menuId, categoryId, platformType, platformCategoryId, metadata, sortId)
        cursor.execute("""
                  INSERT INTO menucategories 
                  (id, merchantid, menuid, categoryid, platformtype, platformcategoryid, metadata, sortid)
                  VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
              """, data)

      connection.commit()
      return True
    except Exception as e:
      print("Error: ", str(e))
      return False
  

  @classmethod
  def sort_menu_categories(cls, merchantId, menuId, categories):
    try:
      connection, cursor = get_db_connection()

      if not len(categories):
        return invalid("invalid request")

      cursor.execute("SELECT * FROM menus WHERE id = %s", (menuId))
      row = cursor.fetchone()
      if not row or row["merchantid"] != merchantId:
        return invalid("invalid request")

      data = list(tuple())
      for row in categories:
        data.append((row["sortId"], menuId, row["categoryId"]))

      cursor.executemany("""
        UPDATE menucategories 
          SET sortid = %s
          WHERE menuid = %s AND categoryid = %s AND platformtype = 1
      """, (data))

      connection.commit()

      return success()
    except Exception as e:
      return unhandled(f"error: {e}")

  @classmethod
  def check_category_platform_mapping(cls, menuId, categoryId):
    try:
      connection, cursor = get_db_connection()
      cursor.execute("SELECT * FROM menucategories WHERE categoryid = %s and menuid = %s and platformtype in (4,11)", (categoryId , menuId))
      row = cursor.fetchone()
      platformtype=None
      if row:
        platformtype=row.get('platformtype')
      return success(jsonify({
            "message": "success",
            "status": 200,
            "data": {"platformtype": platformtype}
          }))
    except Exception as e:
      return unhandled(f"error: {e}")