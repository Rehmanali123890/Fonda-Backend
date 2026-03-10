import uuid
import json

# local imports
from utilities.helpers import get_db_connection 
from models.MenuCategories import MenuCategories
from models.ProductsCategories import ProductsCategories


class Categories():

  ############################################### GET

  @classmethod
  def get_category_by_id(cls, id):
    try:
      connection, cursor = get_db_connection()
      cursor.execute("""SELECT * FROM categories WHERE id=%s""", id)
      row = cursor.fetchone()
      return row
    except Exception as e:
      print(str(e))
      return False


  @classmethod
  def get_category_by_id_str(cls, id):
    try:

      row = cls.get_category_by_id(id)
      if row:
        category = {
          'id': row['id'], 
          'categoryName': row['categoryname'],
          'posName': row['posname'], 
          'categoryDescription': row['categorydescription'],   
          'categoryStatus': row['status'] 
        }
        return category
      else:
        return False
    except Exception as e:
      print(str(e))
      return False


  def get_category_menu_by_id_str(merchantId, id):
    connection, cursor = get_db_connection()
    if id:
      cursor.execute("""select * from menucategories where categoryid = %s""",(id))
      menus = cursor.fetchall()
      menu_ids = [menu['menuid'] for menu in menus]
      menu_ids_tuple = tuple(menu_ids)
      if menu_ids_tuple:
        query = """SELECT * FROM menus WHERE id IN %s"""
        cursor.execute(query, (menu_ids_tuple,))
        menu_details = cursor.fetchall()
      else:
        menu_details = []

      menu_list = []
      cursor.execute("""select merchantname from merchants where id = %s""",(merchantId,))
      merchant_name = cursor.fetchall()
      for menu in menu_details:
        menu_dict = {
          'id': menu['id'],
          'name': menu['name'],
          'cuisine': menu['cusine'],
          'location': merchant_name
        }
        menu_list.append(menu_dict)
      return  menu_list
  @classmethod
  def post_category(cls, merchantId, category, userId=None):
    try:
      connection, cursor = get_db_connection()
      
      _cn = category.get('categoryName')
      _posName = category.get('posName')
      _cd = category.get('categoryDescription')
      _status = category.get('categoryStatus',1)



      catGUID = uuid.uuid4() 
      data = (catGUID, merchantId, _cn, _posName, _cd, _status, userId)
      cursor.execute("""
        INSERT INTO categories (id, merchantid, categoryname, posname, categorydescription, status, created_by) 
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, data)
      connection.commit()

      resp = cls.get_category_by_id_str(catGUID)
      return resp
    except Exception as e:
      print("error: ", str(e))
      return False
  
  ############################################### PUT

  @classmethod
  def update_category_by_id(cls, id, category, userId=None):
    try:
      connection, cursor = get_db_connection()

      _cn = category.get('categoryName')
      _cd = category.get('categoryDescription')
      _posName = category.get('posName')
      _status = category.get('categoryStatus')
      
      data = (_cn, _posName, _cd, _status, userId, id)
      cursor.execute("""UPDATE categories 
      SET categoryname=%s, posname=%s, categorydescription=%s, status=%s, updated_by=%s, updated_datetime=CURRENT_TIMESTAMP
      WHERE id=%s""", data
      )
      connection.commit()
      return True
    except Exception as e:
      print(str(e))
      return False
  

  ############################################### DELETE

  @classmethod
  def delete_category_by_id(cls, id):
    try:
      connection, cursor = get_db_connection()

      pc_del = ProductsCategories.delete_category_item(categoryId=id)
      mc_del = MenuCategories.delete_menucategories(categoryId=id, platformType=1)
      cursor.execute("DELETE from categories WHERE id=%s", id) 

      connection.commit()
      return True
    except Exception as e:
      print(str(e))
      return str(e)