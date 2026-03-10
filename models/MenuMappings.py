import uuid
import json

# local imports
from utilities.helpers import get_db_connection


class MenuMappings():

  ############################################### POST

  @classmethod
  def post_menumappings(cls, merchantId, menuId, platformType, platformMenuId=None, mappingStatus=0, metaData=None, userId=None):
    try:
      connection, cursor = get_db_connection()

      if metaData:
        metaData = json.dumps(metaData)
      
      cursor.execute("""SELECT * FROM platformtype WHERE id=%s""", (platformType))
      row = cursor.fetchone()
      if row:
        mappingGuid = uuid.uuid4()
        data = (mappingGuid, menuId, merchantId, platformType, platformMenuId, mappingStatus, metaData, userId)
        cursor.execute("""INSERT INTO menumappings 
          (id, menuid, merchantid, platformtype, platformmenuid, mappingstatus, metadata, created_by)
          VALUES(%s,%s,%s,%s,%s,%s,%s,%s)""", data)
        connection.commit()
        return mappingGuid
      else:
        print("Platform Type <" + str(platformType) + "> Not found")
        return False
    except Exception as e:
      print("Error: ", str(e))
      return False
      
  ############################################### GET

  @classmethod
  def get_menumappings(cls, menuId=None, merchantId=None, platformType=None, platformMenuId=None, mappingStatus=None, mappingId=None):
    try:
      connection, cursor = get_db_connection()

      if merchantId and platformType:
        cursor.execute("""SELECT * FROM menumappings WHERE merchantid=%s AND platformtype=%s""", (merchantId, platformType))
      elif menuId and platformType:
        cursor.execute("""SELECT * FROM menumappings WHERE menuid=%s AND platformtype=%s""", (menuId, platformType))
      elif menuId:
        cursor.execute("""SELECT * FROM menumappings WHERE menuid=%s""", (menuId))
      elif mappingId:
        cursor.execute("""SELECT * FROM menumappings WHERE id=%s""", (mappingId))
      else:
        cursor.execute("""SELECT * FROM menumappings""")

      rows = cursor.fetchall()
      return rows
    except Exception as e:
      print("Error: ", str(e))
      return False
  

  @classmethod
  def get_menumappings_str(cls, menuId=None, merchantId=None, platformType=None, platformMenuId=None, mappingStatus=None):
    try:
      data = list()
      rows = cls.get_menumappings(menuId=menuId, merchantId=merchantId, platformType=platformType, platformMenuId=platformMenuId, mappingStatus=mappingStatus)
      if rows:
        for row in rows:
          data.append({
            "id": row["id"],
            "menuId": row["menuid"],
            "merchantId": row["merchantid"],
            "platformType": row["platformtype"],
            "platformMenuId": row["platformmenuid"],
            "mappingStatus": row["mappingstatus"],
            "metadata": json.loads(row["metadata"]) if row['metadata'] else None
          })
      return data
    except Exception as e:
      print("Error: ", str(e))
      return False


  @classmethod
  def new_get_menumappings_str(cls, menuId=None, merchantId=None, platformType=None, platformMenuId=None, mappingStatus=None):
    try:
      connection, cursor = get_db_connection()
      data = list()
      rows = cls.get_menumappings(menuId=menuId, merchantId=merchantId, platformType=platformType, platformMenuId=platformMenuId, mappingStatus=mappingStatus)
      if rows:
        for row in rows:
          #data.append({
          #  "id": row["id"],
          #  "platformType": row["platformtype"],
          #})
          
          platform_type = row['platformtype']
          cursor.execute("""SELECT * FROM platformtype WHERE id=%s""", (platform_type))
          platform = cursor.fetchall()
          for type in platform:
            data.append(type['type'])
          
      return data
    except Exception as e:
      print("Error: ", str(e))
      return False
  
  ############################################### DELETE

  @classmethod
  def delete_menumappings(cls, mappingId=None, menuId=None, merchantId=None, platformType=None, platformMenuId=None):
    try:
      connection, cursor = get_db_connection()

      # delete menu-mappings by menuId
      if menuId and platformType:
        cursor.execute("""DELETE FROM menumappings WHERE menuid=%s AND platformtype=%s""", (menuId, platformType))
      elif menuId:
        cursor.execute("""DELETE FROM menumappings WHERE menuid=%s""", (menuId))
      elif mappingId:
        cursor.execute("""DELETE FROM menumappings WHERE id=%s""", (mappingId))
        
      deletedRows = cursor.rowcount
      connection.commit()
      return True 
      
    except Exception as e:
      print("Error: ", str(e))
      return False

