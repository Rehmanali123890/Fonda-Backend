import uuid
import json

# local imports
import config
from utilities.helpers import get_db_connection

# rds config
rds_host  = config.db_host  
username = config.db_username
password = config.db_password
database_name = config.db_name 


class ServiceAvailability():

  @classmethod
  def post_serviceAvailability(cls, menuId, availability):
    try:
      connection, cursor = get_db_connection()
      
      for row in availability:
        startTime = row.get("startTime")
        endTime = row.get("endTime")
        weekDay = row.get("weekDay")

        guid = uuid.uuid4()
        data = (guid, menuId, startTime, endTime, weekDay)
        cursor.execute("""INSERT INTO serviceavailability (id, menuid, starttime, endtime, weekday)
          VALUES (%s,%s,%s,%s,%s)""", data)
      
      connection.commit()
      return True
      
    except Exception as e:
      print("Error: ", str(e))
      return False


  @classmethod
  def get_serviceAvailabilityByMenuId(cls, menuId):
    try:
      connection, cursor = get_db_connection()
      
      cursor.execute("""SELECT id, TIME_FORMAT(starttime, '%%H:%%i') startTime, TIME_FORMAT(endtime, '%%H:%%i') endTime, weekday weekDay FROM serviceavailability WHERE menuId=%s ORDER BY weekday ASC""", (menuId))
      rows = cursor.fetchall()
      return rows
      
    except Exception as e:
      print("Error: ", str(e))
      return False


  @classmethod
  def get_serviceAvailabilityByMenuId(cls, menuId):
    try:
      connection, cursor = get_db_connection()
      
      cursor.execute("""SELECT id, TIME_FORMAT(starttime, '%%H:%%i') startTime, TIME_FORMAT(endtime, '%%H:%%i') endTime, weekday weekDay FROM serviceavailability WHERE menuId=%s ORDER BY weekday ASC""", (menuId))
      rows = cursor.fetchall()
      return rows
      
    except Exception as e:
      print("Error: ", str(e))
      return False


  @classmethod
  def get_serviceAvailabilityId(cls, id):
    try:
      connection, cursor = get_db_connection()
      
      cursor.execute("""SELECT id, menuid menuId, CONVERT(starttime, CHAR) startTime, CONVERT(endtime, CHAR) endTime, weekday weekDay FROM serviceavailability WHERE id=%s""", (id))
      row = cursor.fetchone()
      return row
      
    except Exception as e:
      print("Error: ", str(e))
      return False


  @classmethod
  def put_serviceAvailabilityById(cls, id, startTime, endTime, weekDay):
    try:
      connection, cursor = get_db_connection()
      
      cursor.execute("""UPDATE serviceavailability SET starttime=%s, endtime=%s, weekday=%s WHERE id=%s""", (startTime, endTime, weekDay, id))
      connection.commit()
      return True
      
    except Exception as e:
      print("Error: ", str(e))
      return False


  @classmethod
  def delete_serviceAvailabilityByMenuId(cls, menuId):
    try:
      connection, cursor = get_db_connection()
      
      cursor.execute("""DELETE FROM serviceavailability WHERE menuId=%s""", (menuId))
      connection.commit()
      return True
      
    except Exception as e:
      print("Error: ", str(e))
      return False


  @classmethod
  def delete_serviceAvailabilityById(cls, id):
    try:
      connection, cursor = get_db_connection()
      
      cursor.execute("""DELETE FROM serviceavailability WHERE id=%s""", (id))
      connection.commit()
      return True
      
    except Exception as e:
      print("Error: ", str(e))
      return False

    