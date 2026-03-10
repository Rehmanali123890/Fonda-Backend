

# local imports
from utilities.helpers import get_db_connection
import config



class Metadata():

  ############################################### GET

  @classmethod
  def get_metadata_by_key(cls, key):
    try:
      connection, cursor = get_db_connection()
      cursor.execute("""SELECT * FROM metadata WHERE `key`=%s""", (key))
      row = cursor.fetchone()
      return row
    except Exception as e:
      print(str(e))
      return False

  ############################################### POST

  @classmethod
  def post_metadata(cls, key, value):
    try:
      connection, cursor = get_db_connection()
      cursor.execute("""INSERT INTO metadata (`key`, `value`) VALUES (%s,%s)""", (key, value))
      connection.commit()
      return True
    except Exception as e:
      print(str(e))
      return False
  

############################################### PUT

  @classmethod
  def put_metadata_by_key(cls, key, value):
    try:
      connection, cursor = get_db_connection()
      cursor.execute("""UPDATE metadata 
        SET `value`=%s, `updated_datetime`=CURRENT_TIMESTAMP 
        WHERE `key`=%s""", (value, key))
      connection.commit()
      return True
    except Exception as e:
      print(str(e))
      return False
  

  ############################################### OTHER

  @classmethod
  def update_metadata_by_key(cls, key, value):
    try:
      exists = cls.get_metadata_by_key(key)
      if exists:
        resp = cls.put_metadata_by_key(key, value)
      else:
        resp = cls.post_metadata(key, value)
      return resp
    except Exception as e:
      print(str(e))
      return False

  