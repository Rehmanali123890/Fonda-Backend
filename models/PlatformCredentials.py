import datetime
from flask import g
import uuid
import json

# local imports
from utilities.helpers import get_db_connection

class PlatformCredentials():

  @classmethod
  def get_platform_credentials(cls, merchantId, platformType=None):
    try:
      connection, cursor = get_db_connection()

      if platformType is None:
        cursor.execute("""SELECT * FROM platformcredentials WHERE merchantid = %s""", (merchantId))
      else:
        cursor.execute("""SELECT * FROM platformcredentials WHERE merchantid=%s AND platformtype=%s""", (merchantId, platformType))
      rows = cursor.fetchall()
      return rows
    except Exception as e:
      print("Error: ", str(e))
      return None
  
  ############################################### POST

  @classmethod
  def post_platform_credentails(cls, merchantId, credentials_list, userId):
    try:
      connection, cursor = get_db_connection()

      cursor.execute("""DELETE FROM platformcredentials WHERE merchantid=%s""", (merchantId))

      for credential in credentials_list:
        
        cursor.execute("""INSERT INTO platformcredentials 
          (id, merchantid, platformtype, email, password, created_by)
          VALUES (%s,%s,%s,%s,%s,%s)""", (uuid.uuid4(), merchantId, credential['platformtype'], credential['email'], credential['password'], userId))

      connection.commit()
      return True
    except Exception as e:
      print("Error: ", str(e))
      return False
  
  ############################################### DELETE
