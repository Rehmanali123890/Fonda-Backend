

# local imports
from utilities.helpers import get_db_connection


class Websockets():

  ############################################### POST

  @classmethod
  def create_websocket(cls, connectionId, eventName, userId, role):
    try:
      connection, cursor = get_db_connection()
      cursor.execute("""SELECT * FROM websockets WHERE connectionid=%s""", (connectionId))
      row = cursor.fetchone()
      if row:
        cursor.execute("""UPDATE websockets SET eventname=%s, userid=%s, role=%s WHERE connectionid=%s""", (eventName, userId, role, connectionId))
      else:
        cursor.execute("""INSERT INTO websockets (connectionid, eventname, userid, role) VALUES (%s,%s,%s,%s)""", (connectionId, eventName, userId, role))
      connection.commit()
      return True
    except Exception as e:
      print(str(e))
      return False
  
  ############################################### GET

  @classmethod
  def get_websockets(cls, eventName="order", roles=None):
    try:
      connection, cursor = get_db_connection()
      if eventName and roles:
        cursor.execute("SELECT * FROM websockets WHERE eventname=%s AND role IN (" + roles + ")", (eventName))
      else:
        cursor.execute("SELECT * FROM websockets")
      result = cursor.fetchall()
      data_list = list(dict())
      for row in result:
        data_list.append({
          "connectionId": row['connectionid'],
          "eventName": row['eventname'],
          "userId": row['userid'],
          "role": row['role']
        })
      return data_list
    except Exception as e:
      print(str(e))
      return False
  
  @classmethod
  def get_websocketById(cls, id):
    try:
      connection, cursor = get_db_connection()
      cursor.execute("SELECT * FROM websockets WHERE connectionid=%s", (id))
      result = cursor.fetchone()
      if result:
        data = {
          "connectionId": result['connectionid'],
          "eventName": result['eventname'],
          "userId": result['userid'],
          "role": result['role']
        }
        return data
      else:
        return False
    except Exception as e:
      print(str(e))
      return False
  
  ############################################### DELETE

  @classmethod
  def delete_websocketById(cls, id):
    try:
      connection, cursor = get_db_connection()
      cursor.execute("DELETE FROM websockets WHERE connectionid=%s", (id))
      connection.commit()        
      return True
    except Exception as e:
      print(str(e))
      return False
  
  ############################################### OTHER

  @classmethod
  def get_connection_by_mid_and_eventname(cls, merchantId, eventName=None):
    try:
      eventName = "order" if eventName is None else eventName
      connection, cursor = get_db_connection()
      cursor.execute("SELECT * FROM websockets WHERE eventname=%s", (eventName))
      entries = cursor.fetchall()
      data_list = list(dict())
      for row in entries:
        if row['role'] == 1 or row['role'] == 2:
          data_list.append({
            "connectionId": row['connectionid'],
            "userId": row['userid']
          })
        else:
          userId = row['userid']
          cursor.execute("SELECT * FROM merchantusers WHERE merchantid=%s AND userid=%s", (merchantId, userId))
          result = cursor.fetchone()
          if result:
            data_list.append({
              "connectionId": row['connectionid'],
              "userId": row['userid']
            })
      return data_list
    except Exception as e:
      print(str(e))
      return False