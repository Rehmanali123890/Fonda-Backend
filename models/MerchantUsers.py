

# local imports
from utilities.errors import invalid, not_found, unhandled
from utilities.helpers import get_db_connection, success, publish_sns_message
import config


class MerchantUsers():

  @classmethod
  def get_merchantusers(cls, userId=None, merchantId=None):
    try:
      connection, cursor = get_db_connection()
      if merchantId:
        cursor.execute("""SELECT 
          users.id,
          users.firstname firstName,
          users.lastname lastName,
          users.username,
          users.email,
          users.address,
          users.status userStatus,
          users.phone,
          users.role 
          FROM users, merchantusers 
          WHERE merchantid = %s AND users.id = merchantusers.userid order by users.firstname""", (merchantId))
      elif userId:
        cursor.execute("""SELECT * FROM merchantusers WHERE userid=%s""", (userId))
      rows = cursor.fetchall()
      return rows
    except Exception as e:
      print(str(e))
      return False
  

  @classmethod
  def post_merchantusers(cls, userId, merchantId, ip_address, currentUser=None):
    try:


      connection, cursor = get_db_connection()
      data = (merchantId, userId, currentUser)
      
      cursor.execute("SELECT id FROM merchants WHERE id=%s", merchantId)
      mrow = cursor.fetchone()
      if not mrow:
        return invalid("merchant id not found")
      
      cursor.execute("SELECT * FROM users WHERE id=%s", userId)
      urow = cursor.fetchone()
      if not urow:
        return invalid("user id not found")
      
      # check if user is merchant-admin or merchant-user
      # roles 3 and 4 are only allowed to be assigned with 1 merchant at a time
      if urow["role"] == 4:
        rows = cls.get_merchantusers(userId=userId)
        if rows:
          return invalid("Only 1 merchant can be assigned to merchant standard user")

      cursor.execute("INSERT INTO merchantusers (merchantid, userid, created_by) VALUES (%s, %s, %s)", data) 
      connection.commit()

      # Triggering SNS - merchant.assign_user
      print("Triggering sns - merchant.assign_user ...")
      sns_msg = {
        "event": "merchant.assign_user",
        "body": {
          "merchantId": merchantId,
          "userId": currentUser,
          "updatedUserId": userId,
          "ipAddr": ip_address,
        }
      }
      logs_sns_resp = publish_sns_message(topic=config.sns_activity_logs, message=str(sns_msg), subject="merchant.assign_user")

      return success()
    except Exception as e:
      print(str(e))
      return unhandled()


  @classmethod
  def delete_merchantusers(cls, userId=None, merchantId=None):
    try:
      connection, cursor = get_db_connection()
      if userId and merchantId:
        cursor.execute("DELETE  FROM merchantusers WHERE merchantid=%s AND userid = %s", (merchantId, userId))  
      elif userId:
        cursor.execute("DELETE FROM merchantusers WHERE userid=%s", (userId))
      elif merchantId:
        cursor.execute("DELETE FROM merchantusers WHERE merchantid=%s", (merchantId))
      connection.commit()
      return True
    except Exception as e:
      print(str(e))
      return False

