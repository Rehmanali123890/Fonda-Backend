
# local imports
from utilities.helpers import get_db_connection


class UsersLogin():

  
  @classmethod
  def delete_userslogin(cls, token):
    try:
      connection, cursor = get_db_connection()
      cursor.execute("DELETE FROM userslogin WHERE token=%s", token)
      connection.commit()
      return True
    except Exception as e:
      print(str(e))
      return False

