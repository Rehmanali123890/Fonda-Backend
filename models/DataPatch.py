

# local imports
from utilities.helpers import get_db_connection


class DataPatch():

  ############################################### GET

  @classmethod
  def fix_pending_locked_orders(cls):
    try:
      connection, cursor = get_db_connection()
      cursor.execute("""update orders set status=7 WHERE status = 0 AND locked = 1""")
      cursor.execute("""update ordershistory set status=7 WHERE status = 0 AND locked = 1""")
      connection.commit()
      return True

    except Exception as e:
      print("Error: ", str(e))
      return False
  
  ############################################### POST

