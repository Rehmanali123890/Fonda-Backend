import uuid

# local imports
from utilities.helpers import get_db_connection
import config



class OrderMessages():

    ############################################### GET

    @classmethod
    def get_message(cls, message_id):
        try:
            connection, cursor = get_db_connection()
            cursor.execute("""SELECT * FROM ordermessages WHERE `messageid`=%s""", (message_id))
            return cursor.fetchone()
        except Exception as e:
            print(str(e))
            return False

    ############################################### POST

    @classmethod
    def post_message(cls, message_id):
        try:
            connection, cursor = get_db_connection()
            cursor.execute("""INSERT INTO ordermessages (`id`,`messageid`) VALUES (%s,%s)""", (str(uuid.uuid4()), message_id))
            connection.commit()
            return True
        except Exception as e:
            print(str(e))
            return False

