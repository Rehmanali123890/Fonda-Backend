

# local imports
from utilities.helpers import get_db_connection


class PlatformType():

    @classmethod
    def get(cls, id):
        try:
            connection, cursor = get_db_connection()
            cursor.execute("SELECT * FROM platformtype WHERE id=%s", (id))
            row = cursor.fetchone()
            return row
        except Exception as e:
            print("error: ", str(e))
            return False
