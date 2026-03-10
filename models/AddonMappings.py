import uuid

# local imports
from utilities.helpers import get_db_connection


class AddonMappings():

    @classmethod
    def post_addonmappings(cls, merchantId, menuId, addonId, addonOptionId=None, platformType=None,
                           platformAddonId=None, metadata=None):
        try:
            connection, cursor = get_db_connection()

            data = (uuid.uuid4(), merchantId, menuId, addonId, addonOptionId, platformType, platformAddonId, metadata)
            cursor.execute("""INSERT INTO addonmappings
        (id, merchantid, menuid, addonid, addonoptionid, platformtype, platformaddonid, metadata)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""", data)
            connection.commit()
            return True
        except Exception as e:
            print("AddonMappings Error: ", str(e))
            return False


    @classmethod
    def delete_addonmappings(cls, merchantId=None, menuId=None, platformType=None):
        try:
            connection, cursor = get_db_connection()

            if merchantId and menuId and platformType is not None:
              cursor.execute("""DELETE FROM addonmappings WHERE merchantid=%s AND platformtype=%s AND menuid=%s""",
                             (merchantId, platformType, menuId))
            elif merchantId and platformType is not None:
                cursor.execute("""DELETE FROM addonmappings WHERE merchantid=%s AND platformtype=%s""",
                               (merchantId, platformType))
            elif menuId and platformType is not None:
                cursor.execute("""DELETE FROM itemmappings WHERE menuid=%s AND platformtype=%s""",
                               (menuId, platformType))

            elif menuId:
                cursor.execute("""DELETE FROM itemmappings WHERE menuid=%s""", (menuId))

            print("delete addon-mappings rows: ", str(cursor.rowcount))

            connection.commit()
            return True
        except Exception as e:
            print("AddonMappings Error: ", str(e))
            return False


    @classmethod
    def get_clover_addonmapping(cls, productid, addonoptionid):
            connection, cursor = get_db_connection()
            cursor.execute("""SELECT * FROM addonmappings WHERE platformtype=4 AND addonoptionid=%s""", (addonoptionid))
            mappings = cursor.fetchall()
            resp = None
            for mapping in mappings:
                cursor.execute("""SELECT * FROM productsaddons WHERE productid=%s AND addonid=%s""", (productid, mapping['addonid']))
                row = cursor.fetchone()
                if row:
                    resp = mapping
                    break

            return resp


    @classmethod
    def get_square_addonmapping(cls, productid, addonoptionid):
            connection, cursor = get_db_connection()
            cursor.execute("""SELECT * FROM addonmappings WHERE platformtype=11 AND addonoptionid=%s""", (addonoptionid))
            mappings = cursor.fetchall()
            resp = None
            for mapping in mappings:
                cursor.execute("""SELECT * FROM productsaddons WHERE productid=%s AND addonid=%s""", (productid, mapping['addonid']))
                row = cursor.fetchone()
                if row:
                    resp = mapping
                    break

            return resp