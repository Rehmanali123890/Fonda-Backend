import uuid

# local imports
from utilities.helpers import get_db_connection


class ItemMappings():

    ############################################### GET

    @classmethod
    def get_item_details_by_platformitemid(cls, platformitemid):
        try:
            connection, cursor = get_db_connection()
            cursor.execute("""SELECT itemid FROM itemmappings WHERE platformitemid=%s""", (platformitemid))
            row = cursor.fetchone()
            if not row:
                return None

            cursor.execute("""SELECT * FROM items WHERE id=%s""", (row['itemid']))
            item = cursor.fetchone()
            return item
        except Exception as e:
            print("Error: ", str(e))
            return False

    @classmethod
    def get_item_details_by_platformaddonid(cls, platformitemid):
        try:
            connection, cursor = get_db_connection()
            cursor.execute("""SELECT addonoptionid FROM addonmappings WHERE platformaddonid=%s""", (platformitemid))
            row = cursor.fetchone()
            if not row:
                return None

            cursor.execute("""SELECT * FROM items WHERE id=%s""", (row['addonoptionid']))
            item = cursor.fetchone()
            return item
        except Exception as e:
            print("Error: ", str(e))
            return False
    @classmethod
    def get_addon_option_details_by_platformitemid(cls, platformitemid):
        try:
            connection, cursor = get_db_connection()
            cursor.execute("""SELECT * FROM itemmappings WHERE platformitemid=%s""", (platformitemid))
            row = cursor.fetchone()
            if not row:
                return None
            if row['addonoptionid'] is None:
                return None

            cursor.execute("""SELECT * FROM items WHERE id=%s""", (row['addonoptionid']))
            item = cursor.fetchone()
            return item
        except Exception as e:
            print("Error: ", str(e))
            return False

    ############################################### POST

    @classmethod
    def post_itemmappings(cls, merchantId, menuId, itemId, itemType, platformType, platformItemId=None, metadata=None,
                          categoryId=None, addonId=None, addonOptionId=None):
        try:
            connection, cursor = get_db_connection()

            '''
            itemType: 1.product, 2.addonoption
            '''

            data = (
            uuid.uuid4(), merchantId, menuId, categoryId, itemId, addonId, addonOptionId, itemType, platformType,
            platformItemId, metadata)
            cursor.execute("""INSERT INTO itemmappings
        (id, merchantid, menuid, categoryid, itemid, addonid, addonoptionid, itemtype, platformtype, platformitemid, metadata)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""", data)
            connection.commit()
            return True
        except Exception as e:
            print("ItemMappings Error: ", str(e))
            return False

    ############################################### DELETE

    @classmethod
    def delete_itemmappings(cls, merchantId=None, menuId=None, categoryId=None, itemId=None, addonId=None,
                            addonOptionId=None, platformType=None):
        try:
            connection, cursor = get_db_connection()

            conditions = []
            if merchantId: conditions.append(f'merchantid = "{merchantId}"')
            if menuId: conditions.append(f'menuid = "{menuId}"')
            if categoryId: conditions.append(f'categoryid = "{categoryId}"')
            if itemId: conditions.append(f'itemid = "{itemId}"')
            if addonId: conditions.append(f'addonid = "{addonId}"')
            if addonOptionId: conditions.append(f'addonoptionid = "{addonOptionId}"')
            if platformType is not None: conditions.append(f'platformtype = "{platformType}"')

            WHERE = ' AND '.join(conditions)
            if not WHERE:
                print("no condition is specified. threat to complete table data!")
                return False

            # if categoryId and itemId and addonId and platformType is not None:
            #     cursor.execute(
            #         """DELETE FROM itemmappings WHERE categoryid=%s AND itemid=%s AND addonid=%s AND platformtype=%s""",
            #         (categoryId, itemId, addonId, platformType))

            # elif menuId and categoryId and platformType is not None:
            #     cursor.execute("""DELETE FROM itemmappings WHERE menuid=%s AND categoryid=%s AND platformtype=%s""",
            #                    (menuId, categoryId, platformType))
            # elif itemId and addonId and platformType is not None:
            #     cursor.execute("""DELETE FROM itemmappings WHERE itemid=%s AND addonid=%s AND platformtype=%s""",
            #                    (itemId, addonId, platformType))
            # elif addonId and addonOptionId and platformType is not None:
            #     cursor.execute("""DELETE FROM itemmappings WHERE addonid=%s AND addonoptionid=%s AND platformtype=%s""",
            #                    (addonId, addonOptionId, platformType))
            # elif merchantId and categoryId and platformType is not None:
            #     cursor.execute("""DELETE FROM itemmappings WHERE merchantid=%s AND categoryid=%s AND platformtype=%s""",
            #                    (merchantId, categoryId, platformType))

            # elif merchantId and platformType is not None:
            #     cursor.execute("""DELETE FROM itemmappings WHERE merchantid=%s AND platformtype=%s""",
            #                    (merchantId, platformType))
            # elif menuId and platformType is not None:
            #     cursor.execute("""DELETE FROM itemmappings WHERE menuid=%s AND platformtype=%s""",
            #                    (menuId, platformType))
            # elif categoryId and platformType is not None:
            #     cursor.execute("""DELETE FROM itemmappings WHERE categoryid=%s AND platformtype=%s""",
            #                    (categoryId, platformType))
            # elif itemId and platformType is not None:
            #     cursor.execute("""DELETE FROM itemmappings WHERE itemid=%s AND platformtype=%s""",
            #                    (itemId, platformType))
            # elif addonId and platformType is not None:
            #     cursor.execute("""DELETE FROM itemmappings WHERE addonid=%s AND platformtype=%s""",
            #                    (addonId, platformType))
            # elif addonOptionId and platformType is not None:
            #     cursor.execute("""DELETE FROM itemmappings WHERE addonoptionid=%s AND platformtype=%s""",
            #                    (addonOptionId, platformType))
            # elif menuId:
            #     cursor.execute("""DELETE FROM itemmappings WHERE menuid=%s""", (menuId))

            cursor.execute(f"""DELETE FROM itemmappings WHERE {WHERE}""")

            print("delete item-mappings rows: ", str(cursor.rowcount))

            connection.commit()
            return True
        except Exception as e:
            print("ItemMappings Error: ", str(e))
            return False


    @classmethod
    def get_platform_id(cls, itemid, type=4):

        connection, cursor = get_db_connection()
        cursor.execute("""SELECT * FROM itemmappings WHERE platformtype=%s and itemid=%s""", (type, itemid))
        row = cursor.fetchone()
        if not row:
            return None
        return row
