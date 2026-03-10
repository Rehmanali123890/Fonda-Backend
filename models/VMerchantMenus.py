import uuid

from utilities.errors import invalid, unhandled
from utilities.helpers import get_db_connection, success


class VMerchantMenus():


    @classmethod
    def get_virtual_merchants_by_menuid(cls, menuId):
        try:
            connection, cursor = get_db_connection()

            cursor.execute("""
                SELECT 
                    virtualmerchants.id id,
                    virtualmerchants.virtualname virtaulName,
                    virtualmerchants.status status
                FROM vmerchantmenus, virtualmerchants
                WHERE 
                    vmerchantmenus.vmerchantid = virtualmerchants.id AND 
                    vmerchantmenus.menuid = %s
            """, (menuId))
            rows = cursor.fetchall()
            return rows
        except Exception as e:
            return False
    

    @classmethod
    def get_menus_by_virtual_merchant_id(cls, vMerchantId):
        try:
            connection, cursor = get_db_connection()

            cursor.execute("""
                SELECT 
                    menus.id id,
                    menus.merchantid merchantId,
                    menus.name name,
                    menus.description description,
                    menus.status status
                FROM vmerchantmenus, menus
                WHERE
                    vmerchantmenus.menuid = menus.id AND
                    vmerchantmenus.vmerchantid = %s
            """, (vMerchantId))
            rows = cursor.fetchall()
            return rows
        except Exception as e:
            return False
    

    @classmethod
    def delete_virtual_merchant_menu(cls, menuId):
        try:
            connection, cursor = get_db_connection()
            deleted_rows = list()

            if menuId:
                cursor.execute("""SELECT * FROM vmerchantmenus WHERE menuid = %s""", (menuId))
                deleted_rows = cursor.fetchall()

                cursor.execute("""DELETE FROM vmerchantmenus WHERE menuid = %s""", (menuId))
            
            connection.commit()
            
            return deleted_rows
        except Exception as e:
            return False


    @classmethod
    def assign_menu_to_vmerchant(cls, merchantId, vMerchantId, menuId, userId):
        try:
            connection, cursor = get_db_connection()

            cursor.execute("SELECT * FROM vmerchantmenus WHERE menuid = %s", (menuId))
            row = cursor.fetchone()
            if row: return invalid("menu is already assigned to another virtual merchant!")

            cursor.execute("SELECT * FROM virtualmerchants WHERE id = %s", (vMerchantId))
            vmerchant_row = cursor.fetchone()

            cursor.execute("SELECT * FROM menus WHERE id = %s", (menuId))
            menu_row = cursor.fetchone()

            if (vmerchant_row and 
                menu_row and 
                vmerchant_row["merchantid"] == merchantId and 
                menu_row["merchantid"] == merchantId):
                
                row_guid = str(uuid.uuid4())
                cursor.execute("""
                    INSERT INTO vmerchantmenus (id, merchantid, vmerchantid, menuid, created_by)
                    VALUES (%s,%s,%s,%s,%s)
                    """, (row_guid, merchantId, vMerchantId, menuId, userId))
                connection.commit()

                return success()
            else:
                return invalid("invalid request data")
        except Exception as e:
            return unhandled(f"error: {e}")

    @classmethod
    def assign_new_menu_to_vmerchant(cls, merchantId, vMerchantId, menuId, userId):
        try:
            connection, cursor = get_db_connection()

            row_guid = str(uuid.uuid4())
            cursor.execute("""
                               INSERT INTO vmerchantmenus (id, merchantid, vmerchantid, menuid, created_by)
                               VALUES (%s,%s,%s,%s,%s)
                               """, (row_guid, merchantId, vMerchantId, menuId, userId))
            connection.commit()

        except Exception as e:
                return unhandled(f"error: {e}")

    @classmethod
    def remove_menu_from_vmerchant(cls, merchantId, vMerchantId, menuId):
        try:
            connection, cursor = get_db_connection()

            cursor.execute("SELECT * FROM vmerchantmenus WHERE vmerchantid = %s AND menuid = %s", (vMerchantId, menuId))
            row = cursor.fetchone()

            if  row["merchantid"] != merchantId:
                return invalid("invalid request data, merchantId does not match!")

            # delete from menumappings by menuid: in order to keep main merchant menus secure from overlapping
            cursor.execute("DELETE FROM menumappings WHERE menuid = %s", (menuId))

            # delete from menucategories by merchantid = vMerchantid
            cursor.execute("DELETE FROM menucategories WHERE merchantid = %s", (vMerchantId))

            # delete from itemmappings by vMerchantId
            cursor.execute("DELETE FROM itemmappings WHERE merchantid = %s", (vMerchantId))

            # delete from addonmappings by vMerchantId
            cursor.execute("DELETE FROM addonmappings WHERE merchantid = %s", (vMerchantId))

            # delete from vmerchantmenus
            cursor.execute("DELETE FROM vmerchantmenus WHERE vmerchantid = %s AND menuid = %s", (vMerchantId, menuId))

            connection.commit()
            
            return success()
        except Exception as e:
            return unhandled(f"error: {e}")