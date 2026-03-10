import json
import uuid
from flask import jsonify

# local imports
from utilities.helpers import get_db_connection, success
from utilities.errors import unhandled




class VirtualMerchants():


    @staticmethod
    def get_virtual_merchant(id: str= None, merchantId: str = None, activeOnly: int = 0 , stream:int=0):
        try:
            connection, cursor = get_db_connection()

            conditions = []
            if id: conditions.append(f'id = "{id}"')
            if merchantId: conditions.append(f'merchantid = "{merchantId}"')
            if activeOnly: conditions.append(f'status = 1')
            if stream: conditions.append(f'is_stream_enabled = 1')

            WHERE = ' AND '.join(conditions)
            if not WHERE:
                WHERE = "1"

            cursor.execute(f"""SELECT * FROM virtualmerchants WHERE {WHERE}""")
            rows = cursor.fetchall()  

            return rows
        except Exception as e:
            print(f"error: {e}")
            return False
    

    @staticmethod
    def get_virtual_merchant_str(id=None, merchantId=None, activeOnly=0):
        rows = VirtualMerchants.get_virtual_merchant(id=id, merchantId=merchantId, activeOnly=activeOnly)
        if rows == False: return False
        data = list()
        for row in rows:
            data.append({
                "id": row["id"],
                "merchantId": row["merchantid"],
                "virtualName": row["virtualname"],
                "status": row["status"],
                "marketStatus": row["marketstatus"],
                "is_stream_enabled":row["is_stream_enabled"],
                "grubhubstream":row["grubhubstream"],
                "doordashstream":row["doordashstream"]
            })
        return data
    

    @classmethod
    def post_virtual_merchant(cls, merchantId, json_body, userId):
        try:
            connection, cursor = get_db_connection()

            vmId = str(uuid.uuid4())
            cursor.execute("""
                INSERT INTO virtualmerchants (id, merchantid, virtualname, status, marketstatus, created_by, updated_by)
                VALUES (%s,%s,%s,%s,%s,%s,created_by)
                """, (vmId, merchantId, json_body["virtualName"], 1, 1, userId)
            )
            connection.commit()

            data = cls.get_virtual_merchant_str(id=vmId)

            return success(jsonify({
                "message": "success",
                "status": 200,
                "data": data[0]
            }))
        except Exception as e:
            return unhandled(f"error: {e}")
    

    @classmethod
    def update_virtual_merchant(cls, id, json_body, userId):
        try:
            connection, cursor = get_db_connection()
            new_virtual_name = json_body.get("virtualName")

            cursor.execute("""
                        SELECT virtualname FROM virtualmerchants WHERE id = %s
                    """, (id,))
            result = cursor.fetchone()
            old_virtual_name = result['virtualname'] if result else None
            print(old_virtual_name)
            cursor.execute("""
                UPDATE virtualmerchants 
                SET
                    virtualname      = COALESCE(%s, virtualname),
                    updated_by       = %s,
                    updated_datetime = CURRENT_TIMESTAMP
                WHERE
                    id = %s
                """, (json_body.get("virtualName"), userId, id)
            )
            connection.commit()

            data = cls.get_virtual_merchant_str(id=id)
            if old_virtual_name and old_virtual_name != new_virtual_name:
                change_message = f"Virtual merchant name changed from '{old_virtual_name}' to '{new_virtual_name}'."
            else:
                change_message = "No change in virtual merchant name."
            return success(jsonify({
                "message": "success",
                "status": 200,
                "data": data[0],
                "name_changed": change_message
            }))
        except Exception as e:
            return unhandled(f"error: {e}")
    

    @classmethod
    def virtual_merchant_status_update(cls, id, status, userId):
        try:
            connection, cursor = get_db_connection()
            cursor.execute("""
                UPDATE virtualmerchants
                SET 
                    status           = %s, 
                    updated_by       = %s, 
                    updated_datetime = CURRENT_TIMESTAMP
                WHERE id = %s""", (status, userId, id)
            )
            connection.commit()
            return success()
        except Exception as e:
            return unhandled(f"error: {e}")

