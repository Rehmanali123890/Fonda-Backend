import uuid
import datetime
from dateutil.tz import gettz

# local imports
from utilities.helpers import get_db_connection


class OrderLogs():

    ############################################### GET

    @classmethod
    def get_order_logs(cls, request):
        try:
            connection, cursor = get_db_connection()
            order_external_reference=request.args.get('orderExternalRefrence')
            cursor.execute(f"""
            SELECT order_state,message,gmail_message_id,
            @timezone := (SELECT @timezone := merchants.timezone FROM dashboard.merchants inner join dashboard.orders 
            where merchants.id = orders.merchantid 
            and orders.orderexternalreference='{order_external_reference}' LIMIT 1) as timezone,
            CONCAT ( date_format(convert_tz(event_datetime, '+00:00', @timezone), '%m-%d-%Y %H:%i:%S'), ' (', @timezone, ')' ) orderdatetime FROM dashboard.orderprocessinglogs 
            where orderprocessinglogs.order_external_reference='{order_external_reference}' """)
            rows = cursor.fetchall()

            return rows
        except Exception as e:
            print("Error: ", str(e))
            return False

    ############################################### POST




