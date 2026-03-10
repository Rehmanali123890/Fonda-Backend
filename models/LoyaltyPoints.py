import json
import random
import string
import uuid

import boto3
from flask import Flask, render_template, flash, request
from botocore.exceptions import ClientError
from flask.json import jsonify
import datetime
from dateutil.relativedelta import relativedelta

from models.Merchants import Merchants
from utilities.helpers import get_db_connection, validateLoginToken, success, is_float, \
    publish_sns_message, open_stripe_connect_account, normalize_string, create_log_data
from utilities.errors import unhandled, not_found, invalid
import config


class LoyaltyPoints():

    @classmethod
    def add_remove_points(cls, request, userId):
        try:
            
            ip_address = request.get("ip_address",'')
            if request["remove"] == 1:
                points = 0-int(request["points"])
                check_points = LoyaltyPoints.get_total_points(request["merchantId"])
                check_points = check_points['point'] - int(request["points"])

                if check_points < 0:
                    return False
            else:
                points = int(request["points"])

            id = uuid.uuid4()
            connection, cursor = get_db_connection()
            data = (id, points, request["remarks"], request["merchantId"], userId)
            cursor.execute("""INSERT INTO loyaltypoints
                                      (id, point, remarks, merchantid, createdby)
                                      VALUES (%s,%s,%s,%s,%s)""", data)
            connection.commit()

            print("Triggering sns - loyalty.points ...")
            sns_msg = {
                "event": "loyalty.points",
                "body": {
                    "merchantId": request["merchantId"],
                    "userId": userId,
                    "status": request["remove"],
                    "points": points,
                    "ipAddr":ip_address
                }
            }
            point_message = "added" if request["remove"]==0 else "removed"
            create_log_data(level='[INFO]',
                            Message=f"{points} loyalty points has been {point_message},  IP Address: {ip_address}",
                            functionName="add_remove_points")
            logs_sns_resp = publish_sns_message(topic=config.sns_activity_logs, message=str(sns_msg),
                                                subject="loyalty.points")

            return True

        except Exception as e:
            print(str(e))
            return False

    @classmethod
    def update_points(cls, request):
        try:
            points = int(request["points"])
            connection, cursor = get_db_connection()
            data = (points, request["remarks"], request["pointId"])
            cursor.execute("update loyaltypoints set point =%s, remarks=%s where id=%s", data)
            connection.commit()

        except Exception as e:
            print(str(e))
            return False

    @classmethod
    def get_all_points(cls, merchantId):
        try:
            merchant = Merchants.get_merchant_by_id(merchantId)
            where = f'merchantid = "{merchantId}"'
            timezone =f""" "{merchant['timezone']}" """

            connection, cursor = get_db_connection()
            cursor.execute(f"""SELECT l.*, u.firstname, u.lastname, 
            CONCAT ( date_format(convert_tz(l.createddatetime, '+00:00', {timezone}), '%m-%d-%Y %H:%i:%S'), '(', {timezone}, ')' ) as datetime
            FROM loyaltypoints l left join users u on u.id=l.createdby WHERE {where}  order by l.createddatetime desc""")
            print(cursor._last_executed)
            return cursor.fetchall()

        except Exception as e:
            print(str(e))
            return False

    @classmethod
    def get_total_points(cls, merchantId):
        try:
            connection, cursor = get_db_connection()
            cursor.execute("""SELECT sum(point) as point FROM loyaltypoints WHERE merchantid=%s""",
                           (merchantId))
            return cursor.fetchone()

        except Exception as e:
            print(str(e))
            return False


    @classmethod
    def delete_points(cls, merchantId, pointId):
        try:
            connection, cursor = get_db_connection()
            cursor.execute("""Delete from loyaltypoints WHERE merchantid=%s and id=%s""",
                           (merchantId, pointId))
            return connection.commit()

        except Exception as e:
            print(str(e))
            return False
