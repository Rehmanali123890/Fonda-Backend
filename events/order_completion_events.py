from app import app
import json
import requests
# from flask import Flask, jsonify
import boto3
import logging
import datetime

# local imports
import config
from models.Items import Items
from models.ActivityLogs import ActivityLogs
from models.Merchants import Merchants
from models.Orders import Orders
from models.Users import Users
from models.Websockets import Websockets
from models.Platforms import Platforms
from utilities.helpers import get_db_connection, send_android_notification_api

# init flask in order to keep events from crashing
# app = Flask(__name__)
# app.config.from_object(config)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def order_completion(event, context):
    try:
        with app.app_context():
            ### loggings
            current_time = datetime.datetime.now().time()
            logger.info("Your cron function order completion starts running at " + str(current_time))

            ### database config
            connection, cursor = get_db_connection()
            cursor.execute("SELECT * FROM orders WHERE (UNIX_TIMESTAMP(NOW())-UNIX_TIMESTAMP(orderdatetime))/3600>=2 AND status = 0")
            all_pending_orders = cursor.fetchall()
            cursor.execute("UPDATE orders set status=7 WHERE (UNIX_TIMESTAMP(NOW())-UNIX_TIMESTAMP(orderdatetime))/3600>=2 AND status=0 AND scheduled=0",
                           ())

            cursor.execute("UPDATE orders set status=8 WHERE (UNIX_TIMESTAMP(NOW())-UNIX_TIMESTAMP(orderdatetime))/3600>=2 AND status=10 AND scheduled=0",
                           ())

            cursor.execute(
                "UPDATE ordershistory set status=7 WHERE (UNIX_TIMESTAMP(NOW())-UNIX_TIMESTAMP(orderdatetime))/3600>=2 AND status=0 AND scheduled=0",
                ())

            cursor.execute(
                "UPDATE ordershistory set status=8 WHERE (UNIX_TIMESTAMP(NOW())-UNIX_TIMESTAMP(orderdatetime))/3600>=2 AND status=10 AND scheduled=0",
                ())

            connection.commit()

            if cursor.rowcount > 0:

                ws_client = boto3.client("apigatewaymanagementapi", endpoint_url=config.orders_websocket_url)
                connections = Websockets.get_websockets()
                if type(connections) is list:
                    for connection in connections:
                        connectionId = connection.get("connectionId")
                        eventName = connection.get("eventName")
                        if eventName == "order":
                            try:
                                response = ws_client.post_to_connection(ConnectionId=connectionId, Data=json.dumps({
                                    "event": "order.status",
                                    "body": {
                                        "order": None
                                    }
                                }))
                            except Exception as e:
                                print("Error: ", str(e))
                            pass
                        elif eventName == "android.order":
                            try:
                                response = send_android_notification_api(deviceId=connectionId, subject="order.status",datatype=1)
                                print(response.text)
                                if response.status_code >= 200 and response.status_code < 300:
                                    print("posted notification to android")
                                else:
                                    print(f"Unable to posting notification to android , Device id: {connectionId}, Function Name: order_completion()")
                            except Exception as e:
                                print("Error: ", str(e))

            print("Rows Affected: " + str(cursor.rowcount))
            print("Finished")
            print("all orders to change",all_pending_orders)
            for order in all_pending_orders:
                resp = Orders.get_order_details_str(order["id"])
                try:
                  sns_resp = {
                    "event": "order.status",
                    "unchanged": None,
                    "body": {
                      "order": resp
                    }
                  }
                  print("order status change sns resp")
                  print(sns_resp)
                  sns_client = boto3.client('sns')
                  sns_client.publish(TopicArn=config.sns_order_notification, Message=str(sns_resp), Subject="order.status")
                except Exception as e:
                  print("SNS ERROR")
                  print(str(e))

    except Exception as e:
        print("ERROR: ", str(e))
