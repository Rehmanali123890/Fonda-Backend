from app import app
import json
from flask import jsonify
import boto3
import logging
import datetime
from decimal import *
from dateutil.tz import gettz
from models.Categories import Categories
from models.Addons import Addons
from models.Menus import Menus
# local imports
import config
from models.FinancialLogs import FinancialLogs
from models.Items import Items
from models.ActivityLogs import ActivityLogs
from models.Merchants import Merchants
from models.Orders import Orders
from models.Users import Users
from models.Websockets import Websockets
from models.Platforms import Platforms
from utilities.helpers import closeDbconnection, get_db_connection, is_float, openDbconnection
from models.ErrorLogs import ErrorLogs
from models.AuditLogs import AuditLogs
from models.VirtualMerchants import VirtualMerchants
from utilities.slack_helpers import send_error_message_to_slack_webhook, send_merchant_status_message_to_slack_webhook, \
    send_menu_update_message_to_slack_webhook

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def activity_logs_to_history_table(event, context):
    try:
        ### loggings
        current_time = datetime.datetime.now().time()
        logger.info("Your cron function (Activity Logs To History Table) starts running at " + str(current_time))

        ### database config
        connection, cursor = openDbconnection()

        cursor.execute("call sp_ActivityLogsToHistoryTables();")
        connection.commit()
        print("Rows Affected: " + str(cursor.rowcount))

        ### audit logs to history
        try:
            print("(2) Audit Logs to History...")

            past_date = datetime.datetime.utcnow() - datetime.timedelta(days=7)

            cursor.execute("""
        INSERT IGNORE INTO `auditlogshistory` 
          SELECT * FROM `auditlogs` WHERE eventdatetime <= %s;
        """, (past_date))
            cursor.execute("""DELETE FROM `auditlogs` WHERE eventdatetime <= %s;""", (past_date))
            connection.commit()

            print("Rows Affected: " + str(cursor.rowcount))
        except Exception as e:
            print("ERROR: ", str(e))

        print("Finished")
    except Exception as e:
        print("ERROR: ", str(e))
    finally:
        closeDbconnection(connection)


def error_logs_to_history_table(event, context):
    try:
        ### loggings
        current_time = datetime.datetime.now().time()
        logger.info("Your cron function (Error Logs To History Table) starts running at " + str(current_time))

        ### database config
        connection, cursor = openDbconnection()

        past_date = datetime.datetime.utcnow() - datetime.timedelta(days=7)

        cursor.execute("""
      INSERT IGNORE INTO `errorlogshistory` 
        SELECT * FROM `errorlogs` WHERE errordatetime <= %s;
      """, (past_date))

        cursor.execute("""DELETE FROM `errorlogs` WHERE errordatetime <= %s;""", (past_date))

        connection.commit()

        print("Rows Affected: " + str(cursor.rowcount))
        print("Finished")

    except Exception as e:
        print("ERROR: ", str(e))
    finally:
        closeDbconnection(connection)


def send_notification_to_websocket(message=None):
    try:
        ws_client = boto3.client("apigatewaymanagementapi", endpoint_url=config.orders_websocket_url)

        if not message:
            message = {
                "event": "activity_logs.entry"
            }

        # get socket connections ids
        connections = Websockets.get_websockets(roles="1,2")

        if type(connections) is list:
            for connection in connections:
                connectionId = connection.get("connectionId")
                try:
                    response = ws_client.post_to_connection(ConnectionId=connectionId, Data=json.dumps(message))
                except Exception as e:
                    print("Error: ", str(e))

        return True
    except Exception as e:
        print(str(e))
        return False


def activity_logs_event(event, context):
    with app.app_context():
        print("--------------------- - --------------------------")
        connection, cursor = get_db_connection()
        for record in event['Records']:

            subject = record.get("Sns").get("Subject")
            message = eval(record.get("Sns").get("Message"))



            print(subject)
            print(message)

            merchantId = message.get("body").get("merchantId")
            userId = message.get("body").get("userId")

            merchant_details = dict()
            user_details = dict()

            if merchantId:
                merchant_details = Merchants.get_merchant_or_virtual_merchant(merchantId)
            if userId:
                if userId != "System":
                    user_details = Users.get_user_by_id(userId)




            ### merchant events
            if subject == "merchant.create":
                event_details = message.get("body").get("eventDetails")
                resp = ActivityLogs.post_activity_logs(
                    userid=userId,
                    username=user_details['username'],
                    merchantid=merchantId,
                    merchantname=merchant_details['merchantname'],
                    eventtype=message.get("body").get("eventType"),
                    eventname=message.get("body").get("eventName"),
                    eventdetails=event_details
                )
                if not resp:
                    print("error: while storing activity logs details in database")
                    exit()

            elif subject == "merchant.update":
                event_details = message.get("body").get("eventDetails")
                event_name = message.get("body").get("eventName")
                if userId == "System":
                    userId = ''
                    username="System"
                else:
                    username=user_details['username']
                resp = ActivityLogs.post_activity_logs(
                    userid=userId,
                    username=username,
                    merchantid=merchantId,
                    merchantname=merchant_details['merchantname'],
                    eventtype="activity",
                    eventname=event_name if event_name else subject,
                    eventdetails=event_details
                )

                if event_details and 'busymode' in event_details:
                    currentDateTime = datetime.datetime.now(datetime.timezone.utc).astimezone(
                        gettz('US/Pacific')).strftime(
                        "%m-%d-%Y %H:%M (%Z)")
                    resp2 = send_menu_update_message_to_slack_webhook(
                        webhook_url=config.slack_tablet_warning_logs_channel_webhook,
                        username=user_details['username'],
                        merchantName=merchant_details.get('merchantname'),
                        eventDetails=event_details,
                        eventName="Merchant Mode Change",
                        source="Android Kitchen App",
                        eventDateTime=currentDateTime
                    )
                if not resp:
                    print("error: while storing activity logs details in database")
                    exit()

            elif subject == "merchant.stream_platform_status_change":
                resp = ActivityLogs.post_activity_logs(
                    userid=userId,
                    username=user_details['username'],
                    merchantid=merchantId,
                    merchantname=merchant_details['merchantname'],
                    eventtype="activity",
                    eventname=subject,
                    eventdetails=message.get("body").get("eventDetails")
                )
                if not resp:
                    print("error: while storing activity logs details in database")
                    exit()
            elif subject == "merchant.stream_manual_menu_sync":
                resp = ActivityLogs.post_activity_logs(
                    userid=userId,
                    username=user_details['username'],
                    merchantid=merchantId,
                    merchantname=merchant_details['merchantname'],
                    eventtype="activity",
                    eventname=subject,
                    eventdetails=message.get("body").get("eventDetails")
                )
                if not resp:
                    print("error: while storing activity logs details in database")
                    exit()
            elif subject == "virtual_merchant.created":
                event_details = message.get("body").get("eventDetails")
                event_name = message.get("body").get("eventName")
                resp = ActivityLogs.post_activity_logs(
                    userid=userId,
                    username=user_details['username'],
                    merchantid=merchantId,
                    merchantname=merchant_details['merchantname'],
                    eventtype="activity",
                    eventname=event_name if event_name else subject,
                    eventdetails=event_details
                )
                if not resp:
                    print("error: while storing activity logs details in database")
                    exit()

            elif subject == "virtual_merchant.update":
                event_details = message.get("body").get("eventDetails")
                event_name = message.get("body").get("eventName")
                resp = ActivityLogs.post_activity_logs(
                    userid=userId,
                    username=user_details['username'],
                    merchantid=merchantId,
                    merchantname=merchant_details['merchantname'],
                    eventtype="activity",
                    eventname=event_name if event_name else subject,
                    eventdetails=event_details
                )
                if not resp:
                    print("error: while storing activity logs details in database")
                    exit()
            elif subject == "virtualmerchant.status_change":
                event_details = message.get("body").get("eventDetails")
                event_name = message.get("body").get("eventName")
                resp = ActivityLogs.post_activity_logs(
                    userid=userId,
                    username=user_details['username'],
                    merchantid=merchantId,
                    merchantname=merchant_details['merchantname'],
                    eventtype="activity",
                    eventname=event_name if event_name else subject,
                    eventdetails=event_details
                )
                if not resp:
                    print("error: while storing activity logs details in database")
                    exit()
            elif subject == "menu.update_hours":
                event_details = message.get("body").get("eventDetails")
                event_name = message.get("body").get("eventName")
                resp = ActivityLogs.post_activity_logs(
                    userid=userId,
                    username=user_details['username'],
                    merchantid=merchantId,
                    merchantname=merchant_details['merchantname'],
                    eventtype="activity",
                    eventname=event_name if event_name else subject,
                    eventdetails=event_details
                )
                if not resp:
                    print("error: while storing activity logs details in database")
                    exit()
            elif subject == "storefront.slug_url":
                event_details = message.get("body").get("eventDetails")
                event_name = message.get("body").get("eventName")
                resp = ActivityLogs.post_activity_logs(
                    userid=userId,
                    username=user_details['username'],
                    merchantid=merchantId,
                    merchantname=merchant_details['merchantname'],
                    eventtype="activity",
                    eventname=event_name if event_name else subject,
                    eventdetails=event_details
                )
                if not resp:
                    print("error: while storing activity logs details in database")
                    exit()
            elif subject == "esperdevice.connectivity_status":
                event_details = message.get("body").get("eventDetails")
                event_name = message.get("body").get("eventName")
                resp = ActivityLogs.post_activity_logs(
                    userid=userId,
                    username=user_details['username'],
                    merchantid=merchantId,
                    merchantname=merchant_details['merchantname'],
                    eventtype="activity",
                    eventname=event_name if event_name else subject,
                    eventdetails=event_details
                )
                if not resp:
                    print("error: while storing activity logs details in database")
                    exit()
            elif subject == "stripe.connect":
                resp = ActivityLogs.post_activity_logs(
                    userid=userId,
                    username=user_details['username'],
                    merchantid=merchantId,
                    merchantname=merchant_details['merchantname'],
                    eventtype="activity",
                    eventname=subject,
                    eventdetails=f"Stripe connected for {merchant_details['merchantname']}"
                )
                if not resp:
                    print("error: while storing activity logs details in database")
                    exit()
            elif subject == 'item.delete':
                ipAddr = message.get("body").get("ipAddr")
                event_details = f'Item <{message.get("body")["item_details"]["itemName"]}> deleted ,  IP Address: {ipAddr}'
                resp = ActivityLogs.post_activity_logs(
                    userid=userId,
                    username=user_details['username'],
                    merchantid=merchantId,
                    merchantname=merchant_details['merchantname'],
                    eventtype="Activity",
                    eventname=message.get("event"),
                    eventdetails=event_details
                )
                if not resp:
                    print("error: while storing activity log details in database")
                    exit()
            elif subject == 'item.hours_change':
                event_details = message.get("body").get("event_details")
                resp = ActivityLogs.post_activity_logs(
                    userid=userId,
                    username=user_details['username'],
                    merchantid=merchantId,
                    merchantname=merchant_details['merchantname'],
                    eventtype="Activity",
                    eventname=message.get("event"),
                    eventdetails=event_details
                )
                if not resp:
                    print("error: while storing activity log details in database")
                    exit()
            elif subject == 'item.image_delete':
                ipAddr = message.get("body").get("ipAddr")
                event_details = f'Item <{message["item_details"]["itemName"]}> image deleted,  IP Address: {ipAddr}'
                resp = ActivityLogs.post_activity_logs(
                    userid=userId,
                    username=user_details['username'],
                    merchantid=merchantId,
                    merchantname=merchant_details['merchantname'],
                    eventtype="Activity",
                    eventname=message.get("event"),
                    eventdetails=event_details
                )
                if not resp:
                    print("error: while storing activity log details in database")
                    exit()
            
            elif subject == 'item.image_update':
                ipAddr = message.get("body").get("ipAddr")
                event_details = f'Item <{message["item_details"]["itemName"]}> image added,  IP Address: {ipAddr}'
                resp = ActivityLogs.post_activity_logs(
                    userid=userId,
                    username=user_details['username'],
                    merchantid=merchantId,
                    merchantname=merchant_details['merchantname'],
                    eventtype="Activity",
                    eventname=message.get("event"),
                    eventdetails=event_details
                )
                if not resp:
                    print("error: while storing activity log details in database")
                    exit()
                    
            elif subject == "category.update":
                categoryId = message.get("body").get("categoryId")
                ipAddr = message.get("body").get("ipAddr")
                o_cat_det = message.get("body").get("old_category_details")
                n_cat_det = Categories.get_category_by_id(categoryId)
                eventDetails = ""
                if o_cat_det["categoryname"] != n_cat_det["categoryname"]:
                    eventDetails += f"Category name is changed from <{o_cat_det['categoryname']}> to <{n_cat_det['categoryname']}> \n "
                if o_cat_det["posname"] != n_cat_det["posname"]:
                    eventDetails += f"Posname is changed from <{o_cat_det['posname']}> to <{n_cat_det['posname']}> \n "
                if o_cat_det["categorydescription"] != n_cat_det["categorydescription"]:
                    eventDetails += f"Category description is changed from <{o_cat_det['categorydescription']}> to <{n_cat_det['categorydescription']}> \n "
                if o_cat_det["status"] != n_cat_det["status"]:
                    eventDetails += f"Category status is changed from <{o_cat_det['status']}> to <{n_cat_det['status']}> \n "

                if eventDetails != "":
                    eventDetails = f" --- Category <{n_cat_det['categoryname']}> --- \n " + eventDetails

                eventDetails += f", IP address:{ipAddr}"
                resp = ActivityLogs.post_activity_logs(
                    userid=userId,
                    username=user_details['username'],
                    merchantid=merchantId,
                    merchantname=merchant_details['merchantname'],
                    eventtype="Activity",
                    eventname=message.get("event"),
                    eventdetails=eventDetails
                )
                if not resp:
                    print("error: while storing activity log details in database")
                    exit()
                    
            elif subject == "category.create":
                categoryId = message.get("body").get("categoryId")
                ipAddr = message.get("body").get("ipAddr")
                category_details = Categories.get_category_by_id(categoryId)
                eventDetails = f"Category <{category_details['categoryname']}> is created, IP address:{ipAddr}"
                resp = ActivityLogs.post_activity_logs(
                    userid=userId,
                    username=user_details['username'],
                    merchantid=merchantId,
                    merchantname=merchant_details['merchantname'],
                    eventtype="Activity",
                    eventname=message.get("event"),
                    eventdetails=eventDetails
                )
                if not resp:
                    print("error: while storing activity log details in database")
                    exit()
                    
            
            # category.assign_item & category.unassign_item
            elif subject in ("category.assign_item", "category.unassign_item"):
                categoryId = message.get("body").get("categoryId")
                itemId = message.get("body").get("itemId")
                ipAddr = message.get("body").get("ipAddr")

                category_details = Categories.get_category_by_id(categoryId)
                item_details = Items.get_item_by_id(itemId)

                if subject == "category.assign_item":
                    eventDetails = f"Item <{item_details.get('itemName')}> is assigned to category <{category_details['categoryname']}>"
                else:
                    eventDetails = f"Item <{item_details.get('itemName')}> is removed from category <{category_details['categoryname']}>"

                eventDetails += f", IP address:{ipAddr}"
                resp = ActivityLogs.post_activity_logs(
                    userid=userId,
                    username=user_details['username'],
                    merchantid=merchantId,
                    merchantname=merchant_details['merchantname'],
                    eventtype="Activity",
                    eventname=message.get("event"),
                    eventdetails=eventDetails
                )
                if not resp:
                    print("error: while storing activity log details in database")
                    exit()
            
            elif subject == 'category.delete':
                ipAddr = message.get("body").get("ipAddr")
                event_details = f'Category <{message.get("body")["category_details"]["categoryname"]}> deleted , IP address:{ipAddr}'
                resp = ActivityLogs.post_activity_logs(
                    userid=userId,
                    username=user_details['username'],
                    merchantid=merchantId,
                    merchantname=merchant_details['merchantname'],
                    eventtype="Activity",
                    eventname=message.get("event"),
                    eventdetails=event_details
                )
                if not resp:
                    print("error: while storing activity log details in database")
                    exit()
            
            elif subject == 'category.hours_change':
                event_details = message.get("body").get("event_details")
                resp = ActivityLogs.post_activity_logs(
                    userid=userId,
                    username=user_details['username'],
                    merchantid=merchantId,
                    merchantname=merchant_details['merchantname'],
                    eventtype="Activity",
                    eventname=message.get("event"),
                    eventdetails=event_details
                )
                if not resp:
                    print("error: while storing activity log details in database")
                    exit()
            
            elif subject == 'addon.delete':
                ipAddr = message.get("body").get("ipAddr")
                event_details = f'Addon <{message.get("body")["addon_details"]["addonName"]}> deleted ,  IP Address: {ipAddr}'
                resp = ActivityLogs.post_activity_logs(
                    userid=userId,
                    username=user_details['username'],
                    merchantid=merchantId,
                    merchantname=merchant_details['merchantname'],
                    eventtype="Activity",
                    eventname=message.get("event"),
                    eventdetails=event_details
                )
                if not resp:
                    print("error: while storing activity log details in database")
                    exit()
                    
            elif subject in ("addon.assign_option", "addon.unassign_option"):
                addonId = message.get("body").get("addonId")
                itemId = message.get("body").get("itemId")
                ipAddr = message.get("body").get("ipAddr")

                addon_details = Addons.get_addon_by_id(addonId)
                item_details = Items.get_item_by_id_fk(itemId)

                if subject == "addon.assign_option":
                    eventDetails = f"Addon-Option <{item_details['itemname']}> is assigned to Addon <{addon_details['addonname']}>"
                else:
                    eventDetails = f"Addon-Option <{item_details['itemname']}> is removed from Addon <{addon_details['addonname']}>"
                eventDetails += f", IP address:{ipAddr}"
                resp = ActivityLogs.post_activity_logs(
                    userid=userId,
                    username=user_details['username'],
                    merchantid=merchantId,
                    merchantname=merchant_details['merchantname'],
                    eventtype="Activity",
                    eventname=message.get("event"),
                    eventdetails=eventDetails
                )
                if not resp:
                    print("error: while storing activity log details in database")
                    exit()
                    
            # addon.update
            elif subject == "addon.update":
                addonId = message.get("body").get("addonId")
                ipAddr = message.get("body").get("ipAddr")
                oad_details = message.get("body").get("old_addon_details")
                nad_details = Addons.get_addon_by_id_str(addonId)
                eventDetails = ""
                if oad_details['addonName'] != nad_details['addonName']:
                    eventDetails += f"Addon name is changed from <{oad_details['addonName']}> to <{nad_details['addonName']}> \n "
                if oad_details['posName'] != nad_details['posName']:
                    eventDetails += f"Addon posname is changed from <{oad_details['posName']}> to <{nad_details['posName']}> \n "
                if oad_details['addonDescription'] != nad_details['addonDescription']:
                    eventDetails += f"Addon description is changed from <{oad_details['addonDescription']}> to <{nad_details['addonDescription']}> \n "
                if oad_details['minPermitted'] != nad_details['minPermitted']:
                    eventDetails += f"Addon minimum selected options is changed from <{oad_details['minPermitted']}> to <{nad_details['minPermitted']}> \n "
                if oad_details['maxPermitted'] != nad_details['maxPermitted']:
                    eventDetails += f"Addon maximum selected options is changed from <{oad_details['maxPermitted']}> to <{nad_details['maxPermitted']}> \n "
                
                istatus = {0: "Disable", 1: "Enable"}
                if oad_details['status'] != nad_details['status']:
                    eventDetails += f"Addon status is changed to <{istatus[nad_details['status']]}> \n "
                
                if eventDetails != "":
                    eventDetails = f" --- Addon <{nad_details['addonName']}> --- \n " + eventDetails

                eventDetails += f", IP address:{ipAddr}"
                
                resp = ActivityLogs.post_activity_logs(
                    userid=userId,
                    username=user_details['username'],
                    merchantid=merchantId,
                    merchantname=merchant_details['merchantname'],
                    eventtype="Activity",
                    eventname=message.get("event"),
                    eventdetails=eventDetails
                )
                if not resp:
                    print("error: while storing activity log details in database")
                    exit()
                    
            elif subject == "addon.create":
                addonId = message.get("body").get("addonId")
                ipAddr = message.get("body").get("ipAddr")
                addon_details = Addons.get_addon_by_id(addonId)
                eventDetails = f"Addon <{addon_details['addonname']}> is created, IP address:{ipAddr}"
                resp = ActivityLogs.post_activity_logs(
                    userid=userId,
                    username=user_details['username'],
                    merchantid=merchantId,
                    merchantname=merchant_details['merchantname'],
                    eventtype="Activity",
                    eventname=message.get("event"),
                    eventdetails=eventDetails
                )
                if not resp:
                    print("error: while storing activity log details in database")
                    exit()
                    
                    
            elif subject == "menu.create":
                menuName = message.get("body").get("menuName")
                ipAddr = message.get("body").get("ipAddr")
                eventDetails = f"Menu <{menuName}> is created, IP address:{ipAddr}"
                resp = ActivityLogs.post_activity_logs(
                    userid=userId,
                    username=user_details['username'],
                    merchantid=merchantId,
                    merchantname=merchant_details['merchantname'],
                    eventtype="Activity",
                    eventname=message.get("event"),
                    eventdetails=eventDetails
                )
                if not resp:
                    print("error: while storing activity log details in database")
                    exit()
            
            elif subject == 'menu.delete':
                ipAddr = message.get("body").get("ipAddr")
                event_details = f'Menu <{message.get("body")["menu_details"]["name"]}> deleted, IP address: {ipAddr}'
                resp = ActivityLogs.post_activity_logs(
                    userid=userId,
                    username=user_details['username'],
                    merchantid=merchantId,
                    merchantname=merchant_details['merchantname'],
                    eventtype="Activity",
                    eventname=message.get("event"),
                    eventdetails=event_details
                )
                if not resp:
                    print("error: while storing activity log details in database")
                    exit()
                    
            # menu.update
            elif subject == "menu.update":
                menuId = message.get("body").get("menuId")
                ipAddr = message.get("body").get("ipAddr")
                old_menu_details = message.get("body").get("old_menu_details")
                new_menu_details = Menus.get_menu_by_id_fk(menuId)
                eventDetails = ""
                if old_menu_details["name"] != new_menu_details["name"]:
                    eventDetails += f"Menu name is updated from <{old_menu_details['name']}> to <{new_menu_details['name']}>. \n "
                if old_menu_details["description"] != new_menu_details["description"]:
                    eventDetails += f"Menu description is updated from <{old_menu_details['description']}> to <{new_menu_details['description']}>. \n "
                if eventDetails != "":
                    eventDetails = f" ---Menu <{new_menu_details['name']}> --- \n " + eventDetails

                    eventDetails += f", IP adress:{ipAddr}"
                    resp = ActivityLogs.post_activity_logs(
                        userid=userId,
                        username=user_details['username'],
                        merchantid=merchantId,
                        merchantname=merchant_details['merchantname'],
                        eventtype="activity",
                        eventname=subject,
                        eventdetails=eventDetails
                    )
                    if not resp:
                        print("error: while storing activity logs details in database")
                        exit()
                    
            elif subject == "menu.status":
                menuId = message.get("body").get("menuId")
                status = message.get("body").get("status")
                ipAddr = message.get("body").get("ipAddr")
                menu_detail = Menus.get_menu_by_id_fk(menuId)
                if status == 0:
                    eventDetails = f"The status of '{menu_detail['name']}' has changed to deactivated."
                else:
                    eventDetails = f"The status of '{menu_detail['name']}' has changed to activated."

                eventDetails += f"IP adress:{ipAddr}"
                resp = ActivityLogs.post_activity_logs(
                    userid=userId,
                    username=user_details['username'],
                    merchantid=merchantId,
                    merchantname=merchant_details['merchantname'],
                    eventtype="activity",
                    eventname=subject,
                    eventdetails=eventDetails
                )
                    
            # menu.assign_category & menu.unassign_category
            elif subject in ("menu.assign_category", "menu.unassign_category"):
                menuId = message.get("body").get("menuId")
                categoryId = message.get("body").get("categoryId")
                ipAddr = message.get("body").get("ipAddr")
                menu_details = Menus.get_menu_by_id_fk(menuId)
                category_details = Categories.get_category_by_id(categoryId)

                if subject == "menu.assign_category":
                    eventDetails = f"Category <{category_details['categoryname']}> is assigned to menu <{menu_details['name']}>"
                else:
                    eventDetails = f"Category <{category_details['categoryname']}> is removed from menu <{menu_details['name']}>"
                eventDetails += f",IP Address{ipAddr}"
                resp = ActivityLogs.post_activity_logs(
                    userid=userId,
                    username=user_details['username'],
                    merchantid=merchantId,
                    merchantname=merchant_details['merchantname'],
                    eventtype="activity",
                    eventname=subject,
                    eventdetails=eventDetails
                )
                
            elif subject == "storefront.enabled":
                ipAddr = message.get("body").get("ipAddr")
                resp = ActivityLogs.post_activity_logs(
                    userid=userId,
                    username=user_details['username'],
                    merchantid=merchantId,
                    merchantname=merchant_details['merchantname'],
                    eventtype="activity",
                    eventname=subject,
                    eventdetails=f"Storefront for merchant <{merchant_details['merchantname']}> enabled, IP address:{ipAddr}"
                )
                if not resp:
                    print("error: while storing activity logs details in database")
                    exit()

            elif subject == "storefront.disabled":
                ipAddr = message.get("body").get("ipAddr")
                resp = ActivityLogs.post_activity_logs(
                    userid=userId,
                    username=user_details['username'],
                    merchantid=merchantId,
                    merchantname=merchant_details['merchantname'],
                    eventtype="activity",
                    eventname=subject,
                    eventdetails=f"Storefront for merchant <{merchant_details['merchantname']}> disabled , IP address:{ipAddr}"
                )
                if not resp:
                    print("error: while storing activity logs details in database")
                    exit()


            elif subject == "merchant.assign_user":

                updatedUserId = message.get("body").get("updatedUserId")
                ipAddr = message.get("body").get("ipAddr")
                assigned_user_details = Users.get_user_by_id(updatedUserId)
                eventDetails = f"User <{assigned_user_details['username']}> is Assigned to Merchant"

                eventDetails += f",  IP Address: {ipAddr}"
                
                resp = ActivityLogs.post_activity_logs(
                    userid=userId,
                    username=user_details['username'],
                    merchantid=merchantId,
                    merchantname=merchant_details['merchantname'],
                    eventtype="activity",
                    eventname=subject,
                    eventdetails=eventDetails
                )
                if not resp:
                    print("error: while storing activity log details in database")
                    exit()


            elif subject == "merchant.unassign_user":

                updatedUserId = message.get("body").get("updatedUserId")
                ipAddr = message.get("body").get("ipAddr")
                unassigned_user_details = Users.get_user_by_id(updatedUserId)
                eventDetails = f"User <{unassigned_user_details['username']}> is Un-Assigned to Merchant"
                eventDetails += f",  IP Address: {ipAddr}"
                resp = ActivityLogs.post_activity_logs(
                    userid=userId,
                    username=user_details['username'],
                    merchantid=merchantId,
                    merchantname=merchant_details['merchantname'],
                    eventtype="activity",
                    eventname=subject,
                    eventdetails=eventDetails
                )
                if not resp:
                    print("error: while storing activity log details in database")
                    exit()


            elif subject == "merchant.update_hours":
                event_details = message.get("body").get("eventDetails")
                resp = ActivityLogs.post_activity_logs(
                    userid=userId,
                    username=user_details['username'],
                    merchantid=merchantId,
                    merchantname=merchant_details['merchantname'],
                    eventtype="activity",
                    eventname=subject,
                    eventdetails=event_details
                )
                if not resp:
                    print("error: while storing activity logs details in database")
                    exit()
            elif subject == "TermsAndCondition_activity_logs":
                event_details = message.get("body").get("eventDetails")
                resp = ActivityLogs.post_activity_logs(
                    userid=userId,
                    username=user_details['username'],
                    merchantid=merchantId,
                    merchantname=merchant_details['merchantname'],
                    eventtype=message.get("body").get("eventType"),
                    eventname=message.get("body").get("eventName"),
                    eventdetails=event_details
                )
                if not resp:
                    print("error: while storing activity logs details in database")
                    exit()
            elif subject == "merchant.status_change":
                ipAddr = message.get("body").get("ipAddr")
                pauseTime = int(message.get("body").get("pauseTime")) if is_float(
                    message.get("body").get("pauseTime")) else None
                caller = message.get("body").get("caller")
                username = user_details.get("username") if isinstance(user_details, dict) else caller
                # Merchant Tablet is Offline in Esper. Merchant Status changed to Pause in Dashboard.
                merchant_details['pauseStarted_datetime'] = merchant_details['pauseStarted_datetime'].astimezone(
                    gettz('US/Pacific')).strftime("%m-%d-%Y %H:%M (%Z)")
                if caller == "esper" and merchant_details["marketstatus"] == 0:
                    event_details = "Merchant tablet is Offline in Esper. Merchant status is changed to PAUSED in dashboard."
                    vevent_details = "Merchant tablet is Offline in Esper. Virtual Merchant status is changed to PAUSED in dashboard."
                else:
                    if merchant_details["marketstatus"] == 0:
                        eventtype = "PAUSED"
                        if merchant_details['caller'] == "dashboard":
                            event_details = f"Merchant status is changed to PAUSED from dashboard at {merchant_details['pauseStarted_datetime']}"
                            vevent_details = "Virtual Merchant status is changed to PAUSED"
                        else:
                            if merchant_details['pause_reason'] == "":
                                event_details = f"Merchant status is changed to PAUSED from Tablet at {merchant_details['pauseStarted_datetime']} for {merchant_details['pauseTime_duration']}"
                                vevent_details = "Virtual Merchant status is changed to PAUSED"
                            else:

                                event_details = f"Merchant status is changed to PAUSED from Tablet at {merchant_details['pauseStarted_datetime']} for {merchant_details['pauseTime_duration']}. Reason: {merchant_details['pause_reason']}"
                                vevent_details = "Virtual Merchant status is changed to PAUSED"



                    else:
                        eventtype = "RESUMED"
                        event_details = "Merchant status is changed to RESUMED"
                        vevent_details = "Virtual Merchant status is changed to RESUMED"

                # main merchant notification
                if pauseTime and pauseTime > 0 and pauseTime < 1440 and merchant_details["marketstatus"] == 0:
                    event_details += f" for {pauseTime} minutes"
                    vevent_details += f" for {pauseTime} minutes"

                event_details += f", IP address: {ipAddr}"
                resp = ActivityLogs.post_activity_logs(
                    userid=userId,
                    username=username,
                    merchantid=merchantId,
                    merchantname=merchant_details.get('merchantname'),
                    eventtype="activity",
                    eventname=subject,
                    eventdetails=event_details
                )

                resp2 = send_merchant_status_message_to_slack_webhook(
                    webhook_url=config.slack_merchant_channel_webhook,
                    merchantName=merchant_details.get('merchantname'),
                    username=username,
                    eventDetails=event_details
                )

                ### v-merchant notification
                # virtaul_merchants = VirtualMerchants.get_virtual_merchant(merchantId=merchantId, activeOnly=1)
                #
                # for vm in virtaul_merchants:
                #   resp3 = ActivityLogs.post_activity_logs(
                #     userid=userId,
                #     username=username,
                #     merchantid=merchantId,
                #     merchantname=vm["virtualname"],
                #     eventtype="activity",
                #     eventname=subject,
                #     eventdetails=vevent_details
                #   )
                #
                #   resp4 = send_merchant_status_message_to_slack_webhook(
                #     webhook_url=config.slack_merchant_channel_webhook,
                #     merchantName=vm["virtualname"],
                #     username=username,
                #     eventDetails=vevent_details
                #   )
                #
                #
                # # send notification to websocket
                # resp5 = send_notification_to_websocket()

            elif subject == "merchant.stream_status_change":
                ipAddr = message.get("body").get("ipAddr")
                streamstatus='Active'if merchant_details['is_stream_enabled']==1 else 'Inactive'
                resp = ActivityLogs.post_activity_logs(
                    userid=userId,
                    username=user_details.get('username'),
                    merchantid=merchantId,
                    merchantname=merchant_details.get('virtualname') if 'isVirtual' in merchant_details else merchant_details.get('merchantname'),
                    eventtype="activity",
                    eventname=subject,
                    eventdetails=f"Merchant stream status is {streamstatus} , IP address:{ipAddr}"
                )
                if not resp:
                    print("error: while storing activity logs details in database")
                    exit()
            elif subject == "merchant.battery_status_change":

                batteryPercent = int(message.get("body").get("batteryPercent")) if is_float(
                    message.get("body").get("batteryPercent")) else None
                chargingStatus = message.get("body").get("chargingStatus")

                event_details = None
                if chargingStatus == 'not-charging' and batteryPercent <= config.battery_alert_threshold:
                    # event_details = "Battery not charging and below threshold"
                    event_details = "Low Battery Alert - Tablet battery is running low at {}%".format(batteryPercent)
                elif chargingStatus == 'charging' and batteryPercent <= config.battery_alert_threshold:
                    # event_details = "Battery charging and below threshold"
                    event_details = "Charging Started - Tablet device is now connected to a charger"

                resp = ActivityLogs.post_activity_logs(
                    userid=userId,
                    username=user_details['username'],
                    merchantid=merchantId,
                    merchantname=merchant_details.get('merchantname'),
                    eventtype="activity",
                    eventname="Battery Alert",
                    eventdetails=event_details
                )

                currentDateTime = datetime.datetime.now(datetime.timezone.utc).astimezone(gettz('US/Pacific')).strftime(
                    "%m-%d-%Y %H:%M (%Z)")
                resp2 = send_menu_update_message_to_slack_webhook(
                    webhook_url=config.slack_tablet_warning_logs_channel_webhook,
                    username=user_details['username'],
                    merchantName=merchant_details.get('merchantname'),
                    eventDetails=event_details,
                    eventName="Battery Alert",
                    source="Android Kitchen App",
                    eventDateTime=currentDateTime
                )
            elif subject == "merchant.tablet_offline_status":
                offlineDurationMessage = message.get("body").get("offlineDurationMessage")

                resp = ActivityLogs.post_activity_logs(
                    userid=userId,
                    username=user_details['username'],
                    merchantid=merchantId,
                    merchantname=merchant_details.get('merchantname'),
                    eventtype="activity",
                    eventname="Merchant Offline Alert",
                    eventdetails=offlineDurationMessage
                )
                currentDateTime = datetime.datetime.now(datetime.timezone.utc).astimezone(gettz('US/Pacific')).strftime(
                    "%m-%d-%Y %H:%M (%Z)")
                resp2 = send_menu_update_message_to_slack_webhook(
                    webhook_url=config.slack_tablet_warning_logs_channel_webhook,
                    username=user_details['username'],
                    merchantName=merchant_details.get('merchantname'),
                    eventDetails=offlineDurationMessage,
                    eventName="Merchant Offline Alert",
                    source="Android Kitchen App",
                    eventDateTime=currentDateTime
                )
            elif subject == 'GMB_activity_logs':
                event_details = message.get("body").get("eventDetails")
                resp = ActivityLogs.post_activity_logs(
                    userid=userId,
                    username=user_details['username'],
                    merchantid=merchantId,
                    merchantname=merchant_details['merchantname'],
                    eventtype=message.get("body").get("eventType"),
                    eventname=message.get("body").get("eventName"),
                    eventdetails=event_details
                )
                if not resp:
                    print("error: while storing activity log details in database")
                    exit()
            elif subject == 'Promo_activity_logs':
                event_details = message.get("body").get("eventDetails")
                resp = ActivityLogs.post_activity_logs(
                    userid=userId,
                    username=user_details['username'],
                    merchantid=merchantId,
                    merchantname=merchant_details['merchantname'],
                    eventtype=message.get("body").get("eventType"),
                    eventname=message.get("body").get("eventName"),
                    eventdetails=event_details
                )
                if not resp:
                    print("error: while storing activity log details in database")
                    exit()

            elif subject == "platform.connect":

                platformId = message.get("body").get("platformId")
                ipAddr = message.get("body").get("ipAddr")
                platform_details = Platforms.get_platform_by_id(platformId)

                cursor.execute("SELECT * FROM platformtype WHERE id=%s", (platform_details['platformtype']))
                row = cursor.fetchone()
                if not row:
                    exit()

                eventDetails = f"Platform <{row['type']}> is Connected, IP address:{ipAddr}"

                resp = ActivityLogs.post_activity_logs(
                    userid=userId,
                    username=user_details['username'],
                    merchantid=merchantId,
                    merchantname=merchant_details.get('merchantname') or merchant_details.get('virtualname'),
                    eventtype="activity",
                    eventname=subject,
                    eventdetails=eventDetails
                )
                if not resp:
                    print("error: while storing activity log details in database")
                    exit()

            elif subject == "merchant.battery_status_change":
                batteryPercent = int(message.get("body").get("batteryPercent")) if is_float(
                    message.get("body").get("batteryPercent")) else None
                chargingStatus = message.get("body").get("chargingStatus")

                event_details = None
                if chargingStatus == 'not-charging' and batteryPercent <= config.battery_alert_threshold:
                    # event_details = "Battery not charging and below threshold"
                    event_details = "Low Battery Alert - Tablet battery is running low at {}%".format(batteryPercent)
                elif chargingStatus == 'charging' and batteryPercent <= config.battery_alert_threshold:
                    # event_details = "Battery charging and below threshold"
                    event_details = "Charging Started - Tablet device is now connected to a charger"

                resp = ActivityLogs.post_activity_logs(
                    userid=userId,
                    username=user_details['username'],
                    merchantid=merchantId,
                    merchantname=merchant_details.get('merchantname'),
                    eventtype="activity",
                    eventname="Battery Alert",
                    eventdetails=event_details
                )

                currentDateTime = datetime.datetime.now(datetime.timezone.utc).astimezone(gettz('US/Pacific')).strftime(
                    "%m-%d-%Y %H:%M (%Z)")
                resp2 = send_menu_update_message_to_slack_webhook(
                    webhook_url=config.slack_tablet_warning_logs_channel_webhook,
                    username=user_details['username'],
                    merchantName=merchant_details.get('merchantname'),
                    eventDetails=event_details,
                    eventName="Battery Alert",
                    source="Android Kitchen App",
                    eventDateTime=currentDateTime
                )


            elif subject == "platform.disconnect":

                platformType = message.get("body").get("platformType")
                ipAddr = message.get("body").get("ipAddr")

                cursor.execute("SELECT * FROM platformtype WHERE id=%s", (platformType))
                row = cursor.fetchone()
                if not row:
                    exit()

                eventDetails = f"Platform <{row['type']}> is Disconnected,  IP address:{ipAddr}"

                resp = ActivityLogs.post_activity_logs(
                    userid=userId,
                    username=user_details['username'],
                    merchantid=merchantId,
                    merchantname=merchant_details.get('merchantname') or merchant_details.get('virtualname'),
                    eventtype="activity",
                    eventname=subject,
                    eventdetails=eventDetails
                )
                if not resp:
                    print("error: while storing activity log details in database")
                    exit()


            elif subject == "platform.sync_type_change":

                platformId = message.get("body").get("platformId")
                ipAddr = message.get("body").get("ipAddr")
                platform_details = Platforms.get_platform_by_id(platformId)
                auto_sync = "ON" if platform_details['synctype'] == 1 else "OFF"

                cursor.execute("SELECT * FROM platformtype WHERE id=%s", (platform_details['platformtype']))
                row = cursor.fetchone()
                if not row:
                    exit()

                eventDetails = f"{row['type']} Auto Sync preference is changed to {auto_sync}, IP address:{ipAddr}"

                resp = ActivityLogs.post_activity_logs(
                    userid=userId,
                    username=user_details['username'],
                    merchantid=merchantId,
                    merchantname=merchant_details.get('merchantname') or merchant_details.get('virtualname'),
                    eventtype="activity",
                    eventname=subject,
                    eventdetails=eventDetails
                )


            elif subject == "platform.manual_sync":

                downloadMenu = message.get("body").get("downloadMenu")
                platformId = message.get("body").get("platformId")
                platform_details = Platforms.get_platform_by_id(platformId)

                cursor.execute("SELECT * FROM platformtype WHERE id=%s", (platform_details['platformtype']))
                row = cursor.fetchone()
                if not row:
                    exit()

                if downloadMenu is not None and int(downloadMenu) == 1:
                    eventDetails = f"Downloading the menu from {row['type']}"
                else:
                    eventDetails = f"Uploading the menu to {row['type']}"

                resp = ActivityLogs.post_activity_logs(
                    userid=userId,
                    username=user_details['username'],
                    merchantid=merchantId,
                    merchantname=merchant_details.get('merchantname') or merchant_details.get('virtualname'),
                    eventtype="activity",
                    eventname=subject,
                    eventdetails=eventDetails
                )


            ### platform_credentials events
            elif subject == "platform_credentials.update":
                resp = ActivityLogs.post_activity_logs(
                    userid=userId,
                    username=user_details['username'],
                    merchantid=merchantId,
                    merchantname=merchant_details['merchantname'],
                    eventtype="activity",
                    eventname=subject,
                    eventdetails="Platform (Grubhub) credentails are updated"
                )


            
            elif subject == "item.create":
                itemId = message.get("body").get("itemId")
                ipAddr = message.get("body").get("ipAddr")
                item_details = Items.get_item_by_id(itemId)
                eventDetails = f"Item <{item_details['itemName']}> with price <{item_details['itemUnitPrice']}> is created , IP address:{ipAddr}"
                print('log_events', eventDetails)
                resp = ActivityLogs.post_activity_logs(
                    userid=userId,
                    username=user_details['username'],
                    merchantid=merchantId,
                    merchantname=merchant_details['merchantname'],
                    itemid=itemId,
                    itemname=item_details['itemName'],
                    eventtype="activity",
                    eventname=subject,
                    eventdetails=eventDetails
                )
                if not resp:
                    print("error: while storing activity log details in database")
                    exit()
                    
            ### item events
            elif subject == "item.status_change":
                itemId = message.get("body").get("itemId")
                ipAddr = message.get("body").get("ipAddr")
                item_details = Items.get_item_by_id_fk(itemId)

                itemStatus = "Enable" if item_details['status'] == 1 else "Disable"

                if item_details['status'] == 0:
                    if item_details['pause_type'] is not None:
                        if item_details['pause_type'] == 'today':
                            itemStatus = f"{itemStatus} ({item_details['pause_type']}) - From {item_details['pause_time'].astimezone(gettz('US/Pacific')).strftime('%m-%d-%Y %H:%M (%Z)')} Till {item_details['resume_time'].astimezone(gettz('US/Pacific')).strftime('%m-%d-%Y %H:%M (%Z)')}"
                        else:
                            itemStatus = f"{itemStatus} ({item_details['pause_type']}) - From {item_details['pause_time'].astimezone(gettz('US/Pacific')).strftime('%m-%d-%Y %H:%M (%Z)')}"

                eventDetails = f"Item <{item_details['itemname']}> status changed to: {itemStatus}, IP address:{ipAddr}"
                print('log_events', eventDetails)
                resp = ActivityLogs.post_activity_logs(
                    userid=userId,
                    username=user_details['username'],
                    merchantid=merchantId,
                    merchantname=merchant_details['merchantname'],
                    itemid=itemId,
                    itemname=item_details['itemname'],
                    eventtype="activity",
                    eventname=subject,
                    eventdetails=eventDetails
                )
                if not resp:
                    print("error: while storing activity log details in database")
                    exit()

                # send notification to websocket
                resp = send_notification_to_websocket()
                if not resp:
                    print("error: while sending notification to websocket connections")
                    exit()


            elif subject == "item.price_upload_csv":
                ipAddr = message.get("body").get("ipAddr")
                resp = ActivityLogs.post_activity_logs(
                    userid=userId,
                    username=user_details['username'],
                    merchantid=merchantId,
                    merchantname=merchant_details['merchantname'],
                    eventtype="activity",
                    eventname=subject,
                    eventdetails=f"Items prices are uploaded via csv file, IP address:{ipAddr}"
                )


            elif subject == "item.update":
                itemId = message.get("body").get("itemId")
                ipAddr = message.get("body").get("ipAddr")
                oldItemStatus = message.get("body").get("oldItemStatus")
                oit_details = message.get("body").get("old_item_details")
                nit_details = Items.get_item_by_id(itemId)
                eventDetails = ""

                if oit_details:
                    if oit_details["itemName"] != nit_details["itemName"]:
                        eventDetails += f"Item name is changed from <{oit_details['itemName']}> to <{nit_details['itemName']}>. \n "
                    if oit_details["posName"] != nit_details["posName"]:
                        eventDetails += f"Item posname is changed from <{oit_details['posName']}> to <{nit_details['posName']}>. \n "
                    if oit_details["shortName"] != nit_details["shortName"]:
                        eventDetails += f"Item shortname is changed from <{oit_details['shortName']}> to <{nit_details['shortName']}>. \n "
                    if oit_details["itemDescription"] != nit_details["itemDescription"]:
                        eventDetails += f"Item description is changed from <{oit_details['itemDescription']}> to <{nit_details['itemDescription']}>. \n "
                    if oit_details["itemUnitPrice"] != nit_details["itemUnitPrice"]:
                        eventDetails += f"Item price is changed from <{oit_details['itemUnitPrice']}> to <{nit_details['itemUnitPrice']}>. \n "
                    if oit_details["itemType"] != nit_details["itemType"]:
                        eventDetails += f"Item type is changed from <{oit_details['itemType']}> to <{nit_details['itemType']}>. \n "

                    # Check for platfrom updates
                    if nit_details['itemPriceMappings'] or oit_details['itemPriceMappings']:

                        # Convert lists to dictionaries for easier comparison
                        old_dict = {item["platformType"]: item["platformItemPrice"] for item in oit_details['itemPriceMappings']}
                        new_dict = {item["platformType"]: item["platformItemPrice"] for item in nit_details['itemPriceMappings']}
                        # Mapping of platformType to platform name
                        platform_names = {
                            1: "Apptopus",
                            2: "Flipdish",
                            3: "Ubereats",
                            4: "Clover",
                            5: "Grubhub",
                            6: "Doordash",
                            7: "GMB",
                            8: "Stream",
                            11: "Square",
                            50: "Storefront"
                        }
                        for platformType, new_price in new_dict.items():
                            if platformType in old_dict:
                                if old_dict[platformType] != new_price:
                                    eventDetails +=f"{platform_names.get(platformType)} price changed from <{old_dict[platformType]}> to <{new_price}>.  \n "
                            else:
                                eventDetails += f"{platform_names.get(platformType)} price changed from <0.00> to <{new_price}>.  \n "
                        for platformType, old_price in old_dict.items():
                            if platformType not in new_dict:
                                eventDetails +=f"{platform_names.get(platformType)} price changed from <{old_price} to <0.00>.  \n "

                    
                istatus = {0: "Disable", 1: "Enable"}
                if oldItemStatus != nit_details["itemStatus"]:
                    eventDetails += f"Item status is changed to <{istatus[nit_details['itemStatus']]}> \n "

                if eventDetails != "":
                    eventDetails = f" --- Item <{nit_details['itemName']}> --- \n " + eventDetails
                    
                    eventDetails += f" IP address:{ipAddr}"        
                    resp = ActivityLogs.post_activity_logs(
                        userid=userId,
                        username=user_details['username'] if user_details.get('username') else "pos_webhooks",
                        merchantid=merchantId,
                        merchantname=merchant_details['merchantname'],
                        itemid=itemId,
                        itemname=nit_details['itemName'],
                        eventtype="activity",
                        eventname=subject,
                        eventdetails=eventDetails
                    )
                    # send notification to websocket
                    resp = send_notification_to_websocket()



            # order events
            elif subject == "order.status_change":

                orderId = message.get("body").get("orderId")
                caller = message.get("body").get("caller")
                ipAddr = message.get("body").get("ipAddr")

                order_details = Orders.get_order(orderId=orderId)
                merchant_details = Merchants.get_merchant_by_id(order_details['merchantid'])

                eventDetails = f"Order Status Changed to CANCELLED by {caller}, IP address:{ipAddr}"

                resp = ActivityLogs.post_activity_logs(
                    userid=userId,
                    username=user_details['username'] if userId else caller,
                    merchantid=order_details['merchantid'],
                    merchantname=merchant_details['merchantname'],
                    eventtype="activity",
                    eventname=subject,
                    eventdetails=eventDetails,
                    orderexternalreference=order_details['orderexternalreference']
                )

            # order disputed events
            elif subject == "order.order_disputed":

                orderId = message.get("body").get("orderId")
                caller = message.get("body").get("caller")
                order_details = Orders.get_order(orderId=orderId)
                merchant_details = Merchants.get_merchant_by_id(order_details['merchantid'])

                eventDetails = f"Order disputed by user from {caller}"

                resp = ActivityLogs.post_activity_logs(
                    userid=userId,
                    username=user_details['username'] if userId else caller,
                    merchantid=order_details['merchantid'],
                    merchantname=merchant_details['merchantname'],
                    eventtype="activity",
                    eventname=subject,
                    eventdetails=eventDetails,
                    orderexternalreference=order_details['orderexternalreference']
                )

            # busymode.update
            elif subject == "busymode.update":

                eventDetails = message.get("body").get("message")

                resp = ActivityLogs.post_activity_logs(
                    userid=userId,
                    username=user_details['username'],
                    merchantid=merchantId,
                    merchantname=merchant_details['merchantname'],
                    eventtype="activity",
                    eventname=subject,
                    eventdetails=eventDetails
                )


            # grubhub bot events
            elif subject == "grubhub_bot":

                botOperationEvent = message.get("body").get("botOperationEvent")
                # itemId = message.get("itemId").get("itemId")
                # pauseTime = message.get("body").get("pauseTime")

                if botOperationEvent == "item.status_change" or botOperationEvent == "item.update":
                    eventDetails = f"Grubhub Bot successfully updated item status on grubhub"
                elif botOperationEvent == "merchant.status_change":
                    eventDetails = f"Grubhub Bot successfully updated merchant status to <{'Pause' if merchant_details['marketstatus'] == 0 else 'Resume'}>"

                resp = ActivityLogs.post_activity_logs(
                    userid=userId,
                    username=user_details['username'],
                    merchantid=merchant_details.get("merchantid") or merchant_details.get("id"),
                    merchantname=merchant_details.get('merchantname') or merchant_details.get('virtualname'),
                    eventtype="activity",
                    eventname=subject,
                    eventdetails=eventDetails
                )


            # woflow events
            elif subject == "woflow.job_initiated":
                woflowColumnId = message.get("body").get("woflowColumnId")
                eventDetails = f"job is initiated on woflow for merchant <{merchant_details['merchantname']}>"
                resp = ActivityLogs.post_activity_logs(
                    userid=userId,
                    username=user_details['username'],
                    merchantid=merchantId,
                    merchantname=merchant_details['merchantname'],
                    eventtype="activity",
                    eventname=subject,
                    eventdetails=eventDetails,
                    woflowcolumnid=woflowColumnId
                )

            elif subject == "woflow.status_updated":
                woflowColumnId = message.get("body").get("woflowColumnId")
                operation = message.get("body").get("operation")
                if operation == "accept":
                    # accept
                    eventDetails = f"woflow generated menu is accepted"
                else:
                    # reject
                    eventDetails = f"woflow generated menu is rejected"
                resp = ActivityLogs.post_activity_logs(
                    userid=userId,
                    username=user_details['username'],
                    merchantid=merchantId,
                    merchantname=merchant_details['merchantname'],
                    eventtype="activity",
                    eventname=subject,
                    eventdetails=eventDetails,
                    woflowcolumnid=woflowColumnId
                )

            elif subject == "loyalty.points":
                status = message.get("body").get("status")
                points = message.get("body").get("points")
                ipAddr = message.get("body").get("ipAddr")
                if status == 1:
                    eventDetails = f"{points} Loyalty Points has been redeemed"
                elif status == "delete":
                    eventDetails = f"Loyalty Points Deleted"
                else:
                    eventDetails = f"{points} Loyalty Points has been added"
                
                eventDetails += f",  IP Address: {ipAddr}"
                resp = ActivityLogs.post_activity_logs(
                    userid=userId,
                    username=user_details['username'],
                    merchantid=merchantId,
                    merchantname=merchant_details['merchantname'],
                    eventtype="activity",
                    eventname=subject,
                    eventdetails=eventDetails
                )
            elif subject == "login.success":
                event_details = message.get("body").get("eventDetails")
                event_name = message.get("body").get("eventName")
                print("trigger", subject)
                resp = ActivityLogs.post_activity_logs(
                    userid=userId,
                    username=user_details['username'],
                    eventtype="activity",
                    eventname=event_name if event_name else subject,
                    eventdetails=event_details
                )

            elif subject == "merchant.auto_resume_merchant_scheduler":
                print(' ---------- starting activity logs for merchant.auto_resume_merchant_scheduler')
                eventDetails = message.get("body").get("schedulerDetail")
                resp = ActivityLogs.post_activity_logs(
                    userid=userId,
                    username=user_details['username'],
                    merchantid=merchantId,
                    merchantname=merchant_details['merchantname'],
                    eventtype="Auto Resume",
                    eventname=message.get("event"),
                    eventdetails=eventDetails
                )
                if not resp:
                    print("error: while storing activity log details in database")
                    exit()

                
def financial_logs_event(event, context):
    with app.app_context():
        print("--------------------- - --------------------------")
        for record in event['Records']:

            subject = record.get("Sns").get("Subject")
            message = eval(record.get("Sns").get("Message"))

            print(subject)
            print(message)

            merchantId = message.get("body").get("merchantId")
            userId = message.get("body").get("userId")

            merchant_details = dict()
            user_details = dict()

            if merchantId:
                merchant_details = Merchants.get_merchant_or_virtual_merchant(merchantId)
            if userId:
                user_details = Users.get_user_by_id(userId)

            ### merchant events

            if subject == "stripe.card_issued":
                cardId = message.get("body").get("cardId")
                ipAddr = message.get("body").get("ipAddr")
                resp = FinancialLogs.post_financial_logs(
                    userid=userId,
                    username=user_details['username'],
                    merchantid=merchantId,
                    merchantname=merchant_details['merchantname'],
                    eventname=subject,
                    eventdetails=f"New stripe card with card id:<{cardId}> issued to merchant with name <{merchant_details['merchantname']}>. Request IP:<{ipAddr}>"
                )
                if not resp:
                    print("error: while storing financial logs details in database")
                    exit()

            elif subject == "stripe.financial_account_retrieved":
                ipAddr = message.get("body").get("ipAddr")
                resp = FinancialLogs.post_financial_logs(
                    userid=userId,
                    username=user_details['username'],
                    merchantid=merchantId,
                    merchantname=merchant_details['merchantname'],
                    eventname=subject,
                    eventdetails=f"Stripe financial account details retrieved for merchant with name <{merchant_details['merchantname']}>. Request IP:<{ipAddr}>"
                )
                if not resp:
                    print("error: while storing financial logs details in database")
                    exit()

            elif subject == "stripe.treasury_features_added":
                ipAddr = message.get("body").get("ipAddr")
                resp = FinancialLogs.post_financial_logs(
                    userid=userId,
                    username=user_details['username'],
                    merchantid=merchantId,
                    merchantname=merchant_details['merchantname'],
                    eventname=subject,
                    eventdetails=f"Treasury features added to stripe account for merchant with name <{merchant_details['merchantname']}>. Request IP:<{ipAddr}>"
                )
                if not resp:
                    print("error: while storing financial logs details in database")
                    exit()

            elif subject == "stripe.financial_account_created":
                ipAddr = message.get("body").get("ipAddr")
                financialAccId = message.get("body").get("financialAccId")
                resp = FinancialLogs.post_financial_logs(
                    userid=userId,
                    username=user_details['username'],
                    merchantid=merchantId,
                    merchantname=merchant_details['merchantname'],
                    eventname=subject,
                    eventdetails=f"Stripe financial account created with id:<{financialAccId}> for merchant with name <{merchant_details['merchantname']}>. Request IP:<{ipAddr}>"
                )
                if not resp:
                    print("error: while storing financial logs details in database")
                    exit()

            elif subject == "stripe.funds_transfer_to_merchant":
                ipAddr = message.get("body").get("ipAddr")
                amount = message.get("body").get("amount")
                payoutId = message.get("body").get("payoutId")
                resp = FinancialLogs.post_financial_logs(
                    userid=userId,
                    username=user_details['username'],
                    merchantid=merchantId,
                    merchantname=merchant_details['merchantname'],
                    eventname=subject,
                    eventdetails=f"Stripe funds of amount:<{amount}> transferred from platform to merchant account with name <{merchant_details['merchantname']}>. Request IP:<{ipAddr}>",
                    payoutid=payoutId
                )
                if not resp:
                    print("error: while storing financial logs details in database")
                    exit()

            elif subject == "stripe.funds_transfer_to_financial_account":
                ipAddr = message.get("body").get("ipAddr")
                amount = message.get("body").get("amount")
                payoutId = message.get("body").get("payoutId")
                resp = FinancialLogs.post_financial_logs(
                    userid=userId,
                    username=user_details['username'],
                    merchantid=merchantId,
                    merchantname=merchant_details['merchantname'],
                    eventname=subject,
                    eventdetails=f"Stripe funds of amount:<{amount}> transferred from merchant account to financial account with name <{merchant_details['merchantname']}>. Request IP:<{ipAddr}>",
                    payoutid=payoutId
                )
                if not resp:
                    print("error: while storing financial logs details in database")
                    exit()

            elif subject == "stripe.funds_transfer_to_external_bank_account":
                ipAddr = message.get("body").get("ipAddr")
                amount = message.get("body").get("amount")
                payoutId = message.get("body").get("payoutId")
                accountHolder = message.get("body").get("accountHolder")
                resp = FinancialLogs.post_financial_logs(
                    userid=userId,
                    username=user_details['username'],
                    merchantid=merchantId,
                    merchantname=merchant_details['merchantname'],
                    eventname=subject,
                    eventdetails=f"""Stripe funds of amount:<{amount}> transferred from merchant financial account with name 
                          <{merchant_details['merchantname']} to external bank account with name <{accountHolder}>. 
                          Request IP:<{ipAddr}>""",
                    payoutid=payoutId
                )
                if not resp:
                    print("error: while storing financial logs details in database")
                    exit()

            elif subject == "stripe.add_financial_account_as_default_external_account":
                ipAddr = message.get("body").get("ipAddr")
                resp = FinancialLogs.post_financial_logs(
                    userid=userId,
                    username=user_details['username'],
                    merchantid=merchantId,
                    merchantname=merchant_details['merchantname'],
                    eventname=subject,
                    eventdetails=f"Stripe financial account added as external bank account for merchant with name <{merchant_details['merchantname']}>. Request IP:<{ipAddr}>"
                )
                if not resp:
                    print("error: while storing financial logs details in database")
                    exit()

            elif subject == "stripe.retrieve_recipients_for_connected_account":
                ipAddr = message.get("body").get("ipAddr")
                resp = FinancialLogs.post_financial_logs(
                    userid=userId,
                    username=user_details['username'],
                    merchantid=merchantId,
                    merchantname=merchant_details['merchantname'],
                    eventname=subject,
                    eventdetails=f"Recipients retrieved for merchant with name <{merchant_details['merchantname']}>. Request IP:<{ipAddr}>"
                )
                if not resp:
                    print("error: while storing financial logs details in database")
                    exit()

            elif subject == "stripe.add_recipient_to_connected_account":
                ipAddr = message.get("body").get("ipAddr")
                accountHolderName = message.get("body").get("accountHolderName")
                resp = FinancialLogs.post_financial_logs(
                    userid=userId,
                    username=user_details['username'],
                    merchantid=merchantId,
                    merchantname=merchant_details['merchantname'],
                    eventname=subject,
                    eventdetails=f"Recipient Added with name <{accountHolderName}> for merchant with name <{merchant_details['merchantname']}>. Request IP:<{ipAddr}>"
                )
                if not resp:
                    print("error: while storing financial logs details in database")
                    exit()

            elif subject == "stripe.update_recipient_for_connected_account":
                ipAddr = message.get("body").get("ipAddr")
                accountHolderName = message.get("body").get("accountHolderName")
                resp = FinancialLogs.post_financial_logs(
                    userid=userId,
                    username=user_details['username'],
                    merchantid=merchantId,
                    merchantname=merchant_details['merchantname'],
                    eventname=subject,
                    eventdetails=f"Recipient updated with name <{accountHolderName}> for merchant with name <{merchant_details['merchantname']}>. Request IP:<{ipAddr}>"
                )
                if not resp:
                    print("error: while storing financial logs details in database")
                    exit()

            elif subject == "stripe.remove_recipient_for_connected_account":
                ipAddr = message.get("body").get("ipAddr")
                accountHolderName = message.get("body").get("accountHolderName")
                resp = FinancialLogs.post_financial_logs(
                    userid=userId,
                    username=user_details['username'],
                    merchantid=merchantId,
                    merchantname=merchant_details['merchantname'],
                    eventname=subject,
                    eventdetails=f"Recipient removed with name <{accountHolderName}> for merchant with name <{merchant_details['merchantname']}>. Request IP:<{ipAddr}>"
                )
                if not resp:
                    print("error: while storing financial logs details in database")
                    exit()

            elif subject == "stripe.transfer_funds_to_recipient_for_connected_account":
                ipAddr = message.get("body").get("ipAddr")
                accountHolderName = message.get("body").get("accountHolderName")
                amount = message.get("body").get("amount")
                resp = FinancialLogs.post_financial_logs(
                    userid=userId,
                    username=user_details['username'],
                    merchantid=merchantId,
                    merchantname=merchant_details['merchantname'],
                    eventname=subject,
                    eventdetails=f"Funds transferred of <{amount}> to Recipient update with name <{accountHolderName}> for merchant with name <{merchant_details['merchantname']}>. Request IP:<{ipAddr}>"
                )
                if not resp:
                    print("error: while storing financial logs details in database")
                    exit()

            elif subject == "merchants.add_trasuary_auth_phone":
                ipAddr = message.get("body").get("ipAddr")
                phone = message.get("body").get("phone")
                resp = FinancialLogs.post_financial_logs(
                    userid=userId,
                    username=user_details['username'],
                    merchantid=merchantId,
                    merchantname=merchant_details['merchantname'],
                    eventname=subject,
                    eventdetails=f"Treasury authentication phone number:<{phone}> added for merchant with name <{merchant_details['merchantname']}. Request IP:<{ipAddr}>"
                )
                if not resp:
                    print("error: while storing financial logs details in database")
                    exit()

            elif subject == "merchants.send_trasuary_auth_otp":
                ipAddr = message.get("body").get("ipAddr")
                resp = FinancialLogs.post_financial_logs(
                    userid=userId,
                    username=user_details['username'],
                    merchantid=merchantId,
                    merchantname=merchant_details['merchantname'],
                    eventname=subject,
                    eventdetails=f"Treasury authentication otp generated for merchant with name <{merchant_details['merchantname']}. Request IP:<{ipAddr}>"
                )
                if not resp:
                    print("error: while storing financial logs details in database")
                    exit()

            elif subject == "merchants.change_trasuary_auth_phone":
                ipAddr = message.get("body").get("ipAddr")
                oldPhone = message.get("body").get("oldPhone")
                newPhone = message.get("body").get("newPhone")
                resp = FinancialLogs.post_financial_logs(
                    userid=userId,
                    username=user_details['username'],
                    merchantid=merchantId,
                    merchantname=merchant_details['merchantname'],
                    eventname=subject,
                    eventdetails=f"""Treasury authentication otp generated to old phone number:<{oldPhone}> and 
                      new phone number:<{newPhone}> inorder to change number for merchant with name <{merchant_details['merchantname']}. Request IP:<{ipAddr}>"""
                )
                if not resp:
                    print("error: while storing financial logs details in database")
                    exit()

            elif subject == "merchants.update_trasuary_auth_phone":
                ipAddr = message.get("body").get("ipAddr")
                oldPhone = message.get("body").get("oldPhone")
                newPhone = message.get("body").get("newPhone")
                resp = FinancialLogs.post_financial_logs(
                    userid=userId,
                    username=user_details['username'],
                    merchantid=merchantId,
                    merchantname=merchant_details['merchantname'],
                    eventname=subject,
                    eventdetails=f"""Treasury authentication number changed from old phone number:<{oldPhone}> to 
                          new phone number:<{newPhone}> for merchant with name <{merchant_details['merchantname']}. Request IP:<{ipAddr}>"""
                )
                if not resp:
                    print("error: while storing financial logs details in database")
                    exit()


def error_logs_event(event, context):
    with app.app_context():
        print("---------------------Logs event --------------------------")
        connection, cursor = get_db_connection()
        for record in event['Records']:

            subject, message = record.get("Sns").get("Subject"), eval(record.get("Sns").get("Message"))
            print(subject), print(message)

            merchantId, userId = message.get("body").get("merchantId"), message.get("body").get("userId")
            merchant_details, user_details = dict(), dict()
            if merchantId:
                merchant_details = Merchants.get_merchant_or_virtual_merchant(merchantId)
            if userId:
                user_details = Users.get_user_by_id(userId)

            if subject == "error_logs.entry":
                errorSource = message.get("body").get("errorSource")

                log_resp = ErrorLogs.post_error_logs(
                    userid=userId,
                    username=user_details.get("username"),
                    merchantid=merchant_details.get("merchantid") or merchant_details.get("id"),
                    merchantname=merchant_details.get("merchantname") or merchant_details.get("virtualname"),
                    errorname=message.get("body").get("errorName"),
                    errorsource=errorSource,
                    errorstatus=message.get("body").get("errorStatus"),
                    errordetails=message.get("body").get("errorDetails"),
                    orderexternalreference=message.get("body").get("orderExternalReference")
                )

                # send notification to websocket
                ws_event_message = {
                    "event": "error_logs.entry"
                }
                resp = send_notification_to_websocket(message=ws_event_message)
                if not resp:
                    print("error: while sending notification to websocket connections")

                # send error message to slack webhook
                if errorSource == "grubhub" or errorSource == "doordash" or errorSource == "grubhub_bot" or errorSource == "dashboard" or errorSource == "ubereats" or errorSource == "online_order":
                    res = send_error_message_to_slack_webhook(
                        webhook_url=config.slack_error_logs_channel_webhook,
                        merchantName=merchant_details.get("merchantname") or merchant_details.get("virtualname"),
                        username=user_details.get("username") if user_details.get("username") else "",
                        errorDateTime=datetime.datetime.now(datetime.timezone.utc).astimezone(
                            gettz('US/Pacific')).strftime("%m-%d-%Y %H:%M (%Z)"),
                        errorName=message.get("body").get("errorName"),
                        errorSource=errorSource,
                        errorDetails=message.get("body").get("errorDetails"),
                        orderexternalreference=message.get("body").get("orderExternalReference")
                    )
            elif subject == "error_logs.dispute_order":
                errorSource = message.get("body").get("errorSource")
                if errorSource == "android":
                    res = send_error_message_to_slack_webhook(
                        webhook_url=config.slack_error_charge_team_channel_webhook,
                        merchantName=merchant_details.get("merchantname") or merchant_details.get("virtualname"),
                        username=user_details.get("username") if user_details.get("username") else "",
                        errorDateTime=datetime.datetime.now(datetime.timezone.utc).astimezone(
                            gettz('US/Pacific')).strftime("%m-%d-%Y %H:%M (%Z)"),
                        errorName=message.get("body").get("errorName"),
                        errorSource=errorSource,
                        errorDetails=message.get("body").get("errorDetails"),
                        orderexternalreference=message.get("body").get("orderExternalReference")
                    )

def audit_logs_event(event, context):
    with app.app_context():
        print("--------------------- - --------------------------")
        connection, cursor = get_db_connection()
        for record in event['Records']:

            subject = record.get("Sns").get("Subject")
            message = eval(record.get("Sns").get("Message"))

            print(subject)
            print(message)

            merchantId = message.get("body").get("merchantId")
            userId = message.get("body").get("userId")

            merchant_details = dict()
            user_details = dict()

            if merchantId:
                merchant_details = Merchants.get_merchant_by_id(merchantId)
            if userId:
                user_details = Users.get_user_by_id(userId)

            ### merchant events
            if subject == "merchant.update_business_info":

                old_merchant_details = message.get("body").get("old_merchant_details")
                details_list = list()

                if old_merchant_details['taxrate'] != merchant_details['taxrate']:
                    details_list.append(
                        f" Tax rate is changed from {float(old_merchant_details['taxrate'])} to {float(merchant_details['taxrate'])}")

                if old_merchant_details['stafftipsrate'] != merchant_details['stafftipsrate']:
                    details_list.append(
                        f" Staff Tips rate is changed from {format(old_merchant_details['stafftipsrate'])} to {format(merchant_details['stafftipsrate'])}")

                if old_merchant_details['ubereatscommission'] != merchant_details['ubereatscommission']:
                    details_list.append(
                        f" Ubereats commission is changed from {format(old_merchant_details['ubereatscommission'])} to {format(merchant_details['ubereatscommission'])}")

                if old_merchant_details['squarecommission'] != merchant_details['squarecommission']:
                    details_list.append(
                        f" Square commission is changed from {format(old_merchant_details['squarecommission'])} to {format(merchant_details['squarecommission'])}")

                if old_merchant_details['doordashcommission'] != merchant_details['doordashcommission']:
                    details_list.append(
                        f" Doordash commission is changed from {format(old_merchant_details['doordashcommission'])} to {format(merchant_details['doordashcommission'])}")

                if old_merchant_details['grubhubcommission'] != merchant_details['grubhubcommission']:
                    details_list.append(
                        f" Grubhub commission is changed from {format(old_merchant_details['grubhubcommission'])} to {format(merchant_details['grubhubcommission'])}")

                if old_merchant_details['flipdishcommission'] != merchant_details['flipdishcommission']:
                    details_list.append(
                        f" Flipdish commission is changed from {format(old_merchant_details['flipdishcommission'])} to {format(merchant_details['flipdishcommission'])}")

                if old_merchant_details['processingfeerate'] != merchant_details['processingfeerate']:
                    details_list.append(
                        f" Processing fee percentage is changed from {format(old_merchant_details['processingfeerate'])} to {format(merchant_details['processingfeerate'])}")

                if old_merchant_details['processingfeefixed'] != merchant_details['processingfeefixed']:
                    details_list.append(
                        f" Processing fee fixed value is changed from {format(old_merchant_details['processingfeefixed'])} to {format(merchant_details['processingfeefixed'])}")

                if old_merchant_details['marketplacetaxrate'] != merchant_details['marketplacetaxrate']:
                    details_list.append(
                        f" Market place tax rate is changed from {format(old_merchant_details['marketplacetaxrate'])} to {format(merchant_details['marketplacetaxrate'])}")

                old_subscription_start_date = datetime.date.isoformat(
                    old_merchant_details['subscriptionstartdate']) if isinstance(
                    old_merchant_details['subscriptionstartdate'], datetime.date) else None
                new_subscription_start_date = datetime.date.isoformat(
                    merchant_details['subscriptionstartdate']) if isinstance(merchant_details['subscriptionstartdate'],
                                                                             datetime.date) else None
                if old_merchant_details['subscriptionstartdate'] != merchant_details['subscriptionstartdate']:
                    details_list.append(
                        f" Subscription start date is changed from {old_subscription_start_date} to {new_subscription_start_date}")

                if old_merchant_details['subscriptionamount'] != merchant_details['subscriptionamount']:
                    details_list.append(
                        f" Subscription amount is changed from {format(old_merchant_details['subscriptionamount'])} to {format(merchant_details['subscriptionamount'])}")

                if old_merchant_details['subscriptionfrequency'] != merchant_details['subscriptionfrequency']:
                    details_list.append(
                        f" Subscription frequency is changed from {old_merchant_details['subscriptionfrequency']} to {merchant_details['subscriptionfrequency']}")

                if old_merchant_details['subscriptiontrialperiod'] != merchant_details['subscriptiontrialperiod']:
                    details_list.append(
                        f" Subscription trial period is changed from {old_merchant_details['subscriptiontrialperiod']} to {merchant_details['subscriptiontrialperiod']}")

                if old_merchant_details['stripeaccountid'] != merchant_details['stripeaccountid']:
                    details_list.append(
                        f" Stripe accountId is changed from {old_merchant_details['stripeaccountid']} to {merchant_details['stripeaccountid']}")

                for row in details_list:
                    resp = AuditLogs.post_audit_logs(
                        userid=userId,
                        username=user_details['username'],
                        merchantid=merchantId,
                        merchantname=merchant_details['merchantname'],
                        eventname=subject,
                        eventdetails=row
                    )


            # subscription events
            elif subject == "subscription.create_record":

                detail = message.get("body").get("detail")
                resp = AuditLogs.post_audit_logs(
                    userid='',
                    username='system',
                    merchantid=merchantId,
                    merchantname=merchant_details['merchantname'],
                    eventname=subject,
                    eventdetails=detail
                )

            elif subject == "subscription.waiveoff":
                detail = message.get("body").get("detail")
                ipAddr = message.get("body").get("ipAddr")
                resp = AuditLogs.post_audit_logs(
                    userid=userId,
                    username=user_details.get('username'),
                    merchantid=merchantId,
                    merchantname=merchant_details.get('merchantname'),
                    eventname=subject,
                    eventdetails=f"subscription record is waived-off, IP Address: {ipAddr}",
                    subscriptionid=message.get("body").get("subscriptionId")
                )

            elif subject == "subscription.markPay":
                detail = message.get("body").get("detail")
                ipAddr = message.get("body").get("ipAddr")
                resp = AuditLogs.post_audit_logs(
                    userid=userId,
                    username=user_details.get('username'),
                    merchantid=merchantId,
                    merchantname=merchant_details.get('merchantname'),
                    eventname=subject,
                    eventdetails=f"subscription record is mark payed, IP Address: {ipAddr}",
                    subscriptionid=message.get("body").get("subscriptionId")
                )

            elif subject == "merchant.subscription_change":
                status = "Ended" if int(merchant_details["subscriptionstatus"]) == 0 else "RESUMED"
                ipAddr = message.get("body").get("ipAddr")
                resp = AuditLogs.post_audit_logs(
                    userid=userId,
                    username=user_details['username'],
                    merchantid=merchantId,
                    merchantname=merchant_details['merchantname'],
                    eventname=subject,
                    eventdetails=f"Merchant subscription is changed to {status},IP Address: {ipAddr}"
                )

            elif subject == "payout.created" or subject == "payout.create_error" or subject == "payout.reverted" or subject == "payout.revert_error" or subject == "payout.payout":
                resp = AuditLogs.post_audit_logs(
                    userid=userId,
                    username=user_details['username'],
                    merchantid=merchantId,
                    merchantname=merchant_details['merchantname'],
                    eventname=subject,
                    eventdetails=message.get("body").get("eventDetails"),
                    payoutid=message.get("body").get("payoutId")
                )

            # send notification to websocket
            message = {
                "event": "audit_logs.entry",
                "merchantId": merchantId
            }
            resp = send_notification_to_websocket(message=message)

