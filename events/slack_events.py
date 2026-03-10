from app import app
import json
import datetime
from dateutil.tz import gettz

# local imports
import config
from utilities.slack_helpers import send_menu_update_message_to_slack_webhook
from models.Users import Users
from models.Merchants import Merchants
from models.Items import Items
from models.Menus import Menus
from models.PlatformType import PlatformType
from models.Categories import Categories
from models.Addons import Addons
from models.ActivityLogs import ActivityLogs


def slack_menu_notification_event(event, context):
    with app.app_context():
        print("--------------------- slack menu notification event --------------------------")
        print(event)
        for record in event['Records']:

            subject = record.get("Sns").get("Subject")
            message = eval(record.get("Sns").get("Message"))

            userId, user_details = message.get("body").get("userId"), {}
            if userId and (ud := Users.get_user_by_id(userId)):
                user_details = ud

            # check if subject.startswith (item. | category. | menu. | addon.)
            # then verify user role to be 3 or 4

            # According to the discussion with Prem, Umesh has commented these 3 lines so that all actions would be notified in slack channel regardless of subject or user role
            # if subject.startswith(("menu.", "category.", "item.", "addon.")) and user_details:
            #   if user_details["role"] in (1,2):
            #     continue

            # According to the discussion with Prem, Umesh has uncommented these 3 lines so that only actions from role(3(merchant admin) or 4(merchant standard user)) would be notified in slack channel 
            if subject.startswith(("menu.", "category.", "item.", "addon.")) and user_details:
                if user_details["role"] in (1,2):
                    continue

            merchantId, merchant_details = message.get("body").get("merchantId"), {}
            if merchantId and (md := Merchants.get_merchant_by_id(merchantId)):
                merchant_details = md

            currentDateTime = datetime.datetime.now(datetime.timezone.utc).astimezone(gettz('US/Pacific')).strftime(
                "%m-%d-%Y %H:%M (%Z)")
            eventDetails = ""
            source = ""
            item_details = {}

            # menu.create
            if subject == "menu.create":
                menuName = message.get("body").get("menuName")
                ipAddr = message.get("body").get("ipAddr")
                eventDetails = f"Menu <{menuName}> is created!, IP address:{ipAddr}"


            # menu.update
            elif subject == "menu.update":
                menuId = message.get("body").get("menuId")
                ipAddr = message.get("body").get("ipAddr")
                old_menu_details = message.get("body").get("old_menu_details")
                new_menu_details = Menus.get_menu_by_id_fk(menuId)

                if old_menu_details["name"] != new_menu_details["name"]:
                    eventDetails += f"Menu name is updated from <{old_menu_details['name']}> to <{new_menu_details['name']}>. \n "
                if old_menu_details["description"] != new_menu_details["description"]:
                    eventDetails += f"Menu description is updated from <{old_menu_details['description']}> to <{new_menu_details['description']}>. \n "
                if eventDetails != "":
                    eventDetails = f" ---Menu <{new_menu_details['name']}> --- \n "

                eventDetails += f"IP adress:{ipAddr}"
                
            # menu_mapping.update
            elif subject == "menu_mapping.update":
                menuId = message.get("body").get("menuId")
                platformType = message.get("body").get("platformType")
                ipAddr = message.get("body").get("ipAddr")
                operation = message.get("body").get("operation")

                pt_details = PlatformType.get(id=platformType)
                menu_details = Menus.get_menu_by_id_fk(menuId)

                # operation == (created, deleted)
                if operation == "created":
                    eventDetails = f"Platform <{pt_details['type']}> is assigned to menu <{menu_details['name']}>,  IP address:{ipAddr}"
                else:
                    eventDetails = f"Platform <{pt_details['type']}> is removed from menu <{menu_details['name']}>, IP address:{ipAddr}"


            # menu.assign_category & menu.unassign_category
            elif subject in ("menu.assign_category", "menu.unassign_category"):
                menuId = message.get("body").get("menuId")
                ipAddr = message.get("body").get("ipAddr")
                categoryId = message.get("body").get("categoryId")
                menu_details = Menus.get_menu_by_id_fk(menuId)
                category_details = Categories.get_category_by_id(categoryId)

                if subject == "menu.assign_category":
                    eventDetails = f"Category <{category_details['categoryname']}> is assigned to menu <{menu_details['name']}>"
                else:
                    eventDetails = f"Category <{category_details['categoryname']}> is removed from menu <{menu_details['name']}>"

                eventDetails += f",IP Address{ipAddr}"
            # menu.delete
            elif subject == "menu.delete":
                
                menu_details = message.get("body").get("menu_details")
                ipAddr = message.get("body").get("ipAddr")
                eventDetails = f"Menu <{menu_details['name']}> is deleted"


            # category.create
            elif subject == "category.create":
                categoryId = message.get("body").get("categoryId")
                ipAddr = message.get("body").get("ipAddr")
                category_details = Categories.get_category_by_id(categoryId)
                eventDetails = f"Category <{category_details['categoryname']}> is created., IP address:{ipAddr}"


            # category.update
            elif subject == "category.update":
                categoryId = message.get("body").get("categoryId")
                ipAddr = message.get("body").get("ipAddr")
                o_cat_det = message.get("body").get("old_category_details")
                n_cat_det = Categories.get_category_by_id(categoryId)

                if o_cat_det["categoryname"] != n_cat_det["categoryname"]:
                    eventDetails += f"Category name is changed from <{o_cat_det['categoryname']}> to <{n_cat_det['categoryname']}>. \n "
                if o_cat_det["posname"] != n_cat_det["posname"]:
                    eventDetails += f"Posname is changed from <{o_cat_det['posname']}> to <{n_cat_det['posname']}>. \n "
                if o_cat_det["categorydescription"] != n_cat_det["categorydescription"]:
                    eventDetails += f"Category description is changed from <{o_cat_det['categorydescription']}> to <{n_cat_det['categorydescription']}>. \n "
                if o_cat_det["status"] != n_cat_det["status"]:
                    eventDetails += f"Category status is changed from <{o_cat_det['status']}> to <{n_cat_det['status']}>. \n "

                if eventDetails != "":
                    eventDetails = f" --- Category <{n_cat_det['categoryname']}> --- \n " + eventDetails

                eventDetails += f", IP address:{ipAddr}"
            # category.delete
            elif subject == "category.delete":
                category_details = message.get("body").get("category_details")
                ipAddr = message.get("body").get("ipAddr")
                eventDetails = f"Category <{category_details['categoryname']}> is deleted, IP address:{ipAddr}"


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

            # item.create
            elif subject == "item.create":
                itemId = message.get("body").get("itemId")
                ipAddr = message.get("body").get("ipAddr")
                item_details = Items.get_item_by_id(itemId)
                eventDetails = f"Item <{item_details['itemName']}> with price <{item_details['itemUnitPrice']}> is created , IP address:{ipAddr}!"

            # item.status_change
            elif subject == "item.status_change":
                itemId = message.get("body").get("itemId")
                ipAddr = message.get("body").get("ipAddr")
                item_details = Items.get_item_by_id_fk(itemId)

                istatus = {0: "Disable", 1: "Enable"}

                itemStatus = istatus[item_details['status']]
                if item_details['status'] == 0:
                    if item_details['pause_type'] is not None:
                        if item_details['pause_type'] == 'today':
                            itemStatus = f"{itemStatus} ({item_details['pause_type']}) - From {item_details['pause_time'].astimezone(gettz('US/Pacific')).strftime('%m-%d-%Y %H:%M (%Z)')} Till {item_details['resume_time'].astimezone(gettz('US/Pacific')).strftime('%m-%d-%Y %H:%M (%Z)')}"
                        else:
                            itemStatus = f"{itemStatus} ({item_details['pause_type']}) - From {item_details['pause_time'].astimezone(gettz('US/Pacific')).strftime('%m-%d-%Y %H:%M (%Z)')}"

                eventDetails = f"Item <{item_details['itemname']}> status is changed to <{itemStatus}> , IP address:{ipAddr}"

                print('slack_events', eventDetails)
            # item.update
            elif subject == "item.update":
                itemId = message.get("body").get("itemId")
                oldItemStatus = message.get("body").get("oldItemStatus")
                source = message.get("body").get("source")
                oit_details = message.get("body").get("old_item_details")
                nit_details = Items.get_item_by_id(itemId)

                print("-------------nit_details---------- ", nit_details)
                print("-------------source---------- ", source)

                if oit_details:
                    print("-------------oit_details---------- ", oit_details)
                    if oit_details["itemName"] != nit_details["itemName"]:
                        eventDetails += f"Item <{nit_details['itemName']}> is changed from <{oit_details['itemName']}> to <{nit_details['itemName']}>. \n "
                    if oit_details["posName"] != nit_details["posName"]:
                        eventDetails += f"Item <{nit_details['itemName']}> posname is changed from <{oit_details['posName']}> to <{nit_details['posName']}>. \n "
                    if oit_details["shortName"] != nit_details["shortName"]:
                        eventDetails += f"Item <{nit_details['itemName']}> shortname is changed from <{oit_details['shortName']}> to <{nit_details['shortName']}>. \n "
                    if oit_details["itemDescription"] != nit_details["itemDescription"]:
                        eventDetails += f"Item <{nit_details['itemName']}> description is changed from <{oit_details['itemDescription']}> to <{nit_details['itemDescription']}>. \n "
                    if float(oit_details["itemUnitPrice"]) != float(nit_details["itemUnitPrice"]):
                        eventDetails += f"Item <{nit_details['itemName']}> price is changed from <{float(oit_details['itemUnitPrice']):.2f}> to <{float(nit_details['itemUnitPrice']):.2f}>. \n "
                        subject = "item.price_change"
                        print(" if price is change then eventDetails is ", eventDetails)
                    if oit_details["itemType"] != nit_details["itemType"]:
                        eventDetails += f"Item <{nit_details['itemName']}> Item type is changed from <{oit_details['itemType']}> to <{nit_details['itemType']}>. \n "

                istatus = {0: "Disable", 1: "Enable"}
                if oldItemStatus != nit_details["itemStatus"]:
                    eventDetails += f"Item <{nit_details['itemName']}> status is changed to <{istatus[nit_details['itemStatus']]}> \n "
                    subject = "item.status_change"
                    print(" if status is change then eventDetails is ", eventDetails)





            # item.delete
            elif subject == "item.delete":
                item_details = message.get("body").get("item_details")
                ipAddr = message.get("body").get("ipAddr")
                eventDetails = f"Item <{item_details['itemName']}> with price <{item_details['itemUnitPrice']}> is deleted, IP address:{ipAddr}"

                # item.assign_addon & item.unassign_addon
            elif subject in ("item.assign_addon", "item.unassign_addon"):
                itemId = message.get("body").get("itemId")
                ipAddr = message.get("body").get("ipAddr")
                addonId = message.get("body").get("addonId")
                item_details = Items.get_item_by_id_fk(itemId)
                addon_details = Addons.get_addon_by_id(addonId)

                if subject == "item.assign_addon":
                    eventDetails = f"Addon <{addon_details['addonname']}> is assinged to Item <{item_details['itemname']}> , IP address:{ipAddr}"
                else:
                    eventDetails = f"Addon <{addon_details['addonname']}> is unassinged to Item <{item_details['itemname']}> , IP address:{ipAddr}"


            # addon.create
            elif subject == "addon.create":
                addonId = message.get("body").get("addonId")
                ipAddr = message.get("body").get("ipAddr")
                addon_details = Addons.get_addon_by_id(addonId)
                eventDetails = f"Addon <{addon_details['addonname']}> is created, IP address:{ipAddr}"


            # addon.update
            elif subject == "addon.update":
                addonId = message.get("body").get("addonId")
                ipAddr = message.get("body").get("ipAddr")
                oad_details = message.get("body").get("old_addon_details")
                nad_details = Addons.get_addon_by_id_str(addonId)

                if oad_details['addonName'] != nad_details['addonName']:
                    eventDetails += f"Addon name is changed from <{oad_details['addonName']}> to <{nad_details['addonName']}>. \n "
                if oad_details['posName'] != nad_details['posName']:
                    eventDetails += f"Addon posname is changed from <{oad_details['posName']}> to <{nad_details['posName']}>. \n "
                if oad_details['addonDescription'] != nad_details['addonDescription']:
                    eventDetails += f"Addon description is changed from <{oad_details['addonDescription']}> to <{nad_details['addonDescription']}>. \n "
                if oad_details['minPermitted'] != nad_details['minPermitted']:
                    eventDetails += f"Addon minimum selected options is changed from <{oad_details['minPermitted']}> to <{nad_details['minPermitted']}>. \n "
                if oad_details['maxPermitted'] != nad_details['maxPermitted']:
                    eventDetails += f"Addon maximum selected options is changed from <{oad_details['maxPermitted']}> to <{nad_details['maxPermitted']}>. \n "

                if eventDetails != "":
                    eventDetails = f" --- Addon <{nad_details['addonName']}> --- \n " + eventDetails

                eventDetails += f", IP address:{ipAddr}"

            # addon.delete
            elif subject == "addon.delete":
                addon_details = message.get("body").get("addon_details")
                ipAddr = message.get("body").get("ipAddr")
                eventDetails = f"Addon <{addon_details['addonName']}> is deleted, , IP address:{ipAddr}"


            # addon.assign_option & addon.unassign_option
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
                
            print("event detail before send notification", eventDetails)
            # send notification to slack channel
            if eventDetails != "":
                print("event detail before send notification not empty ", eventDetails)
                res = send_menu_update_message_to_slack_webhook(
                    webhook_url=config.slack_menu_channel_webhook,
                    merchantName=merchant_details.get("merchantname"),
                    username=user_details.get("username") or "API",
                    eventName=subject,
                    eventDetails=eventDetails,
                    eventDateTime=currentDateTime,
                    source=source
                )

                # send notification to activity logs
                if subject not in ("item.status_change", "item.update"):
                    resp = ActivityLogs.post_activity_logs(
                        userid=userId,
                        username=user_details.get("username") or "",
                        merchantid=merchantId,
                        merchantname=merchant_details.get("merchantname"),
                        itemid=message.get("body").get("itemId"),
                        eventtype="activity",
                        eventname=subject,
                        eventdetails=eventDetails
                    )

        return {
            'statusCode': 200,
            'body': json.dumps('Lambda -> Slack Notification Event!')
        }
