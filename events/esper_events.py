from app import app

import config
from models.Merchants import Merchants
from models.esper.Esper import Esper
from utilities.helpers import get_db_connection, publish_sns_message



def esper_sns_topic_handler(event, context):
    print("---------------------- esper_sns_topic_triggered -------------------------")
    print(event)
    # {'Records': [{'EventSource': 'aws:sns', 'EventVersion': '1.0', 'EventSubscriptionArn': 'arn:aws:sns:us-east-1:677331532364:EsperSnsTopic:979e77f6-16d7-4052-99ca-c29580ac3ce1', 'Sns': {'Type': 'Notification', 'MessageId': '719f3ca0-1408-5b2e-9cd2-67d6248e5db7', 'TopicArn': 'arn:aws:sns:us-east-1:677331532364:EsperSnsTopic', 'Subject': 'Esper- Network alert triggered for your device:ESR-NZN-AAAXL', 'Message': "{'name': 'Fonda - Network Alert to SNS', 'device_name': 'ESR-NZN-AAAXL', 'group_name': 'All devices', 'timestamp': '2022-08-16 23:45:48 UTC', 'device_id': '99b03c18-db4d-4e03-bfc6-cf2b9fe9e913', 'group_id': '9b6c84cc-b98e-4422-ae52-47a4ae1e31af', 'alias_name': 'Mariscos El Kora', 'serial': 'R52RC0354FE'}\n\n", 'Timestamp': '2022-08-16T23:54:46.554Z', 'SignatureVersion': '1', 'Signature': 'BsIgtWdAXYtnd9xoEkHkKv3/OX7fte3/1f/HfWQ4z+aaJgQbVtCxO0kKAnR/wIX8oDkEreOT1YuRz8e03lF5fGybVAy4akUBi4AxD+KKG5ngkB7Vs66rPAFdbjaM2uBmdrPNn8mGkQ0MESOvmhS84TtYFr/lZ0mk/rUJTkIzRgeTAd5Ir/ymIaInWIW/Dt6aIp/z2oQE7IxpDxcvxlAdXPBwk2ASy+sSjVbfD1yfK5pEcmwAICdP+m0PPYKsKJV3ac6SPjryLosDzhGGmZ7kBJbEYH6eDCendhRoHFpinCxZ464eLu4ySUchKVjucBEoUpq+uTVEOaXwPs1eSXpYUQ==', 'SigningCertUrl': 'https://sns.us-east-1.amazonaws.com/SimpleNotificationService-56e67fcb41f6fec09b0196692625d385.pem', 'UnsubscribeUrl': 'https://sns.us-east-1.amazonaws.com/?Action=Unsubscribe&SubscriptionArn=arn:aws:sns:us-east-1:677331532364:EsperSnsTopic:979e77f6-16d7-4052-99ca-c29580ac3ce1', 'MessageAttributes': {}}}]}
    # {'name': 'Fonda - Network Alert to SNS', 'device_name': 'ESR-NZN-AAAE3', 'group_name': 'All devices', 'timestamp': '2022-08-16 23:45:48 UTC', 'device_id': '6c7755a7-9fd8-41b1-965b-afb953750643', 'group_id': '9b6c84cc-b98e-4422-ae52-47a4ae1e31af', 'alias_name': 'Mariscos El Kora', 'serial': 'R52RC0354FE'}
    
    with app.app_context():
        connection, cursor = get_db_connection()

        for record in event['Records']:
            subject = record.get("Sns").get("Subject")
            message = eval(record.get("Sns").get("Message"))

            # get merchant details by esper device id
            device_uuid = message.get("device_id")
            alias_name = message.get("alias_name")
            device_name = message.get("device_name")
            cursor.execute("""SELECT * FROM merchants WHERE esperdeviceid = %s""", (device_name))
            merchant = cursor.fetchone()
            print("merchant details: ", merchant)

            if not merchant: continue

            # get device details from esper to double check that device is offline
            esper_device = Esper.get_esper_device_by_uuid(device_uuid)
            if not esper_device:
                print("error!")
                continue
            print("esper device details: ", esper_device)

            # check if merchant is online (1) and device on esper is offline (60)
            # if merchant status is already pause then skip
            # otherwise change merchant status to pause with caller = "esper"
            if merchant["marketstatus"] == 1 and esper_device.get("status") == 60:
            
                resp = Merchants.update_marketplace_status(merchantId=merchant["id"], marketStatus=0, userId=None,caller = None,
                                                           pauseTime_duration = None,pause_reason = None)

                # Triggering SNS - merchant.status_change
                print("Triggering sns - merchant.status_change ...")
                sns_msg = {
                "event": "merchant.status_change",
                "body": {
                    "merchantId": merchant["id"],
                    "userId": "esper",
                    "pauseTime": 0,
                    "caller": "esper"
                }
                }
                logs_sns_resp = publish_sns_message(topic=config.sns_activity_logs, message=str(sns_msg), subject="merchant.status_change")
                merchant_sns_resp = publish_sns_message(topic=config.sns_merchant_notification, message=str(sns_msg), subject="merchant.status_change")


def event_bridge_custom(event, context):
    import json
    print("---------------------- event_bridge_custom -------------------------")
    print(event,context)
    custom_data = json.loads(event['detail']['input'])
    print("Custom data:", custom_data)
    # Implement your notification logic here
    # This function will be triggered by EventBridge rule at the specified time
    print("Sending notification...")
    with app.app_context():
        connection, cursor = get_db_connection()

        for record in event['Records']:
            subject = record.get("Sns").get("Subject")
            message = eval(record.get("Sns").get("Message"))
            print((subject,message))


