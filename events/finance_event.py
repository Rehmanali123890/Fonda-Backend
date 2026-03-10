import time
import ast
from globals import app
from models.Finance import Finance
from utilities.helpers import *
from flask import g, jsonify
from flask import jsonify, request, g
import requests
import ast
import zlib
import base64
from utilities.errors import unhandled
from utilities.slack_helpers import send_menu_update_message_to_slack_webhook
env = config.env
import json



def temporary_transaction_table_insert(event, context):
    with app.app_context():
        print("--------------------- - --------------------------")
        for record in event['Records']:
            print("At the start of sns to enter data in transaction table")
            subject = record.get("Sns").get("Subject")
            message = eval(record.get("Sns").get("Message"))
            key = message.get("body").get("key")
            doctype = message.get("body").get("doctype")
            fileid = message.get("body").get("fileid")
            Finance.insert_into_temporary_table(fileid,doctype,key)
            print("Inserted data in the temporary table")
            Finance.post_csv_files(fileid)
            print("Inserted data in the transaction table")
            Finance.send_transaction_email(key,fileid)
            print("Successfully sent email")


def format_datetime_z(dt):
    try:
        if isinstance(dt, datetime.datetime):
            return dt.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
        return dt
    except Exception as e:
        print(f"Failed to convert date time when creating order in other environment due to error {str(e)}")
        return dt


def replicate_order_to_envs(event, context):
    with app.app_context():
        try:
            print("--------------------- Create Order in Other environments --------------------------")
            create_log_data(
                level="[INFO]",
                Message=f'In the start of function to create order in test.',
                functionName="replicate_order_to_envs",
            )
            for record in event['Records']:
                connection, cursor = get_db_connection()
                subject = record.get("Sns").get("Subject")
                message = eval(record.get("Sns").get("Message"))
                print(f"Sns subject of Create order in test: {subject}")
                print(f"Sns message of Create order in test: {message}")
                create_log_data(level='[INFO]', Message="Successfully get from the sns"
                                , messagebody=f"Message body of sns is  {message}",
                                functionName="replicate_order_to_envs")
                _json = message.get("body").get("order_json")
                cursor.execute("SELECT * FROM config_master WHERE config_type='all_merchants'")
                all_merchants = cursor.fetchone()
                all_merchant_flags = True
                merchant_in_list = False
                if all_merchants and (all_merchants['config_value'] =='False' or all_merchants['config_value'] =='false'):
                    all_merchant_flags = False
                    cursor.execute("SELECT * FROM config_master WHERE config_type='merchant_list'")
                    merchant_list = cursor.fetchone()
                    merchant_ids = ast.literal_eval(merchant_list['config_value'])
                    if len(merchant_ids) != 0 and _json['order']['orderMerchantID'] in merchant_ids:
                        merchant_in_list = True
                if all_merchant_flags or merchant_in_list:
                    cursor.execute("SELECT * FROM config_master WHERE config_type='config_environments'")
                    rows = cursor.fetchall()
                    if rows:
                        for row in rows:
                            try:
                                environment_details = row['config_value']
                                environment_details = ast.literal_eval(environment_details)
                                if environment_details['environment'] != config.env:
                                    url = environment_details['api_url']
                                    headers = {
                                        'Content-Type': 'application/json'
                                    }
                                    _json['token'] = environment_details['api_token']
                                    _json['order']['orderDateTime'] = format_datetime_z(_json['order']['orderDateTime'])
                                    print("json is -->", _json)
                                    api_response = requests.request("POST", url, headers=headers, json=_json)
                                    print("Response from order creation api is --->",api_response)
                                    if api_response.status_code == 200:
                                        print(f"Successfully create order in {environment_details['environment']} environment")
                                        create_log_data(level='[INFO]', Message=f"Successfully create order in {environment_details['environment']} environment"
                                                        , messagebody=f"Response from order creation api is {api_response}",
                                                        functionName="replicate_order_to_envs")
                                    else:
                                        response_json = api_response.json()
                                        print(response_json['message'])
                                        print(f"Unable to create order in {environment_details['environment']} environment due to error {response_json['message']}")
                                        create_log_data(level='[ERROR]',
                                                        Message=f"Error while creating order in {environment_details['environment']}, Error is {response_json['message']}"
                                                        , messagebody=f"Response from order creation api is {api_response}",
                                                        functionName="replicate_order_to_envs")
                            except Exception as e:
                                create_log_data(level='[ERROR]',
                                                Message=f"Exception while creating order in other environment {str(e)}",
                                                functionName="replicate_order_to_envs")
                                print(f"Exception occurred while creating order in other environment {str(e)}")
                    else:
                        create_log_data(level='[ERROR]',
                                        Message=f"No environments are present in config master while creating order of {_json['orderExternalReference']}",
                                        functionName="replicate_order_to_envs")
                        send_menu_update_message_to_slack_webhook(
                            webhook_url=config.slack_error_logs_channel_webhook,
                            merchantName="Replicate order in other environments",
                            username="System",
                            eventName="Replicate order in other environments",
                            eventDetails=f"No environments are present in config master while creating order of {_json['orderExternalReference']}",
                            eventDateTime=datetime.datetime.now(),
                            source="Order Replication",
                            is_merchant=False
                        )
                else:
                    print("All merchants flag is not set to True and merchant id coming in order json is not present in config master table merchant list")
        except Exception as e:
            print(f"Exception occurred while creating order in other environments {str(e)}")
            create_log_data(level='[ERROR]', Message="Error occurred while creating orders"
                            , messagebody=f"Exception occurred while creating order in other environments {str(e)}",
                            functionName="replicate_order_to_envs")


def temporary_transaction_table_insert_data(fileid,doctype, start_date, end_date, key,user,Doordash_new_file):
    with app.app_context():
        create_log_data(
            level="[INFO]",
            Message=f'At the start of function to insert data in temporary transaction table',
            functionName="temporary_transaction_table_insert",
        )
        insert_into_temporary_table = Finance.insert_into_temporary_table(fileid, doctype, key,Doordash_new_file)
        if not isinstance(insert_into_temporary_table,Exception):
            create_log_data(
                level="[INFO]",
                Message=f'Inserted data in temporary transaction table',
                functionName="temporary_transaction_table_insert",
            )
        elif isinstance(insert_into_temporary_table,Exception):
            Finance.send_failled_transaction_email(doctype,user)
            create_log_data(
                level="[ERROR]",
                Message=f'Failed to insert data in temporary transaction table due to error {insert_into_temporary_table}',
                functionName="temporary_transaction_table_insert",
            )
            return
        if doctype == 'doordash':
            doordash_credit_debit_transaction = Finance.update_credit_debit_transaction(fileid,start_date,end_date)
            if not isinstance(doordash_credit_debit_transaction, Exception):
                create_log_data(
                    level="[INFO]",
                    Message=f'Update the Doordash credit debit transaction',
                    functionName="temporary_transaction_table_insert",
                )
            elif isinstance(doordash_credit_debit_transaction, Exception):
                create_log_data(
                    level="[ERROR]",
                    Message=f'Failed to update the doordash credit debit transaction due to error {doordash_credit_debit_transaction}',
                    functionName="temporary_transaction_table_insert",
                )
        insert_into_transaction_table = Finance.post_csv_files(fileid,doctype,start_date,end_date)
        if not  isinstance(insert_into_transaction_table,Exception):
            create_log_data(
                level="[INFO]",
                Message=f'Sucessfully inserted data in platform transaction table',
                functionName="temporary_transaction_table_insert",
            )
        send_email = Finance.send_transaction_email(key, fileid ,doctype,user,Doordash_new_file)
        if send_email.status != 500:
            create_log_data(
                level="[INFO]",
                Message=f'Sucessfully send email of inserted data in platform transaction table',
                functionName="temporary_transaction_table_insert",
            )