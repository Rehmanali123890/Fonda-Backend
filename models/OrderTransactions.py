
# local imports
import config
from utilities.helpers import generate_order_id, success, get_db_connection, publish_sns_message, is_float, \
    store_file_in_s3, send_ses_email, create_log_data
from flask import request
# rds config
rds_host = config.db_host
username = config.db_username
password = config.db_password
database_name = config.db_name


class OrderTransactions:
    @classmethod
    def doordash_transactions(cls,doordash_tran_data):
        connection, cursor = get_db_connection()
        try:
            cursor.execute("""
                        INSERT INTO doordashtransaction(id, Merchantid , Storename, Transactiontype,Transactionid,
                            Orderexternalrefenceid, Description,Finalorderstatus, Subtotal, Taxsubtotal,Comission,
                            Credit,
                            dateandtime, Tip,Fonda_processing_fee,Revenue_processing_fee_percent,Revenue_processing_threshold , Marketingfee
                        )VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s,%s,%s,%s)
                        """,doordash_tran_data)
            print("Doordash Execution--------------------------------------------------------")
        except Exception as e:
            create_log_data(level='[ERROR]',
                        Message=f"Unable to insert data in doordash transaction",
                        messagebody=f"Failed to insert data",
                        functionName="doordash_transactions",
                        statusCode="400 BAD REQUEST",
                        request=request)
            sns_msg = {
                "event": "error_logs.entry",
                "body": {
                    "userId": "",
                    "merchantId": doordash_tran_data[1],
                    "errorName": "Error at insertion.",
                    "errorSource": "OrderTransaction file",
                    "errorStatus": 400,
                    "errorDetails": f"Failed to insert data in Doordash transaction table."
                }
            }
            error_logs_sns_resp = publish_sns_message(topic=config.sns_error_logs, message=str(sns_msg),
                                                        subject="error_logs.entry")
            raise e

    @classmethod
    def online_order_transactions(cls,online_order_tran_data):
        connection, cursor = get_db_connection()
        try:
            cursor.execute("""INSERT INTO storefronttransaction(id, dateandtime , merchantid, status, stafftips, 
                                extrastafftips, ordersubtotal, ordertax, processingfee,
                                commission,
                                ordertotal,
                                orderexternalreference,
                                squarefee,
                                creditcardfee,
                                promodiscount,
                                doordashdelfee,
                                dasher_staff_tips,
                                Transactiontype,Fonda_processing_fee,Revenue_processing_fee_percent,Revenue_processing_threshold
                                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s,%s,%s, %s, %s, %s, %s, %s, %s, %s)
                                """,online_order_tran_data)
            print("Online Order Execution sucsess--------------------------------------------------------")
        except Exception as e:
            create_log_data(level='[ERROR]',
                        Message=f"Unable to insert data in store front transaction",
                        messagebody=f"Failed to insert data",
                        functionName="online_order_transactions",
                        statusCode="400 BAD REQUEST",
                        request=request)
    
            sns_msg = {
                "event": "error_logs.entry",
                "body": {
                    "userId": "",
                    "merchantId": online_order_tran_data[2],
                    "errorName": "Error at insertion.",
                    "errorSource": "OrderTransaction file",
                    "errorStatus": 400,
                    "errorDetails": f"Failed to insert data in store front transaction table."
                }
            }
            error_logs_sns_resp = publish_sns_message(topic=config.sns_error_logs, message=str(sns_msg),
                                                        subject="error_logs.entry")

            raise e

    @classmethod
    def ubereats_transactions(cls,ubereats_tran_data):
        connection, cursor = get_db_connection()
        try:
            cursor.execute("""
                            INSERT INTO ubereatstransaction(id, Merchantid , Storename, Orderexternalrefenceid, Transactiontype, dateandtime,Payoutdate, Salesexcltax, Taxonsales, Salesincltax, Orderprocessingfee,Tips,Totalpayout,Marketplacefee,Marketplacefacilitatortax,Transactiondate,Fonda_processing_fee,Revenue_processing_fee_percent,Revenue_processing_threshold, Taxonpromotionitems , Promotionsonitems , Marketingadjustment
                            ) VALUES(%s, %s, %s,%s, %s,%s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s,%s,%s,%s,%s,%s,%s)
                            """,ubereats_tran_data)

            print("Ubereats Execution sucsess--------------------------------------------------------")
        except Exception as e:
            create_log_data(level='[ERROR]',
                        Message=f"Unable to insert data in Ubereats transaction",
                        messagebody=f"Failed to insert data",
                        functionName="ubereats_transactions",
                        statusCode="400 BAD REQUEST",
                        request=request)
            sns_msg = {
                "event": "error_logs.entry",
                "body": {
                    "userId": "",
                    "merchantId": ubereats_tran_data[1],
                    "errorName": "Error at insertion.",
                    "errorSource": "OrderTransaction file",
                    "errorStatus": 400,
                    "errorDetails": f"Failed to insert data in Ubereats transaction table."
                }
            }
            error_logs_sns_resp = publish_sns_message(topic=config.sns_error_logs, message=str(sns_msg),
                                                        subject="error_logs.entry")
            raise e

    @classmethod
    def grubhub_transactions(cls,grubhub_tran_data):
        connection, cursor = get_db_connection()
         #Delivery Fee	Service Fee	Tax Fee Delivery Commission	Targeted Promotion
        try:
            cursor.execute("""
                            INSERT INTO ghrubhubtransaction(id, Orderexternalrefenceid , Fulfillmenttype, Merchantid, Transactiontype, 
                            Description, Dateandtime, Subtotal, tip,
                            Restauranttotal,
                            Commission,
                            Processingfee,
                            Storename,
                            Taxfee,Fonda_processing_fee,Revenue_processing_fee_percent,Revenue_processing_threshold
                            ) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s,%s,%s)
                            """,grubhub_tran_data)
            print(request)
            print("Grubhub Execution sucsess--------------------------------------------------------")
        except Exception as e:
            print(e)
            create_log_data(level='[ERROR]',
                        Message=f"Unable to insert data in grubhub transaction",
                        messagebody=f"Failed to insert data",
                        functionName="grubhub_transactions",
                        statusCode="400 BAD REQUEST",
                        request=request)
            sns_msg = {
                "event": "error_logs.entry",
                "body": {
                    "userId": "",
                    "merchantId": grubhub_tran_data[3],
                    "errorName": "Error at insertion.",
                    "errorSource": "OrderTransaction file",
                    "errorStatus": 400,
                    "errorDetails": f"Failed to insert data in Grubhub transaction table."
                }
            }
            error_logs_sns_resp = publish_sns_message(topic=config.sns_error_logs, message=str(sns_msg),
                                                        subject="error_logs.entry")

            raise e