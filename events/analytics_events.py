import base64
import csv
import random
import string
from app import app
import json
import boto3
import logging
import datetime
import uuid
import time
import calendar
import requests
import threading
# local imports
import config
from models.Finance import Finance
from models.Merchants import Merchants
from models.Orders import Orders
from utilities.helpers import closeDbconnection, openDbconnection, success, create_log_data,get_db_connection
from controllers.AnalyticsController import email_merchant_summary_report, send_monthly_payout_report,email_gmb_report
from models.Payouts import Payouts

env = config.env
ses_sender_email= config.ses_sender_email

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def weekly_gmb_cronjob(event, context):
    try:
        current_time = datetime.datetime.now().time()
        logging.info("Your weekly_gmb_cronjob cron function starts running at " + str(current_time))
        if config.env == "development" or config.env == "production" or config.env == "test":
            connection, cursor = openDbconnection()
            cursor.execute("SELECT merchantname FROM merchants m LEFT JOIN googlelocations l on m.id = l.merchantid WHERE l.merchantid IS NULL and m.status =1")
            rows = cursor.fetchall()
            merchantids = []
            for row in rows:
                merchantids.append({'Merchant Name': row['merchantname'], 'GMB Connection Status': 'Not Connected', 'GMB Verified Status': ''})

            cursor.execute("SELECT merchantname, l.status FROM merchants m LEFT JOIN googlelocations l on m.id = l.merchantid WHERE l.status != 3 and m.status =1")
            all_rows = cursor.fetchall()
            status_map = {
                0:'Unverified',
                1:'Pending verify',
                2:'Suspended',
                3:'Verified',
                4:'duplicate'
            }
            temp_file_name = "merchant-" + ''.join(random.choices(string.ascii_letters + string.digits, k=7)) + ".csv"
            temp_file_path = "/tmp/" + temp_file_name

            # temp_file_path = 'D:\\pycharm project\\Dashboard-API\\templates\\tmp\\test.csv'
            for row in all_rows:
                merchantids.append({'Merchant Name': row['merchantname'], 'GMB Connection Status': 'Connected', 'GMB Verified Status': f" {status_map[row['status']]}"})

            with open(temp_file_path, 'w+', newline='') as csvfile:

                fieldnames = ['Merchant Name', 'GMB Connection Status', 'GMB Verified Status']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

                writer.writeheader()
                writer.writerows(merchantids)

                csvfile.seek(0)
                # Read and encode the CSV file contents
                attachment_content = base64.b64encode(csvfile.read().encode()).decode('utf-8')
            print("CSV file created successfully.")

            if env == "production":
                email = ["prem@mifonda.io","sandra@mifonda.io"]
            else:
                email = ["fonda498@gmail.com"]

            subject = "GMB - Merchant Status Report"
            body_html = "<p>Hello Success Team,Please find the attached GMB - Merchant Status Report. Please reach out to IT Support in case of any Questions. <br> Regards, <br> IT Team.<p>"
            email_gmb_report(email,subject,body_html,attachment_content)
            return success()

    except Exception as e:
        print("Error: ", str(e))
    finally:
        closeDbconnection(connection)

def weekly_merchant_loop_cronjob(event, context):
    try:
        ### loggings
        current_time = datetime.datetime.now().time()
        logger.info("Your cron function starts running at " + str(current_time))

        # only trigger weekly report on development and production environment
        if config.env == "development" or config.env == "production":

            connection, cursor = openDbconnection()

            cursor.execute("SELECT * FROM merchants WHERE status = 1")
            rows = cursor.fetchall()
            print(len(rows))
            
            # init sqs client
            sqs_client = boto3.resource('sqs')
            queue = sqs_client.get_queue_by_name(QueueName=config.sqs_merchant_analytics)
            messageGroupId = str(uuid.uuid4())

            for row in rows:
                                
                dataObj = {
                    "event": "analytics.weekly_report",
                    "merchantId": row["id"]
                }
                
                response = queue.send_message(
                    MessageBody=json.dumps(dataObj),
                    MessageGroupId=messageGroupId,
                    MessageDeduplicationId=str(uuid.uuid4())
                )
                print(response)

    except Exception as e:
        print("Error: ", str(e))
    finally:
        closeDbconnection(connection)




def monthly_summary_email_cronjob(event, context):
    try:
        ### loggings
        current_time = datetime.datetime.now().time()
        logger.info("Your monthly cron function starts running at " + str(current_time))

        # only trigger weekly report on development and production environment
        if config.env == "development" or config.env == "production" or config.env == "test":
            print("Your monthly cron function starts running")
            connection, cursor = openDbconnection()

            cursor.execute("SELECT * FROM merchants WHERE status = 1")
            rows = cursor.fetchall()
            print(len(rows))
            print("print only merchants data" ,  rows)
            # init sqs client
            sqs_client = boto3.resource('sqs')
            queue = sqs_client.get_queue_by_name(QueueName=config.sqs_monthly_email_analytics)
            messageGroupId = str(uuid.uuid4())

            for row in rows:
                dataObj = {
                    "event": "analytics.monthly_report",
                    "merchantId": row["id"]
                }

                response = queue.send_message(
                    MessageBody=json.dumps(dataObj),
                    MessageGroupId=messageGroupId,
                    MessageDeduplicationId=str(uuid.uuid4())
                )
                print(response)

    except Exception as e:
        print("Error: ", str(e))
    finally:
        closeDbconnection(connection)


def new_monthly_report_email_cronjob(event, context):
    try:
        ### loggings
        current_time = datetime.datetime.now().time()
        logger.info("Your monthly cron function starts running at " + str(current_time))
        create_log_data(
            level="[INFO]",
            Message="In the start of function to send monthly payout report",
            functionName="new_monthly_report_email_cronjob",
        )
        # only trigger weekly report on development and production environment
        if config.env == "development" or config.env == "production" or config.env == "test":
            print("Your monthly cron function starts running")
            connection, cursor = openDbconnection()

            cursor.execute("SELECT * FROM merchants WHERE status = 1")
            rows = cursor.fetchall()
            create_log_data(
                level="[INFO]",
                Message="Retrieved all merchants from database",
                functionName="new_monthly_report_email_cronjob",
            )
            print(len(rows))
            print("print only merchants data" ,  rows)
            # init sqs client
            sqs_client = boto3.resource('sqs')
            queue = sqs_client.get_queue_by_name(QueueName=config.sqs_new_monthly_email_analytics)
            messageGroupId = str(uuid.uuid4())

            for row in rows:
                create_log_data(
                    level="[INFO]",
                    Message="Sending merchants one by one in queue for monthly report",
                    functionName="new_monthly_report_email_cronjob",
                )
                dataObj = {
                    "event": "analytics.monthly_report",
                    "merchantId": row["id"]
                }

                response = queue.send_message(
                    MessageBody=json.dumps(dataObj),
                    MessageGroupId=messageGroupId,
                    MessageDeduplicationId=str(uuid.uuid4())
                )
                print(response)

    except Exception as e:
        print("Error: ", str(e))
        create_log_data(level='[ERROR]',
                        Message=f"Error to get monthly report",
                        messagebody=f"Reason for error {e}",
                        functionName="new_monthly_report_email_cronjob",
                        statusCode="400 BAD REQUEST")
    finally:
        closeDbconnection(connection)


def payout_summary_report_email_cronjob(event, context):
    try:
        ### loggings
        current_time = datetime.datetime.now().time()
        logger.info("Your monthly payout summary report cron function starts running at " + str(current_time))
        create_log_data(
            level="[INFO]",
            Message="In the start of function to send payout summary report",
            functionName="payout_summary_report_email_cronjob",
        )
        # only trigger weekly report on development and production environment
        if config.env == "development" or config.env == "production" or config.env == "test":
            print("Your monthly payout summary report cron function starts running")
            connection, cursor = openDbconnection()

            cursor.execute("SELECT * FROM merchants WHERE status = 1")
            rows = cursor.fetchall()
            create_log_data(
                level="[INFO]",
                Message="Retrieved all merchants from database",
                functionName="payout_summary_report_email_cronjob",
            )
            print(len(rows))
            print("print only merchants data" ,  rows)
            # init sqs client

            sqs_client = boto3.resource('sqs')
            queue = sqs_client.get_queue_by_name(
                QueueName='PayoutSummaryReportProd-prod.fifo',
                QueueOwnerAWSAccountId='677331532364'
            )
            # queue = sqs_client.get_queue_by_name(QueueName=)
            messageGroupId = str(uuid.uuid4())

            for row in rows:
                create_log_data(
                    level="[INFO]",
                    Message="Sending merchants one by one in queue for monthly report",
                    functionName="payout_summary_report_email_cronjob",
                )
                dataObj = {
                    "event": "analytics.monthly_report",
                    "merchantId": row['id']
                }

                response = queue.send_message(
                    MessageBody=json.dumps(dataObj),
                    MessageGroupId=messageGroupId,
                    MessageDeduplicationId=str(uuid.uuid4())
                )
                print(response)

    except Exception as e:
        print("Error: ", str(e))
        create_log_data(level='[ERROR]',
                        Message=f"Error to get monthly payout summary report",
                        messagebody=f"Reason for error {e}",
                        functionName="payout_summary_report_email_cronjob",
                        statusCode="400 BAD REQUEST")
    finally:
        closeDbconnection(connection)
def monthly_summary_email_cronjob_test(event, context):
    try:
        ### loggings
        current_time = datetime.datetime.now().time()
        logger.info("Your monthly cron function starts running at " + str(current_time))
        print('tesssttt funcccc')
        # only trigger weekly report on development and production environment
        if config.env == "development" or config.env == "production" or config.env == "test":
            print("test function Your monthly cron function starts running")
            connection, cursor = openDbconnection()

            cursor.execute("SELECT * FROM merchants WHERE status = 1")
            rows = cursor.fetchall()
            print(len(rows))
            print("print only merchants data" ,  rows)
            # init sqs client
            # sqs_client = boto3.resource('sqs')
            # queue = sqs_client.get_queue_by_name(QueueName=config.sqs_monthly_email_analytics)
            # messageGroupId = str(uuid.uuid4())
            #
            # for row in rows:
            #   if row["id"] == "c6c3a557-4d01-45b4-ac35-80928ca99707":
            #     dataObj = {
            #         "event": "analytics.monthly_report",
            #         "merchantId": row["id"]
            #     }
            #
            #     response = queue.send_message(
            #         MessageBody=json.dumps(dataObj),
            #         MessageGroupId=messageGroupId,
            #         MessageDeduplicationId=str(uuid.uuid4())
            #     )
            #     print(response)

    except Exception as e:
        print("Error: ", str(e))
    finally:
        closeDbconnection(connection)
def merchant_weekly_analytical_report_event(event, context):
    print("----------------------- weekly analytical report --------------------------")

    with app.app_context():
        print(event)

        for record in event['Records']:
            try:
                time.sleep(1)
                
                print('message body: ' +  record["body"])
                message = json.loads(record["body"])

                subject = message.get("event")
                merchantId = message.get("merchantId")

                if subject == "analytics.weekly_report":

                    current_date = datetime.datetime.utcnow()
                    last_sunday = current_date - datetime.timedelta(days=current_date.isoweekday() % 7)
                    endDate = last_sunday
                    startDate = last_sunday - datetime.timedelta(days=6)

                    endDate = endDate.replace(minute=0, hour=0, second=0, microsecond=0)
                    startDate = startDate.replace(minute=0, hour=0, second=0, microsecond=0)
                    
                    print(startDate)
                    print(endDate)

                    if config.env == "production":
                        email = ["jennifer@mifonda.io", "alexander@mifonda.io"]
                        # email_ps = ["awais@mifonda.io", "sarah@mifonda.io"]
                    else:
                        email = [ "prem@paalam.co.uk","saimabdullah@paalam.co.uk","hammad@paalam.co.uk"]
                        email_ps = ["hammad@paalam.co.uk"]
                    

                    print("\nEmail weekly analytics report...")
                    # resp = Orders.generate_merchant_weekly_analytical_report(merchantId, startDate, endDate, email)
                    resp = Finance.generate_merchant_weekly_finance_analytical_report(merchantId, startDate, endDate,email)
                    print(resp.get_json())

                    if config.env == "development":
                        print("\nEmail payout summary report for 2nd last week...")
                        startDate_ps = startDate - datetime.timedelta(days=7)
                        endDate_ps = endDate - datetime.timedelta(days=7)
                        resp = email_merchant_summary_report(
                            merchantId=merchantId,
                            startDate=startDate_ps,
                            endDate=endDate_ps,
                            payoutType=2,
                            email=email_ps
                        )
                        print(resp.get_json())


            except Exception as e:
                print("Error: ", str(e))


def merchant_monthly_analytical_report_event(event, context):
    print("----------------------- Monthly analytical report --------------------------")

    with app.app_context():
        print(event)

        for record in event['Records']:
            try:
                time.sleep(1)

                print('message body: ' + record["body"])
                message = json.loads(record["body"])

                subject = message.get("event")
                merchantId = message.get("merchantId")


                if subject == "analytics.monthly_report":
                    ####################### weekly for testing #############
                    # current_date = datetime.datetime.utcnow()
                    # endDate = current_date - datetime.timedelta(days=1)
                    # startDate = endDate - datetime.timedelta(days=6)
                    #
                    # endDate = endDate.replace(minute=0, hour=0, second=0, microsecond=0)
                    # startDate = startDate.replace(minute=0, hour=0, second=0, microsecond=0)
                    ####################### Monthly ###########################
                    current_date = datetime.datetime.utcnow()

                    # Get the first day of the previous month
                    first_day_of_previous_month = current_date.replace(day=1) - datetime.timedelta(days=1)
                    first_day_of_previous_month = first_day_of_previous_month.replace(day=1)

                    # Get the last day of the previous month
                    _, last_day_of_previous_month = calendar.monthrange(first_day_of_previous_month.year,
                                                                        first_day_of_previous_month.month)

                    # Construct the start date and end date
                    startDate = first_day_of_previous_month.replace(day=1)
                    endDate = first_day_of_previous_month.replace(day=last_day_of_previous_month)

                    endDate = endDate.replace(minute=0, hour=0, second=0, microsecond=0)
                    startDate = startDate.replace(minute=0, hour=0, second=0, microsecond=0)
                    ###########################################################################3
                    print(startDate)
                    print(endDate)
                    print("\nEmail monthly analytics report...for merchant ", merchantId)
                    emails = Payouts.get_emailDistributionList(merchantId=merchantId)
                    if emails[0]['emaildistributionlist'] != None:
                        email = emails[0]['emaildistributionlist'].split(';')
                        print("\nEmail monthly analytics report to... ", emails[0]['emaildistributionlist'])
                        # email = ['fondaabc@gmail.com','hammad@paalam.co.uk']
                        resp = send_monthly_payout_report(merchantId, startDate, endDate, email)
                        print(resp)
                    else:
                        print("\nif emaildistributionlist == None or empty:"  ,merchantId )
                    # else:
                    #     print("\nDistribution list is empty")

            except Exception as e:
                print("Error: ", str(e))


def merchant_new_monthly_analytical_report_event(event, context):
    print("----------------------- Monthly analytical report --------------------------")

    with app.app_context():
        print(event)

        for record in event['Records']:
            try:
                time.sleep(1)
                create_log_data(
                    level="[INFO]",
                    Message="In the start of function to send monthly payout report to each merchant",
                    functionName="merchant_new_monthly_analytical_report_event",
                )
                print('message body: ' + record["body"])
                message = json.loads(record["body"])
                merchantId = message.get("merchantId")
                payoutType = 2
                current_date = datetime.datetime.utcnow()

                # Get the first day of the previous month
                first_day_of_previous_month = current_date.replace(day=1) - datetime.timedelta(days=1)
                first_day_of_previous_month = first_day_of_previous_month.replace(day=1)

                # Get the last day of the previous month
                _, last_day_of_previous_month = calendar.monthrange(first_day_of_previous_month.year,
                                                                    first_day_of_previous_month.month)
                create_log_data(
                    level="[INFO]",
                    Message="Get the first and last day of previous month",
                    messagebody=f'First day {first_day_of_previous_month} and last day {last_day_of_previous_month}',
                    functionName="merchant_new_monthly_analytical_report_event",
                )

                # Construct the start date and end date
                startDate = first_day_of_previous_month.replace(day=1)
                endDate = first_day_of_previous_month.replace(day=last_day_of_previous_month)

                endDate = endDate.replace(minute=0, hour=0, second=0, microsecond=0)
                startDate = startDate.replace(minute=0, hour=0, second=0, microsecond=0)
                ###########################################################################3
                print(startDate)
                print(endDate)
                print("\nEmail monthly analytics report...for merchant ", merchantId)
                emails = Payouts.get_emailDistributionList(merchantId=merchantId)
                create_log_data(
                    level="[INFO]",
                    Message="Successfully get the email distribution list of merchant",
                    messagebody=f'Email distribution list for merchant {emails}',
                    functionName="merchant_new_monthly_analytical_report_event",
                )
                if emails[0]['emaildistributionlist'] != None:
                    email = emails[0]['emaildistributionlist'].split(';')
                    Finance.new_monthly_payout_summary_report(merchantId, startDate, endDate, payoutType, email)
                else:
                    create_log_data(
                        level="[INFO]",
                        Message="Email distribution list is empty therefore monthly report is not sent",
                        messagebody=f"Email distribution list {emails}",
                        functionName="merchant_new_monthly_analytical_report_event",
                    )
                    print("\nif emaildistributionlist == None or empty:"  ,merchantId )
                # else:
                #     print("\nDistribution list is empty")

            except Exception as e:
                create_log_data(level='[ERROR]',
                                Message=f"Error to send monthly payout report",
                                messagebody=f"{str(e)}",
                                functionName="merchant_new_monthly_analytical_report_event",
                                statusCode="400 BAD REQUEST",)
                print("Error: ", str(e))


def merchant_payout_summary_report_event(event, context):
    print("----------------------- Monthly Payout Summary report --------------------------")

    with app.app_context():
        print(event)

        for record in event['Records']:
            try:
                time.sleep(1)
                create_log_data(
                    level="[INFO]",
                    Message="In the start of function to send monthly payout summary report to each merchant",
                    functionName="merchant_payout_summary_report_event",
                )
                print('message body: ' + record["body"])
                message = json.loads(record["body"])
                merchantId = message.get("merchantId")
                merchant_record = Merchants.get_merchant_by_id(merchantId)
                current_date = datetime.datetime.utcnow()

                # Get the first day of the previous month
                first_day_of_previous_month = current_date.replace(day=1) - datetime.timedelta(days=1)
                first_day_of_previous_month = first_day_of_previous_month.replace(day=1)

                # Get the last day of the previous month
                _, last_day_of_previous_month = calendar.monthrange(first_day_of_previous_month.year,
                                                                    first_day_of_previous_month.month)
                create_log_data(
                    level="[INFO]",
                    Message="Get the first and last day of previous month",
                    messagebody=f'First day {first_day_of_previous_month} and last day {last_day_of_previous_month}',
                    functionName="merchant_payout_summary_report_event",
                )

                # Construct the start date and end date
                startDate = first_day_of_previous_month.replace(day=1)
                endDate = first_day_of_previous_month.replace(day=last_day_of_previous_month)

                endDate = endDate.replace(minute=0, hour=0, second=0, microsecond=0)
                startDate = startDate.replace(minute=0, hour=0, second=0, microsecond=0)
                template_endDate = endDate
                endDate = endDate + datetime.timedelta(days=1)
                ###########################################################################3
                print(startDate)
                print(endDate)
                print("\nEmail monthly payout summary analytics report...for merchant ", merchant_record['id'])
                emails = Payouts.get_emailDistributionList(merchantId=merchant_record['id'])
                create_log_data(
                    level="[INFO]",
                    Message="Successfully get the email distribution list of merchant",
                    messagebody=f'Email distribution list for merchant {emails}',
                    functionName="merchant_payout_summary_report_event",
                )
                if emails[0]['emaildistributionlist'] != None:
                    email = emails[0]['emaildistributionlist'].split(';')
                    Finance.newpayout_summary_report(merchant_record, startDate, endDate,template_endDate, email)
                else:
                    create_log_data(
                        level="[INFO]",
                        Message="Email distribution list is empty therefore monthly report is not sent",
                        messagebody=f"Email distribution list {emails}",
                        functionName="merchant_payout_summary_report_event",
                    )
                    print("\nif emaildistributionlist == None or empty:"  ,merchant_record )
                # else:
                #     print("\nDistribution list is empty")

            except Exception as e:
                create_log_data(level='[ERROR]',
                                Message=f"Error to send monthly payout summary report",
                                messagebody=f"{str(e)}",
                                functionName="merchant_payout_summary_report_event",
                                statusCode="400 BAD REQUEST",)
                print("Error: ", str(e))


def merchant_payout_summary_report_event_example(event, context):
    print("----------------------- Monthly Payout Summary report --------------------------")

    with app.app_context():
        print(event)
        for record in event['Records']:
            try:
                time.sleep(1)
                create_log_data(
                    level="[INFO]",
                    Message="In the start of function to send monthly payout summary report to each merchant",
                    functionName="merchant_payout_summary_report_event",
                )
                print('message body: ' + record["body"])
                message = json.loads(record["body"])
                merchantId = message.get("merchantId")
                if merchantId == '672b5857-36d4-492c-9d66-f255741d1ea8':
                    merchant_record = Merchants.get_merchant_by_id(merchantId)
                    current_date = datetime.datetime.utcnow()

                    # Get the first day of the previous month
                    first_day_of_previous_month = current_date.replace(day=1) - datetime.timedelta(days=1)
                    first_day_of_previous_month = first_day_of_previous_month.replace(day=1)

                    # Get the last day of the previous month
                    _, last_day_of_previous_month = calendar.monthrange(first_day_of_previous_month.year,
                                                                        first_day_of_previous_month.month)
                    create_log_data(
                        level="[INFO]",
                        Message="Get the first and last day of previous month",
                        messagebody=f'First day {first_day_of_previous_month} and last day {last_day_of_previous_month}',
                        functionName="merchant_payout_summary_report_event",
                    )

                    # Construct the start date and end date
                    startDate = first_day_of_previous_month.replace(day=1)
                    endDate = first_day_of_previous_month.replace(day=last_day_of_previous_month)

                    endDate = endDate.replace(minute=0, hour=0, second=0, microsecond=0)
                    startDate = startDate.replace(minute=0, hour=0, second=0, microsecond=0)
                    template_endDate = endDate
                    endDate = endDate + datetime.timedelta(days=1)
                    ###########################################################################3
                    print(startDate)
                    print(endDate)
                    print("\nEmail monthly payout summary analytics report...for merchant ", merchant_record['id'])
                    email = ["ahmad@paalam.co.uk","fondaabc@gmail.com"]
                    create_log_data(
                        level="[INFO]",
                        Message="Successfully get the email distribution list of merchant",
                        messagebody=f'Email distribution list for merchant {email}',
                        functionName="merchant_payout_summary_report_event",
                    )
                    Finance.newpayout_summary_report(merchant_record, startDate, endDate, template_endDate, email)

            except Exception as e:
                create_log_data(level='[ERROR]',
                                Message=f"Error to send monthly payout summary report",
                                messagebody=f"{str(e)}",
                                functionName="merchant_payout_summary_report_event",
                                statusCode="400 BAD REQUEST", )
                print("Error: ", str(e))


def merchant_new_monthly_analytical_report_event_example():
    print("----------------------- Monthly analytical report --------------------------")

    try:
        time.sleep(1)

        merchantId = "672b5857-36d4-492c-9d66-f255741d1ea8"
        payoutType = 2
        current_date = datetime.datetime.utcnow()

        # Get the first day of the previous month
        first_day_of_previous_month = current_date.replace(day=1) - datetime.timedelta(days=1)
        first_day_of_previous_month = first_day_of_previous_month.replace(day=1)

        # Get the last day of the previous month
        _, last_day_of_previous_month = calendar.monthrange(first_day_of_previous_month.year,
                                                            first_day_of_previous_month.month)

        # Construct the start date and end date
        startDate = first_day_of_previous_month.replace(day=1)
        endDate = first_day_of_previous_month.replace(day=last_day_of_previous_month)

        endDate = endDate.replace(minute=0, hour=0, second=0, microsecond=0)
        startDate = startDate.replace(minute=0, hour=0, second=0, microsecond=0)
        ###########################################################################3
        print(startDate)
        print(endDate)
        print("\nEmail monthly analytics report...for merchant ", merchantId)
        emails = Payouts.get_emailDistributionList(merchantId=merchantId)
        if emails[0]['emaildistributionlist'] != None:
            email = emails[0]['emaildistributionlist'].split(';')
            Finance.new_monthly_payout_summary_report(merchantId, startDate, endDate, payoutType, email)
            return success()
        else:
            print("\nif emaildistributionlist == None or empty:"  ,merchantId )
        # else:
        #     print("\nDistribution list is empty")

    except Exception as e:
        print("Error: ", str(e))
