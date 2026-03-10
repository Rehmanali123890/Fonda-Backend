import csv
import boto3
from botocore.exceptions import ClientError

# local imports
from utilities.helpers import *
from utilities.errors import *



def send_order_transaction_email(report_data,email):
    sns_order_notification = config.sns_order_notification
    s3_apptopus_bucket = config.s3_apptopus_bucket
    s3_reports_folder = config.s3_reports_folder

    temp_file_name = "report-" + ''.join(random.choices(string.ascii_letters + string.digits, k=7)) + ".csv"
    temp_file_path = "/tmp/" + temp_file_name  # lambda's temp files path
    with open(temp_file_path, mode='w', newline='') as temp_csv:
        writer1 = csv.writer(temp_csv, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        writer1.writerow(['Merchant Name', 'Virtual Merchant Name' , 'Short Order Id', 'Order External Reference', 'Status', 'Customer Name', 'Order Date Time',
                          'Completion Time', 'Order Subtotal', 'Staff Tips', 'Order Tax', 'Ubereats Marketplace Tax', 'Processing Fee', 'Promo Discount', 'Error Charges', 'Order Adjustments', 'Commission', 'Commission Tax','Marketing Fee','Square Fee', 'Order Total', 'Refunded Amount', 'Order Source', "Order Type", "Scheduled"])
        for rec in report_data:
            promo_discount = rec['promodiscount']
            if rec['promo'] is not None:
                promo_discount += f" ({rec['promo']})"
            writer1.writerow([rec['merchantname'], rec['virtualname'], rec['short_order_id'], rec['orderexternalreference'], rec['status_description'], rec['customername'], rec['orderdatetime'],
                              rec['completion_time'], rec['ordersubtotal'], rec['stafftips'], rec['ordertax'], rec['marketplacetax'], rec['processingfee'],  promo_discount, rec['errorcharge'], rec['adjustment'], rec['commission'],rec['Comissiontax'],rec['marketing_fee'], rec['squarefee'], rec['ordertotal'], rec['refund_amount'], rec['ordersource'],
                              rec['ordertype'], rec['scheduled']])
    s3 = boto3.client('s3')
    s3.upload_file(
        temp_file_path,
        s3_apptopus_bucket,
        f"{s3_reports_folder}/{temp_file_name}",
        ExtraArgs={
            "ACL": "public-read"
        }
    )

    # get s3 url of csv
    download_url = s3.generate_presigned_url(
        ClientMethod='get_object',
        Params={
            'Bucket': s3_apptopus_bucket,
            'Key': f"{s3_reports_folder}/{temp_file_name}"
        }
    )
    download_url = download_url.split("?")[0]
    SENDER = f"Fonda <{config.ses_sender_email}>"
    RECIPIENT = email
    SUBJECT = f"Order Transaction Report>"
    BODY_TEXT = (f"Please use the below url to download the report. {download_url}"
                 )
    BODY_HTML = f"""<html>
                        <head></head>
                        <body>
                            <h1>Order Transaction Report</h1>
                            <p>Please use the below url to download the order transaction report
                                <br>
                                <a href='{download_url}'>{download_url}</a>
                            </p>
                        </body>
                        </html>
                    """
    CHARSET = "UTF-8"
    ses_client = boto3.client('ses')

    try:
        # Provide the contents of the email.
        response = ses_client.send_email(
            Destination={
                'ToAddresses': [
                    RECIPIENT,
                ],
            },
            Message={
                'Body': {
                    'Html': {
                        'Charset': CHARSET,
                        'Data': BODY_HTML,
                    },
                    'Text': {
                        'Charset': CHARSET,
                        'Data': BODY_TEXT,
                    },
                },
                'Subject': {
                    'Charset': CHARSET,
                    'Data': SUBJECT,
                },
            },
            Source=SENDER,
        )
    # Display an error if something goes wrong.
    except Exception as e:
        print(e)




def send_financial_report_email(merchantName, subject, report_name, receiver, download_url):
    try:
        # send email to the user
        SENDER = f"Fonda <{config.ses_sender_email}>"
        if isinstance(receiver, str):
            receiver = [receiver, ]
        SUBJECT = f"{subject} <{merchantName}>"
        BODY_TEXT = (f"Please use the below url to download the report. {download_url}"
        )
        BODY_HTML = f"""<html>
            <head></head>
            <body>
                <h1>{subject} {merchantName}</h1>
                <p>Please use the below url to download the {report_name} report
                    <br>
                    <a href='{download_url}'>{download_url}</a>
                </p>
                <br>
                <p><b>Note: </b>The link will be expired after 24 hours</p>
            </body>
            </html>
        """ 
        CHARSET = "UTF-8"
        ses_client = boto3.client('ses')

        try:
            #Provide the contents of the email.
            response = ses_client.send_email(
                Destination={
                    'ToAddresses': receiver,
                },
                Message={
                    'Body': {
                        'Html': {
                            'Charset': CHARSET,
                            'Data': BODY_HTML,
                        },
                        'Text': {
                            'Charset': CHARSET,
                            'Data': BODY_TEXT,
                        },
                    },
                    'Subject': {
                        'Charset': CHARSET,
                        'Data': SUBJECT,
                    },
                },
                Source=SENDER,
            )
        # Display an error if something goes wrong.	
        except ClientError as e:
            print(e.response['Error']['Message'])
            return False, str(e.response['Error']['Message'])

        print(response['MessageId'])
        return True, 'success'
    except Exception as e:
        print(str(e))
        return False, str(e)


def send_payouts_report_email(merchantName, subject, report_name, email, download_url):
    try:
        # send email to the user
        SENDER = f"Fonda <{config.ses_sender_email}>"
        RECIPIENT = email
        SUBJECT = f"{subject} <{merchantName}>"
        BODY_TEXT = (f"Please use the below url to download the report. {download_url}"
        )
        BODY_HTML = f"""<html>
            <head></head>
            <body>
                <h1>{subject} {merchantName}</h1>
                <p>Please use the below url to download the {report_name} report
                    <br>
                    <a href='{download_url}'>{download_url}</a>
                </p>
                <br>
                <p><b>Note: </b>The link will be expired after 24 hours</p>
            </body>
            </html>
        """
        CHARSET = "UTF-8"
        ses_client = boto3.client('ses')

        try:
            #Provide the contents of the email.
            response = ses_client.send_email(
                Destination={
                    'ToAddresses': [
                        RECIPIENT,
                    ],
                },
                Message={
                    'Body': {
                        'Html': {
                            'Charset': CHARSET,
                            'Data': BODY_HTML,
                        },
                        'Text': {
                            'Charset': CHARSET,
                            'Data': BODY_TEXT,
                        },
                    },
                    'Subject': {
                        'Charset': CHARSET,
                        'Data': SUBJECT,
                    },
                },
                Source=SENDER,
            )
        # Display an error if something goes wrong.
        except ClientError as e:
            print(e.response['Error']['Message'])
            return False, str(e.response['Error']['Message'])

        print(response['MessageId'])
        return True, 'success'
    except Exception as e:
        print(str(e))
        return False, str(e)
