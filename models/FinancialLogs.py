import uuid
import datetime
from dateutil.tz import gettz

# local imports
from utilities.helpers import get_db_connection


class FinancialLogs():

    ############################################### GET

    @classmethod
    def get_financial_logs(cls, request):
        try:
            connection, cursor = get_db_connection()

            limit = "30"
            if request.args.get('limit'):
                limit = request.args.get('limit')

            offset = "0"
            if (request.args.get('offset')):
                offset = request.args.get('offset')

            conditions = []

            if request.args.get('userId'):
                conditions.append(f'`userid` = "{request.args.get("userId")}"')
            if request.args.get('userName'):
                conditions.append(f'`username` LIKE "%%{request.args.get("userName")}%%"')
            if request.args.get('merchantId'):
                conditions.append(f'`merchantid` = "{request.args.get("merchantId")}"')
            if request.args.get('eventName'):
                conditions.append(f'`eventname` LIKE "%%{request.args.get("eventName")}%%"')
            if request.args.get('payoutId'):
                conditions.append(f'`payoutid` = "{request.args.get("payoutId")}"')

            if request.args.get("startDate") and request.args.get("endDate"):
                startDate = request.args.get('startDate')
                endDate = request.args.get('endDate')
                startDate = datetime.datetime.strptime(startDate, "%Y-%m-%d")
                endDate = datetime.datetime.strptime(endDate, "%Y-%m-%d") + datetime.timedelta(days=1)
                # startDate = startDate.replace(tzinfo=gettz("US/Pacific")).astimezone(datetime.timezone.utc)
                # endDate = endDate.replace(tzinfo=gettz("US/Pacific")).astimezone(datetime.timezone.utc)
                # conditions.append(f'`eventdatetime` BETWEEN "{startDate}" AND "{endDate}"')
                conditions.append(f"""
          convert_tz(
            eventdatetime, 
            '+00:00', 
            COALESCE((SELECT merchants.timezone FROM merchants where merchants.id = financiallogs.merchantid LIMIT 1), 'UTC')
          ) BETWEEN '{startDate}' AND '{endDate}'
        """)

            # where clause handling
            where = ' AND '.join(conditions)
            if not where:
                where = "1"

            print(where)

            cursor.execute(f"""
        SELECT id, userid, username, merchantid, merchantname, 
          eventname, eventdetails, payoutid, subscriptionid,

          @timezone := (SELECT @timezone := merchants.timezone FROM merchants where merchants.id = financiallogs.merchantid LIMIT 1) as timezone,
          CONCAT ( date_format(convert_tz(eventdatetime, '+00:00', @timezone), '%m-%d-%Y %H:%i:%S'), ' (', @timezone, ')' ) eventdatetime

        FROM financiallogs
          WHERE {where} 
          ORDER BY eventdatetime DESC 
          LIMIT {limit} OFFSET {offset}""")
            rows = cursor.fetchall()

            return rows
        except Exception as e:
            print("Error: ", str(e))
            return False

    ############################################### POST

    @classmethod
    def post_financial_logs(cls, userid, username, merchantid, merchantname, eventname=None, eventdetails=None,
                        payoutid=None, subscriptionid=None):
        try:
            connection, cursor = get_db_connection()

            log_id = uuid.uuid4()
            data = (
            log_id, userid, username, merchantid, merchantname, eventname, eventdetails, payoutid, subscriptionid)
            cursor.execute("""INSERT INTO financiallogs 
                (id, userid, username, merchantid, merchantname, eventname, eventdetails, payoutid, subscriptionid)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""", data)
            connection.commit()

            return log_id
        except Exception as e:
            print("Error: ", str(e))
            return False



