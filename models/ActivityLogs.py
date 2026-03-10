import uuid
import datetime
from dateutil.tz import gettz

# local imports
from utilities.helpers import get_db_connection


class ActivityLogs():

  ############################################### GET

  @classmethod
  def get_activity_logs(cls, request):
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
      if request.args.get('itemName'):
        conditions.append(f'`itemname` LIKE "%%{request.args.get("itemName")}%%"')
      if request.args.get('eventName'):
        conditions.append(f'`eventname` = "{request.args.get("eventName")}"')
      if request.args.get('orderExternalReference'):
        conditions.append(f'`orderexternalreference` = "{request.args.get("orderExternalReference")}"')

      if request.args.get("startDate") and request.args.get("endDate"):
        startDate = request.args.get('startDate')
        endDate = request.args.get('endDate')
        startDate = datetime.datetime.strptime(startDate, "%Y-%m-%d")
        endDate = datetime.datetime.strptime(endDate, "%Y-%m-%d") + datetime.timedelta(days=1)
        # conditions.append(f'`eventdatetime` BETWEEN "{startDate}" AND "{endDate}"')
        conditions.append(f"""
          convert_tz(
            eventdatetime, 
            '+00:00', 
            COALESCE((SELECT merchants.timezone FROM merchants where merchants.id = activitylogs.merchantid LIMIT 1), 'UTC')
          ) BETWEEN '{startDate}' AND '{endDate}'
        """)

      # where clause handling
      where = ' AND '.join(conditions)
      if not where:
        where = "1"

      cursor.execute(f"""
          SELECT id, userid, username, merchantid, merchantname, 
            itemid, itemname, eventtype, eventname, eventdetails,
            orderexternalreference orderExternalReference,
            woflowcolumnid woflowColumnId,

            @timezone := (SELECT COALESCE((SELECT timezone FROM merchants WHERE id = activitylogs.merchantid LIMIT 1), 'US/Pacific')) as timezone,
            CONCAT(date_format(convert_tz(eventdatetime, '+00:00', @timezone), '%m-%d-%Y %H:%i:%S'), ' (', @timezone, ')') eventdatetime
                
          FROM activitylogs
            WHERE {where} 
            ORDER BY eventdatetime DESC 
            LIMIT {limit} OFFSET {offset}
      """)
      rows = cursor.fetchall()
      print(len(rows))

      return rows
    except Exception as e:
      print("Error: ", str(e))
      return False
  
  ############################################### POST

  @classmethod
  def post_activity_logs(cls, userid, username, merchantid=None, merchantname=None, itemid=None, itemname=None, eventtype=None, eventname=None, eventdetails=None, orderexternalreference=None, woflowcolumnid=None):
    try:
      connection, cursor = get_db_connection()

      log_id = uuid.uuid4()
      data = (log_id, userid, username, merchantid, merchantname, itemid, itemname, eventtype, eventname, eventdetails, orderexternalreference, woflowcolumnid)
      cursor.execute("""INSERT INTO activitylogs 
        (id, userid, username, merchantid, merchantname, itemid, itemname, eventtype, eventname, eventdetails, orderexternalreference, woflowcolumnid)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""", data)
      connection.commit()

      return log_id
    except Exception as e:
      print("Error: ", str(e))
      return False



