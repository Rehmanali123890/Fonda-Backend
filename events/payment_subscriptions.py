import copy
import datetime
import uuid
from datetime import timedelta
from dateutil.relativedelta import relativedelta
from dateutil.tz import gettz

import config
from utilities.helpers import openDbconnection, success, publish_sns_message, create_log_data


def insert_into_subscription_table(id, date, amount, status, frequency, istrail,remarks=''):
  connection, cursor = openDbconnection()
  cursor.execute("""INSERT INTO subscriptions ( merchantId, amount, date, status, frequency, istrail,waiveoff_remarks) VALUES (%s,%s,%s,%s,%s,%s,%s) """, (id, amount, date, status, frequency, istrail,remarks))
  connection.commit()
  connection.close()
  return True


def update_logs_subscription(merchantId, detail):
  sns_msg = {
    "event": "subscription.create_record",
    "body": {
      "merchantId": merchantId,
      "detail": detail
    }
  }
  logs_sns_resp = publish_sns_message(topic=config.sns_audit_logs, message=str(sns_msg), subject="subscription.create_record")


def calculate_subscription(event, context):

  # today's date
  utc_today_date = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)
  create_log_data(
    level="[INFO]",
    Message="Get a today date in utc time standard",
    functionName="calculate_subscription",
  )
  
  # db connection
  connection, cursor = openDbconnection()

  # get all merchants whose status = subscriptionstatus = 1 (active and in-business)
  cursor.execute("SELECT * FROM merchants WHERE merchants.status = 1 AND merchants.subscriptionstatus = 1")
  rows = cursor.fetchall()
  create_log_data(
    level="[INFO]",
    Message="Retrieved all merchants whose subscription status is 1 and they are active",
    functionName="calculate_subscription",
  )

  for row in rows:
    try:
      today_date = utc_today_date.astimezone(gettz(row["timezone"])).date()
      create_log_data(
        level="[INFO]",
        Message="Coverted utc date according to merchant time zone",
        functionName="calculate_subscription",
      )
      merchantId=row['id']

      if row['AutoWaivedStatus'] == 1:
        cursor.execute(f""" SELECT  SUM(total) as total FROM
                                    (
                                        (SELECT
                                        SUM(ordertotal) as total
                                        FROM orders WHERE status=7
                                        and merchantid=%s and orderdatetime < %s)
                                        UNION
                                       (SELECT  SUM(ordertotal) as total
                                      FROM ordershistory WHERE status=7
                                        and merchantid=%s and orderdatetime < %s)
                                    ) AS result
                                                  """, (merchantId, today_date, merchantId, today_date))
        lifetime_total_revenue = cursor.fetchone()
        if lifetime_total_revenue['total'] == None:
          lifetime_total_revenue['total'] = 0

        cursor.execute(f"""SELECT 
                                               SUM(totalTime) as total ,
                                               SUM(pauseTime) as totalpauseTime ,
                                               SUM(resumeTime) as totalresumeTime 
                                               FROM downtime 
                                               WHERE merchantId = %s and date >= DATE_SUB(NOW(), INTERVAL 30 DAY)""",
                       (merchantId))
        DownTimeData = cursor.fetchone()
        downtime = 0
        if DownTimeData['totalpauseTime'] != None and DownTimeData['total'] != None:
          downtime = (DownTimeData['totalpauseTime'] / DownTimeData['total']) * 100
          downtime = round(downtime, 2)
          create_log_data(
            level="[INFO]",
            Message=f"Calculate the downtime of merchant which is {downtime}",
            functionName="calculate_subscription",
          )


      subscription_start_date = row['subscriptionstartdate']
      next_charge_date = row['nextsubscriptionchargedate']
      subscription_frequency = int(row['subscriptionfrequency'])
      subscription_trail_period = int(row['subscriptiontrialperiod'])
      subscription_amount = row.get('subscriptionamount', 0)

      create_log_data(
        level="[INFO]",
        Message=f"Fetch all subscriptions detail related to merchant such as  subscription start date {subscription_start_date}, next charge date {next_charge_date}, frequency after which subscription is charged {subscription_frequency} and subscription amount {subscription_amount}",
        functionName="calculate_subscription",
      )

      print(f"\n----- merchant_name : {row['merchantname']} ------")
      print("TODAY DATE IN MERCHANT TIMEZONE: ", today_date)
      print("SUB START DATE: ", subscription_start_date)
      print("NXT CHG DATE: ", next_charge_date)
      print("SUB TRAIL: ", str(subscription_trail_period))
      print("SUB FREQ: ", str(subscription_frequency))
      print("TRAIL PERIOD LAST DATE: ", subscription_start_date + relativedelta(months=subscription_trail_period, days=-1))
      
      ### addition check for -> beta
      if next_charge_date is None:
        create_log_data(
          level="[INFO]",
          Message=f"Subscription start date is set as next charge date",
          functionName="calculate_subscription",
        )
        next_charge_date = subscription_start_date
      
      # check if subscript start date is correct
      if subscription_start_date is None:
        create_log_data(
          level="[ERROR]",
          Message=f"Subscription start date is not correct",
          functionName="calculate_subscription",
        )
        print("error: subscription start date is incorrect!!!")
        update_logs_subscription(merchantId, 'error: subscription start date is incorrect!!!')
        continue

      # if today date is greater than the subscription start date
      if today_date >= subscription_start_date:
        print("0000000000000000000")

        cursor.execute("""SELECT * FROM subscriptions WHERE merchantid= %s ORDER BY date DESC LIMIT 1""", (row['id']))
        previous_subscription = cursor.fetchall()
        create_log_data(
          level="[INFO]",
          Message=f"Fetch all the previous subscriptions of merchant {merchantId}",
          messagebody=f'{previous_subscription}',
          functionName="calculate_subscription",
        )

        ### if this is the first time subscription charge
        if previous_subscription.__len__() == 0:
          print("AAAAAAAAAAAAAAAAAAAA")
          # no previous subscription exists, now add data in subscription table based on merchant subscription startdate from merchant table
          if subscription_trail_period == 0:
            create_log_data(
              level="[INFO]",
              Message=f"Merchant is not in trial period so subscription get charged to merchant {merchantId}",
              functionName="calculate_subscription",
            )
            print("NO TRAIL AND FIRST SUBSCRIPTION -> MONEY CHARGED")

            if row['AutoWaivedStatus'] == 1 and lifetime_total_revenue['total'] < row['minimumLifetimeRevenue'] and downtime < row['DownTimeThreshold']:
              print("total revenue is less than the minimumlife time revenue and downtime % less than the downtime threshold")
              remarks = f"The minimum revenue and downtime threshold requirement is not met as the current revenue ( ${lifetime_total_revenue['total']} ) and downtime ( {downtime}%) falls below the minimum lifetime revenue threshold ( ${row['minimumLifetimeRevenue']} ) and downtime threshold ( {row['DownTimeThreshold']}% ) respectively."
              insert_into_subscription_table(merchantId, subscription_start_date, subscription_amount, 4,
                                             subscription_frequency, 0, remarks=remarks)
              create_log_data(
                level="[INFO]",
                Message=f"Merchant subscription charges is inserted in subscription table with remarks",
                messagebody=f"The minimum revenue and downtime threshold requirement is not met as the current revenue ( ${lifetime_total_revenue['total']} ) and downtime ( {downtime}%) falls below the minimum lifetime revenue threshold ( ${row['minimumLifetimeRevenue']} ) and downtime threshold ( {row['DownTimeThreshold']}% ) respectively.",
                functionName="calculate_subscription",
              )

            else:
              print("total revenue is equal or greater than the minimumlife time revenue")
              insert_into_subscription_table(merchantId, subscription_start_date, subscription_amount, 0,
                                             subscription_frequency, 0)
              create_log_data(
                level="[INFO]",
                Message=f"Merchant subscription charges is inserted in subscription table with",
                functionName="calculate_subscription",
              )
            if subscription_frequency == 4:
              next_charge_date = subscription_start_date + timedelta(weeks=1)
              create_log_data(
                level="[INFO]",
                Message=f"Next charge date is set after 1 week",
                functionName="calculate_subscription",
              )
            else:
              create_log_data(
                level="[INFO]",
                Message=f"Next charge date is set after {subscription_frequency} months",
                functionName="calculate_subscription",
              )
              next_charge_date = subscription_start_date + relativedelta(months=subscription_frequency)
            update_logs_subscription(merchantId,
                                     'Trail period is zero. subscription record created for first time with amount ' + str(
                                       subscription_amount))


          else:
            print("IN TRAIL AND FIRST SUBSCRIPTION -> NO MONEY CHARGED")
            create_log_data(
              level="[INFO]",
              Message=f"Merchant is  in trial period so no subscription get charged to merchant {merchantId} and next charge date is set after 1 month {next_charge_date}",
              functionName="calculate_subscription",
            )
            insert_into_subscription_table(merchantId, subscription_start_date, 0, 0, 1, 1)
            next_charge_date = subscription_start_date + relativedelta(months=1)
            update_logs_subscription(row['id'], 'Subscription record created for first time with zero amount')

          ### TODO: update merchants table -> nextChargeDate
          cursor.execute("UPDATE merchants SET nextsubscriptionchargedate = %s WHERE id = %s", (next_charge_date, merchantId))
          connection.commit()
          create_log_data(
            level="[INFO]",
            Message=f"Update next charge date of merchant in merchants table",
            functionName="calculate_subscription",
          )
          ### END TODO

        else:
          print("BBBBBBBBBBBBBBBBBBBBBB")

          # if today date is greater or equal to next charge date
          if today_date >= next_charge_date:

            # compare start_date + trail_period <= next_charge_date
            # if subscription_start_date + relativedelta(months=subscription_trail_period, days=-1) >= next_charge_date:
            cursor.execute("""SELECT COUNT(*) as trails_count FROM subscriptions WHERE merchantid=%s AND istrail=1 AND date >= %s""", (merchantId, subscription_start_date))
            trails_count = cursor.fetchone()['trails_count']
            print("TRAIL COUNTS: ", str(trails_count))
            if trails_count < subscription_trail_period:
              # means we are still in trail period
              print("IN TRAIL -> NO MONEY CHARGED")
              create_log_data(
                level="[INFO]",
                Message=f"Merchant trial count is less than subscription trial count so no money is charged",
                functionName="calculate_subscription",
              )

              insert_into_subscription_table(merchantId, next_charge_date, 0, 0, 1, 1)
              next_charge_date = next_charge_date + relativedelta(months=1)
              update_logs_subscription(merchantId, 'Trail is not over, subscription record created with zero amount')
            else:
                print("NO TRAIL -> MONEY CHARGED")
                create_log_data(
                  level="[INFO]",
                  Message=f"Merchant is not in trial period so money is charged",
                  functionName="calculate_subscription",
                )
                if row['AutoWaivedStatus'] == 1 and lifetime_total_revenue['total'] < row[
                  'minimumLifetimeRevenue'] and downtime < row['DownTimeThreshold']:
                  print("total revenue is less than the minimumlife time revenue")
                  remarks = f"The minimum revenue and downtime threshold requirement is not met as the current revenue ( ${lifetime_total_revenue['total']} ) and downtime ( {downtime}%) falls below the minimum lifetime revenue threshold ( ${row['minimumLifetimeRevenue']} ) and downtime threshold ( {row['DownTimeThreshold']}% ) respectively."
                  insert_into_subscription_table(merchantId, next_charge_date, subscription_amount,
                                                 4,
                                                 subscription_frequency, 0, remarks=remarks)
                  create_log_data(
                    level="[INFO]",
                    Message=f"Merchant subscription charges is inserted in subscription table with remarks",
                    messagebody=f"The minimum revenue and downtime threshold requirement is not met as the current revenue ( ${lifetime_total_revenue['total']} ) and downtime ( {downtime}%) falls below the minimum lifetime revenue threshold ( ${row['minimumLifetimeRevenue']} ) and downtime threshold ( {row['DownTimeThreshold']}% ) respectively.",
                    functionName="calculate_subscription",
                  )

                else:
                  print("total revenue is equal or greater than the minimumlife time revenue")
                  insert_into_subscription_table(merchantId, next_charge_date, subscription_amount,
                                                 0,
                                                 subscription_frequency, 0)
                if subscription_frequency == 4:
                  next_charge_date = next_charge_date + timedelta(weeks=1)
                  create_log_data(
                    level="[INFO]",
                    Message=f"Next charge date is set after 1 week",
                    functionName="calculate_subscription",
                  )
                  # Adds 1 week
                else:
                  next_charge_date = next_charge_date + relativedelta(months=subscription_frequency)
                  create_log_data(
                    level="[INFO]",
                    Message=f"Next charge date is set after {subscription_frequency} months",
                    functionName="calculate_subscription",
                  )
                update_logs_subscription(merchantId, 'subscription record created with amount: ' + str(
                  subscription_amount))


            ### TODO: update merchants table -> nextChargeDate
            cursor.execute("UPDATE merchants SET nextsubscriptionchargedate = %s WHERE id = %s", (next_charge_date, merchantId))
            connection.commit()
            create_log_data(
              level="[INFO]",
              Message=f"Update next charge date of merchant in merchants table",
              functionName="calculate_subscription",
            )
            ### END TODO

          else:
            # YOUR TIME IS NOT YET ARRIVED
            print("YOUR TIME IS NOT YET ARRIVED")
      else:
        ### today date is not greater than subscription start date.
        ### NOTHING TO DO
        pass
      
    except Exception as e:
      print(f"Error: {row['merchantname']} -> ", str(e))
      create_log_data(
        level="[ERROR]",
        Message=f"Failed to upload next charge date and subscription amount due to error {str(e)}",
        functionName="calculate_subscription",
        statusCode="400 Bad Request",
      )
      
      # Triggering SNS -> error_logs.entry
      sns_msg = {
          "event": "error_logs.entry",
          "body": {
              "userId": None,
              "merchantId": row['id'],
              "errorName": "Subscription Error",
              "errorSource": "dashboard",
              "errorStatus": 500,
              "errorDetails": f"Please forward it to IT-Team: {str(e)}"
          }
      }
      error_logs_sns_resp = publish_sns_message(topic=config.sns_error_logs, message=str(sns_msg), subject="error_logs.entry")
  

  # close db connection
  connection.close()


def scheduler_function(event, context):

    print('---------------scheduler Downtime -------------- ')
    connection, cursor = openDbconnection()
    today_date = datetime.datetime.now()

    one_day = datetime.timedelta(days=1)
    previous_date = today_date - one_day

    day = previous_date.strftime("%A")
    today = previous_date.date()

    cursor.execute("SELECT id FROM merchants")
    merchants = cursor.fetchall()
    merchantcount = 0
    for merchant in merchants:
      merchantcount = merchantcount + 1
      print("merchant id is ", merchant['id'])
      svaetoDB = False
      merchantId = merchant['id']
      
      cursor.execute("""SELECT timezone FROM merchants where id=%s""",(merchantId,))
      _mtz = cursor.fetchone()
      _mtz=_mtz['timezone']
      
      if not _mtz:
        _mtz = "US/Pacific"
      
      cursor.execute(
          """SELECT pauseresumetimes.id, pauseresumetimes.merchantid, pauseresumetimes.userid, pauseresumetimes.eventtype,
              CONVERT_TZ(pauseresumetimes.eventdatetime, 'UTC', %s) AS eventdatetime 
              FROM pauseresumetimes 
              WHERE merchantid = %s 
              AND DATE(CONVERT_TZ(pauseresumetimes.eventdatetime, 'UTC', %s)) = %s
              ORDER BY eventdatetime ASC""",
          (_mtz,merchantId,_mtz, today))
      response = cursor.fetchall()

      length_of_response = len(response) - 1

      cursor.execute("SELECT * FROM merchantopeninghrs WHERE merchantid = %s and day =%s", (merchantId, day))
      fetch_data = cursor.fetchall()
      resumeMinutes = 0
      pauseMinutes = 0
      total = 0

      for openining_time in fetch_data:
        if openining_time['closetime'] != '' and openining_time['opentime'] != '':
          svaetoDB = True
          closetime = openining_time['closetime']
          closetime_obj = datetime.datetime.strptime(closetime, "%I:%M %p").time()
          closetime = closetime_obj.strftime("%H:%M:%S")

          opentime = openining_time['opentime']
          opentime_obj = datetime.datetime.strptime(opentime, "%I:%M %p").time()
          opentime = opentime_obj.strftime("%H:%M:%S")

          start_time = str(today) + ' ' + opentime
          end_time = str(today) + ' ' + closetime

          total_minutes = (datetime.datetime.strptime(end_time, '%Y-%m-%d %H:%M:%S')) - (
            datetime.datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S'))
          total_minutes = round(total_minutes.seconds / 60)
          total = total + total_minutes

          if opentime > closetime:
            next_day = today_date.strftime("%A")
            next_day_date = today_date.date()
            midnight_time = datetime.time(0, 0, 0)  # Create a time object representing midnight
            next_day_start_datetime = datetime.datetime.combine(today_date, midnight_time)
            midnight_time = datetime.time(0, 0, 0)
            next_day_close_datetime = datetime.datetime.combine(today_date, closetime_obj)
            next_day_end_time = next_day_close_datetime.strftime("%Y-%m-%d %H:%M:%S")
            next_day_start_time = next_day_start_datetime.strftime("%Y-%m-%d %H:%M:%S")
            end_time = next_day_start_time
            if next_day_start_datetime != next_day_close_datetime:
              print("--------------------------------------")
              print(next_day_start_time)
              print(next_day_end_time)
              print("--------------------------------------")
              cursor.execute(
                  """SELECT pauseresumetimes.id, pauseresumetimes.merchantid, pauseresumetimes.userid, pauseresumetimes.eventtype,
                  CONVERT_TZ(pauseresumetimes.eventdatetime, 'UTC', %s) AS eventdatetime 
                  FROM pauseresumetimes WHERE merchantid = %s 
                  AND CONVERT_TZ(pauseresumetimes.eventdatetime, 'UTC', %s) 
                  BETWEEN %s AND %s 
                  ORDER BY eventdatetime ASC""",
                  (_mtz, merchantId, _mtz, next_day_start_datetime, next_day_close_datetime)
              )
              
              next_day_response = cursor.fetchall()
              next_day_length_of_response = len(next_day_response) - 1

              resumeMinutes, pauseMinutes, total = calculate_pause_resume_minutes(merchantId, next_day_response,
                                                                                      pauseMinutes, resumeMinutes,
                                                                                      next_day_length_of_response,
                                                                                      next_day_date, total,
                                                                                      next_day_start_time,
                                                                                      next_day_end_time, connection,
                                                                                      cursor)

          resumeMinutes, pauseMinutes, total = calculate_pause_resume_minutes(merchantId, response,
                                                                                  pauseMinutes, resumeMinutes,
                                                                                  length_of_response, today, total,
                                                                                  start_time, end_time, connection,
                                                                                  cursor)

      # pauseMinutes = total - resumeMinutes

      if svaetoDB == True:
        id = uuid.uuid4()
        data = (id, merchantId, total, pauseMinutes, resumeMinutes, today)
        cursor.execute("""INSERT INTO downtime
                          (id, merchantId,totalTime,pauseTime,resumeTime,date)
                          VALUES (%s,%s,%s,%s,%s,%s)""", data)
        connection.commit()
    return resumeMinutes, pauseMinutes, total

def calculate_pause_resume_minutes( merchantId, response, pauseMinutes, resumeMinutes, length_of_response, today,
                                   total, start_time, end_time,connection, cursor):
  print(' call calculate_pause_resume_minutes func')

  if len(response) > 0:
    resp = copy.deepcopy(response)

    for row in resp:
      if str(row['eventdatetime']) < start_time:
        row['eventdatetime'] = datetime.datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S')
      elif str(row['eventdatetime']) > end_time:
        row['eventdatetime'] = datetime.datetime.strptime(end_time, '%Y-%m-%d %H:%M:%S')
      else:
        if '.' in str(row['eventdatetime']):
          row['eventdatetime'] = datetime.datetime.strptime(str(row['eventdatetime']), '%Y-%m-%d %H:%M:%S.%f')
        else:
          row['eventdatetime'] = datetime.datetime.strptime(str(row['eventdatetime']), '%Y-%m-%d %H:%M:%S')

    eventdatetime = resp[0]['eventdatetime']
    eventtype = resp[0]['eventtype']
    start_time = datetime.datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S')
    end_time = datetime.datetime.strptime(end_time, '%Y-%m-%d %H:%M:%S')

    if eventdatetime > start_time:
      if eventtype == "RESUMED":
        time_difference = (eventdatetime - start_time).total_seconds() / 60
        pauseMinutes = pauseMinutes + time_difference
        print('Resumed')
      else:
        time_difference = (eventdatetime - start_time).total_seconds() / 60
        resumeMinutes = resumeMinutes + time_difference
        print('paused')

    for row in resp:
      if row['eventtype'] == "RESUMED":
        time_difference = (row['eventdatetime'] - eventdatetime).total_seconds() / 60
        pauseMinutes = pauseMinutes + time_difference
        print('Resumed')
      else:
        time_difference = (row['eventdatetime'] - eventdatetime).total_seconds() / 60
        resumeMinutes = resumeMinutes + time_difference
        print('paused')
      eventdatetime = row['eventdatetime']

    if resp[length_of_response]['eventdatetime'] < end_time:
      if resp[length_of_response]['eventtype'] == "RESUMED":
        time_difference = (end_time - resp[length_of_response]['eventdatetime']).total_seconds() / 60
        resumeMinutes = resumeMinutes + time_difference
      else:
        time_difference = (end_time - resp[length_of_response]['eventdatetime']).total_seconds() / 60
        pauseMinutes = pauseMinutes + time_difference

    resumeMinutes = round(resumeMinutes)
    pauseMinutes = round(pauseMinutes)
  else:
    # one_day = datetime.timedelta(days=1)
    # yesterday_date = today - one_day

    cursor.execute(
        """SELECT pauseresumetimes.eventtype FROM pauseresumetimes WHERE merchantid = %s 
        AND CONVERT_TZ(pauseresumetimes.eventdatetime, 'UTC', 
        (SELECT timezone FROM merchants WHERE id = pauseresumetimes.merchantid)) < %s 
        order by eventdatetime DESC LIMIT 1 """,
        (merchantId, today))
    
    previous_day_event = cursor.fetchone()
    if previous_day_event != None:
      svaetoDB = True
      previous_day_event = previous_day_event['eventtype']
      if previous_day_event == "RESUMED":
        resumeMinutes = total
        pauseMinutes = 0
      else:
        pauseMinutes = total
        resumeMinutes = 0
  return resumeMinutes, pauseMinutes, total


