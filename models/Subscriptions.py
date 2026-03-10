import datetime

# local imports
from utilities.helpers import get_db_connection



class Subscriptions():

  ############################################### GET

  @classmethod
  def get_subscription_records(cls, merchantId):
    try:
      connection, cursor = get_db_connection()

      cursor.execute("SELECT timezone FROM merchants WHERE id=%s", merchantId)
      row = cursor.fetchone()
      merchantTimezone = row["timezone"]
      cursor.execute(f"""
        SELECT 
          
          id, merchantId, amount, 
          date_format(`date`, '%%m-%%d-%%Y') `date`, status, 
          payoutid payoutId, frequency, istrail,
          waiveoff_amount, waiveoff_remarks,
          waiveoff_by,
          mark_paid_amount,
          mark_paid_remarks,
          
          mark_paid_datetime,
          split_remarks,
          split_status,
          IF(subscriptions.waiveoff_by IS NULL, NULL, (SELECT username FROM users WHERE users.id=subscriptions.waiveoff_by LIMIT 1)) AS waiveoff_username,
          IF(subscriptions.mark_paid_by IS NULL, NULL, (SELECT username FROM users WHERE users.id=subscriptions.mark_paid_by LIMIT 1)) AS mark_paid_username,
          
          CONCAT (
            date_format(convert_tz(createddatetime, '+00:00', '{merchantTimezone}'), '%%m-%%d-%%Y %%H:%%i:%%S'),
            ' (', '{merchantTimezone}', ')'
          ) createddatetime,
          CONCAT(
            date_format(convert_tz(waiveoff_datetime, '+00:00', '{merchantTimezone}'), '%%m-%%d-%%Y %%H:%%i:%%S'),
            ' (', '{merchantTimezone}', ')'
          ) waiveoff_datetime
        
        FROM subscriptions 
        WHERE merchantid=%s 
        ORDER BY FIELD(`status`, 0) DESC, date(`date`) DESC
        """, (merchantId))
      rows = cursor.fetchall()

      return rows
    except Exception as e:
      print("Error: ", str(e))
      return False

  ###############################################

  @classmethod
  def get_subscriptions_due_amount(cls, merchantId: str, payoutType: int, startDate: datetime.datetime, endDate: datetime.datetime,subscriptiontrialperiod=0) -> dict:
    try:
      '''
        -> payoutType 
          1. Include all unpaid subscriptions
          2. Include subscription within specified start and end date
          3. Do not include subscriptions at all
      '''
      connection, cursor = get_db_connection()
      if subscriptiontrialperiod==360:
        row = {'subscriptionDues': 0, 'ids': None}
      else:
        if payoutType == 1:
          cursor.execute("""
            SELECT COALESCE(SUM(amount), 0) subscriptionDues, GROUP_CONCAT(id) ids
            FROM subscriptions 
            WHERE merchantId = %s AND status = 0
            """, (merchantId))
          row = cursor.fetchone()

        elif payoutType == 2:
          cursor.execute("""
            SELECT COALESCE(SUM(amount), 0) subscriptionDues, GROUP_CONCAT(id) ids
            FROM subscriptions 
            WHERE 
              merchantId = %s 
              AND status = 0
              AND `date` BETWEEN %s AND %s
            """, (merchantId, startDate.date(), endDate.date()))
          row = cursor.fetchone()

        elif payoutType == 3:
          row = {'subscriptionDues': 0, 'ids': None}

        else:
          print("invalid payout-type: ", str(payoutType)); return False

      row['subscriptionDues'] = float(row['subscriptionDues'])
      return row
    except Exception as e:
      print(str(e))
      return False


  @classmethod
  def mark_subscriptions_as_paid(cls, payoutId, subscription_ids):
    try:
      connection, cursor = get_db_connection()

      cursor.execute("""
        UPDATE subscriptions SET payoutid = %s, status = 1
          WHERE id IN %s
        """, (payoutId, tuple(subscription_ids)))

      connection.commit()
      return True
    except Exception as e:
      print(str(e))
      return False
  

  @classmethod
  def mark_subscriptions_as_unpaid(cls, payoutId):
    try:
      connection, cursor = get_db_connection()

      cursor.execute("""
        UPDATE subscriptions SET status = 0 WHERE payoutid = %s
        """, (payoutId))

      connection.commit()

      return True
    except Exception as e:
      print("Error: ", str(e))
      return False
  

  @classmethod
  def get_subscription_by_id(cls, subscriptionId):
    try:
      connection, cursor = get_db_connection()
      cursor.execute("SELECT * FROM subscriptions WHERE id = %s", (subscriptionId))
      row = cursor.fetchone()
      return row
    except Exception as e:
      print("Error: ", str(e))
      return False
  

  @classmethod
  def mark_subscriptions_as_waiveoff(cls, subscriptionId, waiveoff_remarks, userId):
    try:
      connection, cursor = get_db_connection()

      cursor.execute("""
        UPDATE subscriptions 
          SET 
            status=2,
            waiveoff_amount=amount,
            waiveoff_remarks=%s,
            waiveoff_by=%s,
            waiveoff_datetime=CURRENT_TIMESTAMP
          WHERE id = %s
        """, (waiveoff_remarks, userId, subscriptionId))

      connection.commit()

      return True
    except Exception as e:
      print("Error: ", str(e))
      return False

  @classmethod
  def mark_subscriptions_as_paid_by_user(cls, subscriptionId, remarks, userId):
    try:
      connection, cursor = get_db_connection()

      cursor.execute("""
          UPDATE subscriptions 
            SET 
              status=3,
              mark_paid_amount=amount,
              mark_paid_remarks=%s,
              mark_paid_by=%s,
              mark_paid_datetime=CURRENT_TIMESTAMP
            WHERE id = %s
          """, (remarks, userId, subscriptionId))

      connection.commit()

      return True
    except Exception as e:
      print("Error: ", str(e))
      return False

  @classmethod
  def mark_subscriptions_as_splited(cls, subscriptionId):
    try:
      connection, cursor = get_db_connection()
      remarks="Adjustment on other dates"
      cursor.execute("""
          UPDATE subscriptions 
            SET 
              status=5,
              split_remarks=%s
              Where id = %s
          """, (remarks, subscriptionId))

      connection.commit()

      return True
    except Exception as e:
      print("Error: ", str(e))
      return False
