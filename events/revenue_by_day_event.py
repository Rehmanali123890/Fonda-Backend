import uuid

# local imports
from utilities.helpers import openDbconnection


def calculate_revenue_by_day_handler(event, context):

  print('------------------------------- Average Revenue By Day Event ------------------------------')
  
  # db connection
  connection, cursor = openDbconnection()

  # get all merchants
  cursor.execute("SELECT * FROM merchants")
  merchants = cursor.fetchall()

  # dictionary of weekdays of mysql
  weekdays = {
    0: 'monday', 1: 'tuesday', 2: 'wednesday', 3: 'thursday', 4: 'friday', 5: 'saturday', 6: 'sunday'
  }

  # loop over merchants
  for merchant in merchants:

    # cursor.execute(f"""
    #   SELECT AVG(ordertotal) revenue, WEEKDAY(DATE(convert_tz(orderdatetime, '+00:00', '{merchant["timezone"]}'))) weekday 
    #     FROM ordersCombined WHERE merchantid = %s
    #     GROUP BY WEEKDAY(DATE(convert_tz(orderdatetime, '+00:00', '{merchant["timezone"]}')));
    #   """, (merchant['id']))
    
    cursor.execute(f"""
      SELECT 
	      COALESCE(AVG(dailyRevenue), 0) as revenue, weekday
	
        FROM (
          SELECT 
            COALESCE(SUM(ordertotal), 0) dailyRevenue, 
            @orderDate := DATE(convert_tz(orderdatetime, '+00:00', '{merchant["timezone"]}')) orderDate,
            WEEKDAY(@orderDate) weekday
              
            FROM ordersCombined WHERE merchantid = %s
            GROUP BY @orderDate
        ) as result1
          
        GROUP BY weekday;
    """, merchant["id"])
    rows = cursor.fetchall()

    daily_revenue = {
      0: 0.00, 1: 0.00, 2: 0.00, 3: 0.00, 4: 0.00, 5: 0.00, 6: 0.00
    }

    # update the daily_revene dict with data
    for row in rows:
      if row['weekday'] is None:
        continue
      daily_revenue[int(row['weekday'])] = row['revenue']
    

    # check if merchant row already exists then only update it, otherwise create new record in table
    cursor.execute("""SELECT id FROM lifetimerevenue WHERE merchantid=%s""", merchant['id'])
    row = cursor.fetchone()
    if row:
      cursor.execute("""
        UPDATE lifetimerevenue
          SET monday=%s, tuesday=%s, wednesday=%s, thursday=%s, friday=%s, saturday=%s, sunday=%s, updated_datetime=CURRENT_TIMESTAMP
          WHERE merchantid=%s
        """, (daily_revenue[0], daily_revenue[1], daily_revenue[2], daily_revenue[3], daily_revenue[4], daily_revenue[5], daily_revenue[6], merchant['id']))
    else:
      cursor.execute("""
        INSERT INTO lifetimerevenue 
          (id, merchantid, monday, tuesday, wednesday, thursday, friday, saturday, sunday, updated_datetime)
          VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,CURRENT_TIMESTAMP)
        """, (uuid.uuid4(), merchant['id'], daily_revenue[0], daily_revenue[1], daily_revenue[2], daily_revenue[3], daily_revenue[4], daily_revenue[5], daily_revenue[6]))


    # commit all the changes in the end
    connection.commit()
  
  # close db connection
  connection.close()