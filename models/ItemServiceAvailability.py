import uuid
import json
import config
# local imports
from utilities.helpers import get_db_connection
import hashlib
import hmac
import requests
import datetime
import json




  
  def sign(key, msg):
      return hmac.new(key, msg.encode("utf-8"), hashlib.sha256).digest()

  def get_signature_key(key, date_stamp, region_name, service_name):
      k_date = ItemServiceAvailability.sign(("AWS4" + key).encode("utf-8"), date_stamp)
      k_region = ItemServiceAvailability.sign(k_date, region_name)
      k_service = ItemServiceAvailability.sign(k_region, service_name)
      k_signing = ItemServiceAvailability.sign(k_service, "aws4_request")
      return k_signing

  def aws_signed_request(method, uri, payload=""):
      t = datetime.datetime.utcnow()
      amz_date = t.strftime("%Y%m%dT%H%M%SZ")
      date_stamp = t.strftime("%Y%m%d")

      canonical_uri = uri
      canonical_querystring = ""
      canonical_headers = f"host:{HOST}\nx-amz-date:{amz_date}\n"
      signed_headers = "host;x-amz-date"
      payload_hash = hashlib.sha256(payload.encode("utf-8")).hexdigest()
      
      canonical_request = f"{method}\n{canonical_uri}\n{canonical_querystring}\n{canonical_headers}\n{signed_headers}\n{payload_hash}"
      algorithm = "AWS4-HMAC-SHA256"
      credential_scope = f"{date_stamp}/{AWS_REGION}/{SERVICE}/aws4_request"
      string_to_sign = f"{algorithm}\n{amz_date}\n{credential_scope}\n{hashlib.sha256(canonical_request.encode('utf-8')).hexdigest()}"

      signing_key = ItemServiceAvailability.get_signature_key(AWS_SECRET_KEY, date_stamp, AWS_REGION, SERVICE)
      signature = hmac.new(signing_key, string_to_sign.encode("utf-8"), hashlib.sha256).hexdigest()
      
      authorization_header = f"{algorithm} Credential={AWS_ACCESS_KEY}/{credential_scope}, SignedHeaders={signed_headers}, Signature={signature}"
      headers = {
          "x-amz-date": amz_date,
          "Authorization": authorization_header,
          "Content-Type": "application/json"
      }

      return requests.request(method, ENDPOINT + canonical_uri, headers=headers, data=payload)

  def create_scheduler(schedule_name, cron_expression, target_arn,itemId, status,weekDay,timezone,merchantId):
    print("create scheduler")
    try:
      uri = "/schedules/" + schedule_name
      payload = json.dumps({
          "ClientToken": str(uuid.uuid4()), 
          "FlexibleTimeWindow": {"Mode": "OFF"},
          "ScheduleExpression": f"{cron_expression}",
          "ScheduleExpressionTimezone": f"{timezone}", 
          "State": "ENABLED",
          "Target": {
                  "Arn": target_arn,
                  "RoleArn": "arn:aws:iam::677331532364:role/service-role/Amazon_EventBridge_Scheduler_LAMBDA_a69ad19f74",
                  "Input": json.dumps({
                    "itemId": itemId,
                    "status": status,
                    "merchantId": merchantId,
                  }) 
                  }
      })

      response = ItemServiceAvailability.aws_signed_request("POST", uri, payload)
      print(response.json(),response.status_code)
      if response.status_code == 200:
        print("Scheduler created successfully.")
        return True
      else:
        return False
    except Exception as e:
      print({"error" : str(e)})
      return False
    
  def get_scheduler(schedule_name):
    print("get scheduler")
    try:
      uri = f"/schedules/{schedule_name}"
      response = ItemServiceAvailability.aws_signed_request("GET", uri)
      print(response.json(),response.status_code)
      if response.status_code == 200:
        print("Scheduler get successfully.")
        return True
      else:
        return False
    except Exception as e:
      print({"error" : str(e)})
      return False
    
  def update_scheduler(schedule_name, cron_expression, target_arn,timezone,itemId, status,merchantId):
    print("update scheduler")
    try:
      uri = "/schedules/" + schedule_name
      payload = json.dumps({
          "ClientToken": str(uuid.uuid4()), 
          "FlexibleTimeWindow": {"Mode": "OFF"},
          "ScheduleExpression": f"{cron_expression}",
          "ScheduleExpressionTimezone": f"{timezone}", 
          "State": "ENABLED",
          "Target": {
                  "Arn": target_arn,
                  "RoleArn": "arn:aws:iam::677331532364:role/service-role/Amazon_EventBridge_Scheduler_LAMBDA_a69ad19f74",
                  "Input": json.dumps({
                    "itemId": itemId,
                    "status": status,
                    "merchantId": merchantId,
                  }) 
                  }
      })

      response = ItemServiceAvailability.aws_signed_request("PUT", uri, payload)
      print(response.json(),response.status_code)
      if response.status_code == 200:
        print("Scheduler updated successfully.")
        return True
      else:
        return False
    except Exception as e:
      print({"error" : str(e)})
      return False
    
  def delete_scheduler(schedule_name):
    print("delete scheduler")
    try:
      uri = f"/schedules/{schedule_name}"
      payload = json.dumps({
        "ClientToken": str(uuid.uuid4())  # Generates a unique token for the request
      })
      response = ItemServiceAvailability.aws_signed_request("DELETE", uri, payload)
      print(response.json(),response.status_code)
      if response.status_code == 200:
        print("Scheduler deleted successfully.")
        return True
      else:
        return False
    except Exception as e:
      print({"error" : str(e)})
      return False
  
  # @classmethod
  # def create_scheduler(cls,schedule_name, cron_expression, lambda_arn, itemId, status,weekDay,timezone,merchantId):
  #   """Creates a new EventBridge Scheduler job"""
  #   print(timezone)
  #   try:
  #     response = eventbridge.create_schedule(
  #         Name=schedule_name,
  #         ScheduleExpression=cron_expression,
  #         ScheduleExpressionTimezone=timezone, 
  #         FlexibleTimeWindow={'Mode': 'OFF'},
  #         Target={
  #             'Arn': lambda_arn,
  #             'RoleArn': 'arn:aws:iam::677331532364:role/service-role/Amazon_EventBridge_Scheduler_LAMBDA_a69ad19f74',
  #             "Input": json.dumps({
  #               "itemId": itemId,
  #               "status": status,
  #               # "weekDay": weekDay,
  #               "merchantId": merchantId,
  #               # "timezone": timezone
  #             })
  #         }
  #     )
  #     print(response)
  #     return schedule_name
  #   except Exception as e:
  #     print(f"Error! Scheduler not created: {str(e)}")

  # @classmethod
  # def delete_scheduler(cls,schedule_name):
  #   """Deletes an existing EventBridge Scheduler job"""
  #   try:
  #     print(schedule_name)
  #     existing_schedule = eventbridge.get_schedule(Name=schedule_name)

  #       # Extract required fields
  #     if existing_schedule:
  #       eventbridge.delete_schedule(Name=schedule_name)
  #   except Exception as e:
  #     print("Scheduler not found")
	
  # @classmethod
  # def update_scheduler(cls,schedule_name, expression):
  #   try:
  #     existing_schedule = eventbridge.get_schedule(Name=schedule_name)

  #     # Extract required fields
  #     if existing_schedule:
  #       current_expression = existing_schedule["ScheduleExpression"]
  #       current_target = existing_schedule["Target"]
  #       timezone = existing_schedule["ScheduleExpressionTimezone"]
  #       current_flexible_window = existing_schedule["FlexibleTimeWindow"]
  #       response = eventbridge.update_schedule(
  #             Name=schedule_name,
  #             ScheduleExpression=expression, 
  #             ScheduleExpressionTimezone=timezone, 
  #             FlexibleTimeWindow=current_flexible_window,
  #             Target=current_target  
  #         )
  #       print("Schedule updated successfully.")
  #       return schedule_name
  #   except Exception as e:
  #     print("Scheduler not found")

  @classmethod
  def post_serviceAvailability(cls, itemId, availability,merchantId):
    try:
      connection, cursor = get_db_connection()
      
      for row in availability:
        startTime = row.get("startTime")
        endTime = row.get("endTime")
        weekDay = row.get("weekDay")
        timezone = row.get("timezone")
        guid = uuid.uuid4()
        groupDays = row.get("groupDays")
        weekdayList =  {
          1: "MON",
          2: "TUES",
          3: "WED",
          4: "THU",
          5: "FRI",
          6: "SAT",
          7: "SUN"
        }
        cron_dayList = ''
        if groupDays:
          groupDays.sort()
          for day in groupDays:
            if cron_dayList:
              cron_dayList = cron_dayList + ',' + weekdayList[day]
            else:
              cron_dayList = weekdayList[day]
        print(cron_dayList)
      
        startSplit = startTime.split(":")
        endSplit = endTime.split(":")
        enable_schedule = ''
        disable_schedule = ''
        cron_enable = f"cron({startSplit[1]} {startSplit[0]} ? * {cron_dayList} *)"

        start_dt = datetime.datetime.strptime(startTime, "%H:%M")
        end_dt = datetime.datetime.strptime(endTime, "%H:%M")
        disable_groupDays = []
        if end_dt <= start_dt:
          for day in groupDays:
            next_day = (day % 7) + 1  # 7 (Sunday) becomes 1 (Monday)
            disable_groupDays.append(next_day)
        else:
          disable_groupDays = groupDays

        disable_groupDays.sort()
        cron_dayList_disable = ''
        for day in disable_groupDays:
          if cron_dayList_disable:
            cron_dayList_disable = cron_dayList_disable + ',' + weekdayList[day]
          else:
            cron_dayList_disable = weekdayList[day]

        cron_disable = f"cron({endSplit[1]} {endSplit[0]} ? * {cron_dayList_disable} *)"
        # Generate unique schedule names
        weekdayList =  {
          1: "M",
          2: "T",
          3: "W",
          4: "TH",
          5: "F",
          6: "S",
          7: "SU"
        }
        cron_dayList = ''
        if groupDays:
          print(groupDays)
          groupDays.sort()
          for day in groupDays:
            if cron_dayList:
              cron_dayList = cron_dayList + '-' + weekdayList[day]
            else:
              cron_dayList = weekdayList[day]
        print(cron_dayList)
        enable_schedule_id = f"i-{itemId}-e-{cron_dayList}"
        disable_schedule_id = f"i-{itemId}-d-{cron_dayList}"

        # Create schedulers
        lambda_arn = ""
        if config.env == 'development':
          lambda_arn = "arn:aws:lambda:us-east-2:677331532364:function:dashboard-api-v2-dev-ItemHoursCheck"
        elif config.env == 'test':
          lambda_arn = "arn:aws:lambda:us-east-2:677331532364:function:test-UAT-dashboard-api-v2-test-UAT-ItemHoursCheck"
        elif config.env == 'production':
          lambda_arn = "arn:aws:lambda:us-west-1:677331532364:function:dashboard-api-v2-prod-ItemHoursCheck"

        
        try:
          enable_schedule = enable_schedule_id
          if ItemServiceAvailability.get_scheduler(enable_schedule_id):
            update_scheduler=ItemServiceAvailability.update_scheduler(enable_schedule_id, cron_enable,lambda_arn,timezone,itemId,"Active", merchantId)
            if not update_scheduler:
              return False
          else:
            enable_schedule = ItemServiceAvailability.create_scheduler(enable_schedule_id, cron_enable, lambda_arn, itemId,"Active", weekDay,timezone,merchantId)
            if not enable_schedule:
              return False
        except Exception as e:
          enable_schedule = enable_schedule_id
          enable_schedule = ItemServiceAvailability.create_scheduler(enable_schedule_id, cron_enable, lambda_arn, itemId,"Active", weekDay,timezone,merchantId)
          if not enable_schedule:
            return False
        
        try:
          if ItemServiceAvailability.get_scheduler(disable_schedule_id):
            update_scheduler=ItemServiceAvailability.update_scheduler(disable_schedule_id, cron_disable,lambda_arn,timezone,itemId,"Inactive", merchantId)
            if not update_scheduler:
              return False
            disable_schedule = disable_schedule_id
          else:
            disable_schedule = disable_schedule_id
            create_scheduler=ItemServiceAvailability.create_scheduler(disable_schedule_id, cron_disable, lambda_arn, itemId,"Inactive", weekDay,timezone,merchantId)
            if not create_scheduler:
              return False

        except Exception as e:
          disable_schedule = ItemServiceAvailability.create_scheduler(disable_schedule_id, cron_disable, lambda_arn, itemId,"Inactive", weekDay,timezone,merchantId)
          if not disable_schedule:
            return False
        data = (guid, itemId, startTime, endTime, weekDay,enable_schedule,disable_schedule,timezone)
        
        cursor.execute("""INSERT INTO itemserviceavailability (id, itemId, starttime, endtime, weekday, activateSchedulerID,deactivateSchedulerID,timezone)
          VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""", data)
      
      connection.commit()
      return True
      
    except Exception as e:
      print("Error: ", str(e))
      return False


  @classmethod
  def get_serviceAvailabilityByitemId(cls, itemId):
    try:
      connection, cursor = get_db_connection()
      
      cursor.execute("""SELECT id, TIME_FORMAT(starttime, '%%H:%%i') startTime, TIME_FORMAT(endtime, '%%H:%%i') endTime, weekday weekDay, activateSchedulerID,deactivateSchedulerID,timezone FROM itemserviceavailability WHERE itemId=%s""", (itemId))
      rows = cursor.fetchall()
      return rows
      
    except Exception as e:
      print("Error: ", str(e))
      return False


  @classmethod
  def get_serviceAvailabilityByitemIdWeekday(cls, itemId, weekDay):
    try:
      connection, cursor = get_db_connection()
      
      cursor.execute("""SELECT id, TIME_FORMAT(starttime, '%%H:%%i') startTime, TIME_FORMAT(endtime, '%%H:%%i') endTime, weekday weekDay, activateSchedulerID,deactivateSchedulerID,timezone FROM itemserviceavailability WHERE itemId=%s and weekday=%s """, (itemId, weekDay))
      rows = cursor.fetchone()
      return rows
      
    except Exception as e:
      print("Error: ", str(e))
      return False


  @classmethod
  def get_serviceAvailabilityId(cls, id):
    try:
      connection, cursor = get_db_connection()
      
      cursor.execute("""SELECT id, itemId itemId, CONVERT(starttime, CHAR) startTime, CONVERT(endtime, CHAR) endTime, weekday weekDay, activateSchedulerID,deactivateSchedulerID,timezone FROM itemserviceavailability WHERE id=%s""", (id))
      row = cursor.fetchone()
      return row
      
    except Exception as e:
      print("Error: ", str(e))
      return False


  @classmethod
  def put_serviceAvailabilityById(cls, id, startTime, endTime, weekDay,timezone,itemId,groupDays,merchantId):
    try:
      connection, cursor = get_db_connection()
      
      serviceData = ItemServiceAvailability.get_serviceAvailabilityId(id)
      startSplit = startTime.split(":")
      endSplit = endTime.split(":")
      weekdayList =  {
        1: "MON",
        2: "TUES",
        3: "WED",
        4: "THU",
        5: "FRI",
        6: "SAT",
        7: "SUN"
      }
      cron_dayList = ''
      if groupDays:
        groupDays.sort()
        print(groupDays)
        for day in groupDays:
          if cron_dayList:
            cron_dayList = cron_dayList + ',' + weekdayList[day]
          else:
            cron_dayList = weekdayList[day]
      print(cron_dayList)
    
      startSplit = startTime.split(":")
      endSplit = endTime.split(":")
      cron_enable = f"cron({startSplit[1]} {startSplit[0]} ? * {cron_dayList} *)"

      start_dt = datetime.datetime.strptime(startTime, "%H:%M")
      end_dt = datetime.datetime.strptime(endTime, "%H:%M")
      disable_groupDays = []
      if end_dt <= start_dt:
        for day in groupDays:
          next_day = (day % 7) + 1  # 7 (Sunday) becomes 1 (Monday)
          disable_groupDays.append(next_day)
      else:
        disable_groupDays = groupDays

      disable_groupDays.sort()
      cron_dayList_disable = ''
      for day in disable_groupDays:
        if cron_dayList_disable:
          cron_dayList_disable = cron_dayList_disable + ',' + weekdayList[day]
        else:
          cron_dayList_disable = weekdayList[day]

      cron_disable = f"cron({endSplit[1]} {endSplit[0]} ? * {cron_dayList_disable} *)"

      # Generate unique schedule names
      weekdayList =  {
        1: "M",
        2: "T",
        3: "W",
        4: "TH",
        5: "F",
        6: "S",
        7: "SU"
      }
      cron_dayList = ''
      if groupDays:
        print(groupDays)
        groupDays.sort()
        for day in groupDays:
          if cron_dayList:
            cron_dayList = cron_dayList + '-' + weekdayList[day]
          else:
            cron_dayList = weekdayList[day]
      print(cron_dayList)
      enable_schedule_id = f"i-{itemId}-e-{cron_dayList}"
      disable_schedule_id = f"i-{itemId}-d-{cron_dayList}"

      # Create schedulers
      lambda_arn = ""
      if config.env == 'development':
        lambda_arn = "arn:aws:lambda:us-east-2:677331532364:function:dashboard-api-v2-dev-ItemHoursCheck"
      elif config.env == 'test':
        lambda_arn = "arn:aws:lambda:us-east-2:677331532364:function:test-UAT-dashboard-api-v2-test-UAT-ItemHoursCheck"
      elif config.env == 'production':
        lambda_arn = "arn:aws:lambda:us-west-1:677331532364:function:dashboard-api-v2-prod-ItemHoursCheck"
      if enable_schedule_id != serviceData['activateSchedulerID']:
        try:
          if ItemServiceAvailability.get_scheduler(serviceData['activateSchedulerID']):
            ItemServiceAvailability.delete_scheduler(serviceData['activateSchedulerID'])
          try:
            if ItemServiceAvailability.get_scheduler(enable_schedule_id):
              update_scheduler=ItemServiceAvailability.update_scheduler(enable_schedule_id, cron_enable,lambda_arn,timezone,itemId,"Active", merchantId)
              if not update_scheduler:
                return False
            else:
              create_scheduler=ItemServiceAvailability.create_scheduler(enable_schedule_id, cron_enable, lambda_arn, itemId,"Active",weekDay,timezone,merchantId)
              if not create_scheduler:
                return False
          except Exception as e:
            create_scheduler=ItemServiceAvailability.create_scheduler(enable_schedule_id, cron_enable, lambda_arn, itemId,"Active",weekDay,timezone,merchantId)
            if not create_scheduler:
              return False
        except Exception as e:
          try:
            if ItemServiceAvailability.get_scheduler(enable_schedule_id):
              update_scheduler=ItemServiceAvailability.update_scheduler(enable_schedule_id, cron_enable,lambda_arn,timezone,itemId,"Active", merchantId)
              if not update_scheduler:
                return False
            else:
              create_scheduler=ItemServiceAvailability.create_scheduler(enable_schedule_id, cron_enable, lambda_arn, itemId,"Active",weekDay,timezone,merchantId)
              if not create_scheduler:
                return False
          except Exception as e:
            create_scheduler=ItemServiceAvailability.create_scheduler(enable_schedule_id, cron_enable, lambda_arn, itemId,"Active",weekDay,timezone,merchantId)
            if not create_scheduler:
              return False
      else:
        try:
          if ItemServiceAvailability.get_scheduler(serviceData['activateSchedulerID']):
            update_scheduler=ItemServiceAvailability.update_scheduler(serviceData['activateSchedulerID'], cron_enable,lambda_arn,timezone,itemId,"Active", merchantId)
            if not update_scheduler:
              return False
          else:
            create_scheduler=ItemServiceAvailability.create_scheduler(serviceData['activateSchedulerID'], cron_enable, lambda_arn, itemId,"Active",weekDay,timezone,merchantId)
            if not create_scheduler:
              return False
        except Exception as e:
          create_scheduler=ItemServiceAvailability.create_scheduler(serviceData['activateSchedulerID'], cron_enable, lambda_arn, itemId,"Active",weekDay,timezone,merchantId)
          if not create_scheduler:
            return False

      if disable_schedule_id != serviceData['deactivateSchedulerID']:
        try:
          if ItemServiceAvailability.get_scheduler(serviceData['deactivateSchedulerID']):
            ItemServiceAvailability.delete_scheduler(serviceData['deactivateSchedulerID'])
          try:
            created_scheduler = ItemServiceAvailability.get_scheduler(disable_schedule_id)
            if created_scheduler:
              update_scheduler=ItemServiceAvailability.update_scheduler(disable_schedule_id, cron_disable,lambda_arn,timezone,itemId,"Inactive", merchantId)
              if not update_scheduler:
                return False
            else:
              create_scheduler=ItemServiceAvailability.create_scheduler(disable_schedule_id, cron_disable, lambda_arn, itemId,"Inactive", weekDay,timezone,merchantId)
              if not create_scheduler:
                return False
          except Exception as e:
            create_scheduler=ItemServiceAvailability.create_scheduler(disable_schedule_id, cron_disable, lambda_arn, itemId,"Inactive", weekDay,timezone,merchantId)
            if not create_scheduler:
              return False
        except Exception as e:
          try:
            created_scheduler = ItemServiceAvailability.get_scheduler(disable_schedule_id)
            if created_scheduler:
              update_scheduler=ItemServiceAvailability.update_scheduler(disable_schedule_id, cron_disable,lambda_arn,timezone,itemId,"Inactive", merchantId)
              if not update_scheduler:
                return False
            else:
              create_scheduler=ItemServiceAvailability.create_scheduler(disable_schedule_id, cron_disable, lambda_arn, itemId,"Inactive", weekDay,timezone,merchantId)
              if not create_scheduler:
                return False
          except Exception as e:
            create_scheduler=ItemServiceAvailability.create_scheduler(disable_schedule_id, cron_disable, lambda_arn, itemId,"Inactive", weekDay,timezone,merchantId)
            if not create_scheduler:
              return False
      else:
        try:
          created_scheduler = ItemServiceAvailability.get_scheduler(serviceData['deactivateSchedulerID'])
          if created_scheduler:
            update_scheduler=ItemServiceAvailability.update_scheduler(serviceData['deactivateSchedulerID'], cron_disable,lambda_arn,timezone,itemId,"Inactive", merchantId)
            if not update_scheduler:
              return False
          else:
            create_scheduler=ItemServiceAvailability.create_scheduler(serviceData['deactivateSchedulerID'], cron_disable, lambda_arn, itemId,"Inactive", weekDay,timezone,merchantId)
            if not create_scheduler:
              return False
        except Exception as e:
          create_scheduler=ItemServiceAvailability.create_scheduler(serviceData['deactivateSchedulerID'], cron_disable, lambda_arn, itemId,"Inactive", weekDay,timezone,merchantId)
          if not create_scheduler:
            return False
      
      cursor.execute("""UPDATE itemserviceavailability SET starttime=%s, endtime=%s, weekday=%s,timezone=%s,activateSchedulerID=%s, deactivateSchedulerID=%s WHERE id=%s""", (startTime, endTime, weekDay,timezone,enable_schedule_id,disable_schedule_id, id))
      connection.commit()
      return True
      
    except Exception as e:
      print("Error: ", str(e))
      return False


  @classmethod
  def delete_serviceAvailabilityByitemId(cls, itemId):
    try:
      connection, cursor = get_db_connection()
      serviceData = ItemServiceAvailability.get_serviceAvailabilityByitemId(itemId)
      
      cursor.execute("""DELETE FROM itemserviceavailability WHERE itemId=%s""", (itemId))

      for service in serviceData:
        ItemServiceAvailability.delete_scheduler(service['activateSchedulerID'])
        ItemServiceAvailability.delete_scheduler(service['deactivateSchedulerID'])
    
      connection.commit()
      return True
      
    except Exception as e:
      print("Error: ", str(e))
      return False


  @classmethod
  def delete_serviceAvailabilityById(cls, id):
    try:
      connection, cursor = get_db_connection()
      serviceData = ItemServiceAvailability.get_serviceAvailabilityId(id)

      cursor.execute("""DELETE FROM itemserviceavailability WHERE id=%s""", (id))

      ItemServiceAvailability.delete_scheduler(serviceData['activateSchedulerID'])
      ItemServiceAvailability.delete_scheduler(serviceData['deactivateSchedulerID'])
      connection.commit()
      return True
      
    except Exception as e:
      print("Error: ", str(e))
      return False

    