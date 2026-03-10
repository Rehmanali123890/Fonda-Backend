from app import app
import json
import datetime
import requests
import csv
from contextlib import closing

# local imports
from models.Platforms import Platforms
from models.ubereats.UberEats import UberEats
from models.ubereats.UbeEatsItemSync import UberEatsItemSync
from models.Merchants import Merchants
from models.UberEatsReports import UberEatsReports
from models.VirtualMerchants import VirtualMerchants
import config
from utilities.helpers import get_db_connection, create_log_data
from controllers.WebHookController import create_ubereats_order, cancel_ubereats_order


def item_autosync_event(event, context):
  with app.app_context():
    print("---------------------- item_autosync_event is triggered --------------------------")
    print(event)
    for record in event['Records']:
      subject = record.get("Sns").get("Subject")
      message = eval(record.get("Sns").get("Message"))
      print("Subject: ", subject)

      if not (subject == "item.update" or subject == "item.status_change"):
        continue

      # extract details from event
      merchantId = message.get("body").get("merchantId")
      itemId = message.get("body").get("itemId")
      addonId = message.get("body").get("addonId")
      userId = message.get("body").get("userId")

      # get merchant + all-virtual-merchant ids list
      vms = VirtualMerchants.get_virtual_merchant(merchantId=merchantId)
      merchants_list = [{
        "syncMerchantId": merchantId,
        "isVirtual": 0,
        "mainMerchantId": merchantId
      }]
      for vm in vms:
        merchants_list.append({
          "syncMerchantId": vm["id"],
          "isVirtual": 1,
          "mainMerchantId": merchantId
        })
      
      # loop over merchants + vmerchants list
      for merchant in merchants_list:

        # check if merchant is connected and auto-synced to ubereats
        platform = Platforms.get_platform_by_merchantid_and_platformtype(merchantid=merchant["syncMerchantId"], platformtype=3)

        if not platform:
          continue

        if platform["synctype"] == 1:
          # auto-sync
          if subject == "item.update" or subject == "item.status_change":
            resp = UberEatsItemSync.change_item_status(
              merchant_obj=merchant,
              itemId=itemId,
              platform=platform
            )
          else:
            pass
        else:
          # manual sync
          print("Merchant synctype preference is manual")

      print("Done")



def merchant_autosync_event(event, context):
  with app.app_context():
    print("---------------------- merchant_autosync_event is triggered -------------------------")
    print(event)
    for record in event['Records']:
      subject = record.get("Sns").get("Subject")
      message = eval(record.get("Sns").get("Message"))
      merchantId = message.get("body").get("merchantId")

      if subject == "merchant.status_change":

        # get main merchant details
        merchant_details = Merchants.get_merchant_by_id(merchantId)
        status = "OFFLINE" if int(merchant_details['marketstatus']) == 0 else "ONLINE"
        
        # get merchant + all-virtual-merchant ids list
        vms = VirtualMerchants.get_virtual_merchant(merchantId=merchantId)
        merchants_list = [{
          "syncMerchantId": merchantId,
          "isVirtual": 0,
          "mainMerchantId": merchantId
        }]
        for vm in vms:
          merchants_list.append({
            "syncMerchantId": vm["id"],
            "isVirtual": 1,
            "mainMerchantId": merchantId
          })

        print(merchants_list)
        
        # loop over merchants + vmerchants list
        for merchant in merchants_list:

          platform = Platforms.get_platform_by_merchantid_and_platformtype(merchantid=merchant["syncMerchantId"], platformtype=3)#3=ubereats

          if not platform:
            continue

          # get access token
          accessToken = UberEats.ubereats_check_and_get_access_token()
          if not accessToken:
            print("unhandled exception while checking ubereats access token")
            continue


          payload = {
            "status": status
          }
          if status == 'OFFLINE':
            now_utc = datetime.datetime.now(datetime.timezone.utc)
            future_time = now_utc + datetime.timedelta(days=365)
            # Format in RFC 3339 format with 'Z' to indicate UTC
            is_offline_until = future_time.isoformat(timespec='seconds').replace('+00:00', 'Z')

            print("is_offline_until :", is_offline_until)
            payload['is_offline_until'] = is_offline_until
          payload=json.dumps(payload)
          # send req to ubereats
          resp = UberEats.ubereats_set_restaurant_status(platform['storeid'], accessToken, payload)
          if not resp:
            print("error: while changing restaurant status on ubereats!")



def ubereats_reports_event(event, context):
  with app.app_context():
    print(event)

    if event.get("source") and event.get("source") == "aws.events":
      print("--- triggered by aws event bridge ---")

      if config.env == "development" or config.env == "production":

        # get current utc datetime and subtract 5 days from it
        utcDatetime = datetime.datetime.utcnow()
        utcDatetime = utcDatetime - datetime.timedelta(days=4)
        startDate = utcDatetime.strftime("%Y-%m-%dT00:00:00")
        endDate = utcDatetime.strftime("%Y-%m-%dT23:59:59")

        # get ubereats access_token with reporting scope
        accessToken = UberEats.ubereats_check_and_get_access_token(key="ubereats_reports_token")
        if accessToken:

          # list all ubereats stores
          store_uuids = list()
          stores = UberEats.ubereats_list_stores(accessToken)
          for store in stores:
            store_uuids.append(store.get("id"))
          
          report_type = "DOWNTIME_REPORT"

          # send request to ubereats reporting api
          response = UberEats.ubereats_request_report(
            accessToken=accessToken,
            report_type=report_type,
            store_uuids=store_uuids,
            start_date=startDate,
            end_date=endDate
          )

          # store the reponse workflow_id in ubereatsreports table
          if response:

            workflow_id = response.get("workflow_id")

            res = UberEatsReports.post(
              jobid=workflow_id,
              reporttype=report_type,
              startdate=startDate,
              enddate=endDate
            )

            if not res:
              print("error while storing workflow_id in database")
          else:
            print("error while calling ubereats reporting api")
        else:
          print("error while getting ubereats access_token")


    elif event.get("Records"):
      print("--- triggered by aws sns topic ---")
      connection, cursor = get_db_connection()

      for record in event.get("Records"):
        subject = record.get("Sns").get("Subject")
        message = eval(record.get("Sns").get("Message"))
        print("subject: ", subject)

        # extract details from event
        subjectEvent = message.get("event")
        entryId = message.get("entryId")
        reportType = message.get("reportType")
        jobId = message.get("jobId")

        if subject == "ubereats.report" and reportType == "DOWNTIME_REPORT":


          ### get entry by job_id from db
          report_entry = UberEatsReports.get(id=entryId)
          print(report_entry)

          webhook_data = json.loads(report_entry.get("webhookdata")) if report_entry.get("webhookdata") else None
          if not webhook_data:
            print("error: webhookdata field in db is null!")
            return
          

          ### loop over csv files and read data
          report_metadata_sections = webhook_data.get("report_metadata").get("sections")
          pause_time_data = list()
          for section in report_metadata_sections:
            
            csv_url = section.get("download_url")

            # read csv file
            with closing(requests.get(csv_url, stream=True)) as r:
              f = (line.decode('utf-8') for line in r.iter_lines())
              reader = csv.DictReader(f, delimiter=',', quotechar='"')

              for row in reader:

                available = False
                for data in pause_time_data:
                  if data["Store"] == row["Store"]:
                    data["Menu Available"] += int(row["Menu Available"])
                    data["Restaurant Online"] += int(row["Restaurant Online"])
                    data["Restaurant Offline"] += int(row["Restaurant Offline"])
                    available = True

                if not available:
                  pause_time_data.append({
                    "Store": row["Store"],
                    "External Store ID": row["External Store ID"],
                    "Country": row["Country"],
                    "Country Code": row["Country Code"],
                    "City": row["City"],
                    "Date": row["Date"],
                    "Restaurant Opened At": row["Restaurant Opened At"],
                    "Menu Available": int(row["Menu Available"]),
                    "Restaurant Online": int(row["Restaurant Online"]),
                    "Restaurant Offline": int(row["Restaurant Offline"])
                  })

          ### update data in merchantpausetime db
          accessToken = UberEats.ubereats_check_and_get_access_token(key="ubereats_reports_token")
          if not accessToken:
            print("error: cannot get ubereats access token!")
            return

          # list all ubereats stores
          ubereats_stores = UberEats.ubereats_list_stores(accessToken)
          if ubereats_stores == False:
            print("error: cannot get ubereats stores!")
            return
          
          for row in pause_time_data:

            fondaMerchantId = row.get("External Store ID")
            storeName = row.get("Store")

            # find fonda merchant id
            if fondaMerchantId:
              merchant_details = Merchants.get_merchant_or_virtual_merchant(fondaMerchantId)

              if not merchant_details:
                print(f"merchant details with id <{fondaMerchantId}> not found at fonda")
                continue
            
            else:  
              u_store = None
              for store in ubereats_stores:    
                if store["name"] == storeName:
                  u_store = store
                  break
              
              if u_store is None:
                print(f"err: no matching store found with name: <{storeName}>")
                continue
              
              uStoreId = u_store.get("id")

              u_store_platform = Platforms.get_platform_by_storeid(uStoreId)
              if not u_store_platform:
                print(f"err: no matching entry found in platforms with store id = {uStoreId}")
                continue

              fondaMerchantId = u_store_platform.get("merchantid")
                

            if not fondaMerchantId:
              print(f"merchant <{storeName}> not found at Fonda!")
              continue
            

            # insert into merchantpausetime db
            entryDate = report_entry.get("startdate").date()
            totalTime = row.get("Menu Available")
            activeTime = row.get("Restaurant Online")
            pauseTime = row.get("Restaurant Offline")

            cursor.execute("""
              INSERT INTO merchantpausetime
                  (merchantid, entrydate, totaltime, activetime, pausetime)
              VALUES (%s, %s, %s, %s, %s)
            """, (fondaMerchantId, entryDate, totalTime, activeTime, pauseTime))
            connection.commit()

            print(f"record inserted successfully for merchant <{storeName}>")
          

          updated = UberEatsReports.update(id=entryId, isprocessed=1)



    else:
      print("--- triggered by (UNKNOWN) ---")
    

    return {
      'statusCode': 200,
      'body': json.dumps('ubereats_reports_event lambda!')
    }


def create_ubereats_order_event(event, context):
  with app.app_context():
    print("--------------------- - --------------------------")
    connection, cursor = get_db_connection()
    for record in event['Records']:
      try:
        create_log_data(
          level="[INFO]",
          Message="In the start of function (create_ubereats_order_event) to create a ubereats webhook order",
          messagebody=record,
          functionName="create_ubereats_order_event",
        )
        subject = record.get("Sns").get("Subject")
        message = eval(record.get("Sns").get("Message"))

        print(f"Sns subject: {subject}")
        print(f"Sns message: {message}")
        if subject=='create_ubereats_order':
          merchant_id = message.get("body").get("merchant_id")
          vmerchantid = message.get("body").get("vmerchantid")
          u_order_details = message.get("body").get("u_order_details")
          eventType = message.get("body").get("eventType")
          print('type of u_order_details ' , type(u_order_details))
          resp=create_ubereats_order(merchant_id ,vmerchantid , u_order_details ,eventType)
        elif subject=='cancel_ubereats_order':
          json_data = message.get("body").get("json_data")
          resp = cancel_ubereats_order(json_data)
        print('---------------- completed create_ubereats_order_event ')
        return {
          'statusCode': 200,
          'body': json.dumps('SNS message processed successfully')
        }

      except Exception as e:
        create_log_data(
          level="[ERROR]",
          Message=f"An exception occured: {str(e)}",
          functionName="create_ubereats_order_event",
        )
        print("Error create_ubereats_order_event: ", str(e))