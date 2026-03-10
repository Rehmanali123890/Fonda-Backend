from flask import jsonify
import boto3
from botocore.config import Config
import uuid
import os
import json
import requests
from flask import render_template
from flask import make_response
import pdfkit
import config
import datetime
from dateutil.tz import gettz



# local imports
from utilities.errors import invalid, unhandled
from utilities.helpers import get_db_connection, success, publish_sns_message, is_float



class Woflow():

  ############################################### WOFLOW APIs

  @classmethod
  def woflowapi_create_a_job(cls, processingType, urls_list, merchantId, woflowColumnId, merchantName, merchantAddress, instructions):
    try:
      
      url = config.woflow_base_url + "/v1/jobs"
      headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Token api_key={config.woflow_api_key}'
      }

      payload = json.dumps({
        "job": {
          "job_type": "new_menu",
          "job_data": {
            "catalog_sources": urls_list,
            "client_mappings": {
              "merchantId": merchantId,
              "woflowColumnId": woflowColumnId
            },
            "merchant_name": merchantName,
            "merchant_address": merchantAddress,
            "language": "en_us",
            "instructions": instructions
          },
          "processing_type": processingType
        }
      })

      response = requests.request("POST", url, headers=headers, data=payload)
      print("RESPONSE..........")
      print(response.text)
      print("END RESPONSE............")

      return response
    except Exception as e:
      print("error: ", str(e))
      return False
  

  @classmethod
  def woflowapi_get_a_job(cls, jobId):
    try:
      url = f"{config.woflow_base_url}/v1/jobs/{jobId}"
      headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Token api_key={config.woflow_api_key}'
      }

      response = requests.request("GET", url, headers=headers, data={})
      print("get_a_job response...")
      print(response.text)
      print("end get_a_job response")

      return response
    except Exception as e:
      print("error: ", str(e))
      return False
  

  @classmethod
  def woflowapi_get_a_menu(cls, menuId):
    try:
      url = f"{config.woflow_base_url}/v1/menus/{menuId}"
      headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Token api_key={config.woflow_api_key}'
      }

      response = requests.request("GET", url, headers=headers, data={})
      print("get_a_menu response...")
      print(response.text)
      print("end get_a_menu response")

      return response
    except Exception as e:
      print("error: ", str(e))
      return False

  ###############################################

  @classmethod
  def post_woflow(cls, merchantid, urls=None, processingtype=None, instructions=None, status=0, created_by=None, updated_by=None, jobid=None, jobstate=None, jobmenuid=None, reason=None):
    try:
      connection, cursor = get_db_connection()
      
      entry_guid = uuid.uuid4()
      data = (entry_guid, merchantid, urls, processingtype, instructions, status, created_by, updated_by, jobid, jobstate, jobmenuid, reason)
      cursor.execute("""
        INSERT INTO woflow
          (id, merchantid, urls, processingtype, instructions, status, created_by, updated_by, jobid, jobstate, jobmenuid, reason)
          VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (data))

      connection.commit()

      return entry_guid
    except Exception as e:
      print("error: ", str(e))
      return False
  

  @classmethod
  def get_woflow(cls, merchantId=None, woflowColumnId=None):
    try:
      connection, cursor = get_db_connection()

      conditions = []

      if woflowColumnId:
        conditions.append(f'`id` = "{woflowColumnId}"')
      if merchantId:
        conditions.append(f'`merchantid` = "{merchantId}"')
      
      # where clause handling
      where = ' AND '.join(conditions)
      if not where:
        where = "1"
      
      data = ()
      cursor.execute(f"""
        SELECT woflow.*,
          IF(woflow.created_by IS NULL, NULL, (SELECT username FROM users WHERE users.id=woflow.created_by LIMIT 1)) AS createdByUser,
          IF(woflow.updated_by IS NULL, NULL, (SELECT username FROM users WHERE users.id=woflow.updated_by LIMIT 1)) AS updatedByUser
        FROM woflow 
        WHERE {where}
        ORDER BY woflow.created_datetime DESC
        """, ())
      rows = cursor.fetchall()

      merchantTimezone = "UTC"
      if len(rows):
        cursor.execute("SELECT timezone FROM merchants WHERE id=%s", rows[0].get("merchantid"))
        merchantTimezone = cursor.fetchone().get("timezone")
        if not merchantTimezone: merchantTimezone = "UTC"

      data = list()
      for row in rows:
        data.append({
          "id": row["id"],
          "merchantId": row["merchantid"],
          "urls": row["urls"].split(","),
          "processingType": row["processingtype"],
          "instructions": row["instructions"],
          "status": row["status"],
          "createdBy": row["created_by"],
          "createdDateTime": row["created_datetime"].replace(tzinfo=datetime.timezone.utc).astimezone(gettz(merchantTimezone)).strftime("%m-%d-%Y %H:%M:%S ") + f"({merchantTimezone})",
          "createdByUser": row["createdByUser"],
          "updatedBy": row["updated_by"],
          "updatedDateTime": row["updated_datetime"].replace(tzinfo=datetime.timezone.utc).astimezone(gettz(merchantTimezone)).strftime("%m-%d-%Y %H:%M:%S ") + f"({merchantTimezone})",
          "updatedByUser": row["updatedByUser"],
          "jobId": row["jobid"],
          "jobState": row["jobstate"],
          "jobMenuId": row["jobmenuid"],
          "reason": row["reason"]
        })

      return data
    except Exception as e:
      print("error: ", str(e))
      return False
  

  @classmethod
  def update_woflow(cls, id, processingtype=None, instructions=None, status=None, jobid=None, jobstate=None, jobmenuid=None, reason=None, updated_by=None):
    try:
      connection, cursor = get_db_connection()
      
      data = (processingtype, instructions, status, jobid, jobstate, jobmenuid, reason, updated_by, id)
      cursor.execute("""
        UPDATE woflow
        SET
          processingtype=COALESCE(%s, processingtype),
          instructions=COALESCE(%s, instructions),
          status=COALESCE(%s, status),
          jobid=COALESCE(%s, jobid),
          jobstate=COALESCE(%s, jobstate),
          jobmenuid=COALESCE(%s, jobmenuid),
          reason=COALESCE(%s, reason),
          updated_by=COALESCE(%s, updated_by),
          updated_datetime=CURRENT_TIMESTAMP
        WHERE id = %s
        """, data)

      connection.commit()

      return True
    except Exception as e:
      print("error: ", str(e))
      return False

  ###############################################
  
  @classmethod
  def generate_presigned_s3_urls(cls, merchantId: str, fileNames: list):
    try:
      s3_apptopus_bucket = config.s3_apptopus_bucket
      woflow_data_folder = config.s3_woflow_data_folder
      

      ### initialize s3_client
      # s3_client = boto3.client(
      #   "s3",
      #   config=Config(region_name = 'us-east-2'), 
      #   aws_access_key_id=os.environ.get("APPTOPUS_KEY"), 
      #   aws_secret_access_key=os.environ.get("APPTOPUS_SECRET")
      # )
      s3_client = boto3.client("s3")


      ### check if woflow folder exists in s3. If not then create folder
      resp = s3_client.list_objects(Bucket=s3_apptopus_bucket, Prefix=woflow_data_folder, Delimiter='/', MaxKeys=1)
      if not 'CommonPrefixes' in resp:
        resp = s3_client.put_object(Bucket=s3_apptopus_bucket, Key=(woflow_data_folder + "/"))
      
      
      urls = list()

      for fileName in fileNames:

        # check if content-type is provided in filename
        contentType = None
        if ":-_-:" in fileName:
          fn = fileName.split(":-_-:")
          fileName, contentType = fn[0], fn[1]

        Fields={"acl": "public-read"}
        Conditions = [{"acl": "public-read"},]
        if contentType:
          Fields["Content-Type"] = contentType
          Conditions.append({"Content-Type": contentType})

        postUrl = s3_client.generate_presigned_post(
          Bucket=s3_apptopus_bucket,
          Key=f"{woflow_data_folder}/{merchantId}/{fileName}",
          Fields=Fields,
          Conditions=Conditions,
          ExpiresIn=3600
        )

        urls.append({
          "fileName": fileName,
          "presignedUrl": postUrl
        })

      return urls
    except Exception as e:
      print("error: ", str(e))
      return False


  @classmethod
  def initialize_menu_processing(cls, merchantId, woflowColumnId, processingType, instructions, userId):
    try:
      connection, cursor = get_db_connection()

      ### get woflow entry details
      cursor.execute("""SELECT * FROM woflow WHERE id = %s""", (woflowColumnId))
      woflow_details = cursor.fetchone()
      if not woflow_details or woflow_details["merchantid"] != merchantId:
        return invalid("invalid woflowColumnId!")
      if woflow_details["jobstate"] and woflow_details["jobstate"] != "uploaded":
        return invalid("job is already initialized on woflow!")
      
      # get merchant details
      cursor.execute("""SELECT * FROM merchants WHERE id = %s""", (merchantId))
      merchant_details = cursor.fetchone()
      if not merchant_details:
        return invalid("invalid merchantId!")
      
      urls = woflow_details.get("urls").split(",")
      merchantName = merchant_details["merchantname"]
      merchantAddress = merchant_details["address"]
      
      ## post job to woflow
      response = cls.woflowapi_create_a_job(processingType, urls, merchantId, woflowColumnId, merchantName, merchantAddress, instructions)
      
      if response.status_code != 200:
        return invalid(f"error from woflow create_a_job with status:{response.status_code} and details: {response.text}")
      response = response.json()

      ### update record in db
      res = cls.update_woflow(
        id=woflowColumnId, 
        processingtype=processingType, 
        instructions=instructions, 
        status=0, 
        jobid=response.get("id"), 
        jobstate=response.get("state")
      )

      # Triggering SNS activity-logs
      eventName = "woflow.job_initiated"
      sns_msg = { "event": eventName, "body": { "merchantId": merchantId, "userId": userId, "woflowColumnId": woflowColumnId } }
      logs_sns_resp = publish_sns_message(topic=config.sns_activity_logs, message=str(sns_msg), subject=eventName)

      ### return woflow entry details to frontend
      entry_details = cls.get_woflow(woflowColumnId=woflowColumnId)

      return success(jsonify({
        "message": "success",
        "status": 200,
        "data": entry_details[0]
      }))
    except Exception as e:
      print("error: ", str(e))
      return unhandled()


  @classmethod
  def refresh_job_on_woflow(cls, woflowColumnId):
    try:
      connection, cursor = get_db_connection()
      
      ### get woflow column details from database
      cursor.execute("SELECT * FROM woflow WHERE id = %s", (woflowColumnId))
      row = cursor.fetchone()
      
      if not row:
        return invalid("woflow column id is invalid!")
      
      # if job status is 0 (uploaded), then return invalid()
      if row.get("jobstate") == "uploaded":
        return invalid("please submit job to woflow!")
      
      ### get job details from woflow api
      response = cls.woflowapi_get_a_job(jobId=row.get("jobid"))

      if response.status_code != 200:
        return invalid(f"error from woflow get_a_job with status:{response.status_code} and details: {response.text}") 
      response = response.json()

      jobstate = response.get("state")
      status = row.get("status")
      jobmenuid = row.get("jobmenuid")

      # if jobstate == "completed":
      if response.get("results").get("menu_id"):
        jobmenuid = response.get("results").get("menu_id")

      ### update record in db
      res = cls.update_woflow(
        id=woflowColumnId,
        status=status, 
        jobstate=jobstate,
        jobmenuid=jobmenuid
      )

      ### return woflow entry details to frontend
      entry_details = cls.get_woflow(woflowColumnId=woflowColumnId)

      return success(jsonify({
        "message": "success",
        "status": 200,
        "data": entry_details[0]
      }))
      
    except Exception as e:
      print("error: ", str(e))
      return unhandled()
  


  @classmethod
  def menu_pdf(cls, menu_id):
    url = config.woflow_base_url+"/v1/menus/"+menu_id
    payload = {}
    headers = {
      'Authorization': 'Token api_key='+config.woflow_api_key
    }
    response = requests.request("GET", url, headers=headers, data=payload)
    response = response.json()

    if response:
      data = []
      cat = []

      for category in response['sections']:
        print(category['name'])

        items=[]
        addonopt = []
        for item in category['items']:
          print("--"+item['name'])

          for modifier in item['child_modifiers']:
            print("----"+modifier['name'])


            addonopt.append({
                "name": modifier['name'],
                "description": modifier['description'],
                "options": modifier['child_items']
              })
          items.append({
              "name": item['name'],
              "description": item['description'],
              "price": int(item['price'])/100,
              "addon": addonopt
            })
          addonopt = []
        cat.append({
          "name": category['name'],
          "description": category['description'],
          "items": items
        })
        data.append(cat)
        cat = []

      menu = {
        "name": "WoFlow",
        "menu": "Menu",
        "data": data
      }
      print(data)

      html = render_template(
        "woflowmenupdf.html",
        resp=menu)
      path_wkhtmltopdf = os.environ.get("wkhtmltopdf_path")
      configg = pdfkit.configuration(wkhtmltopdf=path_wkhtmltopdf)
      pdf = pdfkit.from_string(html, False, configuration=configg)
      response = make_response(pdf)
      response.headers["Content-Type"] = "application/pdf"
      response.headers['Access-Control-Allow-Origin'] = '*'
      response.headers['Access-Control-Allow-Headers'] = '*'
      response.headers['Access-Control-Allow-Methods'] = '*'
      response.headers["Content-Disposition"] = "inline; filename=menu.pdf"

      return response



  @classmethod
  def accept_reject_job(cls, woflowColumnId, operation, reason, userId):
    try:
      connection, cursor = get_db_connection()

      cursor.execute("SELECT * FROM woflow WHERE id = %s", (woflowColumnId))
      tblwoflow_row = cursor.fetchone()
      if not tblwoflow_row:
        return invalid("invalid woflow column id!")
      if not tblwoflow_row["jobmenuid"]:
        return invalid("job-menu-id is empty!")
      merchantId = tblwoflow_row['merchantid']
      
      if operation == "accept":
        # accept
        
        ### ### ### get menu details from woflow api
        response = cls.woflowapi_get_a_menu(menuId=tblwoflow_row.get("jobmenuid"))
        
        if response.status_code != 200:
          return invalid(f"error from woflow create_a_menu api with status:{response.status_code} and details: {response.text}")
        response = response.json()
        
        if not response.get("sections"):
          return invalid("menu have no sections!")

        ### ### ### replace all the ids with new uuids in reponse
        items_ids_list, addons_ids_list, options_ids_list = list(), list(), list()   # [{"old_id": "", "new_id": ""},]

        ### replacing categories ids
        for category in response.get("sections"):
          category["id"] = str(uuid.uuid4())

          ### replacing items ids
          for item in category.get("items"):
            oldItemId = item.get("id")
            newItemId = str(uuid.uuid4())

            exists = False
            for r in items_ids_list:
              if r["old_id"] == oldItemId:
                item["id"] = r["new_id"]
                exists = True
                break

            if not exists:
              items_ids_list.append({
                "old_id": oldItemId,
                "new_id": newItemId	
              })
              item["id"] = newItemId


            ### replacing addons ids
            for addon in item.get("child_modifiers"):
              oldAddonId = addon.get("id")
              newAddonId = str(uuid.uuid4())

              exists = False
              for r in addons_ids_list:
                if r["old_id"] == oldAddonId:
                  addon["id"] = r["new_id"]
                  exists = True
                  break

              if not exists:
                addons_ids_list.append({
                  "old_id": oldAddonId,
                  "new_id": newAddonId	
                })
                addon["id"] = newAddonId


              ### replacing addon_option ids
              for option in addon.get("child_items"):
                oldOptionId = option.get("id")
                newOptionId = str(uuid.uuid4())

                exists = False
                for r in options_ids_list:
                  if r["old_id"] == oldOptionId:
                    option["id"] = r["new_id"]
                    exists = True
                    break

                if not exists:
                  options_ids_list.append({
                    "old_id": oldOptionId,
                    "new_id": newOptionId
                  })
                  option["id"] = newOptionId
        
        ### release the variables
        del items_ids_list; del addons_ids_list; del options_ids_list;

        # menu payload (id, merchantid, name, description, status, created_by)
        menuId = str(uuid.uuid4())
        menu_payload = (menuId, merchantId, f"woflow-{datetime.date.today().isoformat()}", "", 1, userId)
        
        items_ids_list = list()
        addons_ids_list = list()
        menu_categories_payload = list(tuple()) # (id, merchantid, menuid, categoryid, platformtype)
        categories_payload = list(tuple())  # (id, merchantid, categoryname, categorydescription, status, created_by)
        items_categories_payload = list(tuple()) # (id, productid, categoryid, created_by)
        items_payload = list(tuple()) # (id, merchantid, itemsku, itemname, itemdescription, itemprice, itemtype, status, taxrate, created_by)
        items_addons_payload = list(tuple()) # (id, productid, addonid, created_by)
        addons_payload = list(tuple()) # (id, merchantid, addonname, addondescription, minpermitted, maxpermitted, multiselect, status, created_by)
        addons_options_payload = list(tuple()) # (id, itemid, addonid, created_by)

        ### loop over categories
        for category in response.get("sections"):
          
          # append category details to categories_payload
          categories_payload.append((category.get("id"), merchantId, category.get("name"), category.get("description"), 1, userId))

          # append mappings to menu_categories_payload
          menu_categories_payload.append(( str(uuid.uuid4()), merchantId, menuId, category.get("id"), 1 ))


          ### loop over items
          for item in category.get("items"):

            # append item details to items_payload
            if item.get("id") not in items_ids_list:
              itemPrice = (int(item.get("price")) / 100) if is_float(item.get("price")) else 0
              items_payload.append(( item.get("id"), merchantId, "", item.get("name"), item.get("description"), itemPrice, 1, 1, 0, userId))

            # append mappings to items_categories_payload
            items_categories_payload.append(( str(uuid.uuid4()), item.get("id"), category.get("id"), userId ))

            # append itemid to item_ids_list
            items_ids_list.append(item.get("id"))
            

            ### loop over addons
            for addon in item.get("child_modifiers"):

              # append addon details to addons_payload
              if addon.get("id") not in addons_ids_list:
                minPermitted = int(addon.get("min_selection")) if is_float(addon.get("min_selection")) else 1
                maxPermitted = int(addon.get("max_selection")) if is_float(addon.get("max_selection")) else minPermitted
                addons_payload.append(( addon.get("id"), merchantId, addon.get("name"), addon.get("description"),  minPermitted, maxPermitted, 1, 1, userId))

              # append mappings to item_addons_payload
              items_addons_payload.append(( str(uuid.uuid4()), item.get("id"), addon.get("id"), userId ))

              # append addonid to addons_ids_list
              addons_ids_list.append(addon.get("id"))
              
            
              ### loop over addon-options
              for option in addon.get("child_items"):
                
                # append option details to items_payload
                if option.get("id") not in items_ids_list:
                  optionPrice = (int(option.get("price")) / 100) if is_float(option.get("price")) else 0
                  items_payload.append(( option.get("id"), merchantId, "", option.get("name"), option.get("description"), optionPrice, 2, 1, 0, userId))

                # append mappings to addon_options_payload
                addons_options_payload.append(( str(uuid.uuid4()), option.get("id"), addon.get("id"), userId ))

                # append optionid to items_ids_list
                items_ids_list.append(option.get("id"))
        

        ### ### ### bulk insert into database
        # menu_payload, menu_categories_payload, categories_payload, items_categories_payload
        # items_payload, items_addons_payload, addons_payload, addons_options_payload

        print("menus insert...")
        cursor.execute("""
          INSERT INTO menus (id, merchantid, name, description, status, created_by)
          VALUES (%s,%s,%s,%s,%s,%s)
        """, menu_payload)
        print(cursor.rowcount)

        print("categories insert...")
        cursor.executemany("""
          INSERT INTO categories (id, merchantid, categoryname, categorydescription, status, created_by)
          VALUES (%s,%s,%s,%s,%s,%s)
        """, (categories_payload))
        print(cursor.rowcount)

        print("menucategories insert...")
        cursor.executemany("""
          INSERT INTO menucategories (id, merchantid, menuid, categoryid, platformtype)
          VALUES (%s,%s,%s,%s,%s)
        """, (menu_categories_payload))
        print(cursor.rowcount)

        print("items insert...")
        cursor.executemany("""
          INSERT INTO items (id, merchantid, itemsku, itemname, itemdescription, itemprice, itemtype, status, taxrate, created_by)
          VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (items_payload))
        print(cursor.rowcount)

        print("productscategories insert...")
        cursor.executemany("""
          INSERT IGNORE INTO productscategories (id, productid, categoryid, created_by)
          VALUES (%s,%s,%s,%s)
        """, (items_categories_payload))
        print(cursor.rowcount)

        print("addons insert...")
        cursor.executemany("""
          INSERT INTO addons (id, merchantid, addonname, addondescription, minpermitted, maxpermitted, multiselect, status, created_by)
          VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (addons_payload))
        print(cursor.rowcount)
        
        print("productsaddons insert...")
        cursor.executemany("""
          INSERT IGNORE INTO productsaddons (id, productid, addonid, created_by)
          VALUES (%s,%s,%s,%s)
        """, (items_addons_payload))
        print(cursor.rowcount)

        print("addonsoptions insert...")
        cursor.executemany("""
          INSERT IGNORE INTO addonsoptions (id, itemid, addonid, created_by)
          VALUES (%s,%s,%s,%s)
        """, (addons_options_payload))
        print(cursor.rowcount)

        print("committing inserts...")
        connection.commit()

        ### ### ### update the record in db
        res = cls.update_woflow(
          id=woflowColumnId,
          status=1,
          updated_by=userId
        )

      else:
        # reject
        
        ### ### ### update the record in db
        res = cls.update_woflow(
          id=woflowColumnId,
          status=2,
          reason=reason,
          updated_by=userId
        )
      


      # Triggering SNS activity-logs
      eventName = "woflow.status_updated"
      sns_msg = { "event": eventName, "body": { "merchantId": merchantId, "userId": userId, "woflowColumnId": woflowColumnId, "operation": operation } }
      logs_sns_resp = publish_sns_message(topic=config.sns_activity_logs, message=str(sns_msg), subject=eventName)

      ### return woflow entry details to frontend
      entry_details = cls.get_woflow(woflowColumnId=woflowColumnId)

      return success(jsonify({
        "message": "success",
        "status": 200,
        "data": entry_details[0]
      }))
      
    except Exception as e:
      print("error: ", str(e))
      return unhandled()
