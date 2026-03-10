from datetime import datetime, timedelta

from flask import g, request
import uuid
import json
from square.client import Client

# local imports
import config
import requests
from flask import jsonify

from models.VirtualMerchants import VirtualMerchants
from models.clover.Clover import Clover
from utilities.helpers import success, get_db_connection, publish_sns_message, create_log_data
from utilities.errors import invalid, unhandled
import models.ubereats.UberEats
from models.Merchants import Merchants


class Platforms():

  @classmethod
  def add_store(cls, request, merchantId):
    try:
      '''
          THIS API IS ONLY COMPATIBLE WITH FLIPDISH
      '''

      connection, cursor = get_db_connection()

      print(request.json)
      _json = request.json
      accountid = _json.get('accountId')
      storeid = _json.get('storeId')
      clientid = _json.get('clientId')
      platformtype = _json.get('platformType')
      secretkey = _json.get('secretKey')
      accesstoken = _json.get('accessToken')

      userId = g.userId

      platfom_check = cls.get_platform_by_storeid(storeid)
      if not platfom_check:
        url = config.flipdish_base_url + "/stores/" + storeid
        headers = {
          "Accept": "application/json",
          "Authorization": "Bearer " + accesstoken
        }
        response = requests.request("GET", url, headers=headers)
        json_data = response.json() if response and response.status_code == 200 else None

        if json_data and 'Data' in json_data:
          if not (response.json() and 'Data' in response.json()):
            return invalid("There is error in your flipdish details. Please check the details again!!!")
          # store data in db
          storeid = json_data['Data']['StoreId']

          location = json_data['Data']['Address']
          location['address'] = location['DisplayForCustomer']
          metadata_payload = json.dumps({
            "store": {
              "name": json_data['Data']['Name'],
              "location": location,
              "store_id": json_data['Data']['StoreId']
            }
          })

          platformId = uuid.uuid4()
          data = (platformId, merchantId, accountid, storeid, clientid, secretkey, accesstoken, platformtype, 1, 0,
                  0, metadata_payload, userId)
          cursor.execute("""INSERT INTO platforms
                          (id, merchantid, accountid, storeid, clientid, secretkey, accesstoken, platformtype, integrationstatus,syncstatus,
                           synctype, metadata, created_by)
                          VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""", data)
          connection.commit()

          # Triggering SNS
          sns_msg = {
            "event": "platform.connect",
            "body": {
              "merchantId": merchantId,
              "userId": userId,
              "platformId": str(platformId)
            }
          }
          logs_sns_resp = publish_sns_message(topic=config.sns_activity_logs, message=str(sns_msg),
                                              subject="platform.connect")

          return success(jsonify({
            "message": "success",
            "status": 200,
            "data": cls.get_platform_by_id_str(platformId)
          }))
        else:
          return invalid("The provided Flip dish details are incorrect!")
      else:
        return invalid("The Flip dish store with this store id exists already!")

    except Exception as e:
      print("Error: ", str(e))
      return unhandled()

  @classmethod
  def get_platform_by_id(cls, id):
    connection, cursor = get_db_connection()
    cursor.execute("""SELECT * FROM platforms WHERE id =%s""", (id))
    row = cursor.fetchone()
    return row

  @classmethod
  def get_platform_by_id_str(cls, id):
    try:
      row = cls.get_platform_by_id(id)
      if not row:
        return False

      # do something
      platform = {
        'id': row['id'],
        'integrationstatus': row['integrationstatus'],
        'merchantid': row['merchantid'],
        'platformtype': row['platformtype'],
        'storeid': row['storeid'],
        'syncstatus': row['syncstatus'],
        'synctype': row['synctype'],
        'metadata': {
          'store': json.loads(row['metadata']).get('store') if row['metadata'] else None
        }
      }
      return platform

    except Exception as e:
      print("Error: ", str(e))
      return False

  @classmethod
  def get_platform_by_storeid(cls, id):
    connection, cursor = get_db_connection()
    cursor.execute(

      """SELECT id,storeid,accesstoken,accountid,syncstatus,integrationstatus,merchantid,platformtype,synctype,token_metadata FROM platforms WHERE storeid =%s""",

      (id))
    row = cursor.fetchone()
    return row

  @classmethod
  def delete_platform(cls, id):
    connection, cursor = get_db_connection()
    cursor.execute("""DELETE FROM platforms WHERE id=%s""", (id))
    connection.commit()
    if cursor.rowcount > 0:
      return True
    else:
      return False

  @classmethod
  def revoke_square(cls, id):
    connection, cursor = get_db_connection()
    cursor.execute("""DELETE FROM platforms WHERE storeid=%s and platformtype=11""", (id))
    connection.commit()
    if cursor.rowcount > 0:
      return True
    else:
      return False

  @classmethod
  def get_all_platform_by_merchantid(cls, id):
    connection, cursor = get_db_connection()
    platforms = list(dict())
    cursor.execute("""SELECT * FROM platforms WHERE merchantid =%s""", (id))
    rows = cursor.fetchall()
    for row in rows:
      platform = {
        'id': row['id'],
        'integrationstatus': row['integrationstatus'],
        'merchantid': row['merchantid'],
        'platformtype': row['platformtype'],
        'storeid': row['storeid'],
        'syncstatus': row['syncstatus'],
        'synctype': row['synctype'],
        'metadata': {
          'store': json.loads(row['metadata']) if row['metadata'] else None
        }
      }

      platforms.append(platform)
    is_virtual_merchant = VirtualMerchants.get_virtual_merchant(id=id)
    if is_virtual_merchant:
      cursor.execute("""SELECT is_stream_enabled , doordashstream , grubhubstream FROM virtualmerchants WHERE id =%s""", (is_virtual_merchant[0]['id']))
    else:
      cursor.execute("""SELECT is_stream_enabled , doordashstream , grubhubstream FROM merchants WHERE id =%s""", (id))
    stream_status = cursor.fetchone()

    if stream_status :
      matching_id = ''
      synctype=0
      for item in platforms:
        if item['platformtype'] == 8:
          matching_id = item['id']
          synctype=item['synctype']
          platforms.remove(item)
          break
      platform = {
        'id': matching_id,
        'integrationstatus': 0,
        'merchantid': id,
        'platformtype': 8,
        'storeid': '',
        'syncstatus': stream_status.get('is_stream_enabled'),
        'synctype': synctype,
        'metadata': {},
        "doordashstream": True if stream_status.get('doordashstream')==1 else False ,
        "grubhubstream": True if stream_status.get('grubhubstream') == 1 else False
      }
      platforms.append(platform)

    return platforms

  @classmethod
  def get_platform_by_merchantid_and_platformtype(cls, merchantid, platformtype):
    connection, cursor = get_db_connection()
    cursor.execute("""SELECT * FROM platforms WHERE merchantid =%s AND platformtype=%s""", (merchantid, platformtype))
    row = cursor.fetchone()
    return row

  @classmethod
  def update_sync_type(cls, platformId, syncType):
    connection, cursor = get_db_connection()
    cursor.execute("""UPDATE platforms SET synctype=%s WHERE id=%s""", (syncType, platformId))
    connection.commit()
    return True

  @classmethod
  def update_stream_status(cls, status, merchantid, is_main_merchant=None , vmerchantId=None):
    connection, cursor = get_db_connection()
    mid=''
    if is_main_merchant:
      mid=merchantid
      cursor.execute("""UPDATE merchants SET is_stream_enabled=%s WHERE id=%s""", (status, merchantid))
      if status == 0:
        cursor.execute("""UPDATE merchants SET grubhubstream=0 ,doordashstream=0 WHERE id=%s""", (merchantid))
    else:
      mid=vmerchantId
      cursor.execute("""UPDATE virtualmerchants SET is_stream_enabled=%s WHERE id=%s""", (status, vmerchantId))
      if status == 0:
        cursor.execute("""UPDATE virtualmerchants SET grubhubstream=0 ,doordashstream=0 WHERE id=%s""", (merchantid))
    if status==0:
      cursor.execute("""DELETE FROM platforms WHERE merchantid=%s and platformtype=8""", (mid))
      cursor.execute("""DELETE FROM itemmappings WHERE merchantid=%s and platformtype=8""", (mid))
    connection.commit()
    return True

  @classmethod
  def get_access_token(cls, merchantId):
    connection, cursor = get_db_connection()
    cursor.execute("""SELECT * FROM platforms WHERE platformtype=2 and merchantid =%s""", (merchantId))
    row = cursor.fetchone()
    return row['accesstoken']

  @classmethod
  def provision_ubereats(cls, merchantId, code, redirect_uri, userId=None):
    
    ip_address = None
    if request:
        ip_address = request.environ.get('HTTP_X_FORWARDED_FOR', request.remote_addr)
    if ip_address:
        ip_address = ip_address.split(',')[0].strip()
        
    create_log_data(level='[INFO]',
                    Message=f"In the beginning of provision_ubereats code function to connect and map merchant with uber eats., IP address:{ip_address}",
                    merchantID=merchantId, functionName="provision_ubereats", request=request)

    connection, cursor = get_db_connection()
    # get merchant by id
    # merchant_details = Merchants.get_merchant_by_id(merchantId)
    merchant_details = Merchants.get_merchant_or_virtual_merchant(merchantId)
    if not merchant_details:
        create_log_data(level='[ERROR]',
                        Message="Failed to get merchant detail",
                        merchantID=merchantId, functionName="provision_ubereats", request=request)
        return invalid("Failed to get merchant detail")
    create_log_data(level='[INFO]',
                    Message="Get merchant detail by merchant id",messagebody=merchant_details ,
                    merchantID=merchantId, functionName="provision_ubereats", request=request)
    merchantName = merchant_details.get("virtualname") if merchant_details.get(
      "isVirtual") == 1 else merchant_details.get("merchantname")

    # send code to get the provisioning access_token and refresh_token`
    payload = 'client_id=' + config.uber_client_id + '&' \
                                                     'client_secret=' + config.uber_client_secret + '&' \
                                                                                                    'grant_type=authorization_code&' \
                                                                                                    'code=' + code + '&' \
                                                                                                                     'redirect_uri=' + redirect_uri
    create_log_data(level='[INFO]',
                    Message="Payload for auth API ", messagebody=payload,
                    merchantID=merchantId, functionName="provision_ubereats", request=request)
    headers = {
      'Content-Type': 'application/x-www-form-urlencoded'
    }

    provision = requests.request("POST", config.uber_login_endpoint, headers=headers, data=payload)
    create_log_data(level='[INFO]',
                    Message="Response from auth API", messagebody=provision.text,
                    merchantID=merchantId, functionName="provision_ubereats", request=request)
    print(provision.text)
    provision_json = provision.json()
    provision = provision.json() if provision and provision.status_code == 200 else None
    if not provision:
      create_log_data(level='[ERROR]',
                      Message="Failed to get auth response",messagebody=f"{provision_json['error']} + ' :' + {provision_json['error_description']}, IP address:{ip_address}",
                      merchantID=merchantId, functionName="provision_ubereats", request=request)
      return invalid(provision_json["error"] + " :" + provision_json["error_description"])


    # now list all the stores using the provisioning token

    stores = models.ubereats.UberEats.UberEats.ubereats_list_stores(provision['access_token'])
    create_log_data(level='[INFO]',
                    Message=f"Geting list all the uber eats stores using the provisioning token : {provision['access_token']}, IP address:{ip_address}", messagebody=stores,
                    merchantID=merchantId, functionName="provision_ubereats", request=request)

    if not stores:
      create_log_data(level='[ERROR]',
                      Message=f"Failed to get uber eats stores, IP address:{ip_address}",
                      merchantID=merchantId, functionName="provision_ubereats", request=request)
      return invalid("This merchant has no stores")
    # stores = stores['stores']
    store = None

    if len(stores) == 1:
      store = stores[0]
    else:
      for st in stores:
        st_name = st['name']
        merchant_name = merchantName
        if st_name.lower() == merchant_name.lower():
          store = st
          break

    if store is None:
      create_log_data(level='[ERROR]',
                      Message=f"merchant name does not matched with any store name!, IP address:{ip_address}",
                      merchantID=merchantId, functionName="provision_ubereats", request=request)
      return invalid('merchant name does not matched with any store name!')

    if not cls.get_platform_by_storeid(store['id']):

      # perform the pos integration with store
      provisioned = models.ubereats.UberEats.UberEats.ubereats_setup_pos_integration(merchantId, store['id'],
                                                                                     provision['access_token'])
      if not provisioned:
        create_log_data(level='[ERROR]',
                        Message="error occured while provisioning the store",
                        merchantID=merchantId, functionName="provision_ubereats", request=request)
        return unhandled('error occured while provisioning the store')

      store_json_data = {
          "name": store["name"],
          "status": store["onboarding_status"].lower(),  # e.g. "ACTIVE" -> "active"
          "web_url": "",  # This field isn't in second_json, you can add logic to generate it if needed
          "location": {
            "city": store["location"]["city"] if 'city' in store["location"] else '',
            "state": "",  # Not present in second_json
            "address": store["location"]["street_address_line_one"] if 'street_address_line_one' in store[
              "location"] else '',
            "country": store["location"]["country"] if 'country' in store["location"] else '',
            "latitude": float(store["location"]["latitude"]) if 'latitude' in store["location"] else 0,
            "longitude": float(store["location"]["longitude"]) if 'longitude' in store["location"] else 0,
            "postal_code": store["location"]["postal_code"] if 'postal_code' in store["location"] else ''
          },
          "pos_data": {
            "integration_enabled": True
          },
          "store_id": store["id"],
          "timezone": store["timezone"],
          "price_bucket": "$",  # Not present, defaulted
          "raw_hero_url": "",  # Not present
          "avg_prep_time": store["prep_times"]["default_value"] // 60 if store.get(
            'prep_times') and 'default_value' in store.get('prep_times') else 0,  # convert seconds to minutes
          "contact_emails": [store["contact"]["email"]],
          "partner_store_id": merchantId,
          "merchant_store_id": merchantId
        }
      # Get order detail from previous api for web url
      eats_store_resp = models.ubereats.UberEats.UberEats.ubereats_generate_access_token()
      create_log_data(level='[INFO]',
                      Message=f"Geting the access token for getting store detail to get web url, IP address:{ip_address}",
                      messagebody=eats_store_resp,
                      merchantID=merchantId, functionName="provision_ubereats", request=request)
      print("eats_store_resp " , eats_store_resp)
      if eats_store_resp:
        storeaccessToken = eats_store_resp.get('access_token')
        print("storeaccessToken" , storeaccessToken)
        if storeaccessToken:
          url = f"https://api.uber.com/v1/eats/stores/{store['id']}"
          headers = {
            'Authorization': f'Bearer { storeaccessToken}'
          }
          response = requests.request("GET", url, headers=headers)
          create_log_data(level='[INFO]',
                          Message=f"Response from get ubereats store api ( Previous Version ), IP address:{ip_address}",
                          messagebody=f" status code :{response.status_code}",
                          merchantID=merchantId, functionName="provision_ubereats", request=request)
          if response and response.status_code >= 200 and response.status_code < 300:
            response = response.json()
            create_log_data(level='[INFO]',
                            Message=f"Response json from get ubereats api, IP address:{ip_address}",
                            messagebody=response,
                            merchantID=merchantId, functionName="provision_ubereats", request=request)
            if response.get('web_url'):
              store_json_data['web_url'] = response.get('web_url')


      store_data = {
        "store": store_json_data,
        "access_token": provision['access_token'],
        "refresh_token": provision['refresh_token']
      }
      print('store_data ' , store_data )
      platform_id = uuid.uuid4()
      data = (platform_id, merchantId, '', store_json_data['store_id'], '', '', 3, 1, 0, 0, json.dumps(store_data), userId)
      cursor.execute("""INSERT INTO platforms
                (id, merchantid, accountid, storeid, clientid, secretkey, platformtype, integrationstatus,syncstatus,
                synctype, metadata, created_by)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""", data)
      connection.commit()

      # Triggering SNS
      print("Triggering sns - platform.connect ...")
      sns_msg = {
        "event": "platform.connect",
        "body": {
          "merchantId": merchantId,
          "userId": userId,
          "platformId": str(platform_id),
          "ipAddr": ip_address
        }
      }
      logs_sns_resp = publish_sns_message(topic=config.sns_activity_logs, message=str(sns_msg),
                                          subject="platform.connect")

      return success(jsonify({
        "message": "success",
        "status": 200,
        "data": cls.get_platform_by_id_str(platform_id)
      }))
    else:
      return invalid("This store has been already attached with any other merchant")

  @classmethod
  def connectClover(cls, request, merchantId, userId=None):
    try:
      
      ip_address = None
      if request:
          ip_address = request.environ.get('HTTP_X_FORWARDED_FOR', request.remote_addr)
      if ip_address:
          ip_address = ip_address.split(',')[0].strip()
      
      connection, cursor = get_db_connection()
      _json = request.json
      code = _json.get('code')
      store_id = _json.get('merchant_id')

      connection, cursor = get_db_connection()
      cursor.execute("""SELECT * FROM platforms WHERE storeid =%s""", (store_id,))
      row = cursor.fetchone()
      if row:
        return invalid("The Clover integration with this store exists already!")

      platform_check = cls.get_platform_by_merchantid_and_platformtype(merchantId, 4)
      if not platform_check:
        print('-----------------------  Calling the clover token api --------------------')
        url = f"{config.clover_base_url}/oauth/v2/token"

        # Prepare the payload with necessary parameters
        payload = {
          'client_id': config.clover_client_id,
          'code': code,
          'client_secret': config.clover_client_secret
        }

        # Set the headers with the correct Content-Type
        headers = {
          'Content-Type': 'application/json'
        }

        response = requests.post(url, headers=headers, data=json.dumps(payload))
        print("response " , response)

        json_data = response.json() if response and response.status_code == 200 else None
        print("json_data ", json_data)
        if json_data:
          token_metadata=json.dumps(json_data)
          token = json_data['access_token']
          headers = {
            'accept': 'application/json',
            'authorization': "Bearer " + token
          }

          response2 = requests.get(config.clover_base_url + '/v3/merchants/' + store_id, headers=headers)
          json_data2 = response2.json() if response2 and response2.status_code == 200 else None
          metadata_payload = json.dumps(json_data2)

          tenders_verified = Clover.verify_tenders(store_id, token,
                                                   tenders=['Fonda-DD', 'Fonda-UE', 'Fonda-GH', 'Fonda-SF'])
          if not tenders_verified:
            return invalid("Tenders are not successfully verified!")

          tender_id = None
          platformId = uuid.uuid4()
          data = (
            platformId, merchantId, tender_id, store_id, "", "", token, 4, 1, 0,
            0, metadata_payload,token_metadata, "")
          cursor.execute("""INSERT INTO platforms
                          (id, merchantid, accountid, storeid, clientid, secretkey, accesstoken, platformtype, integrationstatus, syncstatus,
                           synctype, metadata,token_metadata ,created_by)
                          VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""", data)

          connection.commit()

          # Triggering SNS
          print("Triggering sns - platform.connect ...")
          sns_msg = {
              "event": "platform.connect",
              "body": {
                  "merchantId": merchantId,
                  "userId": userId,
                  "platformId": str(platformId),
                  "ipAddr": ip_address
              }
          }
          logs_sns_resp = publish_sns_message(topic=config.sns_activity_logs, message=str(sns_msg), subject="platform.connect")

          return success(jsonify({
              "message": "success",
              "status": 200,
              "data": cls.get_platform_by_id(platformId)
          }))
        else:
            return invalid("The provided Clover details are incorrect!")
      else:
          return invalid("The Clover integration with this store exists already!")

    except Exception as e:
        print("Error: ", str(e))
        return unhandled()


  @classmethod
  def convert_time_to_int(cls, time_str):
      """Converts time in string format to integer format."""
      if not time_str:
          return {"hours": 0, "minutes": 0}

      time_obj = datetime.strptime(time_str, '%I:%M %p')
      return {
          "hours": int(time_obj.strftime('%H')) if time_obj.strftime('%H') else 0,
          "minutes": int(time_obj.strftime('%M')) if time_obj.strftime('%M') else 0
      }

  @classmethod
  def connectSquare(cls, request, merchantId, userId=None):
    try:
      
      ip_address = None
      if request:
          ip_address = request.environ.get('HTTP_X_FORWARDED_FOR', request.remote_addr)
      if ip_address:
          ip_address = ip_address.split(',')[0].strip()
      connection, cursor = get_db_connection()
      _json = request.json
      code = _json.get('code')
      store_id = _json.get('merchant_id')

      platform_check = cls.get_platform_by_merchantid_and_platformtype(merchantId, 11)
      if not platform_check:
        client = Client(
          access_token=config.square_client_secret,
          environment=config.square_env,
        )

        result = client.o_auth.obtain_token(
          body={
            "client_id": config.square_client_id,
            "client_secret": config.square_client_secret,
            "code": code,
            "grant_type": "authorization_code"
          }
        )

        if result.is_success():
          print(result.body)
          print("sswwewew")
        elif result.is_error():
          print(result.errors)
          print("errorrrrrr")

        json_data = result.body if result.body and result.status_code == 200 else None

        if json_data:
          token = json_data['access_token']
          print(json_data['merchant_id'])

          client = Client(
            access_token=token,
            environment=config.square_env,
          )
          result = client.merchants.retrieve_merchant(
            merchant_id=json_data['merchant_id']
          )
          print("result after square oauth, ", result)
          print("result after square oauth, only body ", result.body)
          result.body["refresh_token"] = json_data['refresh_token']

          if result.is_success():
            print(result.body)
          elif result.is_error():
            print(result.errors)

          location_id = result.body['merchant']['main_location_id']
          metadata_payload = json.dumps(result.body)

          platformId = uuid.uuid4()
          data = (
            platformId, merchantId, location_id, json_data['merchant_id'], "", "", token, 11, 1, 0,
            0, metadata_payload, userId)
          cursor.execute("""INSERT INTO platforms
                          (id, merchantid, accountid, storeid, clientid, secretkey, accesstoken, platformtype, integrationstatus, syncstatus,
                           synctype, metadata, created_by)
                          VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""", data)
          connection.commit()

          locations = client.locations.list_locations()
          print("getting locations from square after oauth:")
          if locations.is_success():
            print(locations.body)
          elif locations.is_error():
            print("error getting locations from square after oauth:")
            print(locations.errors)

          # Triggering SNS
          print("Triggering sns - platform.connect ...")
          sns_msg = {
            "event": "platform.connect",
            "body": {
              "merchantId": merchantId,
              "userId": userId,
              "platformId": str(platformId),
              "ipAddr": ip_address
            }
          }
          logs_sns_resp = publish_sns_message(topic=config.sns_activity_logs, message=str(sns_msg),
                                              subject="platform.connect")

          return success(jsonify({
            "message": "success",
            "status": 200,
            "data": cls.get_platform_by_id(platformId),
            "locations": locations.body if locations.is_success() else locations.errors
          }))
        else:
          return invalid("The Square Clover details are incorrect!")
      else:
        return invalid("The Square integration with this store exists already!")

    except Exception as e:
      print("Error: ", str(e))
      return unhandled()

  @classmethod
  def saveSquareLocation(cls, merchantid, location):
    connection, cursor = get_db_connection()
    cursor.execute("""UPDATE platforms SET accountid=%s where merchantid=%s and platformtype=11""",
                   (location, merchantid))
    connection.commit()

  @classmethod
  def saveToken(cls, token, merchantid):
    connection, cursor = get_db_connection()

    cursor.execute("""SELECT * FROM googleauth where merchantid=%s""", (merchantid))
    row = cursor.fetchone()

    if not row:
      data = (
        token['access_token'], token['refresh_token'], merchantid)
      cursor.execute("""INSERT INTO googleauth
                                    (accesstoken, refreshtoken,merchantid)
                                    VALUES (%s, %s,%s)""", data)

    else:
      cursor.execute("""UPDATE googleauth SET accesstoken=%s, refreshtoken=%s where merchantid=%s""",
                     (token['access_token'], token['refresh_token'], merchantid))

    connection.commit()

  @classmethod
  def getGoogleToken(cls, merchantid=None):
    try:
      create_log_data(level='[INFO]', Message="getting google auth token",
                      functionName="getGoogleToken")
      connection, cursor = get_db_connection()
      if merchantid:
        cursor.execute("""SELECT * FROM googleauth where merchantid=%s""", (merchantid))
      else:
        cursor.execute("""SELECT * FROM googleauth""", ())
      current_time = datetime.now()
      token = cursor.fetchone()
      one_hour_delta = timedelta(seconds=3299)
      if token:
        if abs(current_time - token['updatedaccesstokentime']) < one_hour_delta:
          return token['accesstoken']
        else:
          url = config.google_oauth_base + "token"

          payload = 'refresh_token=' + token[
            'refreshtoken'] + '&redirect_uri=' + config.redirect_uri + '&client_id=' + config.google_client_id + '&client_secret=' + config.google_secret_id + '&grant_type=refresh_token&scope=https://www.googleapis.com/auth/business.manage'
          headers = {
            'Content-Type': 'application/x-www-form-urlencoded'
          }

          token = requests.request("POST", url, headers=headers, data=payload)
          token = token.json()
          print(token)
          print(token['access_token'])
          if "access_token" in token:
            cursor.execute("""UPDATE googleauth SET accesstoken=%s, updatedaccesstokentime=%s where merchantid=%s""",
                           (token['access_token'], current_time, merchantid))
            connection.commit()
            create_log_data(level='[INFO]', Message="Successfully set token against that merchant",
                            functionName="getGoogleToken")

            return token['access_token']

      return False
    except Exception as e:
      print(str(e))
      return False

  @classmethod
  def getLinkedLication(cls, merchantid):
    connection, cursor = get_db_connection()
    cursor.execute("""SELECT * FROM googlelocations where merchantid=%s""", (merchantid))
    return cursor.fetchone()

  @classmethod
  def linkGoogleLocation(cls, request):
    print(request)

    create_log_data(level='[INFO]', Message=f"In the beginning of linking google location method",
                    functionName="linkGoogleLocation")

    platform_detail=cls.getLinkedLication(request['merchantId'])
    if platform_detail:
      create_log_data(level='[ERROR]',
                      Message=f"Merchnat against merchantid {request['merchantId']} is already linked with some other location",
                      functionName="linkGoogleLocation")
      return invalid("This merchant is already linked with some other location")

    location = cls.getGoogleLocation(request['locationId'], request['merchantId'],request['accountId'] ,linklocation=1)
    if location.status_code != 200:
      return invalid(location)
    location = location.json['location']
    print(location)

    connection, cursor = get_db_connection()
    data = (location['locationId'], location['title'], "location address", request['merchantId'], request['accountId'],
            json.dumps(location), location['status'])
    cursor.execute("""INSERT INTO googlelocations
                                           (locationid, title, address, merchantid, accountid, meta , status)
                                           VALUES (%s,%s,%s,%s,%s, %s,%s)""", data)
    cursor.execute("""Select * from googleauth
                                           where merchantid=%s""", request['merchantId'])
    refreshtoken = cursor.fetchone()
    print(refreshtoken)
    if refreshtoken:
      cursor.execute("""UPDATE googleauth 
                          JOIN googlelocations ON googleauth.merchantid = googlelocations.merchantid 
                          SET refreshtoken = %s 
                          WHERE googlelocations.accountid = %s;""",
                     (refreshtoken['refreshtoken'], request['accountId']))
      connection.commit()
    return success(jsonify({
      "message": "Merchant linked successfully",
      "status": 200,
      "location": location
    }))

  @classmethod
  def unlinkGoogleLocation(cls, request):

    connection, cursor = get_db_connection()
    cursor.execute("""DELETE FROM googlelocations WHERE merchantid=%s""", (request['merchantId']))
    cursor.execute("""DELETE FROM googleauth WHERE merchantid=%s""", (request['merchantId']))
    connection.commit()
    return True

  @classmethod
  def getGoogleLocation(cls, locationId, merchantId,accountId, linklocation=0):
    token = Platforms.getGoogleToken(merchantId)
    if not token:
      create_log_data(level='[ERROR]', Message=f"Access token is not valid for specific merchantid {merchantId}",
                      functionName="getGoogleLocation")
      return invalid("Access token invalid")
    url = config.google_business_account_base + "v1/" + locationId + "?read_mask=storeCode,regularHours,name,languageCode,title,phoneNumbers,categories,storefrontAddress,websiteUri,regularHours,specialHours,serviceArea,labels,adWordsLocationExtensions,latlng,openInfo,metadata,profile,relationshipData,moreHours"
    payload = {}
    headers = {
      'Authorization': 'Bearer ' + token
    }

    response = requests.request("GET", url, headers=headers, data=payload)
    location = response.json()
    if response.status_code == 404:
      create_log_data(level='[ERROR]', Message=f"Location not found against specific locationid: {locationId}",
                      messagebody=location,
                      functionName="getGoogleLocation", statusCode="404 Bad Request", request=request)
      return invalid("Location not found")

    if linklocation == 0:
      connection, cursor = get_db_connection()
      cursor.execute("""SELECT status FROM googlelocations where locationid=%s """, (locationId))
      status = cursor.fetchone()
      status = status['status']
    else:
      url = config.google_business_verification_base + "v1/" + location['name'] + "/VoiceOfMerchantState"

      payload = {}
      headers = {
        'Authorization': 'Bearer ' + token
      }

      response = requests.request("GET", url, headers=headers, data=payload)
      response = response.json()
      # print(response)

      status = 0

      if 'verify' in response:
        status = 1
      elif 'complyWithGuidelines' in response:
        status = 2
      elif 'hasVoiceOfMerchant' in response and 'hasBusinessAuthority' in response:
        status = 3
      elif 'resolveOwnershipConflict' in response:
        status = 4

    regularHours=location['regularHours']["periods"] if 'regularHours' in location and "periods" in location['regularHours'] else []
    if len(regularHours) > 0:
      regularHours=cls.format_regularHours(regularHours)

    menuHours=[]
    if "moreHours" in location:
      for menuhour in location['moreHours']:
        menuHours= menuhour['periods'] if menuhour['hoursTypeId'] == 'DELIVERY' else []
    if len(menuHours) > 0:
      menuHours = cls.format_regularHours(menuHours)

    location = {
      "locationId": location['name'] if location['name'] else '',
      "accountId":accountId,
      "title": location['title'] if 'title' in location else '',
      "address": {
                  'regionCode':'United States',
                  'postalCode' : location['storefrontAddress']['postalCode'] if "postalCode" in location['storefrontAddress'] else '',
                  'state': location['storefrontAddress']['administrativeArea'] if "administrativeArea" in location['storefrontAddress'] else '',
                  'town': location['storefrontAddress']['locality'] if "locality" in location['storefrontAddress'] else '',
                  'addressLines': location['storefrontAddress']['addressLines'] if "addressLines" in location['storefrontAddress'] else []
                  },
      "websiteUri": location['websiteUri'] if 'websiteUri' in location else '',
      "phoneNumber": location['phoneNumbers']["primaryPhone"] if 'phoneNumbers' in location and "primaryPhone" in  location['phoneNumbers']else "",
      "category": location['categories']['primaryCategory']['displayName'] if 'categories' in location and "primaryCategory" in location['categories'] else '',
      "profile": location['profile']["description"] if  'profile' in location and "description" in location['profile'] else '',
      "regularHours": regularHours,
      "menuHours":menuHours,
      "merchantId": merchantId,
      "status": status,
    }

    return success(jsonify({
      "message": "Location get successfully",
      "status": 200,
      "location": location
    }))

  @classmethod
  def format_regularHours(cls,regularHours):
    create_log_data(level='[INFO]', Message="Starting regular hours formatter function",
                    functionName="format_regularHours")
    weekDay_dict = {
      1: 'MONDAY',
      2: 'TUESDAY',
      3: 'WEDNESDAY',
      4: 'THURSDAY',
      5: 'FRIDAY',
      6: 'SATURDAY',
      7: 'SUNDAY'
    }

    days_mapping = {
      "MONDAY": "SUNDAY",
      "TUESDAY": "MONDAY",
      "WEDNESDAY": "TUESDAY",
      "THURSDAY": "WEDNESDAY",
      "FRIDAY": "THURSDAY",
      "SATURDAY": "FRIDAY",
      "SUNDAY": "SATURDAY"
    }

    new_hour = []
    day_dict = {}
    previous_day_dict = {}
    prev_index = regularHours[-1]

    regular_hour_check_list = []
    for hour in regularHours:

        open_time_hours = hour["openTime"].get("hours", '00')
        open_time_minute = hour["openTime"].get("minutes", '00')
        close_time_hours = hour["closeTime"].get("hours", '00')
        close_time_minute = hour["closeTime"].get("minutes", '00')
        day = hour["openDay"]

        time_check = True
        if prev_index['closeTime'].get("hours") != 24:
            time_check = False

        if open_time_hours == '00' and open_time_minute == '00' and time_check:
            previous_day_dict.update({days_mapping[day]:{'closed_time_previous_day_hours':close_time_hours,'closed_time_previous_day_minutes':close_time_minute}})
            regular_hour_check_list.append(hour)
        prev_index = hour

    merchant_regular_hour = [x for x in regularHours if x not in regular_hour_check_list]
    for each_day in merchant_regular_hour:
      open_time_hours = each_day["openTime"].get("hours") if each_day["openTime"].get("hours") else '00'
      open_time_minute = each_day["openTime"].get("minutes") if each_day["openTime"].get("minutes") else '00'
      close_time_hours = '00' if each_day["closeTime"].get("hours", '00') == 24 else each_day["closeTime"].get("hours", '00')
      close_time_minute = each_day["closeTime"].get("minutes", '00')
      day = each_day["openDay"]

      if day in day_dict:
        if day in previous_day_dict:
            day_dict[day].append((f'{open_time_hours}:{open_time_minute}', f'{previous_day_dict[day]["closed_time_previous_day_hours"]}:{previous_day_dict[day]["closed_time_previous_day_minutes"]}'))
        else:
            day_dict[day].append((f'{open_time_hours}:{open_time_minute}', f'{close_time_hours}:{close_time_minute}'))
      elif open_time_hours:
        if day in previous_day_dict:
          day_dict[day] = [(f'{open_time_hours}:{open_time_minute}', f'{previous_day_dict[day]["closed_time_previous_day_hours"]}:{previous_day_dict[day]["closed_time_previous_day_minutes"]}')]
        else:
          day_dict[day] = [(f'{open_time_hours}:{open_time_minute}', f'{close_time_hours}:{close_time_minute}')]

    for day_number, day_name in weekDay_dict.items():
      day = day_name
      if day in day_dict:
        times = day_dict[day]
        time_ranges = []
        for time in times:
          time_ranges.append(f"{time[0]}-{time[1]}")
      else:
        time_ranges = ["close"]

      new_hour.append({"Day": day, "time": time_ranges})

    return new_hour

  @classmethod
  def getAllLinkedLocations(cls):
    connection, cursor = get_db_connection()
    cursor.execute("""SELECT * FROM googlelocations""", ())
    rows = cursor.fetchall()
    return rows

  @classmethod
  def LinkedLocationsTomerchant(cls, filtered_locations, merchantid):
    connection, cursor = get_db_connection()
    for location in filtered_locations:
      cursor.execute("""SELECT * FROM googlelocations where merchantid=%s and locationid=%s""",
                     (merchantid, location['locationId']))
      row = cursor.fetchone()
      if not row:
        data = (location['locationId'], location['locationId'], location['locationId'],
                location['merchantId'], location['accountId'], json.dumps(location))

        cursor.execute("""INSERT INTO googlelocations
                                                           (locationid, title, address, merchantid, accountid, meta)
                                                           VALUES (%s,%s,%s,%s,%s, %s)""", data)

        connection.commit()

  @classmethod
  def getLinkedMerchant(cls, locationid):
    connection, cursor = get_db_connection()
    cursor.execute("""SELECT * FROM googlelocations where locationid=%s""", (locationid))
    return cursor.fetchone()

  @classmethod
  def getLinkedUsingMerchant(cls, merchantid):
    connection, cursor = get_db_connection()
    cursor.execute("""SELECT * FROM googlelocations where merchantid=%s""", (merchantid))
    return cursor.fetchone()

  @classmethod
  def get_merchants_by_platform(cls, type):
    connection, cursor = get_db_connection()
    cursor.execute("""SELECT * FROM platforms WHERE platformtype =%s""", (type))
    return cursor.fetchall()


  @classmethod
  def update_square_token(cls, platformId, accesstoken, metadata):
    connection, cursor = get_db_connection()
    cursor.execute("""UPDATE platforms SET accesstoken=%s, metadata=%s WHERE id=%s""",
                   (accesstoken, metadata, platformId))
    connection.commit()
    return True

  @classmethod
  def insert_into_google_auth(cls, merchantid, accesstoken, refreshToken):
    connection, cursor = get_db_connection()
    cursor.execute("""INSERT INTO googleauth (accesstoken, refreshtoken, merchantid) VALUES (%s, %s, %s)""", (accesstoken, refreshToken, merchantid))

    connection.commit()
    return True

  @classmethod
  def check_google_auth_token(cls, merchantid):
    connection, cursor = get_db_connection()
    cursor.execute("""SELECT * FROM googleauth WHERE merchantid =%s""", (merchantid))
    return cursor.fetchall()