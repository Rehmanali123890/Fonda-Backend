import base64

from flask import jsonify, request, redirect

import config
import json
from models.Merchants import Merchants
from models.Users import Users
from models.UsersLogin import UsersLogin
from models.esper.Esper import Esper
from utilities.helpers import check_email_format, validateLoginToken, validateMerchantUser, success, create_log_data,publish_sns_message
from utilities.errors import invalid, not_found, unhandled, unauthorised
import requests
import string
import random

ses_sender_email = config.ses_sender_email

def getUserById(userId):
  try:
    token = request.args.get('token')
    if token and request.method == 'GET':
      if not validateLoginToken(token):
        return invalid("Invalid Token")
      
      user = Users.get_user_by_id(id=userId)
      if user:
        return success(jsonify(user))
      else:
        return not_found("User not found")
    else:
      return not_found(params=["token"])
  except Exception as e:
    print("Error: ", str(e))
    return unhandled("Unhandled Exception")
  
def getUserByEmail():
  try:
    _json = request.json
    email = _json.get("email")

    user = Users.get_user_by_email(email=email)
    if user:
      return success(jsonify(user))
    else:
      return not_found("User not found")

  except Exception as e:
    create_log_data(level='[error]', Message=str(e),
                        functionName="getUserByEmail", request=request, statusCode="500 Unhandled Exception"
                        )
    return unhandled("Unhandled Exception")

def getUserOnboardingById():
  try:
    _json = request.json
    userId = _json.get("userId")

    user = Users.get_user_onboarding_data(userId=userId)
    if user:
      return success(jsonify(user))
    else:
      return not_found("Merchant data not found")

  except Exception as e:
    create_log_data(level='[error]', Message=str(e),
                        functionName="getUserOnboardingById", request=request, statusCode="500 Unhandled Exception"
                        )
    return unhandled("Unhandled Exception")


def getUserByEsperId():
  try:
      _json = request.json
      esperId = _json.get("esperid")
      serialNumber = _json.get("serialNumber")
      user , status= Users.get_user_by_esper_id(id=esperId)
      if user:
        device = Esper.get_all_devices_in_an_enterprise(limit=1, state="" ,status=1, serialNumber=serialNumber,esperId=esperId)
        if device:
          if esperId==device[0]['device_name'] and serialNumber==device[0]['hardwareInfo']['serialNumber']:
            return Users.login_user(email=user['email'], password=user['password'])
          else:
            return not_found("The Esper ID or Serial Number provided do not match.")
        else:
          return not_found("The Esper ID and Serial Number provided do not exist to any Esper devices.")
        return success(jsonify(user))
      else:
        if status==1:
          return not_found("This device is not linked to any merchant. Please try manual login.")
        else:
          return not_found("No Standard User for this merchant. Please try manual login.")
  except Exception as e:
    print("Error: ", str(e))
    return unhandled("Unhandled Exception")
def getUsers():
  try:
    token = request.args.get('token')
    merchant = request.args.get('merchant')

    if (request.args.get('limit')):
      limit = int(request.args.get('limit'))
    else:
      limit = 25  # Default limit value

    if (request.args.get('from')):
      _from = int(request.args.get('from'))
    else:
      _from = 0  # Default offset value

    if token and request.method == 'GET':
      if not validateLoginToken(token):
        return invalid("Invalid Token")

      users = []

      userId = validateLoginToken(token)
      user_detail = Users.get_users(conditions=[f"id = '{userId}'"])
      if user_detail:
        if user_detail[0]['role'] == 3 or user_detail[0]['role'] == 4:
          merchants = Merchants.get_merchants(request, only_merchant=1, logintoken=token)
          if hasattr(merchants, '__len__'):
            if len(merchants) == 0:
              return success(jsonify(users))
      rows = Users.get_users(limit=limit, offset=_from, merchant=merchant)
      for row in rows:
        users.append({
          'id': row.get("id"),
          'firstName': row.get("firstname"),
          'lastName': row.get("lastname"),
          'username': row.get("username"),
          'email': row.get("email"),
          'address': row.get("address"),
          'phone': row.get("phone"),
          'role': row.get("role"),
          'userStatus': row.get("status"),
          'password': row.get("password"),
          'withSSO': row.get("withSSO",0),
          'SSOId':row.get("withSSOSSOId",''),

          'is_jwt_token': row.get("is_jwt_token")
        })
      return success(jsonify(users))

    else:
      return not_found(params=["token"])
  except Exception as e:
    print("Error: ", str(e))
    return unhandled("Unhandled Exception")


################################################## POST

def createUser():
  try:
    _json = request.json
    token = _json.get("token")
    userData = _json.get("user")
    
    if token and userData and request.method == 'POST':
      userId = validateLoginToken(token)
      if not userId:
        return invalid("Invalid Token")
      
      return Users.post_user(data=userData, createUserId=userId)

    else:
      fields = {
        "token": "required",
        "user": ["required"]
      }
      return not_found(body=fields)
  except Exception as e:
    print("Error: ", str(e))
    return unhandled("Unhandled Exception")
  
def postUserOnboard():
  try:
    userData = request.json

    chars=string.ascii_uppercase + string.digits
    password = ''.join(random.choice(chars) for _ in range(6))

    if userData.get('assistantName'):
      assitantName = userData.get('assistantName').split(" ", 1)
      userData['firstName'] = userData.get('assistantName')
      if type(assitantName) == list:
          userData['firstName'] = assitantName[0]
      if len(assitantName) > 1:
          userData['lastName'] = assitantName[1]

    userData['userStatus'] = 1
    userData['password'] = password
    userData['role'] = 3
    return Users.post_user_email(data=userData)

  except Exception as e:
    print("Error: ", str(e))
    return unhandled("Unhandled Exception")

def sendUserActivationEmail():
  try:
    userData = request.json
    userEmail = userData.get('email')
    return Users.send_user_activate_email(email=userEmail)
  except Exception as e:
    print("Error: ", str(e))
    return unhandled("Unhandled Exception",500, str(e))
    

def loginUser():
  try:
    ip_address = None
    if request:
      ip_address = request.environ.get('HTTP_X_FORWARDED_FOR', request.remote_addr)
    if ip_address:
      ip_address = ip_address.split(',')[0].strip()
    create_log_data(level='[INFO]', Message=f"In the beginning of loginuser() function, IP Address: {ip_address}",
                    functionName="loginUser",  statusCode="200 OK"
                    )
    _json = request.json
    email = _json.get("email")
    password = _json.get("password")
    withSSO= _json.get("withSSO" , 0)
    SSOToken = _json.get("SSOToken", '')
    SSOId=_json.get("SSOId", None)


    is_stream = request.args.get('is_stream', None)
    create_log_data(level='[INFO]',
                    Message=f"In the beginning of loginuser() function, Email: {email}, IP Address: {ip_address}",
                    functionName="loginUser", statusCode="200 OK."
                    )
    if email and password and request.method == 'POST':
      user_detail = Users.login_user(email=email, password=password,withSSO=withSSO ,SSOToken=SSOToken , SSOId=SSOId, is_stream=is_stream)

      if is_stream != 'true':
        user_log_detail = {}
        if hasattr(user_detail, 'status_code') and user_detail.status_code is not None:
          if user_detail.status_code == 400:
            print("Invalid")
        else:
          user_log_detail = {k: v for k, v in user_detail.items()}
          user_log_detail['user'] = {k: v for k, v in user_detail['user'].items() if k != 'password'}


      status_code = getattr(user_detail, 'status_code', None)
      message = getattr(user_detail, 'data', None)
      response_dict_message = None
      if message is not None:
        decoded_response_message = message.decode('utf-8')
        response_dict_message = json.loads(decoded_response_message)
      if status_code is not None:
        if status_code == 400:
          if response_dict_message.get('message') == 'User is not active':
            return invalid("User is not active, Please contact back office team")
          elif response_dict_message.get('message') == 'Username/Email or password is incorrect':
            return invalid("Username/Email or password is incorrect")
          elif response_dict_message.get('message') == 'SSO User':
            return invalid("You are registered as an SSO user. Please log in using SSO.")
          elif response_dict_message.get('message') == 'Simple User':
            return invalid("You are not registered as an SSO user. Please log in using your credentials.")
          elif response_dict_message.get('message') == 'User jwt token toggle is disabled':
            return invalid("User is disabled for stream")

      redirect_uri = request.args.get('redirect_uri', None)
      if is_stream == 'true' and redirect_uri != '':
        stream_auth_url = f"{redirect_uri}?authorization_code={user_detail.get('token')}"
        # return redirect(stream_auth_url)
        # return success(jsonify({
        #   'stream_auth_callback_url': stream_auth_url,
        # }))
        return success(jsonify(user_detail))
      request.merchantEmail = email
      merchants = Merchants.get_merchants(request, only_merchant=1, logintoken=user_detail['token'])
      merchantid = ''
      if hasattr(merchants, '__len__'):
        if len(merchants) > 0:
         merchantid = merchants[0]['id']
        elif len(merchants) == 0:
          return invalid("User account not linked to any restaurant")
      else:
       if 'merchantid' in user_detail:
         merchantid = user_detail['merchantid']
       else:
         merchantid = ''
      user_detail['merchantid'] = merchantid



      Users.login_user_history(user_detail, ip_address)

      create_log_data(level='[INFO]',
                      Message=f"Successfully get user, Email: {email}, IP Address: {ip_address}, Detail: {user_log_detail}",
                      functionName="loginUser", statusCode="200 OK"
                      )

      print("Triggering sns - login.success ...")
      sns_msg = {
        "event": "login.success",
        "body": {
          "userId": user_detail['user']['id'],
          "eventName": "login.success",
          "eventDetails": f"This user is logged in with the IP address: {ip_address}"
        }
      }
      logs_sns_resp = publish_sns_message(topic=config.sns_activity_logs, message=str(sns_msg),
                                          subject="login.success")


      return success(jsonify(user_detail))
    else:
      fields = {
        "email": "required",
        "password": "required"
      }
      create_log_data(level='[INFO]',
                      Message=f"Email or password in not valid function, Email: {email}, IP Address: {ip_address}",
                      functionName="loginUser",  statusCode="200 OK"
                      )
      return not_found(body=fields)
  except Exception as e:
    print("Error: ", str(e))
    create_log_data(level='[INFO]',
                    Message=f"An exception occurs when login user, Email: {email},  IP Address: {ip_address}, Exception: {e}",
                    functionName="loginUser",  statusCode="200 OK"
                    )
    return unhandled("Unhandled Exception")




def updatePasswordInactive():
  try:
    _json = request.json
    userId = _json.get("userId")
    password = _json.get("password")
    if userId and password:
      return Users.update_user_password(userId, password)
    else:
      return invalid("invalid request")
  except Exception as e:
    print("Error: ", str(e))
    return unhandled()

def logoutUser():
  try:
    _json = request.json
    token = _json.get("token")
    
    if token and request.method == 'POST':
      if not validateLoginToken(token):
        return invalid("Invalid Token")
      resp = UsersLogin.delete_userslogin(token=token)
      return success()
    else:
      return not_found(body={"token":"required"})
  except Exception as e:
    print("Error: ", str(e))
    return unhandled()
  

def sendForgotPasswordEmail():
  try:
    _json = request.json
    email = _json.get("email")
    if email and check_email_format(email):
      return Users.forget_user_password(email)
    else:
      return invalid("The email format is invalid. Please verify and try again")
  except Exception as e:
    print("Error: ", str(e))
    return unhandled()


def resetUserPassword():
  try:
    _json = request.json
    resetToken = _json.get("resetToken")
    password = _json.get("password")
    if resetToken and password:
      return Users.reset_user_password(resetToken, password)
    else:
      return invalid("invalid request")
  except Exception as e:
    print("Error: ", str(e))
    return unhandled()



########################################################### PUT

def changeUserPassword(userId):
  try:
    _json = request.json
    token = _json.get('token')
    password = _json.get('password')
    
    if token and password and request.method == 'PUT':
      currentUser = validateLoginToken(token)
      if not currentUser:
        return invalid("Invalid Token")

      return Users.change_user_password(userId=userId, password=password, currentUser=currentUser)
    else:
      fields = {
        "token": "required",
        "password": "required" 
      }
      return not_found(body=fields)
  except Exception as e:
    print("Error: ", str(e))
    return unhandled("Unhandled Exception")
  

def updateUser(userId):
  try:
    _json = request.json
    token = _json.get('token')
    user = _json.get('user')
    
    if token and user and request.method == 'PUT':
      currentUser = validateLoginToken(token)
      if not currentUser:
        return invalid("Invalid Token")
      
      return Users.put_user(userId=userId, user=user, currentUser=currentUser)

    else:
      fields = {
        "token": "required",
        "user": {"field" : "value"}
      }
      return not_found(body=fields)
  except Exception as e:
    print("Error: ", str(e))
    return unhandled("Unhandled Exception")



########################################################### DELETE

def deleteUser(userId):
  try:
    token = request.args.get('token')
    
    if token and request.method == 'DELETE':
      if not validateLoginToken(token):
        return invalid("Invalid Token")
      return Users.delete_user(userId)
    else:
      return not_found(params=["token"])
  except Exception as e:
    print("Error: ", str(e))
    return unhandled("Unhandled Exception")


