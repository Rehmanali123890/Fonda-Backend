from flask import jsonify, request


# local imports
from models.Users import Users
from models.Websockets import Websockets
from utilities.helpers import validateLoginToken, success
from utilities.errors import invalid, not_found, unhandled
 


def createWebsocket():
  try:
    _json = request.json
    token = _json.get("token")
    connectionId = _json.get("connectionId")
    eventName = _json.get("eventName")
    if token and connectionId and eventName and request.method == "POST":
      userId = validateLoginToken(token)
      if userId:
        print("userId: ", userId)
        user = Users.get_user_by_id(userId)
        if user:
          role = user.get("role")
          resp = Websockets.create_websocket(connectionId, eventName, userId, role)
          if resp:
            return success(jsonify({
              "message": "success",
              "status": 200,
              "data": []
            }))
          else:
            return unhandled("Unhandled exception while inserting record")
      else:
        return invalid("Invalid Token")
    else:
      return not_found("Invalid request body")
  except Exception as e:
    print("Error: ", str(e))
    return unhandled(str(e))


def getWebsockets():
  try:
    data_list = Websockets.get_websockets()
    return success(jsonify({
      "message": "success",
      "status": 200,
      "data": data_list
    }))
  except Exception as e:
    print(str(e))
    return unhandled(str(e))


def getWebsocketById(id):
  try:
    resp = Websockets.get_websocketById(id=id)
    if resp:
      return success(jsonify({
        "message": "success",
        "status": 200,
        "data": resp
      }))
    else:
      return invalid("Invalid Connection Id")
  except Exception as e:
    print("Error: ", str(e))
    return unhandled(str(e))


def deleteWebsocketById(id):
  try:
    resp = Websockets.delete_websocketById(id=id)
    return success(jsonify({
      "message": "success",
      "status": 200
    }))
  except Exception as e:
    print("Error")
    print(str(e))
    return unhandled(str(e))
  

def getWebsocketConnectionsByMerchantId():
  try:
    merchantId = request.args.get("merchantId")
    eventName = request.args.get("eventName")
    resp_list = Websockets.get_connection_by_mid_and_eventname(merchantId, eventName=eventName)
    return success(jsonify({
      "message": "success",
      "status": 200,
      "data": resp_list
    }))
  except Exception as e:
    print("Error")
    print(str(e))
    return unhandled(str(e))


"""
postWebsocket
{
  "token": "user token",
  "connectionId": "connection id",
  "event": "order.create"
}
"""

"""
getWebsocket
request params: token
"""