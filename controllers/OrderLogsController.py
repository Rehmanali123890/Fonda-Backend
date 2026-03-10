from flask import jsonify, g, request

# local imports
from controllers.Middleware import validate_token_middleware
from models.OrderLogs import OrderLogs
from utilities.errors import unauthorised, unhandled
from utilities.helpers import success, validateAdminUser


################################################# GET

@validate_token_middleware
def getOrderLogs():
  try:
    userId = g.userId
    if not validateAdminUser(userId):
      return unauthorised("user is not authorized")

    logs = OrderLogs.get_order_logs(request)

    return success(jsonify({
      "status": 200,
      "message": "success",
      "data": logs
    }))
  except Exception as e:
    print("Error: ", str(e))
    return unhandled()

#################################################

