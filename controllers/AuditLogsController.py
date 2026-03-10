from flask import jsonify, g, request

# local imports
from controllers.Middleware import validate_token_middleware
from utilities.errors import unauthorised, unhandled
from utilities.helpers import success, validateAdminUser
from models.AuditLogs import AuditLogs


################################################# GET

@validate_token_middleware
def getAuditLogs():
  try:
    userId = g.userId
    if not validateAdminUser(userId):
      return unauthorised("user is not authorized")

    logs = AuditLogs.get_audit_logs(request)

    return success(jsonify({
      "status": 200,
      "message": "success",
      "data": logs
    }))
  except Exception as e:
    print("Error: ", str(e))
    return unhandled()

#################################################

