from flask import jsonify, g, request

# local imports
from controllers.Middleware import validate_token_middleware
from models.DataPatch import DataPatch
from utilities.errors import  unhandled
from utilities.helpers import success


################################################# GET

@validate_token_middleware
def pending_locked_orders_script():
  try:


    response = DataPatch.fix_pending_locked_orders()

    return success(jsonify({
      "status": 200,
      "message": "success",
    }))
  except Exception as e:
    print("Error: ", str(e))
    return unhandled()

#################################################

