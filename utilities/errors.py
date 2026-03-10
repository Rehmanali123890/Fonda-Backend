from flask import Blueprint, jsonify


# init blueprint
errors = Blueprint("errors", __name__)


@errors.errorhandler(403)
def unauthorised(errorMsg=None):
  message = { 
    'error': errorMsg,
    'status': 403,
    'data': {}
  }
  resp = jsonify(message)
  resp.status_code = 403
  resp.headers['Content-Type'] = 'application/json'
  resp.headers['Access-Control-Allow-Origin'] = '*'
  resp.headers['Access-Control-Allow-Credentials'] = 'true' 
  return resp


@errors.errorhandler(404)
def not_found(errorMsg="", body=None, params=None, form=None):
  data_obj = {}
  msg = errorMsg
  
  if body:
    msg = "Missing some required fields"
    data_obj["json_body"] = body
  if form:
    msg = "Missing some required fields"
    data_obj["form_body"] = form
  if params:
    msg = "Missing some required fields"
    data_obj["query_params"] = params


  message = { 
    'status': 404,
    'message': msg,
    'data': data_obj
  }

  resp = jsonify(message)
  resp.status_code = 404
  resp.headers['Content-Type'] = 'application/json'
  resp.headers['Access-Control-Allow-Origin'] = '*'
  resp.headers['Access-Control-Allow-Credentials'] = 'true' 
  return resp


@errors.errorhandler(400)
def invalid(errorMsg, data={}):
  # print(errorMsg)
  message = { 
    'status': 400,
    'message': errorMsg,
    'data': data
  }
  resp = jsonify(message)
  resp.status_code = 400
  resp.headers['Content-Type'] = 'application/json'
  resp.headers['Access-Control-Allow-Origin'] = '*'
  resp.headers['Access-Control-Allow-Credentials'] = 'true' 
  return resp


@errors.errorhandler(500)
def unhandled(errorMsg=None, statusCode=500, data={}):
  if not errorMsg:
    errorMsg = "Unhandled exception"
  print(errorMsg)
  message = { 
    'status': statusCode,
    'message': errorMsg,
    'data': data
  }
  resp = jsonify(message)
  resp.status_code = statusCode
  resp.headers['Content-Type'] = 'application/json'
  resp.headers['Access-Control-Allow-Origin'] = '*'
  resp.headers['Access-Control-Allow-Credentials'] = 'true' 
  return resp


@errors.errorhandler(401)
def invalid_stream_response(errmsg=None , status=401):
    if errmsg is None:
      errmsg='invalid'
    message = {
      'status': status,
      'message': errmsg,
    }
    resp = jsonify(message)
    resp.status_code = status
    resp.headers['Content-Type'] = 'application/json'
    resp.headers['Access-Control-Allow-Origin'] = '*'
    resp.headers['Access-Control-Allow-Credentials'] = 'true'
    return resp