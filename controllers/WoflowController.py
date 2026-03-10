from flask import request, g


# local imports
from utilities.errors import *
from utilities.helpers import success, validateAdminUser
from controllers.Middleware import validate_token_middleware
from models.Woflow import Woflow





@validate_token_middleware
def woflowUploadMenuToS3():
  try:
    _json = request.json
    userId = g.userId

    if not validateAdminUser(userId):
      return unauthorised("user is not authorized!")

    merchantId = _json.get("merchantId")
    generatePresigendUrls = _json.get("generatePresigendUrls")
    files = _json.get("files")

    if not files:
      return invalid("no files found!")


    if generatePresigendUrls:
      ### if request is for generation of presigned urls

      data = Woflow.generate_presigned_s3_urls(merchantId, files)

      return success(jsonify({
        "message": "success",
        "status": 200,
        "data": data
      }))
    

    else:
      ### else store entry in woflow table
      
      entry_guid = Woflow.post_woflow(
        merchantid=merchantId,
        urls=','.join(files),
        status=0,
        created_by=userId,
        jobstate="uploaded"
      )

      return success(jsonify({
        "message": "success",
        "status": 200,
        "data": {
          "id": entry_guid 
        }
      }))
    
  except Exception as e:
    print("Error: ", str(e))
    return unhandled()



@validate_token_middleware
def woflowGetRecordsFromDb():
  try:
    userId = g.userId

    if not validateAdminUser(userId):
      return invalid("user is not authorized!")
    
    merchantId = request.args.get("merchantId")
    woflowColumnId = request.args.get("woflowColumnId")

    data = Woflow.get_woflow(merchantId=merchantId, woflowColumnId=woflowColumnId)
    
    return success(jsonify({
      "message": "success",
      "status": 200,
      "data": data
    }))
  except Exception as e:
    print("Error: ", str(e))
    return unhandled()


@validate_token_middleware
def woflowInitializeMenuProcessing():
  try:
    _json = request.json
    userId = g.userId

    if not validateAdminUser(userId):
      return invalid("user is not authorized!")
    
    merchantId = _json.get("merchantId")
    woflowColumnId = _json.get("woflowColumnId")
    processingType = _json.get("processingType") # standard OR rush
    instructions = _json.get("instructions")

    if processingType not in ("standard", "rush"):
      return invalid("processing type is invalid!")

    return Woflow.initialize_menu_processing(merchantId, woflowColumnId, processingType, instructions, userId=userId)

  except Exception as e:
    print("Error: ", str(e))
    return unhandled()



@validate_token_middleware
def woflowRefreshJob():
  try:
    userId = g.userId

    if not validateAdminUser(userId):
      return invalid("user is not authorized!")
    
    woflowColumnId = request.args.get("woflowColumnId")

    return Woflow.refresh_job_on_woflow(woflowColumnId)
    
  except Exception as e:
    print("Error: ", str(e))
    return unhandled()


@validate_token_middleware
def woflowAcceptRejectJob():
  try:
    userId = g.userId
    _json = request.json

    if not validateAdminUser(userId):
      return invalid("user is not authorized!")

    woflowColumnId = _json.get("woflowColumnId")
    operation = _json.get("operation")
    reason = _json.get("reason")

    if operation not in ("accept", "reject"):
      return invalid("invalid job operation!")
    
    return Woflow.accept_reject_job(woflowColumnId, operation, reason, userId)

  except Exception as e:
    print("Error: ", str(e))
    return unhandled()


@validate_token_middleware
def downloadMenu(menu_id):
  try:

    return Woflow.menu_pdf(menu_id)

  except Exception as e:
    print("Error: ", str(e))
    return unhandled()
