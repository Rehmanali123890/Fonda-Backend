from flask import jsonify, request, g


# local imports
import config
from models.Categories import Categories
from models.CategoryServiceAvailability import *
from models.ProductsCategories import ProductsCategories
from utilities.helpers import validateLoginToken, validateMerchantUser, success, publish_sns_message, create_log_data
from utilities.errors import invalid, not_found, unhandled, unauthorised
from controllers.Middleware import validate_token_middleware

# config
sns_category_notification = config.sns_category_notification


################################################# GET

def getMerchantCategoryByID(merchantId, categoryId):
  try:
    token = request.args.get('token')

    userId = validateLoginToken(token)
    if not userId:
      return invalid("Invalid Token") 
  
    if not validateMerchantUser(merchantId, userId):
      return unauthorised("user Not authorised to access merchant information")
    
    resp = Categories.get_category_by_id_str(categoryId)
    return success(jsonify(resp))

  except Exception as e:
    print("Error: ", str(e)) 
    return unhandled()


def getMerchantCategoryMenuByID(merchantId, categoryId):
  try:
    token = request.args.get('token')

    userId = validateLoginToken(token)
    if not userId:
      return invalid("Invalid Token")

    if not validateMerchantUser(merchantId, userId):
      return unauthorised("user Not authorised to access merchant information")

    resp = Categories.get_category_menu_by_id_str(merchantId,categoryId)
    return success(jsonify(resp))

  except Exception as e:
    print("Error: ", str(e))
    return unhandled()


################################################# POST

def createMerchantProductCategory(merchantId):
  try:
    
    ip_address = None
    if request:
        ip_address = request.environ.get('HTTP_X_FORWARDED_FOR', request.remote_addr)
    if ip_address:
        ip_address = ip_address.split(',')[0].strip()
        
    _json = request.json
    token = _json.get('token') 
    category = _json.get('category') 

    create_log_data(level='[INFO]', Message=f"In the start of createMerchantProductCategory,IP address: {ip_address}, Token:{token}",
                    functionName="createMerchantProductCategory", request=request)
    
    if token and category and request.method == 'POST': 
      userId = validateLoginToken(token)
      if not userId:
        return invalid("Invalid Token") 
      
      if not validateMerchantUser(merchantId, userId):
        return unauthorised("user Not authorised to access merchant information") 

      resp = Categories.post_category(merchantId=merchantId, category=category, userId=userId)
      if not resp:
        return unhandled("error occured during category creation")
      
      create_log_data(level='[INFO]', Message=f"Successfully createMerchantProductCategory,IP address: {ip_address}, Token:{token}",
                    functionName="createMerchantProductCategory", request=request)
      
      # SNS
      subject = "category.create"
      sns_msg = {
        "event": subject,
        "body": {
          "merchantId": merchantId,
          "categoryId": resp["id"],
          "userId": userId,
          "ipAddr": ip_address
        }
      }
      sns_resp = publish_sns_message(topic=sns_category_notification, message=str(sns_msg), subject=subject)
      publish_sns_message(topic=config.sns_activity_logs, message=str(sns_msg),
                                  subject="category.create")
      return success(jsonify(resp))                                
    else:
      return not_found(body={'token': 'required', 'category': {}})
  except Exception as e:
    create_log_data(level='[INFO]', Message=f"An error occured , {e}, createMerchantProductCategory,IP address: {ip_address}, Token:{token}",
                    functionName="createMerchantProductCategory", request=request)
    return unhandled(str(e))

################################################# PUT

def updateMerchantCategory(merchantId, categoryId):
  try:
    ip_address = None
    if request:
        ip_address = request.environ.get('HTTP_X_FORWARDED_FOR', request.remote_addr)
    if ip_address:
        ip_address = ip_address.split(',')[0].strip()
    
    _json = request.json  
    token = _json.get('token')
    
    create_log_data(level='[INFO]', Message=f"In the start of updateMerchantCategory,id: {categoryId},IP address: {ip_address}, Token:{token}",
                    functionName="updateMerchantCategory", request=request)
    
    category = _json.get("category")

    if token and category and request.method == 'PUT': 
      userId = validateLoginToken(token)
      if not userId:
        return invalid("Invalid Token")

      if (not validateMerchantUser(merchantId, userId)):
        return unauthorised("User Not authorised to access merchant information")

      old_category_details = Categories.get_category_by_id(categoryId)
      
      resp = Categories.update_category_by_id(id=categoryId, category=category, userId=userId)
      if not resp:
        return unhandled()
      
      # SNS
      create_log_data(level='[INFO]', Message=f"Successfully updateMerchantCategory,IP address: {ip_address}, Token:{token}",
                    functionName="updateMerchantCategory", request=request)
      
      print("Triggering category sns...")
      sns_msg = {
        "event": "category.update",
        "body": {
          "merchantId": merchantId,
          "categoryId": categoryId,
          "ipAddr": ip_address,
          "userId": userId,
          "old_category_details": {
            "categoryname": old_category_details["categoryname"],
            "posname": old_category_details["posname"],
            "categorydescription": old_category_details["categorydescription"],
            "status": old_category_details["status"]
          }
        }
      }
      sns_resp = publish_sns_message(topic=sns_category_notification, message=str(sns_msg), subject="category.update")
      publish_sns_message(topic=config.sns_activity_logs, message=str(sns_msg),
                          subject="category.update")
      resp = Categories.get_category_by_id_str(id=categoryId)
      return success(jsonify(resp)) if resp else unhandled()
    else:
      return not_found(body={"token": "required", "category": {}})
  except Exception as e:
    create_log_data(level='[INFO]', Message=f"An error occured while updateMerchantCategory Error: {e},id: {categoryId},IP address: {ip_address}, Token:{token}",
                    functionName="updateMerchantCategory", request=request)
    return unhandled(f"Error {str(e)}")

################################################# DELETE  

def deleteMerchantCategory(merchantId, categoryId):
  try:
    token = request.args.get('token')
    ip_address = None
    if request:
        ip_address = request.environ.get('HTTP_X_FORWARDED_FOR', request.remote_addr)
    if ip_address:
        ip_address = ip_address.split(',')[0].strip()
        
    create_log_data(level='[INFO]', Message=f"In the start of deleting menu category, , IP address:{ip_address}, Token:{token}",
                    functionName="deleteMerchantCategory", request=request)
    

    if token and request.method == 'DELETE': 
      userId = validateLoginToken(token)
      if not userId:
        create_log_data(level='[INFO]',
                        Message="The API token is invalid.",
                        messagebody=f"Unable to find the user on the basis of provided token., IP address:{ip_address}, Token:{token}",
                        functionName="deleteMerchantCategory", request=request, statusCode="400 Bad Request")
        return invalid("Invalid Token")

      if (not validateMerchantUser(merchantId, userId)):
        create_log_data(level='[INFO]', Message=f"User Not authorised to access merchant information, IP address:{ip_address}, Token:{token}",
                        functionName="deleteMerchantCategory", request=request, statusCode="400 Bad Request"
                        )
        return unauthorised("User Not authorised to access merchant information")
      
      category_details = Categories.get_category_by_id(categoryId)
      create_log_data(level='[INFO]', Message=f"Successfully retrieved category information, IP address:{ip_address}, Token:{token}",
                      functionName="deleteMerchantCategory", request=request, statusCode="400 Bad Request"
                      )
      products_ids_list = list()
      products_categories = ProductsCategories.get_productscategories(categoryId)
      for pc in products_categories:
        products_ids_list.append(pc['productid'])
      
      resp = Categories.delete_category_by_id(id=categoryId)
      if isinstance(resp,Exception):
        create_log_data(level='[ERROR]', Message=f"Failed to delete category information, IP address:{ip_address}, Token:{token}",
                        messagebody=f"An error occured while deleting category: {resp}",
                        functionName="deleteMerchantCategory", request=request, statusCode="200 OK"
                        )
        return unhandled()
      
      # SNS
      print("Triggering category sns...")
      sns_msg = {
        "event": "category.delete",
        "body": {
          "merchantId": merchantId,
          "categoryId": categoryId,
          "userId": userId,
          "ipAddr": ip_address,
          "products_ids_list": products_ids_list,
          "category_details": {
            "categoryname": category_details["categoryname"],
            "posname": category_details["posname"],
            "categorydescription": category_details["categorydescription"],
            "status": category_details["status"]
          }
        }
      }
      sns_resp = publish_sns_message(topic=sns_category_notification, message=str(sns_msg), subject="category.delete")
      publish_sns_message(topic=config.sns_activity_logs, message=str(sns_msg),
                          subject="category.delete")
      create_log_data(level='[INFO]', Message=f"Successfully deleted category, IP address:{ip_address}",
                      functionName="deleteMerchantCategory", request=request, statusCode="200 OK"
                      )
      return success()
    else:
      create_log_data(level='[ERROR]', Message=f'Token not found in the request, IP address:{ip_address}',
                      functionName="deleteMerchantCategory", request=request, statusCode="400 Bad Request"
                      )
      return not_found(params=["token"])
  except Exception as e:
    create_log_data(level='[ERROR]', Message=f'Failed to delete category',
                    messagebody=f"An error occured while deleting category: {str(e)}, IP address:{ip_address}",
                    functionName="deleteMerchantCategory", request=request, statusCode="400 Bad Request"
                    )
    return unhandled(f"Error: {str(e)}")

@validate_token_middleware  
def createCategoryServiceAvailability(merchantId, categoryId):
  try:
    
    ip_address = None
    if request:
        ip_address = request.environ.get('HTTP_X_FORWARDED_FOR', request.remote_addr)
    if ip_address:
        ip_address = ip_address.split(',')[0].strip()
    _json = request.json
    availability = _json.get("serviceAvailability")
    category_details = Categories.get_category_by_id(categoryId)

    if request.method == "POST":
      userId = g.userId
      getservice =  CategoryServiceAvailability.get_serviceAvailabilityBycategoryId(categoryId=categoryId)
      print(availability)
      if len(availability) != 0:
        if getservice:
          array1_ids = {obj.get('id') for obj in getservice}
          array2_ids = {obj.get('id') for obj in availability}
          missing_ids = array1_ids - array2_ids
          if missing_ids:
            for id in missing_ids:
              CategoryServiceAvailability.delete_serviceAvailabilityById(id)
          for row in availability:            
            if row.get('id'):
              update_category_scheduler=CategoryServiceAvailability.put_serviceAvailabilityById(id=row.get('id'), endTime=row.get('endTime'),startTime=row.get("startTime"),weekDay=row.get("weekDay"),timezone=row.get("timezone"),categoryId=categoryId,groupDays=row.get('groupDays'),merchantId=merchantId)
              if not update_category_scheduler:
                return invalid('An error occurred while updating the scheduler.')
            else:
              create_category_scheduler=CategoryServiceAvailability.post_serviceAvailability(categoryId, [row],merchantId)
              if not create_category_scheduler:
                return invalid('An error occurred while creating the scheduler.')
        else:
          create_category_scheduler=CategoryServiceAvailability.post_serviceAvailability(categoryId, availability,merchantId)
          if not create_category_scheduler:
            return invalid('An error occurred while creating the scheduler.')
      else:
        if getservice:
          for row in getservice:
              if row.get('id'):
                CategoryServiceAvailability.delete_serviceAvailabilityById(row.get('id'))

       
      getservice_updated = CategoryServiceAvailability.get_serviceAvailabilityBycategoryId(categoryId=categoryId)
      messagebody = addedUpdatedDeletedFields(getservice, getservice_updated)
      if messagebody:
        messagebody = 'For ' + category_details.get('categoryname') +', ' + messagebody
        messagebody += f", IP address:{ip_address}"
        print("Triggering item sns - category.hours_change ...")
        sns_msg = {
          "event": "category.hours_change",
          "body": {
            "merchantId": merchantId,
            "categoryId": categoryId,
            "userId": userId,
            "event_details": messagebody
          }
        }
        sns_resp = publish_sns_message(topic=config.sns_activity_logs, message=str(sns_msg), subject="category.hours_change")

                  
        create_log_data(level='[INFO]', Message=f"Item service availability add/updated successfully,IP address: {ip_address}",
                      functionName="createCategoryServiceAvailability", request=request)

      return success(jsonify({
        "message": "success",
        "status": 200
      }))

    else:
      fields = {
        "serviceAvailability": [
          {
            "startTime": "required",
            "endTime": "required",
            "weekDay": "required"
          }
        ]
      }

      create_log_data(level='[ERROR]', Message=f"Item service availability fields are missing,IP address: {ip_address}",
                    functionName="createCategoryServiceAvailability", request=request)
      return not_found(body=fields)
  except Exception as e:
    print("Error: ", str(e))
    create_log_data(level='[ERROR]', Message=f"ERROR: {str(e)},IP address: {ip_address}",
                    functionName="createCategoryServiceAvailability", request=request)
    return unhandled()


def getCategoryServiceAvailability(merchantId, categoryId):
  try:
    if request.method == "GET":
      
      getResp = CategoryServiceAvailability.get_serviceAvailabilityBycategoryId(categoryId=categoryId)

      return success(jsonify({
        "message": "success",
        "status": 200,
        "data": getResp
      }))
    else:
      return not_found("invalid request method")
  except Exception as e:
    print("Error: ", str(e))
    return unhandled()
  
def addedUpdatedDeletedFields(beforeUpdated, afterupdated):
  day_mapping = {
    1: "Monday",
    2: "Tuesday",
    3: "Wednesday",
    4: "Thursday",
    5: "Friday",
    6: "Saturday",
    7: "Sunday"
  }
  days_before_updating = [entry['weekDay'] for entry in beforeUpdated]

  updated_fields = []
  for beforeUpdatedData in beforeUpdated:
    for afterUpdatedData in afterupdated:
      if beforeUpdatedData["weekDay"] == afterUpdatedData["weekDay"]:
        different_values = []
        for key in beforeUpdatedData:
          if key=='id' or key == 'deactivateSchedulerID' or key == 'activateSchedulerID':
            continue
          if beforeUpdatedData[key] != afterUpdatedData[key]:
            keyName = key
            if key == 'starttime' or key == 'startTime':
              keyName = 'Start Time'
            if key == 'endtime' or key == 'endTime':
              keyName = 'End Time'
            different_values.append(f"{keyName} <{beforeUpdatedData[key]}> to <{afterUpdatedData[key]}>")
        if different_values:
          updated_fields.append(f"{day_mapping[beforeUpdatedData['weekDay']]}: {','.join(different_values)}")
        break
    else:
      updated_fields.append(f"{day_mapping[beforeUpdatedData['weekDay']]} category hours deleted ")
  for afterUpdatedData in afterupdated:
      if afterUpdatedData["weekDay"] not in days_before_updating:
        updated_fields.append(f"{day_mapping[afterUpdatedData['weekDay']]} added: Start Time {afterUpdatedData['startTime']}, End Time {afterUpdatedData['endTime']}")
  print(updated_fields)
  return ','.join(updated_fields)