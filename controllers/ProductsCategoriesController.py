from flask import jsonify, request, g


# local imports
import config
from controllers.Middleware import validate_token_middleware
from models.ProductsCategories import ProductsCategories
from utilities.helpers import validateLoginToken, validateMerchantUser, success, publish_sns_message, create_log_data, get_ip_address
from utilities.errors import invalid, not_found, unhandled, unauthorised

# config
sns_category_notification = config.sns_category_notification


################################################# GET

def getMerchantCategoriesWithItems(merchantId):
  try: 
    token = request.args.get('token')
    limit = request.args.get('limit') if request.args.get('limit') else '25'
    offset = request.args.get('from') if request.args.get('from') else '0'      
    categoryName = request.args.get('categoryName') if request.args.get('categoryName') else None
    menuid = request.args.get('menuid') if request.args.get('menuid') else None
    categoryid = request.args.get('categoryid') if request.args.get('categoryid') else None

    ip_address = get_ip_address(request)

    create_log_data(level='[INFO]', Message=f"In the start of getMerchantCategoriesWithItems,IP address: {ip_address}, Token:{token}",
                    functionName="getMerchantCategoriesWithItems", request=request)
      
    userId = validateLoginToken(token)
    if not userId:
      return invalid("Invalid Token")

    if not validateMerchantUser(merchantId, userId):
      return unauthorised("User Not authorised to access Merchant Information")

    
    resp = ProductsCategories.get_categories_with_items(merchantId, limit=limit, offset=offset,
                                                        categoryName=categoryName,menuid = menuid,categoryid = categoryid)
    
    
    if resp is False:
      return unhandled()
    
    create_log_data(level='[INFO]', Message=f"Successfully getMerchantCategoriesWithItems,IP address: {ip_address}, Token:{token}",
                    functionName="getMerchantCategoriesWithItems", request=request)
    return success(jsonify(resp))
        
  except Exception as e:
    print(e) 
    create_log_data(level='[INFO]', Message=f"Error at {e}, getMerchantCategoriesWithItems,IP address: {ip_address}, Token:{token}",
                    functionName="getMerchantCategoriesWithItems", request=request)
    return unhandled("Unhandled Exception")


################################################# POST

def updateItemsToCategory(merchantId, categoryId):
  try:
    _json = request.json  
    token = _json.get('token') 
    items = _json.get("items")

    ip_address = get_ip_address(request)
    
    create_log_data(level='[INFO]', Message=f"In the start of updateItemsToCategory,IP address: {ip_address}, Token:{token}",
                    functionName="updateItemsToCategory", request=request)
    
    if token and items and request.method == 'POST': 
      userId = validateLoginToken(token)
      if not userId:
        return invalid("Invalid Token")

      if (not validateMerchantUser(merchantId, userId)):
        return unauthorised("User Not authorised to access merchant information")
      
      resp = ProductsCategories.update_category_items(categoryId=categoryId, items=items, userId=userId)
      if resp:
        create_log_data(level='[INFO]', Message=f"Successfuly updateItemsToCategory,IP address: {ip_address}, Token:{token}",
                    functionName="updateItemsToCategory", request=request)
        return success()
      else:
        create_log_data(level='[INFO]', Message=f"Error while updateItemsToCategory,IP address: {ip_address}, Token:{token}",
                    functionName="updateItemsToCategory", request=request)
        return unhandled()
    else:
      fields = {
        "token": "required",
        "items": [ {"itemID": "required"} ]
      }
      return not_found(body=fields)
  except Exception as e:
    create_log_data(level='[INFO]', Message=f"Error while updateItemsToCategory,{e},IP address: {ip_address}, Token:{token}",
                    functionName="updateItemsToCategory", request=request)
    print("Error: ", str(e))
    return unhandled()


@validate_token_middleware
def createOrDeleteCategoryProduct(merchantId, categoryId):
  try:
    
   
    _json = request.json  
    itemId = _json.get("itemId")
    isDelete = _json.get("delete")
    ip_address = None
    if request:
        ip_address = request.environ.get('HTTP_X_FORWARDED_FOR', request.remote_addr)
    if ip_address:
        ip_address = ip_address.split(',')[0].strip()
  
    token = g.token
    
    create_log_data(level='[INFO]', Message=f"In the start of createOrDeleteCategoryProduct,IP address: {ip_address}, Token:{token}",
                    functionName="createOrDeleteCategoryProduct", request=request)
    
    if itemId is not None and isDelete is not None and request.method == 'POST': 
      userId = g.userId
      if (not validateMerchantUser(merchantId, userId)):
        return unauthorised("User Not authorised to access merchant information")
      
      if isDelete == 0:
        # add
        resp = ProductsCategories.post_category_item(categoryId=categoryId, itemId=itemId, userId=userId, merchantId=merchantId , ip_address=ip_address)
        if resp:
          
          create_log_data(level='[INFO]', Message=f"Successfully createOrDeleteCategoryProduct,IP address: {ip_address}, Token:{token}",
                    functionName="createOrDeleteCategoryProduct", request=request)

          return success()
      elif isDelete == 1:
        # delete
        resp = ProductsCategories.delete_category_item(categoryId=categoryId, itemId=itemId)
        if resp:

          print("Triggering category sns...")
          sns_msg = {
            "event": "category.unassign_item",
            "body": {
              "merchantId": merchantId,
              "categoryId": categoryId,
              "itemId": itemId,
              "userId": userId,
              "ipAddr": ip_address
            }
          }
          sns_resp = publish_sns_message(topic=sns_category_notification, message=str(sns_msg), subject="category.unassign_item")
          publish_sns_message(topic=config.sns_activity_logs, message=str(sns_msg),
                          subject="category.unassign_item")
          return success()
      return unhandled()
    else:
      return not_found(body={"itemId":"required", "delete": "required"})
  except Exception as e:
    create_log_data(level='[INFO]', Message=f"Error occured while createOrDeleteCategoryProduct,{e},IP address: {ip_address}, Token:{token}",
                    functionName="createOrDeleteCategoryProduct", request=request)
    print("Error: ", str(e))
    return unhandled()


@validate_token_middleware
def createOrDeleteCategoryProductNewMenu(merchantId, itemId):
  try:
    _json = request.json
    categoryId = _json.get("categoryIds")
    isDelete = _json.get("delete")
    token = g.token
    
    ip_address = get_ip_address(request)
    
    create_log_data(level='[INFO]', Message=f"In the start of createOrDeleteCategoryProductNewMenu,IP address: {ip_address}, Token:{token}",
                    functionName="createOrDeleteCategoryProductNewMenu", request=request)
    
    if categoryId is not None and isDelete is not None and request.method == 'POST':
      userId = g.userId
      if (not validateMerchantUser(merchantId, userId)):
        return unauthorised("User Not authorised to access merchant information")

      if isDelete == 0:
        # add
        resp = ProductsCategories.post_category_item(categoryId=categoryId, itemId=itemId, userId=userId,merchantId=merchantId, ip_address=ip_address)

        return success()
      elif isDelete == 1:
        # delete
        resp = ProductsCategories.delete_category_item(categoryId=categoryId, itemId=itemId)
        if resp:
          print("Triggering category sns...")
          sns_msg = {
            "event": "category.unassign_item",
            "body": {
              "merchantId": merchantId,
              "categoryId": categoryId,
              "itemId": itemId,
              "userId": userId,
              "ipAddr": ip_address
            }
          }
          sns_resp = publish_sns_message(topic=sns_category_notification, message=str(sns_msg), subject="category.unassign_item")
          publish_sns_message(topic=config.sns_activity_logs, message=str(sns_msg),
                          subject="category.unassign_item")
          return success()
      return unhandled()
    else:
      return not_found(body={"itemId": "required", "delete": "required"})
  except Exception as e:
    create_log_data(level='[INFO]', Message=f"Error at createOrDeleteCategoryProductNewMenu,{e},IP address: {ip_address}, Token:{token}",
                    functionName="createOrDeleteCategoryProductNewMenu", request=request)
    print("Error: ", str(e))
    return unhandled()


@validate_token_middleware
def sortCategoryItems(merchantId, categoryId):
  try:
    userId = g.userId

    if not validateMerchantUser(merchantId, userId):
      return unauthorised("user is not authorized")

    _json = request.json
    items = _json.get("items") if isinstance(_json.get("items"), list) else []

    return ProductsCategories.sort_category_items(merchantId, categoryId, items)
  except Exception as e:
    return unhandled(f"Error: {e}")