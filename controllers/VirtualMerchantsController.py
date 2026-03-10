from flask import g, request, jsonify

from controllers.Middleware import validate_token_middleware
from utilities.errors import invalid, unauthorised, unhandled
from utilities.helpers import success, validateAdminUser, create_log_data, publish_sns_message,get_ip_address
from models.VirtualMerchants import VirtualMerchants
import config


@validate_token_middleware
def createVirtualMerchant(merchantId):
    try:
        ip_address = None
        if request:
            ip_address = request.environ.get(
                'HTTP_X_FORWARDED_FOR', request.remote_addr)
        if ip_address:
            ip_address = ip_address.split(',')[0].strip()
        userId = g.userId
        json_body = request.json
        create_log_data(level='[INFO]', Message=f"In the start of creating virtual merchant function, ,IP address:{ip_address}",
                        functionName="createVirtualMerchant", merchantID=merchantId, request=request)
        if not validateAdminUser(userId):
            create_log_data(level='[INFO]', Message=f"User is not authorized to create virtual merchant ,IP address:{ip_address}",
                            functionName="createVirtualMerchant", merchantID=merchantId, request=request)
            return unauthorised("user is not authorized!")

        virtual_merchant = VirtualMerchants.post_virtual_merchant(
            merchantId, json_body, userId)
        print(virtual_merchant.json.get('message'))
        if virtual_merchant.status_code == 200:
            create_log_data(level='[INFO]', Message=f"Virtual merchant created successfully ,IP address:{ip_address}",
                            functionName="createVirtualMerchant", merchantID=merchantId, request=request)
            sns_msg = {
                "event": "virtual_merchant.created",
                "body": {
                    "merchantId": merchantId,
                    "userId": userId,
                    "eventName": "virtual_merchant.created",
                    "eventDetails": f"Virtual merchant {json_body['virtualName']} created, IP address:{ip_address}"
                }
            }
            logs_sns_resp = publish_sns_message(topic=config.sns_activity_logs, message=str(sns_msg),
                                                subject="virtual_merchant.created")
            return virtual_merchant
        create_log_data(level='[INFO]', Message=f"Error in creating virtual merchant,IP address:{ip_address}", messagebody=f"{virtual_merchant.json.get('message')}",
                        functionName="createVirtualMerchant", merchantID=merchantId, request=request)
        return virtual_merchant

    except Exception as e:
        create_log_data(level='[INFO]', Message=f"Error in creating virtual merchant ,IP address:{ip_address}",
                        messagebody=f"{str(e)}",
                        functionName="createVirtualMerchant", merchantID=merchantId, request=request)
        return unhandled(f"Error: {e}")


@validate_token_middleware
def getMerchantVirtualMerchants(merchantId):
    try:
        activeOnly = request.args.get("activeOnly")  # can be either 0 or 1
        if activeOnly:
            activeOnly = int(activeOnly)

        data = VirtualMerchants.get_virtual_merchant_str(
            merchantId=merchantId, activeOnly=activeOnly)
        return success(jsonify({
            "message": "success",
            "status": 200,
            "data": data
        }))
    except Exception as e:
        return unhandled(f"Error: {e}")


@validate_token_middleware
def getVirtualMerchantById(merchantId, id):
    try:
        data = VirtualMerchants.get_virtual_merchant_str(id=id)
        return success(jsonify({
            "message": "success",
            "status": 200,
            "data": data[0]
        }))
    except Exception as e:
        return unhandled(f"Error: {e}")


@validate_token_middleware
def updateVirtualMerchant(merchantId, id):
    try:
        userId = g.userId
        json_body = request.json
        ip_address = get_ip_address(request)
        resp = VirtualMerchants.update_virtual_merchant(id, json_body, userId)
        if resp.status_code == 200:
            response_data = resp.get_json()  # This will give you the JSON data as a dictionary
            name_change_message = response_data.get('name_changed', 'No name change message found')
            name_change_message += f" IP address: {ip_address}"
            sns_msg = {
                "event": "virtual_merchant.update",
                "body": {
                    "merchantId": merchantId,
                    "userId": userId,
                    "eventName": "virtual_merchant.update",
                    "eventDetails": name_change_message
                }
            }
            logs_sns_resp = publish_sns_message(topic=config.sns_activity_logs, message=str(sns_msg),
                                                subject="virtual_merchant.update")

        return resp
    except Exception as e:
        return unhandled(f"Error: {e}")


@validate_token_middleware
def changeVirtualMerchantStatus(merchantId, id):
    try:
        ip_address = None
        if request:
            ip_address = request.environ.get(
                'HTTP_X_FORWARDED_FOR', request.remote_addr)
        if ip_address:
            ip_address = ip_address.split(',')[0].strip()
        create_log_data(level='[INFO]', Message=f"In the beginning of function to change virtual merchant status, ,IP address:{ip_address}",
                        functionName="changeVirtualMerchantStatus", merchantID=merchantId, request=request)
        userId = g.userId
        _json = request.json
        status = int(_json.get("status")) if _json.get(
            "status") is not None else None
        statusTerm = "paused" if status == 0 else "resumed"
        if status not in (0, 1):
            return invalid("invalid status!")
        statusChange = VirtualMerchants.virtual_merchant_status_update(
            id, status, userId)
        if statusChange.status_code == 200:
            print("Triggering sns - virtualmerchant.status_change ...")
            sns_msg = {
                "event": "virtualmerchant.status_change",
                "body": {
                    "merchantId": _json['merchantId'],
                    "userId": userId,
                    "eventName": "virtualmerchant.status_change",
                    "eventDetails": f"Virtual merchant {_json['virtualName']} status is changed to {statusTerm}, ,IP address:{ip_address}"
                }
            }
            logs_sns_resp = publish_sns_message(topic=config.sns_activity_logs, message=str(sns_msg),
                                                subject="virtualmerchant.status_change")
            create_log_data(level='[INFO]', Message=f"Successfully updated virtual merchant status,IP address:{ip_address}",
                            messagebody=f"{_json['virtualName']} virtual merchant status changed to {statusTerm}",
                            functionName="changeVirtualMerchantStatus", merchantID=merchantId, request=request)
        return statusChange
    except Exception as e:
        create_log_data(level='[ERROR]',
                        Message=f"Unable to change virtual merchant status", messagebody=f"An error occured {str(e)} ,IP address:{ip_address}",
                        functionName="changeVirtualMerchantStatus", merchantID=merchantId, request=request)
        return unhandled(f"Error: {e}")
