import urllib.parse

import config
import requests
from controllers.Middleware import validate_token_middleware
from flask import g, has_request_context, jsonify, request
from models.Merchants import Merchants
from collections import  OrderedDict
from models.TermsAndCondition import TermsAndCondition
from utilities.errors import invalid, not_found, unauthorised, unhandled
from utilities.helpers import *



def uploadTermsAndConditions():
    """Upload Terms and Condition Document"""
    ip_address = None
    if request:
        ip_address = request.environ.get(
            'HTTP_X_FORWARDED_FOR', request.remote_addr)
    if ip_address:
        ip_address = ip_address.split(',')[0].strip()
    try:
        create_log_data(
            level="[INFO]",
            Message=f"In the start of function to upload terms and conditions document, IP address {ip_address}",
            functionName="uploadTermsAndConditions",
            request=request,
        )
        json_data = request.form["terms_data"]
        terms_data = json.loads(json_data)
        user_token = request.args.get("token")
        if user_token:
            user = validateLoginToken(user_token, userFullDetail=1)
            if not user:
                create_log_data(
                    level="[ERROR]",
                    Message="The API token is invalid.",
                    messagebody=f"Unable to find the user on the basis of provided token., IP address {ip_address}",
                    functionName="uploadTermsAndConditions",
                    merchantID=terms_data["merchantid"],
                    statusCode="400 Bad Request",
                )
                return invalid("Invalid Token")
        else:
            create_log_data(
                level="[ERROR]",
                Message="The API token is not found.",
                messagebody=f"Unable to get api token in request argument, IP address {ip_address}",
                functionName="uploadTermsAndConditions",
                merchantID=terms_data["merchantid"],
                statusCode="400 Bad Request",
            )
            return invalid("Api token not found")
        document = None
        documentfilename = None
        if request.files["document"].filename != "":
            document = request.files["document"]
            documentfilename = request.files["document"].filename
        if document:
            documenturl = TermsAndCondition.upload_terms_and_condition_document(
                document
            )
            create_log_data(
                level="[INFO]",
                Message=f"Uploaded document to S3 bucket against merchant {terms_data['merchantid']}, IP address {ip_address}",
                user=user,
                merchantID=terms_data["merchantid"],
                functionName="uploadTermsAndConditions",
            )
            document_upload = TermsAndCondition.post_terms_and_condition_document(
                terms_data["merchantid"], terms_data["documenttype"], documenturl,documentfilename
            )
            if not isinstance(document_upload, Exception):
                documenttype = TermsAndCondition.get_document_type(
                    terms_data["documenttype"]
                )
                create_log_data(
                    level="[INFO]",
                    Message=f"Document {documenttype['documenttype']} successfully saved in database, IP address {ip_address}",
                    user=user,
                    merchantID=terms_data["merchantid"],
                    functionName="uploadTermsAndConditions",
                )

                message = f"{documenttype['documenttype']}  T&C document uploaded, IP address {ip_address}"

                sns_msg = {
                    "event": "TermsAndCondition.uploaded",
                    "body": {
                        "merchantId": terms_data["merchantid"],
                        "userId": user["id"],
                        "eventDetails": message,
                        "eventType": "activity",
                        "eventName": "TermsAndCondition.uploaded",
                    },
                }

                publish_sns_message(
                    topic=config.sns_activity_logs,
                    message=str(sns_msg),
                    subject="TermsAndCondition_activity_logs",
                )
                create_log_data(
                    level="[INFO]",
                    Message=f"Successfully upload document, IP address {ip_address}",
                    user=user,
                    merchantID=terms_data["merchantid"],
                    functionName="uploadTermsAndConditions",
                )
                return success(
                    jsonify(
                        {
                            "message": "success",
                            "status": 200,
                            "data": "Document uploaded successfully",
                        }
                    )
                )
            sns_msg = {
                "event": "error_logs.entry",
                "body": {
                    "userId": user["id"],
                    "merchantId": terms_data["merchantid"],
                    "errorName": "Uploading Terms and Conditions document",
                    "errorSource": "T&C document",
                    "errorStatus": 400,
                    "errorDetails": f"Please forward it to IT-Team {str(document_upload)}, IP address {ip_address}"
                }
            }
            error_logs_sns_resp = publish_sns_message(topic=config.sns_error_logs, message=str(sns_msg),
                                                      subject="error_logs.entry")
            create_log_data(
                level="[INFO]",
                Message=f"An error occurred while uploading Terms and Conditions document {document_upload}, IP address {ip_address}",
                user=user,
                merchantID=terms_data["merchantid"],
                functionName="uploadTermsAndConditions",
            )
            return invalid("Error in uploading document")
        return invalid("No document found to uploading document")

    except Exception as e:

        create_log_data(
            level="[ERROR]",
            Message=f"Failed to upload terms and condition document due to error {e}, IP address {ip_address}",
            messagebody=f"Uploading terms and condition document failed",
            functionName="uploadTermsAndConditions",
            statusCode="400 Bad Request",
            request=request,
        )
        sns_msg = {
            "event": "error_logs.entry",
            "body": {
                "userId": user["id"],
                "merchantId": terms_data["merchantid"],
                "errorName": "Uploading Terms and Conditions document",
                "errorSource": "T&C document",
                "errorStatus": 400,
                "errorDetails": f"PlePlease forward it to IT-Team {str(e)}, IP address {ip_address}"
            }
        }
        error_logs_sns_resp = publish_sns_message(topic=config.sns_error_logs, message=str(sns_msg),
                                                  subject="error_logs.entry")
        return invalid(str(e))


def deleteTermsAndCondition():
    """Delete terms and condition document"""
    ip_address = None
    if request:
        ip_address = request.environ.get(
            'HTTP_X_FORWARDED_FOR', request.remote_addr)
    if ip_address:
        ip_address = ip_address.split(',')[0].strip()
    try:
        create_log_data(
            level="[INFO]",
            Message=f"In the start of function to delete terms and conditions document, IP address {ip_address}",
            functionName="deleteTermsAndCondition",
            request=request,
        )
        documentid = request.args.get("documentid")
        merchantid = request.args.get("merchantid")
        if not merchantid or not documentid:
            create_log_data(
                level="[ERROR]",
                Message=f"Merchant id or document id is missing, IP address {ip_address}",
                functionName="deleteTermsAndCondition",
                request=request,
            )
            return invalid("Merchant id or document id is missing")
        document = TermsAndCondition.get_documents(documentid)
        user_token = request.args.get("token")
        if user_token:
            user = validateLoginToken(user_token, userFullDetail=1)
            if not user:
                create_log_data(
                    level="[ERROR]",
                    Message="The API token is invalid.",
                    messagebody=f"Unable to find the user on the basis of provided token., IP address {ip_address}",
                    functionName="deleteTermsAndCondition",
                    merchantID=merchantid,
                    statusCode="400 Bad Request",
                )
                return invalid("Invalid Token")
        else:
            create_log_data(
                level="[ERROR]",
                Message="The API token is not found.",
                messagebody=f"Unable to get api token in request argument, IP address {ip_address}",
                functionName="deleteTermsAndCondition",
                merchantID=merchantid,
                statusCode="400 Bad Request",
            )
            return invalid("Api token not found")
        if document:
            TermsAndCondition.delete_terms_and_condition_document_s3(
                document["documenturl"]
            )
            document_deleted = TermsAndCondition.delete_terms_and_condition_documents(
                documentid
            )
            if not isinstance(document_deleted, Exception):
                documenttype = TermsAndCondition.get_document_type(
                    document["documenttype"]
                )
                message = f"{documenttype['documenttype']} T&C document deleted, IP address {ip_address}"
                sns_msg = {
                    "event": "TermsAndCondition.deleted",
                    "body": {
                        "merchantId": merchantid,
                        "userId": user["id"],
                        "eventDetails": message,
                        "eventType": "activity",
                        "eventName": "TermsAndCondition.deleted",
                    },
                }

                publish_sns_message(
                    topic=config.sns_activity_logs,
                    message=str(sns_msg),
                    subject="TermsAndCondition_activity_logs",
                )
                create_log_data(
                    level="[INFO]",
                    Message=f"Successfully deleted document, IP address {ip_address}",
                    user=user,
                    merchantID=merchantid,
                    functionName="deleteTermsAndCondition",
                    statusCode="200 OK",
                )
                return success(
                    jsonify(
                        {
                            "message": "success",
                            "status": 200,
                            "data": "Document deleted successfully",
                        }
                    )
                )
            elif isinstance(document_deleted, Exception):
                sns_msg = {
                    "event": "error_logs.entry",
                    "body": {
                        "userId": user["id"],
                        "merchantId": request.args.get("merchantid"),
                        "errorName": "Deleting Terms and Conditions document",
                        "errorSource": "T&C document",
                        "errorStatus": 400,
                        "errorDetails": f"PlePlease forward it to IT-Team {str(document_deleted)}, IP address {ip_address}"
                    }
                }
                error_logs_sns_resp = publish_sns_message(topic=config.sns_error_logs, message=str(sns_msg),
                                                          subject="error_logs.entry")
                create_log_data(
                    level="[ERROR]",
                    Message=f"An error occurred while deleting T&C doccument {document_deleted}, IP address {ip_address}",
                    user=user,
                    merchantID=merchantid,
                    functionName="deleteTermsAndCondition",
                    statusCode="400 Bad Request",
                )
                return invalid(str(document_deleted))

        create_log_data(
            level="[ERROR]",
            Message=f"No document record is found to delete, IP address {ip_address}",
            user=user,
            merchantID=merchantid,
            functionName="deleteTermsAndCondition",
            statusCode="400 Bad Request",
        )

        return invalid("No document record found to delete")

    except Exception as e:

        create_log_data(
            level="[ERROR]",
            Message=f"Failed to delete terms and condition document due to error {e}, IP address {ip_address}",
            messagebody=f"Deleting terms and condition document failed",
            functionName="deleteTermsAndCondition",
            statusCode="400 Bad Request",
            merchantID=request.args.get("merchantid"),
            request=request,
        )
        sns_msg = {
            "event": "error_logs.entry",
            "body": {
                "userId": user["id"],
                "merchantId": request.args.get("merchantid"),
                "errorName": "Deleting Terms and Conditions document",
                "errorSource": "T&C document",
                "errorStatus": 400,
                "errorDetails": f"PlePlease forward it to IT-Team {str(e)}, IP address {ip_address}"
            }
        }
        error_logs_sns_resp = publish_sns_message(topic=config.sns_error_logs, message=str(sns_msg),
                                                  subject="error_logs.entry")

        return invalid(str(e))


def getTermsAndCondition():
    """Get terms and condition documents"""
    try:
        create_log_data(
            level="[INFO]",
            Message="In the start of function to get terms and conditions document",
            functionName="getTermsAndCondition",
            request=request,
        )
        merchantid = request.args.get("merchantid")
        user_token = request.args.get("token")
        if user_token:
            user = validateLoginToken(user_token, userFullDetail=1)
            if not user:
                create_log_data(
                    level="[ERROR]",
                    Message="The API token is invalid.",
                    messagebody="Unable to find the user on the basis of provided token.",
                    functionName="getTermsAndCondition",
                    merchantID=merchantid,
                    statusCode="400 Bad Request",
                )
                return invalid("Invalid Token")
        else:
            create_log_data(
                level="[ERROR]",
                Message="The API token is not found.",
                messagebody="Unable to get api token in request argument",
                functionName="getTermsAndCondition",
                merchantID=merchantid,
                statusCode="400 Bad Request",
            )
            return invalid("Api token not found")
        documents = TermsAndCondition.get_document_by_merchantid(merchantid)
        documents_url = {
            "Fonda":[],
            "Doordash":[],
            "Grubhub":[],
            "UberEat":[],
            "Square":[],
            "Clover":[]
        }
        for document in documents:
            documents_url[f'{document["documenttype"]}'].append({'id':document['id'], 'docurl':document["documenturl"],'docfilename':document['documentname']})

        create_log_data(
            level="[INFO]",
            Message="Successfully get all document records",
            functionName="getTermsAndCondition",
            merchantID=merchantid,
            statusCode="200 OK",
        )
        return documents_url
    except Exception as e:
        create_log_data(
            level="[ERROR]",
            Message=f"Failed to get terms and condition document due to error {e}",
            messagebody=f"Retriving terms and condition document failed",
            merchantID=request.args.get("merchantid"),
            functionName="getTermsAndCondition",
            statusCode="400 Bad Request",
            request=request,
        )
        sns_msg = {
            "event": "error_logs.entry",
            "body": {
                "userId": user["id"],
                "merchantId": request.args.get("merchantid"),
                "errorName": "Getting Terms and Conditions document",
                "errorSource": "T&C document",
                "errorStatus": 400,
                "errorDetails": f"PlePlease forward it to IT-Team {str(e)}"
            }
        }
        error_logs_sns_resp = publish_sns_message(topic=config.sns_error_logs, message=str(sns_msg),
                                                  subject="error_logs.entry")
        return invalid(str(e))
