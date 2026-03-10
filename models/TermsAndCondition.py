import copy
import json
import math
import random
import re
import string
import time
import uuid
from random import choices
from string import ascii_uppercase, digits

import boto3
import config
import jwt.utils
import stripe
from botocore.config import Config
from dateutil.parser import parse
from flask import Flask, flash, render_template, request
from utilities.helpers import (
    create_log_data,
    get_db_connection,
    is_float,
    normalize_string,
    open_stripe_connect_account,
    publish_sns_message,
    success,
    validateLoginToken,
)

s3_apptopus_bucket = config.s3_apptopus_bucket
document_folder = config.s3_document_folder


class TermsAndCondition:

    @classmethod
    def upload_terms_and_condition_document(cls, document):
        client = boto3.client("s3")
        ext = document.filename.split(".")[-1]
        documentName = str(uuid.uuid4()) + "." + ext

        print("Creating New Document...")
        client.upload_fileobj(
            document,
            s3_apptopus_bucket,
            f"{document_folder}/{documentName}",
            ExtraArgs={"ACL": "public-read", "ContentType": document.content_type},
        )

        documentUrl = client.generate_presigned_url(
            ClientMethod="get_object",
            Params={
                "Bucket": s3_apptopus_bucket,
                "Key": f"{document_folder}/{documentName}",
            },
        )
        documentUrl = documentUrl.split("?")[0]
        return documentUrl

    @classmethod
    def post_terms_and_condition_document(cls, merchantid, documenttype, documenturl,documentfilename):
        try:
            connection, cursor = get_db_connection()
            id = uuid.uuid4()
            data = (id, merchantid, documenttype, documenturl,documentfilename)
            cursor.execute(
                """INSERT INTO TCdocs 
                     (id, merchantid,documenttype,documenturl,documentname)
                     VALUES (%s,%s,%s,%s,%s)""",
                data,
            )
            connection.commit()
            return True
        except Exception as e:
            print("Error: ", str(e))
            return e

    @classmethod
    def get_document_type(cls, documenttype):
        try:
            connection, cursor = get_db_connection()
            cursor.execute(
                """SELECT documenttype FROM  TCdocstype WHERE id=%s""", (documenttype)
            )
            row = cursor.fetchone()
            return row
        except Exception as e:
            print("Error: ", str(e))
            return False

    @classmethod
    def get_documents(cls, id):
        try:
            connection, cursor = get_db_connection()
            cursor.execute("""SELECT * FROM  TCdocs WHERE id=%s""", (id))
            row = cursor.fetchone()
            return row
        except Exception as e:
            print("Error: ", str(e))
            return False

    @classmethod
    def get_document_by_merchantid(cls, merchantid):
        try:
            connection, cursor = get_db_connection()
            cursor.execute(
                """SELECT t.documenttype, d.documenturl as documenturl, d.id,d.documentname FROM TCdocs as d JOIN TCdocstype as t on d.documenttype=t.id  WHERE d.merchantid=%s; """,
                (merchantid),
            )
            row = cursor.fetchall()
            return row
        except Exception as e:
            print("Error: ", str(e))
            return False

    @classmethod
    def delete_terms_and_condition_document_s3(cls, document):
        try:
            client = boto3.client("s3")
            if document is not None:
                print("Deleting terms and condtions document...")
                documentName = document.split("/")[-1]
                client.delete_object(
                    Bucket=s3_apptopus_bucket, Key=f"{document_folder}/{documentName}"
                )
                print("Terms and condtions document delete from s3")
        except Exception as e:
            print("Error: ", str(e))
            return True

    @classmethod
    def delete_terms_and_condition_documents(cls, id):
        try:
            connection, cursor = get_db_connection()
            cursor.execute(""" DELETE FROM TCdocs WHERE id=%s""", (id))
            connection.commit()
            return True
        except Exception as e:
            print("Error: ", str(e))
            return e
