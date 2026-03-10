import os
import sys

from flask import Flask, g
# Get the absolute path of the root directory
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

# Add the root directory to sys.path
sys.path.insert(0, ROOT_DIR)

# local imports
import config
from globals import app
from utilities.helpers import get_db_connection

from controllers.ItemsController import *
from controllers.ProductsAddonsController import *

with app.app_context():
    connection, cursor = get_db_connection()
    cursor.execute("SELECT * FROM items WHERE status = 0 AND pause_type IS NOT NULL AND pause_type!='unavailable' AND resume_time<NOW()")
    rows = cursor.fetchall()

    # print(rows)
    # quit()
    noOfItems = len(rows)
    currentIndex = 0
    for row in rows:
        currentIndex=currentIndex+1
        reqJSON = {'itemStatus':1}
        systemUserID = config.API_USER_FOR_SENDING_SLACK_NOTIFICATIONS
        merchantID = row['merchantid']
        itemID = row['id']
        print(f"Item : {currentIndex}/{noOfItems} ({itemID})")
        try:
            updateMerchantItemStatusInternal(merchantId=merchantID, itemId=itemID, _json=reqJSON, userId=systemUserID)
        except Exception as e:
            print("Error: ", str(e))
    print(f"Items Updated : {noOfItems}")
    quit()