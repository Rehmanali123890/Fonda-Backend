from app import app
import json
import requests

# local imports
from utilities.helpers import success
import config
from models.Platforms import Platforms
from models.square.SquareItemSync import SquareItemSync


def item_autosync_event(event, context):
  with app.app_context():
    print("---------------------- item_autosync_event is triggered -------------------------")

    for record in event['Records']:
      subject, message = record.get("Sns").get("Subject"), eval(record.get("Sns").get("Message"))
      print("subject: ", subject)

      merchantId = message.get("body").get("merchantId")
      itemId = message.get("body").get("itemId")
      addonId = message.get("body").get("addonId")
      unchanged = message.get("body").get("unchanged")

      if unchanged and type(unchanged) is list and 'pos' in unchanged:
        print("square remains unchanged, handling loop. exiting...")
        continue

      platform = Platforms.get_platform_by_merchantid_and_platformtype(merchantid=merchantId, platformtype=11)# 11=square

      if platform:
        if platform["synctype"] == 1:

          if subject == "item.update" or subject == "item.status_change":
            resp = SquareItemSync.update_item(
              merchantId=merchantId,
              itemId=itemId,
              platform=platform
            )

        else:
          print("Merchant synctype preference is manual")

def refeshSquareToken(event, context):
  with app.app_context():

    print("---------------------- Square token refresh is triggered -------------------------")

    merchants = Platforms.get_merchants_by_platform(11)
    for merchant in merchants:
      metedata = json.loads(merchant['metadata'])
      refresh_token = metedata['refresh_token']

      url = config.square_base_url + "/oauth2/token"
      payload = json.dumps({
        "client_id": config.square_client_id,
        "client_secret": config.square_client_secret,
        "grant_type": "refresh_token",
        "refresh_token": refresh_token
      })
      headers = {
        'Square-Version': '2022-03-16',
        'Content-Type': 'application/json'
      }

      response = requests.request("POST", url, headers=headers, data=payload)
      response = response.json()

      new_metedata = {
        "refresh_token": response['refresh_token'],
        "merchant": metedata['merchant']
      }

      new_metedata = json.dumps(new_metedata)
      Platforms.update_square_token(merchant['id'], response['access_token'], new_metedata)
