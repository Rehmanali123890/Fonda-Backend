

# local imports
from utilities.helpers import publish_sns_message
import config



def trigger_item_update_sns_topic(merchantId, itemId, userId=None, unchanged=None, oldItemStatus=None, old_item_details=None , source=None):
  try:
    print("Triggering item sns - item.update ...")
    sns_msg = {
      "event": "item.update",
      "body": {
        "merchantId": merchantId,
        "itemId": itemId,
        "userId": userId,
        "unchanged": unchanged,
        "oldItemStatus": oldItemStatus,
        "old_item_details": old_item_details,
        "source":source
      }
    }
    sns_resp = publish_sns_message(topic=config.sns_item_notification, message=str(sns_msg), subject="item.update")

    return True
  except Exception as e:
    print("error: ", str(e))
    return False