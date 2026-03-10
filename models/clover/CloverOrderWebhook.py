

# local imports
import config
from utilities.helpers import get_db_connection
from models.clover.Clover import Clover
from models.Orders import Orders
from models.Platforms import Platforms


# import config
sns_item_notification = config.sns_item_notification


class CloverOrderWebhook():

  #########################################################
  #########################################################
  #########################################################

  @classmethod
  def trigger_order_event(cls, platform, event):
    try:
      connection, cursor = get_db_connection()
      objectId = event['objectId'].split("O:")[1]

      '''
        event types = CREATE, UPDATE, DELETE
        event = {
          "objectId":"O:ABCVJTABCRRSC",
          "type":"UPDATE",
          "ts":1536156558000
        }
      '''

      # check if event type is CREATE then exit
      if event['type'] == "CREATE":
        return True

      # check if clover order id is stored in external_order_id field in orders table
      order_details = Orders.get_order(externalOrderId=objectId)
      if not order_details:
        print(f"order not found on our dashboard with externalOrderId={objectId}")
        return False
      
      # check if event type is DELETE then cancel the order on our dashboard
      if event['type'] == "DELETE":
        if int(order_details['status']) == 9:
          return True
        return Orders.update_order_status(order_details['id'], 9, unchanged=['pos'], caller="Clover Webhook")

      # check if platform is assigned to merchant
      platform = Platforms.get_platform_by_merchantid_and_platformtype(merchantid=order_details['merchantid'], platformtype=4)#4=clover
      if not platform:
        print(f"clover is not assigned to the merchant with id={order_details['merchantid']}")
        return False
      
      # get order details by id from clover
      platform["accesstoken"], msg, is_error = Clover.generate_clover_access_token(platform)
      if is_error:
        print(msg)
        return False
      c_order_details = Clover.clover_get_order_by_id(cMid=platform['storeid'], cOrderId=objectId, accessToken=platform['accesstoken'], refunds=True)
      if not c_order_details:
        print("error: while getting order details from clover!")
        return False

      refund_amount = 0
      refunds = c_order_details.get("refunds")
      if refunds:
        for refund in refunds['elements']:
          refund_amount += int(refund['amount'])

        # change order_status on our dashboard and send sns
        partial_refund = 1
        if float(order_details['ordertotal']) * 100 <= refund_amount:
          partial_refund = 0
        return Orders.update_order_status(order_details['id'], 9, unchanged=['pos'], caller="Clover Webhook",
                                          _json={
                                            "partialRefund": partial_refund,
                                            "orderType": "",
                                            "remarks": "refunded from clover",
                                            "amount": int(refund_amount) / 100
                                          })
      
    except Exception as e:
        print("Error: ", str(e))
        return False
