# local imports
import config
from utilities.helpers import get_db_connection
from models.Orders import Orders

# import config
sns_item_notification = config.sns_item_notification


class SquareOrderWebhook():

    #########################################################
    #########################################################
    #########################################################

    @classmethod
    def trigger_order_event(cls, event):
        try:
            connection, cursor = get_db_connection()
            # check if event type is CREATE then exit
            if event.get("type") == "refund.updated":
                payment_id = event['data']['object']['refund']['payment_id']
                event_order_id=event['data']['object']['refund']['order_id']
                # check if square order id is stored in external_order_id field in orders table
                order_details = Orders.get_order(externalOrderIdSquare=payment_id)
                if not order_details:
                    print(f"order not found on our dashboard with externalOrderIdSquare={payment_id}")
                    return False

                # check if event type is DELETE then cancel the order on our dashboard
                # if event['type'] == "refund.created":
                if int(order_details['status']) == 9:
                    return True
                partial_refund = 0
                # if  event['data']['object']['refund']["amount_money"]["amount"] <  float(order_details['ordertotal']) * 100:
                #     partial_refund = 1
                return Orders.update_order_status(order_details['id'],  unchanged=['pos'], caller="Square Webhook",
                                                  _json={
                                                      "payment_id":payment_id,
                                                      "event_order_id":event_order_id,
                                                      "partialRefund": partial_refund,
                                                      "orderType": "",
                                                      "remarks": event['data']['object']['refund']["reason"],
                                                      "amount": int(
                                                          event['data']['object']['refund']["amount_money"]["amount"]) / 100
                                                  } , OrderSquareType=event.get("type"))

            if event.get("type") == "order.updated":
                state = event['data']['object']['order_updated']['state']
                if state == 'COMPLETED':
                    order_id = event['data']['object']['order_updated']['order_id']
                    order_details = Orders.get_order(externalOrderIdSquare=order_id)

                    return Orders.update_order_status(order_details['id'], 7, unchanged=['pos'],
                                                      caller="Square Webhook",
                                                      _json={})

        except Exception as e:
            print("Error: ", str(e))
            return False
