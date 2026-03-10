from app import app
import json

# local imports
from models.Orders import Orders


def pos_order_notification_subscriber(event, context):
  with app.app_context():
    print("--------------------- process order notification --------------------------")
    print(event)
    for record in event['Records']:

      subject = record.get("Sns").get("Subject")
      message = eval(record.get("Sns").get("Message"))
      
      unchanged = message.get("unchanged")
      merchantId = message.get("body").get("order").get("orderMerchant").get("id")
      orderId = message.get('body').get('order').get('id')
      orderStatus = message.get("body").get("order").get("orderStatus")

      if subject == "order.create":
        Orders.send_to_clover(orderId, merchantId)
        Orders.send_to_square(orderId, merchantId)

      elif subject == "order.status":
        if orderStatus == 9 or orderStatus == 8:
          # if 'pos' in un-changed list then it means that we would not send refund notification to any pos_platform
          if unchanged is None or 'pos' not in unchanged:

            print("send cancel notification to clover...")
            Orders.refund_order_clover(orderId, merchantId)

            print("send cancel notification to square...")
            Orders.refund_order_square(orderId, merchantId)
        if orderStatus == 7:
          if unchanged is None or 'pos' not in unchanged:
            Orders.complete_order_square(orderId, merchantId)

    return {
        'statusCode': 200,
        'body': json.dumps('Lambda -> Order Notification Handler!')
    }
