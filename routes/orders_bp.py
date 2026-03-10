from flask import Blueprint

# local imports
from controllers.OrdersController import *

# init blueprint
orders_bp = Blueprint("orders_bp", __name__)


orders_bp.route("/order", methods=['POST']) (createMerchantOrder)
orders_bp.route('/order/<orderId>/updatestatus', methods=['PUT']) (updateMerchantOrderStatus)
orders_bp.route('/order/<orderId>/edit', methods=['PUT']) (updateMerchantOrder)
orders_bp.route('/order/<orderId>', methods=['GET']) (getMerchantOrder)
orders_bp.route('/order/createSquare', methods=['POST']) (create_square_order_test)
orders_bp.route('/orderpdf/<orderId>', methods=['GET']) (orderPdfDownload)
orders_bp.route('/orderpdfrecipt', methods=['GET']) (orderPdfRecipt)
orders_bp.route('/merchant/order_completion_time', methods=['GET'])(orderCompletionTime)
orders_bp.route('/merchant/<merchantId>/importToastOrders', methods=['POST']) (importToastOrders)
orders_bp.route('/order/<orderId>/delivery-info', methods=['GET']) (getOrderDeliveryInfo)
orders_bp.route('/order/<orderId>/v2/delivery-info', methods=['GET']) (getOrderDeliveryInfoV2)


orders_bp.route('/orders', methods=['POST']) (getAllMerchantsOrdersSummary)
orders_bp.route('/polling_orders/merchant/<merchantId>', methods=['GET']) (getPollingOrders)

# orders_bp.route('/doordashEmailParseur', methods=['POST']) (doordashEmailParseur)
# orders_bp.route('/grubhubEmailParser', methods=['POST']) (grubhubEmailParser)