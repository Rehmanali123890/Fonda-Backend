from flask import Blueprint

# local imports

from controllers.OrderLogsController import getOrderLogs

# init blueprint
order_logs_bp = Blueprint("order_logs_bp", __name__)


# routes
order_logs_bp.route('/order-logs', methods=['GET']) (getOrderLogs)