from flask import Blueprint

# local imports
from controllers.negativeNoOrderController import *

# init blueprint
negative_no_order_report_bp = Blueprint("negative_no_order_report_bp", __name__)


# routes
negative_no_order_report_bp.route('/negative_no_order_report', methods=['POST']) (generate_negative_and_no_order_report)


