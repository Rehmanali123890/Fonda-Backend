from flask import Blueprint

from controllers.DataPatchController import pending_locked_orders_script

# local imports


# init blueprint
datapatch_bp = Blueprint("databatch_bp", __name__)


# routes
# We have to change the order status to complete=7 if any order have status pending=0 and orderlocked = 1
datapatch_bp.route('/pending-locked-orders-script', methods=['GET']) (pending_locked_orders_script)
