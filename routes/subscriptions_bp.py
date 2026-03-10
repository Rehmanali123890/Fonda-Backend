from flask import Blueprint

# local imports
from controllers.SubscriptionsController import *

# init blueprint
subscriptions_bp = Blueprint("subscriptions_bp", __name__)


# Routes
subscriptions_bp.route('/merchant/<merchantId>/subscription/<subscriptionId>/changestatus', methods=['POST']) (waiveoffSubscriptionAmount)

subscriptions_bp.route('/merchant/<merchantId>/get-subscription-records', methods=['GET']) (get_subscriptions_for_merchant)
subscriptions_bp.route('/merchant/<merchantId>/subscription/<subscriptionId>/splitSubscriptionAmount', methods=['POST']) (splitSubscriptionAmount)