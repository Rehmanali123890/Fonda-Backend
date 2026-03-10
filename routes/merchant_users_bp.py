from flask import Blueprint

# local imports
from controllers.MerchantUsersController import *

# init blueprint
m_user_bp = Blueprint("m_user_bp", __name__)


# Routes
m_user_bp.route('/merchant/<merchantId>/users', methods=['GET']) (getMerchantUsers)
m_user_bp.route('/merchant/<merchantId>/user', methods=['POST']) (createMerchantUser)
m_user_bp.route('/merchant/<merchantId>/user/<userId>', methods=['DELETE']) (removeMerchantUser)
