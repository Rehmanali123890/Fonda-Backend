from flask import Blueprint

# local imports
from controllers.PlatformCredentialsController import *

# init blueprint
platform_credentials_bp = Blueprint("platform_credentials_bp", __name__)


# platforms
platform_credentials_bp.route("/merchant/<merchantId>/platform-credentials", methods=['GET'])(getMerchantPlatformCredentials)
platform_credentials_bp.route("/merchant/<merchantId>/platform-credentials", methods=['POST'])(postMerchantPlatformCredentials)
