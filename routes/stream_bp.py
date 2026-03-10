from flask import Blueprint

# local imports
from controllers.StreamController import *

# init blueprint
stream_bp = Blueprint("stream_bp", __name__)


# routes
stream_bp.route("/stream-pos/v1/authorize", methods=['GET']) (stream_authorize)
stream_bp.route("/stream-pos/v1/token", methods=['POST']) (stream_token_validation)
stream_bp.route("/stream-pos/v1/locations", methods=['GET']) (generate_location_json)
stream_bp.route("/stream-pos/v1/orders", methods=['POST']) (stream_webhook)
stream_bp.route("/stream-pos/v1/catalog", methods=['GET']) (generate_menu_json)
stream_bp.route("/test_stream_update_status", methods=['GET']) (test_stream_update_status)

