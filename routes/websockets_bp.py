from flask import Blueprint

# local imports
from controllers.WebsocketsController import *

# init blueprint
ws_bp = Blueprint("websocket_bp", __name__)


# routes
ws_bp.route("/websocket", methods=['POST']) (createWebsocket)
ws_bp.route("/websocket/<id>", methods=['GET']) (getWebsocketById)
ws_bp.route("/websocket/<id>", methods=['DELETE']) (deleteWebsocketById)
ws_bp.route("/websockets", methods=['GET']) (getWebsockets)
ws_bp.route("/websockets/getMerchantConnections", methods=['GET']) (getWebsocketConnectionsByMerchantId)