from flask import Blueprint

# local imports
from controllers.ErrorLogsController import *

# init blueprint
error_logs_bp = Blueprint("error_logs_bp", __name__)


# routes
error_logs_bp.route('/error-logs', methods=['GET']) (getErrorLogs)
error_logs_bp.route('/error-logs', methods=['POST']) (createErrorLogs)
