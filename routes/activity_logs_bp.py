from flask import Blueprint

# local imports
from controllers.ActivityLogsController import *

# init blueprint
activity_logs_bp = Blueprint("activity_logs_bp", __name__)


# routes
activity_logs_bp.route('/activity-logs', methods=['GET']) (getActivityLogs)

activity_logs_bp.route('/android-sec-payload', methods=['GET']) (getSecretPayloadForAndroidApp)


