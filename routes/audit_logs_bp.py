from flask import Blueprint

# local imports
from controllers.AuditLogsController import *

# init blueprint
audit_logs_bp = Blueprint("audit_logs_bp", __name__)


# routes
audit_logs_bp.route('/audit-logs', methods=['GET']) (getAuditLogs)
