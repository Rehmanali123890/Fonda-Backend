from flask import Blueprint

# local imports
from controllers.FinancialLogsController import getFinancialLogs


# init blueprint
financial_logs_bp = Blueprint("financial_logs_bp", __name__)


# routes
financial_logs_bp.route('/financial-logs', methods=['GET']) (getFinancialLogs)