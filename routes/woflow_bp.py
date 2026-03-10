from flask import Blueprint

# local imports
from controllers.WoflowController import *

# init blueprint
woflow_bp = Blueprint("woflow_bp", __name__)


# routes
woflow_bp.route("/woflow/upload-menu", methods=['POST']) (woflowUploadMenuToS3)
woflow_bp.route("/woflow/process-menu", methods=['POST']) (woflowInitializeMenuProcessing)
woflow_bp.route("/woflow/get-records", methods=['GET']) (woflowGetRecordsFromDb)
woflow_bp.route("/woflow/<menu_id>/download-menu", methods=['GET']) (downloadMenu)
woflow_bp.route("/woflow/refresh-job", methods=['GET']) (woflowRefreshJob)
woflow_bp.route("/woflow/update-job-status", methods=['POST']) (woflowAcceptRejectJob)
