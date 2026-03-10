from controllers.TermsAndConditionController import *
from flask import Blueprint

# init blueprint
document_bp = Blueprint("terms_and_condition_bp", __name__)

document_bp.route("/merchant-document", methods=["GET"])(getTermsAndCondition)
document_bp.route("/merchant-document-upload", methods=["POST"])(
    uploadTermsAndConditions
)
document_bp.route("/merchant-document-delete", methods=["DELETE"])(
    deleteTermsAndCondition
)
