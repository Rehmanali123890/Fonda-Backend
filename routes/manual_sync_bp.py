from flask import Blueprint

# local imports
from controllers.SyncController import *

# init blueprint
manual_sync = Blueprint("manual_sync", __name__)


# menus
# manual_sync.route("/merchant/<merchantId>/platform/<platformId>/manual-sync", methods=['POST']) (menuManualSync)
manual_sync.route("/merchant/<merchantId>/platform/<platformId>/manual-sync", methods=['POST']) (testMenuManualSync)
## testing 123
manual_sync.route("/merchant/<merchantId>/platform/<platformId>/testing", methods=['POST']) (testMenuManualSync)