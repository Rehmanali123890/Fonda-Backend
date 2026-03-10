from flask import Blueprint

# local imports
from controllers.AddonsOptionsController import *
from controllers.AddonsController import *

# init blueprint
addons_bp = Blueprint("addons_bp", __name__)


# routes


# addons - options
addons_bp.route('/merchant/<merchantId>/addon/<addonId>/option', methods=['POST']) (createOrDeleteAddonOption)
addons_bp.route('/merchant/<merchantId>/addon/<addonId>/usedAddonItems', methods=['GET']) (getusedAddonItems)
addons_bp.route("/merchant/<merchantId>/addon/<addonId>/sort-options", methods=['PUT']) (sortAddonOptions)
addons_bp.route('/merchant/<merchantId>/option/<optionId>/getaddons', methods=['GET']) (getOptionAddons)


addons_bp.route('/merchant/<merchantId>/addon/<addonId>', methods=['PUT']) (updateMerchantAddon)
addons_bp.route('/merchant/<merchantId>/addon/<addonId>', methods=['GET']) (getMerchantAddonByID)
addons_bp.route('/merchant/<merchantId>/addon/<addonId>', methods=['DELETE']) (deleteMerchantAddon)
addons_bp.route('/merchant/<merchantId>/addonsWithOptions', methods=['GET']) (getMerchantaddonListwithOptions)
addons_bp.route('/merchant/<merchantId>/addonsWithoutOptions', methods=['GET']) (getMerchantaddonListwithoutOptions)
addons_bp.route('/merchant/<merchantId>/addon', methods=['POST']) (createMerchantAddon)

