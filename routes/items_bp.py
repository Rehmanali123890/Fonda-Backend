from flask import Blueprint

# local imports
from controllers.ItemsController import *
from controllers.ProductsAddonsController import *

# init blueprint
items_bp = Blueprint("items_bp", __name__)


# routes
items_bp.route("/merchant/<merchantId>/items", methods=['GET']) (getMerchantItems)
items_bp.route("/merchant/<merchantId>/item/<itemId>", methods=['GET']) (getMerchantItemById)
items_bp.route('/merchant/<merchantId>/item', methods=['POST']) (createMerchantitem)
items_bp.route('/merchant/<merchantId>/item/<itemId>', methods=['PUT']) (updateMerchantItem)
items_bp.route('/merchant/<merchantId>/item/<itemId>', methods=['DELETE']) (deleteMerchantItem)

# item status
items_bp.route('/merchant/<merchantId>/item/<itemId>/status', methods=['PUT']) (updateMerchantItemStatus)
# category status
items_bp.route('/merchant/<merchantId>/category/<categoryId>/status', methods=['PUT']) (updateMerchantCategoryStatus)

# item image APIs
items_bp.route('/merchant/<merchantId>/item/<itemId>/image', methods=['PUT']) (uploadMerchantItemImage)
items_bp.route('/merchant/<merchantId>/item/<itemId>/image', methods=['DELETE']) (deleteMerchantItemImage)

# product - addons
items_bp.route('/merchant/<merchantId>/item/<itemId>/addon', methods=['POST']) (createOrDeleteProductAddon)
items_bp.route("/merchant/<merchantId>/item/<itemId>/sort-addons", methods=['PUT']) (sortItemAddons)

# download/upload items csv
items_bp.route('/merchant/<merchantId>/items/generate-csv', methods=['GET']) (generateItemsCsv)
items_bp.route('/merchant/<merchantId>/items/upload-pricemappings', methods=['POST']) (uploadItemsPriceMappings)
items_bp.route("/generate_top_items", methods=['GET']) (CreateTopItems)
items_bp.route("/merchant/<merchantId>/get_top_items", methods=['GET']) (getMerchantTopItems)

# item service availability
items_bp.route("/merchant/<merchantId>/item/<itemId>/service-availability", methods=['POST']) (createItemServiceAvailability)
items_bp.route("/merchant/<merchantId>/item/<itemId>/service-availability", methods=['GET']) (getItemServiceAvailability)


