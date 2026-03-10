from flask import Blueprint

# local imports
from controllers.MenuController import *
from controllers.MenuCategoriesController import *
from controllers.MenuMappingsController import *

# init blueprint
menu_bp = Blueprint("menu_bp", __name__)


# menus
menu_bp.route("/merchant/<merchantId>/menu", methods=['POST']) (createMerchantMenu)
menu_bp.route("/merchant/<merchantId>/menu/<menuId>", methods=['GET']) (getMerchantMenuById)
menu_bp.route("/merchant/<merchantId>/menu/get-all-connected-platforms", methods=['GET']) (getAllConnectedPlatforms)
menu_bp.route("/merchant/downloadmenu/<menuId>", methods=['GET']) (downloadMenu)
menu_bp.route("/merchant/<merchantId>/uploadMenuToGoogle", methods=['POST']) (uploadMenuToGoogle)

menu_bp.route("/merchant/<merchantId>/menus", methods=['GET']) (getMerchantMenus)

menu_bp.route("/merchant/<merchantId>/new-menus", methods=['GET']) (newGetMerchantMenus)

menu_bp.route("/merchant/<merchantId>/menu/<menuId>", methods=['DELETE']) (deleteMerchantMenuById)
menu_bp.route("/merchant/<merchantId>/menu/<menuId>", methods=['PUT']) (updateMerchantMenu)

# menu service availability
menu_bp.route("/merchant/<merchantId>/menu/<menuId>/service-availability", methods=['POST']) (createMenuServiceAvailability)
menu_bp.route("/merchant/<merchantId>/menu/<menuId>/service-availability", methods=['GET']) (getMenuServiceAvailability)

# menu - categories
menu_bp.route("/merchant/<merchantId>/menu/<menuId>/categories", methods=['PUT']) (updateMenuCategories)
menu_bp.route("/merchant/<merchantId>/menu/<menuId>/category", methods=['POST']) (createOrDeleteMenuCategory)
menu_bp.route("/merchant/<merchantId>/menu/<categoryId>/category-mapping", methods=['POST']) (createOrDeleteMenuCategoryMapping)
menu_bp.route("/merchant/<merchantId>/menu/<menuId>/sort-categories", methods=['PUT']) (sortMenuCategories)

# menumappings
menu_bp.route("/merchant/<merchantId>/menu/<menuId>/mappings", methods=['POST']) (createMenuMappings)


menu_bp.route("/merchant/<merchantId>/menu/<menuId>/mappings/<mappingId>", methods=['DELETE']) (deleteMenuMapping)

# download menu
menu_bp.route("/merchant/<merchantId>/menu/<menuId>/download-csv", methods=['POST']) (downloadMenuCsv)


menu_bp.route('/merchant/<merchantId>/menu/<menuId>/status', methods=['PUT']) (updateMerchantMenuStatus)

#cuisine type
menu_bp.route("/addConfigOption", methods=['POST']) (addConfigOption)
menu_bp.route("/getConfigOption", methods=['GET']) (getConfigOption)