from flask import Blueprint

# local imports
from controllers.CategoriesController import *
from controllers.ProductsCategoriesController import *

# init blueprint
categories_bp = Blueprint("categories_bp", __name__)


# routes
categories_bp.route('/merchant/<merchantId>/category/<categoryId>', methods=['PUT']) (updateMerchantCategory)
categories_bp.route('/merchant/<merchantId>/category/<categoryId>', methods=['DELETE']) (deleteMerchantCategory)
categories_bp.route('/merchant/<merchantId>/category', methods=['POST']) (createMerchantProductCategory)
categories_bp.route('/merchant/<merchantId>/category/<categoryId>', methods=['GET']) (getMerchantCategoryByID)

categories_bp.route('/merchant/<merchantId>/category-menus/<categoryId>', methods=['GET']) (getMerchantCategoryMenuByID)

# products - categories
categories_bp.route('/merchant/<merchantId>/category/<categoryId>/updateitems', methods=['POST']) (updateItemsToCategory)
categories_bp.route('/merchant/<merchantId>/category/<categoryId>/item', methods=['POST']) (createOrDeleteCategoryProduct)
# new menu
categories_bp.route('/merchant/<merchantId>/item/<itemId>/category', methods=['POST']) (createOrDeleteCategoryProductNewMenu)

categories_bp.route('/merchant/<merchantId>/categoriesWithItems', methods=['GET']) (getMerchantCategoriesWithItems)
categories_bp.route("/merchant/<merchantId>/category/<categoryId>/sort-items", methods=['PUT']) (sortCategoryItems)

# category service availability
categories_bp.route("/merchant/<merchantId>/category/<categoryId>/service-availability", methods=['POST']) (createCategoryServiceAvailability)
categories_bp.route("/merchant/<merchantId>/category/<categoryId>/service-availability", methods=['GET']) (getCategoryServiceAvailability)