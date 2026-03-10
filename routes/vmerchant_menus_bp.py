from flask import Blueprint

from controllers.VMerchantMenusController import *



vm_menus_bp = Blueprint("vm_menu_bp", __name__)


# create / delete virtual merchant menu-mappings
vm_menus_bp.route('/merchant/<merchantId>/virtual-merchant/<vMerchantId>/menu/<menuId>/mappings', methods=['POST']) (assignOrRemoveMenuToVirtualMerchant)