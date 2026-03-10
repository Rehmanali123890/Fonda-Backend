from flask import g, request

from controllers.Middleware import validate_token_middleware
from utilities.errors import unhandled
from models.VMerchantMenus import VMerchantMenus


@validate_token_middleware
def assignOrRemoveMenuToVirtualMerchant(merchantId, vMerchantId, menuId):
    try:
        userId = g.userId
        _json = request.json
        isDelete = _json.get("delete")
        if isDelete == 0:
            return VMerchantMenus.assign_menu_to_vmerchant(merchantId, vMerchantId, menuId, userId)
        else:
            return VMerchantMenus.remove_menu_from_vmerchant(merchantId, vMerchantId, menuId)
    except Exception as e:
        return unhandled(f"error: {e}")
