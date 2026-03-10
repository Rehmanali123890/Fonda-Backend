from flask import Blueprint

from controllers.VirtualMerchantsController import *



vm_bp = Blueprint("vm_bp", __name__)


# Routes
vm_bp.route('/merchant/<merchantId>/virtual-merchant', methods=['POST']) (createVirtualMerchant)
vm_bp.route('/merchant/<merchantId>/virtual-merchant/<id>', methods=['PUT']) (updateVirtualMerchant)
vm_bp.route('/merchant/<merchantId>/virtual-merchant/<id>/status', methods=['PUT']) (changeVirtualMerchantStatus)

vm_bp.route('/merchant/<merchantId>/virtual-merchant', methods=['GET']) (getMerchantVirtualMerchants)
vm_bp.route('/merchant/<merchantId>/virtual-merchant/<id>', methods=['GET']) (getVirtualMerchantById)