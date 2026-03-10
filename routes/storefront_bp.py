from flask import Blueprint

# local imports
from controllers.StoreFrontController import *

# init blueprint
storefront_bp = Blueprint("storefront_bp", __name__)


# Routes

storefront_bp.route('/storefront/<slug>', methods=['GET']) (getStoreFront)
storefront_bp.route('/storefront/deliveryfee/<slug>', methods=['POST']) (getDeliveryFee)
storefront_bp.route('/storefront/menu/<slug>', methods=['GET']) (getStoreFrontMenu)

storefront_bp.route("/storefront/order/<slug>", methods=['POST']) (createStoreFrontOrder)
storefront_bp.route('/storefront/stripeToken/<slug>', methods=['POST']) (CreatePaymentIntent)
storefront_bp.route('/qrcodestorefront/<merchentid>', methods=['GET']) (qrStoreFront)
storefront_bp.route('/storefront/addsourceqr/<merchentid>', methods=['POST']) (AddSourceQr)
storefront_bp.route('/storefront/deletesourceqr/<sourceqrid>', methods=['GET']) (DeleteSourceQr)
storefront_bp.route('/storefront/getsourceqr/<sourceqrid>', methods=['GET']) (GetSourceQrCode)
storefront_bp.route('/storefront/getallsourceqr/<merchentid>', methods=['GET']) (GetAllSourceQr)
storefront_bp.route('/storefront/addpromo/<merchentid>', methods=['POST']) (AddPromo)
storefront_bp.route('/storefront/editpromo/<merchentid>', methods=['POST']) (EditPromo)
storefront_bp.route('/storefront/getallpromo/<merchentid>', methods=['GET']) (GetAllPromo)
storefront_bp.route('/storefront/getstorefrontpromo/<merchentid>', methods=['GET']) (GetStorefrontPromo)
storefront_bp.route('/storefront/validatepromo/<slug>/<promo>', methods=['GET']) (CheckPromo)
storefront_bp.route('/qrcodestorefront/<merchentid>/<promoid>', methods=['GET']) (qrPromoCode)
storefront_bp.route('/storefront/GenerateStoreFrontLogs/<merchantid>', methods=['POST']) (GenerateFrontEndLogs)