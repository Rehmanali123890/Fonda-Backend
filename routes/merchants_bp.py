from flask import Blueprint

# local imports
from controllers.MerchantsController import *

# init blueprint
merchants_bp = Blueprint("merchants_bp", __name__)


# Routes

merchants_bp.route('/test-db', methods=['GET']) (testDB)
# merchants_bp.route('/test-eb/<counter>', methods=['GET']) (lambda_handler)


merchants_bp.route('/merchant', methods=['POST']) (createMerchant)
merchants_bp.route('/onboardmerchant', methods=['POST']) (onBoardMerchant)

merchants_bp.route('/onboardmerchantuser', methods=['POST']) (onBoardMerchantUser)
merchants_bp.route('/merchant/<merchantId>', methods=['GET']) (getMerchantByID)

merchants_bp.route('/merchant/schedulerFunction', methods=['GET']) (schedulerFunction)
merchants_bp.route('/merchant/<merchantId>/calculate_subscription', methods=['GET']) (calculate_subscription)
merchants_bp.route("/merchant/<merchantId>/battery-status", methods=["POST"]) (initBatteryStatusAlert)
merchants_bp.route("/merchant/<merchantId>/offline-notification", methods=["POST"]) (offlineStatusAlert)
merchants_bp.route('/merchantWithoutToken/<merchantId>', methods=['GET']) (getMerchantByIDWithoutToken)
merchants_bp.route('/merchants', methods=['GET']) (getMerchants)
merchants_bp.route("/merchants", methods=["POST"]) (getMerchantsV2)
merchants_bp.route("/merchant-account-detail/<merchantId>", methods=["GET"])(getMerchantAccountDetails)


merchants_bp.route('/merchant/<merchantId>', methods=['PUT']) (updateMerchant)

# storefront microservices for on/off and slug-change
merchants_bp.route('/merchant/<merchantId>/storefront-slug-change', methods=['PUT']) (storefront_slug_change)
merchants_bp.route('/merchant/<merchantId>/storefront-status-change', methods=['PUT']) (storefront_status_change)

merchants_bp.route('/merchant-account/<merchantId>', methods=['PUT']) (updateMerchantAccount)
merchants_bp.route('/merchant-connect-stripe/<merchantId>', methods=['PUT']) (connectMerchantStripe)
merchants_bp.route('/merchant-settings/<merchantId>', methods=['PUT']) (saveMerchantSettings)
merchants_bp.route('/merchant/<merchantId>/business-info', methods=['PUT']) (updateMerchantBusinessInfo)

merchants_bp.route("/merchant/<merchantId>/market-status", methods=['PUT']) (updateMerchantMarketplaceStatus)
merchants_bp.route("/merchant/<merchantId>/subscription", methods=['PUT']) (updateMerchantSubscription)
merchants_bp.route("/merchant/<merchantId>/stream-platform-status", methods=['PUT']) (updateMerchantstreamPlatformStatus)

merchants_bp.route('/merchant/stripeToken', methods=['GET']) (onBoardMerchentFee)
merchants_bp.route('/merchant/<merchantId>/completePayment', methods=['POST']) (paymentValidation)
merchants_bp.route('/merchant/<merchantId>/reminderEmail', methods=['POST']) (reminderEmail)

# Merchant - Esper Device Connection
merchants_bp.route("/merchant/<merchantId>/esper-device-connection", methods=["PUT"]) (connectDisconnectEsperDevice)

# Esper apis
merchants_bp.route("/merchants/get-esper-devices", methods=["GET"]) (getAllEsperDevices)

# Twilio apis
merchants_bp.route("/merchant/<merchantId>/send-message-twilio", methods=["POST"]) (sendMessageWithTwilio)
merchants_bp.route("/merchant/<merchantId>/battery-status", methods=["POST"]) (initBatteryStatusAlert)


merchants_bp.route("/merchant/add-remove-loyalty-points", methods=["POST"]) (manageLoyaltyPoints)
merchants_bp.route("/merchant/update-loyalty-points", methods=["POST"]) (updateLoyaltyPoints)
merchants_bp.route("/merchant/<merchantId>/getall-loyalty-points", methods=["GET"]) (getAllLoyaltyPoints)
merchants_bp.route("/merchant/<merchantId>/delete-loyalty-points/<pointId>", methods=["DELETE"]) (deleteLoyaltyPoints)

merchants_bp.route("/merchant/<merchantId>/draftpayout", methods=["POST"]) (draftPayout)
merchants_bp.route("/merchant/<merchantId>/transfer-history", methods=["POST"]) (transferHistory)
merchants_bp.route("/merchant/<merchantId>/storefront-logo", methods=["PUT"]) (storefrontLogo)
merchants_bp.route("/merchant/<merchantId>/storefront-banner", methods=["PUT"]) (storefrontBanner)
merchants_bp.route("/merchant-remove-media", methods=["GET"]) (removeMedia)

merchants_bp.route('/onboardnewmerchant', methods=['POST']) (onBoardNewMerchent)
merchants_bp.route("/merchant/onboardnewmerchant-media", methods=["PUT"]) (merchantonboard_media)

# stripe treasury apis
merchants_bp.route('/merchant/<merchantId>/activatetreasury', methods=['POST']) (activateTrasuary)
merchants_bp.route('/merchant/<merchantId>/createfinacialaccount', methods=['POST']) (createFinancialAccount)
merchants_bp.route('/merchant/<merchantId>/getstripebalances', methods=['GET']) (getStripeBalances)
merchants_bp.route('/merchant/<merchantId>/getfinacialaccountdetails', methods=['GET']) (getFinacialAccountDetails)
merchants_bp.route('/merchant/<merchantId>/getstripeconnectaccount', methods=['GET']) (getStripeConnectAccount)
merchants_bp.route('/merchant/<merchantId>/issuecard', methods=['POST']) (issueCard)
merchants_bp.route('/merchant/<merchantId>/getissuedcards', methods=['GET']) (getIssuedCards)

merchants_bp.route('/merchant/<merchantId>/getfinancialaccounttransections', methods=['GET']) (getFinancialAccountTransections)
merchants_bp.route('/merchant/<merchantId>/fundstransfertomerchant', methods=['POST']) (fundsTransferToMerchant)
merchants_bp.route('/merchant/<merchantId>/addfinancialaccountdefaultexternalaccount', methods=['POST']) (addFinancialAccountDefaultExternalAccount)
merchants_bp.route('/merchant/<merchantId>/fundstransfertofinancialaccount', methods=['POST']) (fundsTransferToFinancialAccount)

merchants_bp.route('/merchant/<merchantId>/addtrasuaryauthphone', methods=['POST']) (addTrasuaryAuthPhone)
merchants_bp.route('/merchant/<merchantId>/sendtrasuaryauthotp', methods=['POST']) (sendTrasuaryAuthOtp)
merchants_bp.route('/merchant/<merchantId>/validatetrasuaryauthphone', methods=['POST']) (validateTrasuaryAuthPhone)
merchants_bp.route('/merchant/<merchantId>/changetrasuaryauthphone', methods=['POST']) (changeTrasuaryAuthPhone)
merchants_bp.route('/merchant/<merchantId>/updatetrasuaryauthphone', methods=['POST']) (updateTrasuaryAuthPhone)
merchants_bp.route('/merchant/<merchantId>/fundstransfertoexternalbankaccount', methods=['POST']) (fundsTransferToExternalBankAccount)

merchants_bp.route('/merchant/<merchantId>/getrecipients', methods=['GET']) (getRecipients)
merchants_bp.route('/merchant/<merchantId>/addrecipient', methods=['POST']) (addRecipient)
merchants_bp.route('/merchant/<merchantId>/deleterecipient', methods=['POST']) (deleteRecipient)
merchants_bp.route('/merchant/<merchantId>/transferfundstorecipient', methods=['POST']) (transferFundsToRecipient)


# merchants_bp.route('/merchant/<merchantId>/updaterecipient', methods=['POST']) (updateRecipient)
# merchants_bp.route('/merchant/updateconnectedaccountowner/<merchantId>', methods=['POST']) (updateConnectedAccountOwner)
