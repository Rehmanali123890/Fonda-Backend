from flask import Blueprint

# local imports
from controllers.PayoutsController import *

# init blueprint
payouts_bp = Blueprint("payouts_bp", __name__)


# Routes
payouts_bp.route('/merchant/<merchantId>/payouts', methods=['GET']) (getMerchantPayouts)
payouts_bp.route('/merchant/<merchantId>/sendgridemailsummary', methods=['GET']) (SendGridEmailSummary)

payouts_bp.route('/merchant/<merchantId>/payouts-bank-transfer/<payoutId>', methods=['GET']) (transfer_to_bank)
payouts_bp.route('/merchant/get-stripe-balance', methods=['GET']) (get_stripe_balance)
payouts_bp.route('/merchant-bank-detail/<merchantId>/payout/<payoutId>', methods=['GET'])(getBankDetails)
payouts_bp.route('/merchant/get-stripe-balance', methods=['GET']) (get_stripe_balance)
payouts_bp.route('/merchant/<merchantId>/payout/<payoutId>', methods=['GET']) (getMerchantPayoutById)
payouts_bp.route('/merchant/<merchantId>/payout-detail/<payoutId>', methods=['GET']) (getPayoutType)
payouts_bp.route('/merchant/<merchantId>/new-payout/<payoutId>', methods=['GET']) (getNewPayout)
payouts_bp.route('/merchant/<merchantId>/payout/<payoutId>/send_payout_report', methods=['Post']) (send_payout_report)

payouts_bp.route("/merchant/<merchantId>/financials/create-payout-report", methods=['POST']) (generateMerchantPayoutReport)
payouts_bp.route("/merchant/<merchantId>/payout/<payoutId>/revert", methods=['POST']) (revertMerchantPayout)

payouts_bp.route('/merchants/financials/bulk-payout', methods=['GET']) (getBulkPayoutRecords)
payouts_bp.route("/merchants/financials/create-bulk-payout-report", methods=['POST']) (generateBulkPayoutReport)

payouts_bp.route('/merchants/financials/monthly-report-test-func', methods=['GET']) (monthly_payout_report_test_func)
payouts_bp.route('/merchants/financials/replace-test-data-script', methods=['GET']) (replace_test_data_script)

