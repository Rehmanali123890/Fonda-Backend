from controllers.FinanceController import *
from events.finance_event import temporary_transaction_table_insert
from models.Finance import Finance
from flask import Blueprint

# init blueprint
finance_bp = Blueprint("finance_bp", __name__)

finance_bp.route("/transaction-csv", methods=["POST"])(
    uploadTransactionCSV
)
finance_bp.route("/transaction-email", methods=["GET"])(send_transaction_email)
finance_bp.route("/finance/revenue-report", methods=["GET"])(getMerchantReportFinance)
finance_bp.route('/finance-payout/<merchantId>', methods=['POST'])(FinancePayout)
finance_bp.route('/transaction-insertion',methods=['POST'])(temporary_transaction_table_insert)
finance_bp.route('/merchant/<merchantId>/merchant_finance_consolidate_nonconsolidate_email', methods=['POST']) (merchant_finance_consolidate_nonconsolidate_email)
finance_bp.route('/merchant/<merchantId>/finance-payout/<payoutId>', methods=['GET']) (getFinanceMerchantPayoutById)
finance_bp.route('/merchant/<merchantId>/finance-payouts', methods=['GET']) (getFinanceMerchantPayouts)
finance_bp.route('/merchant/<merchantId>/payout/<payoutId>/send_finance_payout_report', methods=['Post']) (send_financepayout_report)
finance_bp.route('/merchant-bank-detail/<merchantId>/finance-payout/<payoutId>', methods=['GET'])(getBankDetailsFinancePayout)
finance_bp.route('/merchant/<merchantId>/payouts-bank-transfer-finance-payout/<payoutId>', methods=['GET']) (transfer_to_bank_finance_payout)
finance_bp.route("/merchant/<merchantId>/finance-payout/<payoutId>/revert", methods=['POST']) (revertMerchantFinancePayout)

finance_bp.route('/weekly-report/<merchantId>', methods=['GET']) (merchantWeeklyAnalyticalReportTesting)
finance_bp.route("/merchants/financials/create-new-bulk-payout-report", methods=['POST']) (generateNewBulkPayoutReport)
finance_bp.route('/merchants/financials/new-bulk-payout', methods=['GET']) (getNewBulkPayoutRecords)
finance_bp.route('/merchants/financials/example_new-bulk-payout', methods=['GET']) (merchant_calculate_NewBulkPayout_api)
#new_payout_report
finance_bp.route('/script_new_payout_report', methods=['GET']) (script_generating_csv_for_new_payout_report)
# adding revnue fee
finance_bp.route('/adding_revenue_processing_fee/<platform>', methods=['GET']) (added_revenue_processing_fee)

#
finance_bp.route('/download-transaction-summary-report/<merchantId>', methods=['POST']) (download_transaction_summary_report)
#new payout summary report
finance_bp.route('/download-payout-summary-report/<merchantId>', methods=['POST']) (new_payout_summary_report)
finance_bp.route('/adding_revenue_new', methods=['GET']) (added_revenue_fee_cancelled)
finance_bp.route('/test_treasury_transfer', methods=['GET']) (test_treasury_transfer)
finance_bp.route('/merchant-bank-detail/<merchantId>/finance-payout', methods=['GET'])(getBankDetailsFinancePayoutByMerchantID)

finance_bp.route('/platform-bank-reconciliation-upload-files', methods=['POST']) (upload_reconcile_payouts)
finance_bp.route('/platform-bank-reconciliation', methods=['GET']) (reconcile_payout_by_date)
