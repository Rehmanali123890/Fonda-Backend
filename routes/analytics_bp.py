# local imports
from controllers.AnalyticsController import *
# from events.analytics_events import *

# init blueprint
analytics_bp = Blueprint("analytics_bp", __name__)


# routes
analytics_bp.route('/analytics/getMerchantReport', methods=['GET'])(getMerchantReportByDate)
analytics_bp.route('/analytics/merchantProductsOverview', methods=['GET'])(getMerchantProductsOverview)
analytics_bp.route('/analytics/getMerchantAppAnalytics', methods=['GET'])(getMerchantAppAnalytics)
analytics_bp.route('/analytics/getDashboardReport', methods=['GET'])(getDashboardAnalytics)
analytics_bp.route('/getOrderTransactionReport', methods=['GET']) (get_order_transaction_report)

analytics_bp.route('/merchant/<merchantId>/financials/reports', methods=['POST']) (merchantFinancialReportByDate)
analytics_bp.route('/merchant/<merchantId>/financials/merchant_consolidate_nonconsolidate_email', methods=['POST']) (merchant_consolidate_nonconsolidate_email)
# analytics_bp.route('/merchant/<merchantId>/financials/merchant_consolidate_nonconsolidate_email_bulk_payout_testing', methods=['POST']) (merchant_consolidate_nonconsolidate_email_bulk_payout)
analytics_bp.route('/merchant/financials/merchant_consolidate_nonconsolidate_email_bulk_payout', methods=['POST']) (send_email_to_queue_function)
analytics_bp.route('/merchant/financials/merchant_consolidate_nonconsolidate_email_new_bulk_payout', methods=['POST']) (send_new_bulk_email_to_queue_function)
analytics_bp.route('/merchant/<merchantId>/analytics/weekly-report', methods=['GET']) (merchantWeeklyAnalyticalReport)

analytics_bp.route('/merchant/<merchantId>/analytics/down-time', methods=['GET']) (getMerchantDownTimeAnalytics)

analytics_bp.route('/merchant/<merchantId>/analytics/virtual-merchant-report', methods=['GET']) (getVirtualMerchantsAnalytics)

# analytics_bp.route('/monthly_report', methods=['GET']) (merchant_new_monthly_analytical_report_event_example)







