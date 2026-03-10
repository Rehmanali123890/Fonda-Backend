import os
import datetime

import xlsxwriter
from dateutil.tz import gettz
from flask import current_app

import config
from models.Merchants import Merchants
from models.Payouts import Payouts
from utilities.helpers import get_db_connection


def generate_payouts_report():
  try:
      connection, cursor = get_db_connection()
      all_merchant_payouts = []

      cursor.execute("select id as 'id' from dashboard.merchants")
      merchants_ids = cursor.fetchall()
      for merchants_id in merchants_ids:
        payouts = Payouts.get_payouts(merchants_id['id'], startDate='2022-01-01 00:00:00', endDate='2022-12-31 23:59:59')
        all_merchant_payouts.extend(payouts)

      excel_file_name = f'payoutReports_{datetime.datetime.now().strftime("%H-%M-%S")}.xlsx'
      reports_directory = os.path.join(current_app.root_path, config.reports_directory)
      file_comp_path = os.path.join(reports_directory, excel_file_name)
      workbook = xlsxwriter.Workbook(file_comp_path)
      worksheet = workbook.add_worksheet()

      headers = ["Merchant Id", "Merchant Name", "Merchant Status", "From Date", "To Date", "Payout Created Date & Time",
                 "Payout Done By", "Number of Orders", "Sub Total", "Tax", "Commission", "Square Fee",
                 "Processing Fee", "Error Charges", "Staff Tips", "Order Adjustments",
                 "Market Price Facilitator Tax", "Promo Discount", "Payout Type", "Payout Adjustments",
                 "Payout Remarks", "Subscription Adjustments", "Subscription Date(s)", "Net Payout", "Status"]

      # Write header row in excel file
      for index, header in enumerate(headers):
        worksheet.write(0, index, header)

      for row_index, payout in enumerate(all_merchant_payouts):

        subscriptions = ", ".join([f"{subscription['date']} (${subscription['amount']})" for subscription in payout['subscriptions']])

        if payout['payoutType'] == 1: payout_type = 'Include all the unpaid subscriptions'
        elif payout['payoutType'] == 2: payout_type = 'Include subscriptions within the specified date range'
        else: payout_type = 'Do not include subscription in payout'

        if payout['status'] == 1: status = 'Transferred'
        elif payout['status'] == 2: status = f"Reverted (by {payout['revertedByName']} on {payout['revertedDateTime']})"
        else: status = f"Paid out to bank (by {payout['transferredToBankBy']} on {payout['transferredToBankTime']})"

        worksheet.write(row_index+1, 0, payout['merchantId'])
        worksheet.write(row_index+1, 1, payout['merchantName'])
        worksheet.write(row_index+1, 2, ("Active" if payout['merchantStatus'] == 1 else "Inactive"))
        worksheet.write(row_index+1, 3, payout['startDate'])
        worksheet.write(row_index+1, 4, payout['endDate'])
        worksheet.write(row_index+1, 5, payout['created_datetime'])
        worksheet.write(row_index+1, 6, payout['doneByUser'])
        worksheet.write(row_index+1, 7, payout['numberOfOrders'])
        worksheet.write(row_index+1, 8, float(payout['subTotal']))
        worksheet.write(row_index+1, 9, float(payout['tax']))
        worksheet.write(row_index+1, 10, float(payout['commission']))
        worksheet.write(row_index+1, 11, float(payout['squarefee']))
        worksheet.write(row_index+1, 12, float(payout['processingFee']))
        worksheet.write(row_index+1, 13, float(payout['errorCharges']))
        worksheet.write(row_index+1, 14, float(payout['staffTips']))
        worksheet.write(row_index+1, 15, float(payout['orderAdjustments']))
        worksheet.write(row_index+1, 16, float(payout['marketplaceTax']))
        worksheet.write(row_index+1, 17, float(payout['promoDiscount']))
        worksheet.write(row_index+1, 18, payout_type)
        worksheet.write(row_index+1, 19, float(payout['payoutAdjustments']))
        worksheet.write(row_index+1, 20, payout['remarks'])
        worksheet.write(row_index+1, 21, float(payout['subscriptionAdjustments']))
        worksheet.write(row_index+1, 22, subscriptions)
        worksheet.write(row_index+1, 23, float(payout['netPayout']))
        worksheet.write(row_index+1, 24, status)

      workbook.close()
      return file_comp_path
  except Exception as e:
      return e


def generate_menu_report():
    try:
        connection, cursor = get_db_connection()
        cursor.execute("""select m.merchantname, m.status, i.itemname, i.itemdescription, i.itemprice 
                        from dashboard.items i
                        join dashboard.merchants m on m.id=i.merchantid where itemtype=1 order by m.id;""")
        all_items = cursor.fetchall()

        excel_file_name = f'menuReports_{datetime.datetime.now().strftime("%H-%M-%S")}.xlsx'
        reports_directory = os.path.join(current_app.root_path, config.reports_directory)
        file_comp_path = os.path.join(reports_directory, excel_file_name)
        workbook = xlsxwriter.Workbook(file_comp_path)
        worksheet = workbook.add_worksheet()

        headers = ["Merchant Name", "Merchant Status", "Product/Item Name", "Item Price", "Product/Item Description"]
        for index, header in enumerate(headers):
            worksheet.write(0, index, header)

        for row_index, item in enumerate(all_items):
            worksheet.write(row_index + 1, 0, item['merchantname'])
            worksheet.write(row_index + 1, 1, ("Active" if item['status'] == 1 else "Inactive"))
            worksheet.write(row_index + 1, 2, item['itemname'])
            worksheet.write(row_index + 1, 3, float(item['itemprice']))
            worksheet.write(row_index + 1, 4, item['itemdescription'])

        workbook.close()
        return file_comp_path
    except Exception as e:
        return e


def generate_revenue_report(start_date, end_date):
    try:
        connection, cursor = get_db_connection()
        cursor.execute("SELECT * FROM dashboard.merchants where status='1';")
        all_merchants = cursor.fetchall()

        merchants_data = []
        query_dates = []
        for merchant in all_merchants:
            merchantTimezone = merchant.get("timezone")

            startDate = datetime.datetime.strptime(start_date, "%Y-%m-%d")
            endDate = datetime.datetime.strptime(end_date, "%Y-%m-%d")

            # convert datetime to python format
            utcStartDate = startDate.replace(tzinfo=gettz(merchantTimezone)).astimezone(datetime.timezone.utc)
            utcEndDate = endDate.replace(tzinfo=gettz(merchantTimezone)).astimezone(
                datetime.timezone.utc) + datetime.timedelta(days=1)

            cursor.execute(f"""SELECT 
                                DATE(convert_tz(orderdatetime, '+00:00', '{merchantTimezone}')) date,
                                IFNULL(SUM(ordertotal), 0) revenue
                                FROM orders
                                WHERE merchantid = %s AND status = 7
                                AND orderdatetime BETWEEN %s AND %s 
                                GROUP BY date;
                            """, (merchant.get("id"), utcStartDate, utcEndDate))
            ordersTableData = cursor.fetchall()

            cursor.execute(f"""SELECT 
                                DATE(convert_tz(orderdatetime, '+00:00', '{merchantTimezone}')) date,
                                IFNULL(SUM(ordertotal), 0) revenue
                                FROM ordershistory
                                WHERE merchantid = %s AND status = 7
                                AND orderdatetime BETWEEN %s AND %s 
                                GROUP BY date;
                            """, (merchant.get("id"), utcStartDate, utcEndDate))
            ordersHistoryTableData = cursor.fetchall()

            temp_dict = dict()
            total_revenue = 0
            temp_dict['name'] = merchant.get('merchantname')
            for n in range(int((endDate.date() - startDate.date()).days) + 1):
                dateval = startDate.date() + datetime.timedelta(n)
                dateval_str = datetime.datetime.strftime(dateval, "%Y-%m-%d")
                if dateval_str not in query_dates:
                    query_dates.append(dateval_str)

                revenue = 0
                for row in ordersHistoryTableData:
                    if row.get("date") == dateval:
                        revenue += float(row.get("revenue"))
                        break

                for row in ordersTableData:
                    if row.get("date") == dateval:
                        revenue += float(row.get("revenue"))
                        break

                temp_dict[dateval_str] = revenue
                total_revenue += revenue

            temp_dict['totalRevenue'] = total_revenue
            merchants_data.append(temp_dict)

        excel_file_name = f'revenueReports_{datetime.datetime.now().strftime("%H-%M-%S")}.xlsx'
        reports_directory = os.path.join(current_app.root_path, config.reports_directory)
        file_comp_path = os.path.join(reports_directory, excel_file_name)
        workbook = xlsxwriter.Workbook(file_comp_path)
        worksheet = workbook.add_worksheet()
        #
        headers = ["Restaurant Name"]
        headers.extend(query_dates)
        headers.append("Total Revenue")

        for index, header in enumerate(headers):
            worksheet.write(0, index, header)

        for row_index, item in enumerate(merchants_data):
            worksheet.write(row_index + 1, 0, item['name'])
            for index, date in enumerate(query_dates):
                worksheet.write(row_index + 1, index + 1, item[date])
            worksheet.write(row_index + 1, len(query_dates) + 1, item['totalRevenue'])

        workbook.close()
        return file_comp_path
    except Exception as e:
        return e
