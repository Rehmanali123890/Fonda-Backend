import uuid
import csv
from models.Merchants import Merchants
from utilities.errors import unhandled
import datetime
import json
from dateutil.tz import gettz
import io
import config
import boto3
import pytz
# local imports
from utilities.helpers import get_db_connection, success, publish_sns_message, create_log_data

s3_apptopus_bucket = config.s3_apptopus_bucket
images_folder = config.s3_images_folder
from utilities.emails import send_financial_report_email

class Payouts():

  ############################################### POST

  @classmethod
  def post_payout(cls, merchantId, startDate, endDate, status, report, userId, stripe_response_data, transfer_type=1,doordash=0,ubereats=0,
                  grubhub=0,storefront=0,others=0,TotalRevenue=0,TotalFondaPayout=0,FondaRevenue=0,FondaRevenuePercentage=0):
    try:
      connection, cursor = get_db_connection()

      payoutId = uuid.uuid4()
      if transfer_type == 3:
        stripe_response_data=json.dumps({})
        status=3
      else:
        stripe_response_data_id=stripe_response_data.get('id')
        stripe_response_data=json.dumps(stripe_response_data)

      # 1 = trasnfered, 2 = reverted_payemnt, 0 = only_record [for bulk payout]

      if report['payoutType'] == 1 and transfer_type!=3:
        data = (payoutId, merchantId, startDate, endDate, status,doordash,ubereats,grubhub,storefront,others,
                TotalRevenue,TotalFondaPayout,FondaRevenue,FondaRevenuePercentage,
                report['numberOfOrders'], report['subTotal'], report['staffTips'], report['tax'],
                report['processingFee'], report['promoDiscount'], report['commission'], report['squarefee'],
                report['marketplaceTax'], report['errorCharges'], report['orderAdjustments'],
                report['subscriptionAdjustments'], report['payoutType'], report['payoutAdjustments'],
                report['netPayout'],
                stripe_response_data_id, stripe_response_data, report['remarks'], userId, transfer_type,
                report['RevenueProcessingFee'],report['lifetime_total_revenue'],report['Revenue_processing_fee_Reason'],
                report['marketingFee'],report['commisionAdjustment']
                )
        cursor.execute("""
                  INSERT INTO payouts (id, merchantid, startdate, enddate, status,doordash,ubereats,grubhub,storefront,others,
                    totalrevenue, totalfondapayout, fondarevenue, fondarevenuepercentage,
                    numberoforders, subtotal, stafftips, tax, processingfee,promodiscount, commission, 
                    squarefee, marketplacetax, errorcharges, orderadjustments, subscriptionadjustments, 
                    payouttype, payoutadjustments, netpayout, transferid, metadata, remarks, created_by,
                     transfer_type,RevenueProcessingFee,lifetime_total_revenue,Revenue_processing_fee_Reason,marketingFee,commisionAdjustment)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s,%s, %s, %s, %s,%s,%s, %s, %s,%s,%s)
                  """, (data))

      elif report['payoutType'] == 2 or report['payoutType'] == 3 or transfer_type==3:
        data = (payoutId, merchantId, startDate, endDate, status,doordash,ubereats,grubhub,storefront,others,
                TotalRevenue, TotalFondaPayout, FondaRevenue, FondaRevenuePercentage,
                report['numberOfOrders'], report['subTotal'], report['staffTips'], report['tax'],
                report['processingFee'], report['promoDiscount'], report['commission'], report['squarefee'],
                report['marketplaceTax'], report['errorCharges'], report['orderAdjustments'],
                report['subscriptionAdjustments'], report['payoutType'], report['payoutAdjustments'],
                report['netPayout'], stripe_response_data, report['remarks'], userId, transfer_type,
                report['RevenueProcessingFee'],report['lifetime_total_revenue'],report['Revenue_processing_fee_Reason'],
                report['marketingFee'],report['commisionAdjustment']
                )
        cursor.execute("""
                  INSERT INTO payouts (id, merchantid, startdate, enddate, status,doordash,ubereats,grubhub,storefront,others,
                    totalrevenue, totalfondapayout, fondarevenue, fondarevenuepercentage,
                    numberoforders, subtotal, stafftips, tax, processingfee,promodiscount, 
                    commission, squarefee, marketplacetax, errorcharges, orderadjustments, subscriptionadjustments, 
                    payouttype, payoutadjustments, netpayout, metadata, remarks, created_by,
                     transfer_type,RevenueProcessingFee,lifetime_total_revenue,Revenue_processing_fee_Reason,marketingFee,commisionAdjustment)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s,%s, %s, %s, %s,%s,%s, %s, %s,%s,%s)
                  """, (data))
      connection.commit()

      return str(payoutId)
    except Exception as e:
      print(str(e))
      return False

  @classmethod
  def post_NewBulkPayout(cls, merchantId, startDate, endDate,merchant_status_and_name, csv_report, dashboard_report, userId):
    try:
        connection, cursor = get_db_connection()
        create_log_data(level='[INFO]',
                        Message="In the beginning of function (post_NewBulkPayout) to insert new bulk payout into database",
                        merchantID=merchantId, functionName="post_NewBulkPayout")
        payoutId = uuid.uuid4()
        merchant_status = merchant_status_and_name['status']
        merchant_name = merchant_status_and_name['merchantname']
        DD_CSV_Earning = csv_report['doordash_earning']
        UE_CSV_Earning = csv_report['ubereats_earning']
        GH_CSV_Earning = csv_report['grubhub_earning']
        SF_CSV_Earning = csv_report['storefront_earning']
        Total_CSV_Earning = csv_report['total_all_earning']
        DD_dashboard_earning = dashboard_report['doordash_earning']
        UE_dashboard_earning = dashboard_report['ubereats_earning']
        GH_dashboard_earning = dashboard_report['GH_earning']
        SF_dashboard_earning = dashboard_report['storefront_earning']
        dashboard_payout_before_deduction = DD_dashboard_earning + UE_dashboard_earning + GH_dashboard_earning + SF_dashboard_earning
        create_log_data(level='[INFO]',
                        Message="Calculated Dashboard payout before deduction",
                        messagebody=f'{dashboard_payout_before_deduction}',
                        merchantID=merchantId, functionName="post_NewBulkPayout")
        Subscription_fee = dashboard_report['subscriptionAdjustments']
        fonda_share = -(dashboard_report['fonda_revenue_share'])
        dashboard_net_payout = dashboard_payout_before_deduction + Subscription_fee + fonda_share
        create_log_data(level='[INFO]',
                        Message="Calculated Dashboard payout after deduction",
                        messagebody=f'{dashboard_net_payout}',
                        merchantID=merchantId, functionName="post_NewBulkPayout")
        direct_payout = 0
        fonda_share_difference = Total_CSV_Earning - dashboard_payout_before_deduction - direct_payout
        create_log_data(level='[INFO]',
                        Message="Calculated Fonda Share difference",
                        messagebody=f'{fonda_share_difference}',
                        merchantID=merchantId, functionName="post_NewBulkPayout")
        fonda_share_percentage = ((fonda_share_difference/Total_CSV_Earning) * 100) if fonda_share_difference > 0.0 and Total_CSV_Earning >0.0 else 0.0
        create_log_data(level='[INFO]',
                        Message="Calculated Fonda Share difference percentage",
                        messagebody=f'{fonda_share_percentage}',
                        merchantID=merchantId, functionName="post_NewBulkPayout")
        fonda_share_trauncated =0.00
        if fonda_share_percentage > 0.0:
          fonda_share_trauncated = int(fonda_share_percentage * 100)/100.0
        data = (payoutId, merchantId,merchant_name,merchant_status, startDate, endDate, DD_CSV_Earning, UE_CSV_Earning, GH_CSV_Earning, SF_CSV_Earning, Total_CSV_Earning,
                DD_dashboard_earning,  GH_dashboard_earning,SF_dashboard_earning, UE_dashboard_earning,
                dashboard_payout_before_deduction, Subscription_fee, fonda_share, dashboard_net_payout,
                fonda_share_difference,fonda_share_trauncated)
        cursor.execute("""
                   INSERT INTO newBulkPayout (id,merchantid,restaurant_name,resturant_status,transaction_start_date,transaction_end_date,DD_CSV_Earning,UE_CSV_Earning,GH_CSV_Earning,SF_Earning,Total_CSV_Earning,DD_Dashboard_Earning,GH_Dashboard_Earning,SF_Dashboard_Earning,UE_Dashboard_Earning,dashboard_payout_before_deduction,subscription_fee,fonda_share,dashboard_net_payout,CSV_dashboard_net_payout_difference,payout_difference_percentage)
                     VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                   """, (*data,))
        connection.commit()
        create_log_data(level='[INFO]',
                        Message="Successfully inserted data in database",
                        merchantID=merchantId, functionName="post_NewBulkPayout")
    except Exception as e:
      print(f'Error post_NewBulkPayout {str(e)}')
      return False

  ############################################### GET
  
  @classmethod
  def get_payout_by_id(cls, payoutId):
    try:
      connection, cursor = get_db_connection()
      cursor.execute("""SELECT * FROM payouts WHERE id = %s""", (payoutId))
      row = cursor.fetchone()
      return row
    except Exception as e:
      print("Error get_payout_by_id : ", str(e))
      return False
  @classmethod
  def get_draft(cls,merchantId,utcStartDate,utcEndDate,newPayout=0):
    try:
      connection, cursor = get_db_connection()
      cursor.execute("""
         SELECT * FROM payoutdraft where merchantid=%s and start_date=%s and end_date=%s and newpayout=%s""",(merchantId,utcStartDate,utcEndDate,newPayout))
      row = cursor.fetchone()
      return row

    except Exception as e:
      print("Error: ", str(e))
      return False

  @classmethod
  def delete_draft(cls,merchantId,utcStartDate,utcEndDate):
    try:
      connection, cursor = get_db_connection()
      cursor.execute("""
         delete FROM payoutdraft where merchantid=%s and start_date=%s and end_date=%s """,(merchantId,utcStartDate,utcEndDate))
      connection.commit()
      return True

    except Exception as e:
      print("Error: ", str(e))
      return False

  @classmethod
  def delete_payout(cls, payoutId):
    try:
      connection, cursor = get_db_connection()
      cursor.execute("""
          delete FROM newpayouts where id=%s  """,payoutId)
      connection.commit()
      return True

    except Exception as e:
      print("Error: ", str(e))
      return False
  @classmethod
  def get_payouts(cls, merchantId, payoutId=None, startDate=None, endDate=None,MonthlyEmailStatus=0):
    try:
      connection, cursor = get_db_connection()

      WHERE = 1
      if payoutId:
        WHERE = f'`payouts`.`id` = "{payoutId}"'
      else:
        WHERE = f'`payouts`.`merchantid` = "{merchantId}"'

      if startDate and endDate:
        DATEFILTER = f'AND `payouts`.`created_datetime` BETWEEN "{startDate}" AND "{endDate}"'

      else:
        DATEFILTER = ''
      StatusFilter=f'AND `payouts`.`status` IN (1,2,3)'
      if MonthlyEmailStatus==1:
        StatusFilter = f'AND `payouts`.`status` IN (3)'
      cursor.execute(f"""
        SELECT 
            payouts.*,
            subscriptions.id subscriptionId,
            subscriptions.amount subscriptionAmount,
            subscriptions.status subscriptionStatus,
            date_format(subscriptions.date, '%%m-%%d-%%Y') subscriptionDate,
            subscriptions.frequency frequency,
            subscriptions.istrail istrail,
            merchants.merchantname merchantName,
            merchants.status merchantStatus,
            merchants.RevenueProcessingThreshold RevenueProcessingThreshold,
            

            IF(payouts.created_by IS NULL, NULL, (SELECT username FROM users WHERE users.id=payouts.created_by LIMIT 1)) as doneByUser,
            IF(payouts.updated_by IS NULL, NULL, (SELECT username FROM users WHERE users.id=payouts.updated_by LIMIT 1)) as revertedByName,
            IF(payouts.transferred_to_bank_by IS NULL, NULL, (SELECT username FROM users WHERE users.id=payouts.transferred_to_bank_by LIMIT 1)) as transferredToBankBy

          FROM payouts
          LEFT JOIN subscriptions ON payouts.id = subscriptions.payoutid
          LEFT JOIN merchants ON payouts.merchantid = merchants.id
          WHERE {WHERE} {StatusFilter}  {DATEFILTER}
          ORDER BY payouts.created_datetime DESC
      """, ())
      rows = cursor.fetchall()

      merchantTimezone = "UTC"
      if len(rows):
        cursor.execute("SELECT timezone FROM merchants WHERE id=%s", (rows[0].get("merchantid")))
        merchantTimezone = cursor.fetchone().get("timezone")

      payouts_list = list()

      for row in rows:
          
        exists = False
        for payout in payouts_list:
          if row['id'] == payout['id']:

            payout['subscriptions'].append({
              'id': row['subscriptionId'],
              'amount': row['subscriptionAmount'],
              'status': row['subscriptionStatus'],
              'date': row['subscriptionDate'],
              'frequency': row['frequency'],
              'istrail': row['istrail']
            })

            exists = True
            break
        
        if not exists:
          payouts_list.append({
            'id': row['id'],
            'merchantId': row['merchantid'],
            'merchantStatus': row['merchantStatus'],
            'RevenueProcessingThreshold': row['RevenueProcessingThreshold'],
            'startDate': datetime.date.isoformat(row['startdate']),
            'endDate': datetime.date.isoformat(row['enddate']),
            'status': row['status'],
            'numberOfOrders': row['numberoforders'],
            'subTotal': format(row['subtotal']),
            'staffTips': format(row['stafftips']),
            'tax': format(row['tax']),
            'processingFee': format(row['processingfee']),
            'promoDiscount': format(-abs(row['promodiscount'])),
            'commission': format(row['commission']),
            'squarefee': format(row['squarefee']),
            'marketingFee': format(row['marketingFee']),
            'marketplaceTax': format(row['marketplacetax']),
            'RevenueProcessingFee': format(row['RevenueProcessingFee']),
            'lifetime_total_revenue': row['lifetime_total_revenue'],
            'Revenue_processing_fee_Reason': row['Revenue_processing_fee_Reason'],
            'errorCharges': format(row['errorcharges']),
            'orderAdjustments': format(row['orderadjustments']),
            'subscriptionAdjustments': format(row['subscriptionadjustments']),
            'netPayout': format(row['netpayout']),
            'transferId': row['transferid'],
            'metadata': json.loads(row['metadata']) if row['metadata'] else {},
            'created_by': format(row['created_by']),
            'created_datetime': row['created_datetime'].replace(tzinfo=datetime.timezone.utc).astimezone(gettz(merchantTimezone)).strftime("%m-%d-%Y %H:%M:%S ")  + f"({merchantTimezone})",
            'reverted_by': row['updated_by'],
            'revertedDateTime': row['updated_datetime'].replace(tzinfo=datetime.timezone.utc).astimezone(gettz(merchantTimezone)).strftime("%m-%d-%Y %H:%M:%S ")  + f"({merchantTimezone})" if row["updated_datetime"] else None,
            'doneByUser': row['doneByUser'],
            'revertedByName': row['revertedByName'],
            'merchantName': row['merchantName'],
            'payoutType': row['payouttype'],
            'transferType': row['transfer_type'],
            'payoutAdjustments': format(row['payoutadjustments']),
            'commisionAdjustment': format(row['commisionAdjustment']),
            'doordash': format(row['doordash']),
            'ubereats': format(row['ubereats']),
            'grubhub': format(row['grubhub']),
            'storefront': format(row['storefront']),
            'others': format(row['others']),
            'remarks': row['remarks'],
            'transferredToBankBy': row['transferredToBankBy'],
            'transferredToBankTime': row['transferred_to_bank_time'],
            'bankName': row['bankname'],
            'accountHolderName': row['accountholdername'],
            'last4': row['last4'],
            'TotalRevenue': format(row['totalrevenue']),
            'TotalFondaPayout': format(row['totalfondapayout']),
            'FondaRevenue': format(row['fondarevenue']),
            'FondaRevenuePercentage': format(row['fondarevenuepercentage']),
            
            'subscriptions': [
              {
                'id': row['subscriptionId'],
                'amount': row['subscriptionAmount'],
                'status': row['subscriptionStatus'],
                'date': row['subscriptionDate'],
                'frequency': row['frequency'],
                'istrail': row['istrail']
              }
            ] if row['subscriptionId'] is not None else []
          })

      return payouts_list
    except Exception as e:
      print("Error: ", str(e))
      return unhandled()

  @classmethod
  def get_payout_type(cls, merchantId, payoutId=None, startDate=None, endDate=None, MonthlyEmailStatus=0):
    try:
      connection, cursor = get_db_connection()
      cursor.execute(f"""
          SELECT * FROM payouts WHERE merchantid=%s and id=%s
        """, (merchantId,payoutId))
      row = cursor.fetchone()

      if row is None:
        cursor.execute(f"""
                  SELECT * FROM newpayouts WHERE merchantid=%s and id=%s
                """, (merchantId,payoutId))
        row = cursor.fetchone()
        if row:
          return "Newpayout"
        return None
      return "Oldpayout"
    except Exception as e:
      print("Error: ", str(e))
      return None

  @classmethod
  def get_new_payout(cls, merchantId, payoutId=None, startDate=None, endDate=None, MonthlyEmailStatus=0):
    try:
      connection, cursor = get_db_connection()
      cursor.execute(f"""
                  SELECT * FROM newpayouts WHERE merchantid=%s and id=%s
                """, (merchantId, payoutId))
      row = cursor.fetchone()
      if row:
        return row
      return None
    except Exception as e:
      print("Error: ", str(e))
      return None

  @classmethod
  def get_new_payouts(cls, merchantId, payoutId=None, startDate=None, endDate=None, MonthlyEmailStatus=0):
    try:
      connection, cursor = get_db_connection()

      WHERE = 1
      if payoutId:
        WHERE = f'`newpayouts`.`id` = "{payoutId}"'
      else:
        WHERE = f'`newpayouts`.`merchantid` = "{merchantId}"'

      if startDate and endDate:
        DATEFILTER = f'AND `newpayouts`.`created_datetime` BETWEEN "{startDate}" AND "{endDate}"'

      else:
        DATEFILTER = ''
      StatusFilter = f'AND `newpayouts`.`status` IN (1,2,3)'
      if MonthlyEmailStatus == 1:
        StatusFilter = f'AND `newpayouts`.`status` IN (3)'
      cursor.execute(f"""
          SELECT 
              newpayouts.*,
              subscriptions.id subscriptionId,
              subscriptions.amount subscriptionAmount,
              subscriptions.status subscriptionStatus,
              date_format(subscriptions.date, '%%m-%%d-%%Y') subscriptionDate,
              subscriptions.frequency frequency,
              subscriptions.istrail istrail,
              merchants.merchantname merchantName,
              merchants.status merchantStatus,
              merchants.RevenueProcessingThreshold RevenueProcessingThreshold,


              IF(newpayouts.created_by IS NULL, NULL, (SELECT username FROM users WHERE users.id=newpayouts.created_by LIMIT 1)) as doneByUser,
              IF(newpayouts.updated_by IS NULL, NULL, (SELECT username FROM users WHERE users.id=newpayouts.updated_by LIMIT 1)) as revertedByName,
              IF(newpayouts.transferred_to_bank_by IS NULL, NULL, (SELECT username FROM users WHERE users.id=newpayouts.transferred_to_bank_by LIMIT 1)) as transferredToBankBy

            FROM newpayouts
            LEFT JOIN subscriptions ON newpayouts.id = subscriptions.payoutid
            LEFT JOIN merchants ON newpayouts.merchantid = merchants.id
            WHERE {WHERE} {StatusFilter}  {DATEFILTER}
            ORDER BY newpayouts.created_datetime DESC
        """, ())
      rows = cursor.fetchall()

      merchantTimezone = "UTC"
      if len(rows):
        cursor.execute("SELECT timezone FROM merchants WHERE id=%s", (rows[0].get("merchantid")))
        merchantTimezone = cursor.fetchone().get("timezone")

      payouts_list = list()

      for row in rows:

        exists = False
        for payout in payouts_list:
          if row['id'] == payout['id']:
            payout['subscriptions'].append({
              'id': row['subscriptionId'],
              'amount': row['subscriptionAmount'],
              'status': row['subscriptionStatus'],
              'date': row['subscriptionDate'],
              'frequency': row['frequency'],
              'istrail': row['istrail']
            })

            exists = True
            break

        if not exists:
          payouts_list.append({
            'id': row['id'],
            'merchantId': row['merchantid'],
            'merchantStatus': row['merchantStatus'],
            'RevenueProcessingThreshold': row['RevenueProcessingThreshold'],
            'startDate': datetime.date.isoformat(row['startdate']),
            'endDate': datetime.date.isoformat(row['enddate']),
            'status': row['status'],
            'numberOfOrders': row['numberoforders'],
            'subTotal': format(row['subtotal']),
            'staffTips': format(row['stafftips']),
            'tax': format(row['tax']),
            'processingFee': format(row['processingfee']),
            'promoDiscount': format(-abs(row['promodiscount'])),
            'commission': format(row['commission']),
            'squarefee': format(row['squarefee']),
            'marketingFee': format(row['marketingFee']),
            'marketplaceTax': format(row['marketplacetax']),
            'RevenueProcessingFee': format(row['RevenueProcessingFee']),
            'lifetime_total_revenue': row['lifetime_total_revenue'],
            'Revenue_processing_fee_Reason': row['Revenue_processing_fee_Reason'],
            'errorCharges': format(row['errorcharges']),
            'orderAdjustments': format(row['orderadjustments']),
            'subscriptionAdjustments': format(row['subscriptionadjustments']),
            'netPayout': format(row['netpayout']),
            'transferId': row['transferid'],
            'metadata': json.loads(row['metadata']) if row['metadata'] else {},
            'created_by': format(row['created_by']),
            'created_datetime': row['created_datetime'].replace(tzinfo=datetime.timezone.utc).astimezone(
              gettz(merchantTimezone)).strftime("%m-%d-%Y %H:%M:%S ") + f"({merchantTimezone})",
            'reverted_by': row['updated_by'],
            'revertedDateTime': row['updated_datetime'].replace(tzinfo=datetime.timezone.utc).astimezone(
              gettz(merchantTimezone)).strftime("%m-%d-%Y %H:%M:%S ") + f"({merchantTimezone})" if row[
              "updated_datetime"] else None,
            'doneByUser': row['doneByUser'],
            'revertedByName': row['revertedByName'],
            'merchantName': row['merchantName'],
            'payoutType': row['payouttype'],
            'transferType': row['transfer_type'],
            'payoutAdjustments': format(row['payoutadjustments']),
            'commisionAdjustment': format(row['commisionAdjustment']),
            'doordash': format(row['doordash']),
            'ubereats': format(row['ubereats']),
            'grubhub': format(row['grubhub']),
            'storefront': format(row['storefront']),
            'others': format(row['others']),
            'remarks': row['remarks'],
            'transferredToBankBy': row['transferredToBankBy'],
            'transferredToBankTime': row['transferred_to_bank_time'],
            'bankName': row['bankname'],
            'accountHolderName': row['accountholdername'],
            'last4': row['last4'],
            'TotalRevenue': format(row['totalrevenue']),
            'TotalFondaPayout': format(row['totalfondapayout']),
            'FondaRevenue': format(row['fondarevenue']),
            'FondaRevenuePercentage': format(row['fondarevenuepercentage']),

            'subscriptions': [
              {
                'id': row['subscriptionId'],
                'amount': row['subscriptionAmount'],
                'status': row['subscriptionStatus'],
                'date': row['subscriptionDate'],
                'frequency': row['frequency'],
                'istrail': row['istrail']
              }
            ] if row['subscriptionId'] is not None else []
          })

      return payouts_list
    except Exception as e:
      print("Error: ", str(e))
      return unhandled()


  @classmethod
  def get_monthly_payouts(cls, merchantId, startDate, endDate):
    try:
      connection, cursor = get_db_connection()
      merchant_details = Merchants.get_merchant_by_id(merchantId)
      date = f"""{startDate.day} - {endDate.day} {startDate.strftime("%B %Y")}"""

      startDate = startDate.replace(tzinfo=gettz(merchant_details["timezone"])).astimezone(datetime.timezone.utc)
      endDate = endDate.replace(tzinfo=gettz(merchant_details["timezone"])).astimezone(
        datetime.timezone.utc) + datetime.timedelta(days=1)

      cursor.execute(f"""
          SELECT 
             sum(numberoforders) as numberoforders,sum(subtotal) as subtotal,sum(tax) as tax,sum(commission) as commission,sum(processingfee) as processingfee,sum(errorcharges) as errorcharges
             ,sum(squarefee) as squarefee,sum(marketingFee) as marketingFee,sum(orderadjustments) as orderadjustments,sum(marketplacetax) as marketplacetax,
             sum(RevenueProcessingFee) as RevenueProcessingFee,sum(payoutadjustments) as payoutadjustments,sum(stafftips) as stafftips,sum(subscriptionadjustments) as subscriptionadjustments
             ,sum(netpayout) as netpayout,sum(commisionAdjustment) as commisionAdjustment ,sum(promodiscount) as promodiscount 
               FROM dashboard.payouts
           WHERE merchantid =%s  AND status=3 AND created_datetime BETWEEN %s AND %s
       """, (merchantId, startDate, endDate))
      rows = cursor.fetchall()
      print("total payouts records " , rows)
      if rows[0]['numberoforders']==None or rows[0]['subtotal']==None:
        print("none total payouts records ", rows)
        return False
      payouts_list = list()
      cursor.execute(f"""
                    SELECT 
                         startdate,enddate
                         FROM dashboard.payouts
                     WHERE merchantid =%s  AND status=3 AND created_datetime BETWEEN %s AND %s
                 """, (merchantId, startDate, endDate))
      payout_dates = cursor.fetchall()

      for row in rows:
        payouts_list.append({

          'numberOfOrders': row['numberoforders'],
          'subTotal': format(row['subtotal']),
          'staffTips': format(row['stafftips']),
          'tax': format(row['tax']),
          'processingFee': format(row['processingfee']),

          'commission': format(row['commission']),
          'squarefee': format(row['squarefee']),
          'marketingFee': format(row['marketingFee']),
          'marketplaceTax': format(row['marketplacetax']),
          'RevenueProcessingFee': float(row['RevenueProcessingFee']),

          'errorCharges': format(row['errorcharges']),
          'orderAdjustments': format(row['orderadjustments']),
          'subscriptionAdjustments': format(row['subscriptionadjustments']),
          'netPayout': format(row['netpayout']),

          'promodiscount': format(row['promodiscount']),
          'commisionAdjustment': format(row['commisionAdjustment']),
          'stafftips': format(row['stafftips']),

          'payoutAdjustments': format(row['payoutadjustments']),
          'date': date,
          "payout_dates":payout_dates

        })

      return payouts_list
    except Exception as e:
      print("Error: ", str(e))
      return False
  @classmethod
  def get_SendGridEmailSummary(cls, merchantId):
    try:
      connection, cursor = get_db_connection()
      # cursor.execute(f"""
      #     SELECT * from sendgridemailsummary where merchantId=%s order by datetime
      #   """, [merchantId])
      cursor.execute(f"""
             SELECT * FROM sendgridemailsummary WHERE merchantId = %s ORDER BY datetime desc  ;
           """, [merchantId])
      rows = cursor.fetchall()

      sendgridemail_list = list()
      merchantTimezone='US/Pacific'


      for row in rows:

        sendgridemail_list.append({
          'id': row['id'],
          'merchantId': row['merchantId'],
          'msg_id': row['msg_id'],
          'to_email': row['to_email'],
          'event': row['event'].capitalize(),
          'datetime': datetime.datetime.isoformat(row['datetime']),
          'subject': row['subject'],
          'processed_datetime': datetime.datetime.isoformat(row['processed_datetime'])
            })

      return sendgridemail_list
    except Exception as e:
      print("Error: ", str(e))
      return unhandled()


  @classmethod
  def get_bulk_payouts(cls):
    try:
      connection, cursor = get_db_connection()
      cursor.execute("""
        SELECT 
          payouts.*, merchants.merchantname merchantName , merchants.RevenueProcessingThreshold RevenueProcessingThreshold
          FROM payouts
          LEFT JOIN merchants ON payouts.merchantid = merchants.id
          WHERE payouts.status = 0 and merchants.status=1""")
      rows = cursor.fetchall()
      payouts_list = list()
      for row in rows:
        payouts_list.append({
          'id': row['id'],
          'merchantId': row['merchantid'],
          'startDate': datetime.date.isoformat(row['startdate']),
          'endDate': datetime.date.isoformat(row['enddate']),
          'status': row['status'],
          'numberOfOrders': row['numberoforders'],
          'subTotal': format(row['subtotal']),
          'staffTips': format(row['stafftips']),
          'tax': format(row['tax']),
          'processingFee': format(row['processingfee']),
          'promoDiscount': format(-abs(row['promodiscount'])),
          'commission': format(row['commission']),
          'RevenueProcessingFee': format(row['RevenueProcessingFee']),
          'squarefee': format(row['squarefee']),
          'marketplaceTax': format(row['marketplacetax']),
          'errorCharges': format(row['errorcharges']),
          'orderAdjustments': format(row['orderadjustments']),
          'subscriptionAdjustments': format(row['subscriptionadjustments']),
          'netPayout': format(row['netpayout']),
          'transferId': row['transferid'],
          'metadata': json.loads(row['metadata']) if row['metadata'] else {},
          'created_by': format(row['created_by']),
          'payouttype': format(row['payouttype']),
          'created_datetime': row['created_datetime'].replace(tzinfo=datetime.timezone.utc).astimezone(gettz("UTC")).isoformat(),
          'merchantName': row['merchantName'],
          'RevenueProcessingThreshold': row['RevenueProcessingThreshold'],
          'Revenue_processing_fee_Reason': row['Revenue_processing_fee_Reason']
        })
      return payouts_list
    except Exception as e:
      print("Error: ", str(e))
      return False


  @classmethod
  def get_new_bulk_payouts(cls):
    try:
      connection, cursor = get_db_connection()
      cursor.execute("""
        SELECT * FROM newBulkPayout""")
      rows = cursor.fetchall()
      payouts_list = list()
      for row in rows:
        payouts_list.append({
          'id': row['id'],
          'merchantId': row['merchantid'],
          'startDate': datetime.date.isoformat(row['transaction_start_date']),
          'endDate': datetime.date.isoformat(row['transaction_end_date']),
          'status': row['resturant_status'],
          'resturantName': row['restaurant_name'],
          'DD_CSV_Earning': format(row['DD_CSV_Earning']),
          'UE_CSV_Earning': format(row['UE_CSV_Earning']),
          'GH_CSV_Earning': format(row['GH_CSV_Earning']),
          'SF_Earning': format(row['SF_Earning']),
          'SF_Dashboard_Earning': format(row['SF_Dashboard_Earning']),
          'Total_CSV_Earning': format(row['Total_CSV_Earning']),
          'DD_Dashboard_Earning': format(row['DD_Dashboard_Earning']),
          'GH_Dashboard_Earning': format(row['GH_Dashboard_Earning']),
          'UE_Dashboard_Earning': format(row['UE_Dashboard_Earning']),
          'dashboard_payout_before_deduction': format(row['dashboard_payout_before_deduction']),
          'subscription_fee': format(row['subscription_fee']),
          'fonda_share': format(row['fonda_share']),
          'dashboard_net_payout': format(row['dashboard_net_payout']),
          'CSV_dashboard_net_payout_difference': format(row['CSV_dashboard_net_payout_difference']),
          'payout_difference_percentage': format(row['payout_difference_percentage']),
        })
      return payouts_list
    except Exception as e:
      print("Error: ", str(e))
      return False


  @classmethod
  def get_active_merchants(cls):
    try:
      connection, cursor = get_db_connection()
      cursor.execute("""
         SELECT * FROM merchants where status=1""")
      rows = cursor.fetchall()
      counts=len(rows)
      return counts
    except Exception as e:
      print("Error: ", str(e))
      return False

  @classmethod
  def get_merchants(cls):
    try:
      connection, cursor = get_db_connection()
      cursor.execute("""
           SELECT * FROM merchants""")
      rows = cursor.fetchall()
      counts = len(rows)
      return counts
    except Exception as e:
      print("Error get_merchants: ", str(e))
      return False

  ############################################### UPDATE

  @classmethod
  def update_payout_status(cls, payoutId, status, userId=None):
    try:
      # status:- 2 = reverted, 1 = paid,
      connection, cursor = get_db_connection()

      cursor.execute("""UPDATE payouts SET status=%s, updated_by=%s, updated_datetime=CURRENT_TIMESTAMP WHERE id=%s""", (status, userId, payoutId))
      connection.commit()
      return True
    except Exception as e:
      print("Error: ", str(e))
      return False

  @classmethod
  def update_payout_status_transfer_bank(cls, payoutId, bankName, accountHolderName,last4, userId=None):
    try:

      connection, cursor = get_db_connection()

      cursor.execute("""UPDATE payouts SET status=3, transferred_to_bank_by=%s, transferred_to_bank_time=CURRENT_TIMESTAMP, bankname=%s, accountholdername=%s, last4=%s WHERE id=%s""", ( userId, bankName,accountHolderName,last4, payoutId,))

      connection.commit()
      return True
    except Exception as e:
      print("Error: ", str(e))
      return False
  ############################################### DELETE

  @classmethod
  def update_transaction_payoutid_payoutDate(cls,payoutId,transafer_type=None):
    try:

      connection, cursor = get_db_connection()
      cursor.execute(""" SELECT * FROM newpayouts WHERE id=%s""",(payoutId))
      row = cursor.fetchone()
      if row:
          startDate = row['startdate']
          endDate = row['enddate']
          merchantID = row['merchantid']
          endDate_one_day_after = endDate + datetime.timedelta(days=1)
          if transafer_type and transafer_type == 3:
            transfered_to_bank_time = row['created_datetime']
          else:
            transfered_to_bank_time = row['transferred_to_bank_time']

          cursor.execute(
            """UPDATE doordashtransaction SET transfered_payoutid=%s, transferred_payout_date=%s WHERE merchantid=%s AND dateandtime BETWEEN %s AND %s AND locked=1""",
            (payoutId, transfered_to_bank_time, merchantID, startDate, endDate_one_day_after))

          cursor.execute(
            """UPDATE ubereatstransaction SET transfered_payoutid=%s, transferred_payout_date=%s WHERE merchantid=%s AND Transactiondate BETWEEN %s AND %s AND locked=1""",
            (payoutId, transfered_to_bank_time, merchantID, startDate, endDate))

          cursor.execute(
            """UPDATE ghrubhubtransaction SET transfered_payoutid=%s, transferred_payout_date=%s WHERE merchantid=%s AND Dateandtime BETWEEN %s AND %s AND locked=1""",
            (payoutId, transfered_to_bank_time, merchantID, startDate, endDate_one_day_after))

          cursor.execute(
            """UPDATE storefronttransaction SET transfered_payoutid=%s, transferred_payout_date=%s WHERE merchantid=%s AND dateandtime BETWEEN %s AND %s AND locked=1""",
            (payoutId, transfered_to_bank_time, merchantID, startDate, endDate_one_day_after))

          connection.commit()
          return True
      return False
    except Exception as e:
      print("Error: update_transaction_payoutid_payoutDate ", str(e))
      return False






  @classmethod
  def delete_bulk_payout_entries(cls):
    '''
      Delete entries of bulk payout whose status = 0
    '''
    try:
      connection, cursor = get_db_connection()
      cursor.execute("""DELETE FROM payouts WHERE status = 0""")
      connection.commit()
      return True
    except Exception as e:
      print("Error: ", str(e))
      return False

  @classmethod
  def delete_new_bulk_payout_entries(cls):
    '''
      Delete entries of new bulk payout
    '''
    try:
      connection, cursor = get_db_connection()
      cursor.execute("""DELETE FROM newBulkPayout""")
      connection.commit()
      return True
    except Exception as e:
      print("Error: ", str(e))
      return False

  @classmethod
  def post_draft(cls, merchantId, data):
    try:
      connection, cursor = get_db_connection()
      merchant_details = Merchants.get_merchant_by_id(merchantId)
      commission_adjustment = data.get("commission_adjustment",0)
      marketing_fee = data.get("marketing_fee",0)
      doordash = float(data.get("doordash",0))
      ubereats = data.get("ubereats",0)
      grubhub = data.get("grubhub",0)
      storefront = data.get("storefront",0)
      others = data.get("others",0)
      payout_adjustment = data.get("payout_adjustment",0)
      newPayout = data.get("newPayout",0)
      remarks = data.get("remarks",'')
      # datetime to utc formatting
      startDate = datetime.datetime.strptime(data.get("startDate"), "%Y-%m-%d")
      endDate = datetime.datetime.strptime(data.get("endDate"), "%Y-%m-%d")

      utcStartDate = startDate.replace(tzinfo=gettz(merchant_details["timezone"])).astimezone(datetime.timezone.utc)
      utcEndDate = endDate.replace(tzinfo=gettz(merchant_details["timezone"])).astimezone(
        datetime.timezone.utc) + datetime.timedelta(days=1)
      draft_payouts = Payouts.get_draft(merchantId, utcStartDate, utcEndDate,newPayout)
      if draft_payouts:
        cursor.execute("""
                            update payoutdraft set remarks=%s, commission_adjustment=%s,marketing_fee=%s,payout_adjustment=%s,doordash=%s,ubereats=%s,grubhub=%s,storefront=%s,others=%s where merchantid=%s and start_date=%s and end_date=%s and newpayout=%s"""
                       , ( remarks,commission_adjustment, marketing_fee, payout_adjustment,doordash,ubereats,grubhub,storefront,others, merchantId, utcStartDate, utcEndDate,newPayout))
      else:
        draft_id = uuid.uuid4()
        cursor.execute("""
                        INSERT INTO payoutdraft (remarks,id, commission_adjustment,marketing_fee,payout_adjustment,merchantid,start_date,end_date,doordash,ubereats,grubhub,storefront,others,newpayout)
                          VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s,%s)
                        """, (remarks,draft_id,commission_adjustment,marketing_fee,payout_adjustment,merchantId,utcStartDate,utcEndDate,doordash,ubereats,grubhub,storefront,others,newPayout))
      connection.commit()


    except Exception as e:
      print("Error: ", str(e))
      return False

  @classmethod
  def downloadTransferHistory(cls, merchantId, data=None, startDate=None, endDate=None, MonthlyEmailStatus=0):
    try:
      merchant_details = Merchants.get_merchant_by_id(merchantId)

      # temp_file_name='test.csv'
      # temp_file_path = 'D:\\pycharm project\\Dashboard-API\\templates\\tmp\\test.csv'

      temp_file_name = merchant_details['merchantname'] + "_transfer_history.csv"
      temp_file_path = "/tmp/" + temp_file_name

      if MonthlyEmailStatus==0:
        startDate = datetime.datetime.strptime(data.get("startDate"), "%Y-%m-%d")
        endDate = datetime.datetime.strptime(data.get("endDate"), "%Y-%m-%d")

      startDate = startDate.replace(tzinfo=gettz(merchant_details["timezone"])).astimezone(datetime.timezone.utc)
      endDate = endDate.replace(tzinfo=gettz(merchant_details["timezone"])).astimezone(datetime.timezone.utc) + datetime.timedelta(days=1)

      payouts = cls.get_payouts(merchantId, startDate=startDate, endDate=endDate,MonthlyEmailStatus=MonthlyEmailStatus)
      print(payouts)

      transfer_history = []

      for history in payouts:
        subscriptions = history['subscriptions']
        all_dates = ''
        for subscription in subscriptions:
          all_dates = subscription['date']+", "+ all_dates

        transfer_history.append({
          "startDate": history['startDate'],
          "endDate": history['endDate'],
          "created_datetime": history['created_datetime'],
          "doneByUser": history['doneByUser'],
          "numberOfOrders": history['numberOfOrders'],
          "subTotal": history['subTotal'],
          "tax": history['tax'],
          "commission": history['commission'],
          "squarefee": history['squarefee'],
          "marketingFee": history['marketingFee'],
          "processingFee": history['processingFee'],
          "promoDiscount": -abs(float(history['promoDiscount'])),
          "errorCharges": history['errorCharges'],
          "staffTips": history['staffTips'],
          "orderAdjustments": history['orderAdjustments'],
          "marketplaceTax": history['marketplaceTax'],
          "payoutType": history['payoutType'],
          "payoutAdjustments": history['payoutAdjustments'],
          "remarks": history['remarks'],
          "subscriptionAdjustments": history['subscriptionAdjustments'],
          "subscriptions": all_dates,
          "netPayout": history['netPayout'],
          "status": history['status'],
          "transferredToBankTime": history['transferredToBankTime'],
          "RevenueProcessingFee": float(history['RevenueProcessingFee']),
          "commisionAdjustment": history['commisionAdjustment'],
          "revertedDateTime": history['revertedDateTime'],
          "bankName": history['bankName'],
          "accountHolderName": history['accountHolderName'],
          "last4": history['last4']

        })

      with open(temp_file_path, mode='w', newline='') as temp_csv:
        writer = csv.writer(temp_csv, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        writer.writerow(["From Date", "To Date", "Payout Created Date & Time", "Payout Done By", "Number of Orders", "Sub Total", "Tax", "Commission",
                      "Square Fee", "Marketing Fee", "Processing Fee", "Revenue Processing Fee","Promo Discount","Error Charges","Staff Tips","Order Adjustments","Market Price Facilitator Tax","Payout Type","Payout Adjustments",
                      "Payout Remarks","commisionAdjustment","Subscription Adjustments","Subscription Date(s)","Net Payout", "Status"])
        for row in transfer_history:
          transfer_status = ''
          if row['RevenueProcessingFee'] > 0:
            row['RevenueProcessingFee']=-row['RevenueProcessingFee']

          if row['status'] == 0:
            transfer_status = "Yet to Transfer"
          elif row['status'] == 1:
            transfer_status = "Transferred"

          elif row['status'] == 2:
            user = ''
            time = ''
            if row['doneByUser']:
              user = row['doneByUser']

            if row['revertedDateTime']:
              time = row['revertedDateTime']

            transfer_status = "Reverted by "  + user + " on " + time
          elif row['status'] == 3:
            user = ''
            time = ''
            if row['doneByUser']:
              user = row['doneByUser']

            if row['transferredToBankTime']:
              time = row['transferredToBankTime']

            if row['bankName'] and row['accountHolderName'] and row['last4']:
              transfer_status = f"Paid out to bank {row['bankName']}, account holder {row['accountHolderName']} with account number ********{row['last4']} by  {user} on  {time}"

          writer.writerow([
            row['startDate'],
            row['endDate'],
            row['created_datetime'],
            row['doneByUser'],
            row['numberOfOrders'],
            row['subTotal'],
            row['tax'],
            row['commission'],
            row['squarefee'],
            row['marketingFee'],
            row['processingFee'],
            row['RevenueProcessingFee'],
            row['promoDiscount'],
            row['errorCharges'],
            row['staffTips'],
            row['orderAdjustments'],
            row['marketplaceTax'],
            row['payoutType'],
            row['payoutAdjustments'],
            row['remarks'],
            row['commisionAdjustment'],
            row['subscriptionAdjustments'],
            row['subscriptions'],
            row['netPayout'],
            transfer_status
          ])
      if MonthlyEmailStatus == 1:
        return temp_file_path
      s3 = boto3.client('s3')
      s3.upload_file(
        temp_file_path,
        s3_apptopus_bucket,
        f"{config.s3_reports_folder}/{temp_file_name}",
        ExtraArgs={
          "ACL": "public-read"
        }
      )

      # get s3 url of csv
      download_url = s3.generate_presigned_url(
        ClientMethod='get_object',
        Params={
          'Bucket': s3_apptopus_bucket,
          'Key': f"{config.s3_reports_folder}/{temp_file_name}"
        }
      )
      download_url = download_url.split("?")[0]
      print(download_url)


      # send email to the user
      emails = data.get("email")
      if not (emails and isinstance(emails, list)):
        emails = [
          config.reports_email_address
        ]
      emails = list(filter(None, emails))
      resp, msg = send_financial_report_email(merchant_details['merchantname'], "Transfer Summary Report for Merchant",
                                       "financial summary", emails, download_url)
      print(emails)
      # resp2 = Merchants.update_email_distribution_list(merchantId=merchantId, emails=";".join(emails))

      return True
    except Exception as e:
      print("Error: ", str(e))
      return False


  def get_emailDistributionList(merchantId):
    connection, cursor = get_db_connection()
    cursor.execute("SELECT emaildistributionlist FROM merchants WHERE id=%s", merchantId)
    emails = cursor.fetchall()
    return emails

  @classmethod
  def update_to_test_data(cls):
   try:
    connection, cursor = get_db_connection()
    ###################################    Replace all merchant phone numbers with test phone number and emaildistributionlist

    cursor.execute("UPDATE merchants SET emaildistributionlist = 'hammad@paalam.co.uk;fondaabc@gmail.com', phone = '12345678', businessnumber = '12345678' ")

    ####################################    Remove all merchant Stripe connect account details

    cursor.execute("update merchants set  bankaccountroutingnumber='', bankaccountnumber=''")


    ####################################    Replace all merchant emails with test emails
    cursor.execute("SELECT * FROM merchants")
    rows = cursor.fetchall()
    num=1
    for row in rows:
      print("num is. " , num)
      email=f"""hammad{num}@test.com"""
      num=num+1
      cursor.execute("update merchants set stripeaccountid=%s ,email=%s WHERE id=%s", (num,email,row["id"]))

    ###################################   trucncate/remove platform connectivity tokens for all the merchants
    cursor.execute("TRUNCATE TABLE platforms")
    cursor.execute("TRUNCATE TABLE websockets")
    connection.commit()
    return True
   except Exception as e:
     print("Error: ", str(e))
     return False