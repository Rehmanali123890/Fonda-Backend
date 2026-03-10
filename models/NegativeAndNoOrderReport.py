from utilities.helpers import *
from decimal import Decimal
import csv
from io import StringIO
from flask import jsonify
import os
import json
from datetime import datetime, timedelta

class NegativeAndNoOrderReport:


  @classmethod
  def get_negative_transaction_text(cls):
    try:
      connection, cursor = get_db_connection()
      cursor.execute("""SELECT * FROM config_master where config_type='Negative_transaction_text'""", )
      negative_text = cursor.fetchone()
      return negative_text['config_value']
    except Exception as e:
      print(f"Error while processing Storefront data: {e}")
      raise

  @classmethod
  def get_no_order_transaction_text(cls):
    try:
      connection, cursor = get_db_connection()
      cursor.execute("""SELECT * FROM config_master where config_type='No_order_transaction_text'""", )
      no_order_text = cursor.fetchone()
      return no_order_text['config_value']
    except Exception as e:
      print(f"Error while processing Storefront data: {e}")
      raise

  @classmethod
  def process_doordash_data(cls, doordash_transactions, header_mapping, headers, sheet):
    """
    Processes the Doordash transactions and appends rows directly to the provided sheet.
    """
    try:
      create_log_data(
        level="[INFO]",
        Message=f"In the start of function to process doordash transactions",
        messagebody=f'Doordash transactions are {doordash_transactions}',
        functionName="process_doordash_data"
      )
      for transaction in doordash_transactions:
        row = {}
        temp_net_earning = 0.00
        temp_platform_net_earning = 0.00
        for header in headers:
          if header == "Platform":
            row[header] = "Doordash"
          elif header == "Fonda Earning":
            credit = float(transaction.get("Credit") or 0.00)
            debit = float(transaction.get("Debit") or 0.00)
            temp_net_earning = credit - debit
            row[header] = f"{temp_net_earning:.2f}"
          elif header == "Platform Earning":
            platform_credit = float(transaction.get("Platform_credit") or 0.00)
            platform_debit = float(transaction.get("Platform_debit") or 0.00)
            temp_platform_net_earning = platform_credit - platform_debit
            row[header] = f"{temp_platform_net_earning:.2f}"
          elif header == "Platform Commission":
            platform_comission = float(transaction.get("Platform_comission") or 0.00)
            row[header] = f"{-platform_comission:.2f}"
          elif header == "Platform Commission Tax":
            platform_comission_tax = float(transaction.get("Platform_comissiontax") or 0.00)
            row[header] = f"{-platform_comission_tax:.2f}"
          elif header == "Difference":
            net_diff = temp_net_earning - temp_platform_net_earning
            row[header] = f"{net_diff:.2f}"
          elif header == "Transaction Type":
            row[header] = 'Order'
          elif header == 'Reason':
            if temp_platform_net_earning !=0.00:
              row[header] = cls.get_negative_transaction_text()
            else:
              row[header] = cls.get_no_order_transaction_text()
          else:
            field = header_mapping.get(header)
            value = transaction.get(field) if field else 0.00

            if isinstance(value, datetime):
              value = value.strftime('%Y-%m-%d %H:%M:%S')
            elif isinstance(value, (int, float, Decimal)):
              value = f"{float(value):.2f}"
            elif value in [None, ""]:
              value = "0.00"
            row[header] = value
        sheet.append([row.get(header, "0.00") for header in headers])

      create_log_data(
        level="[INFO]",
        Message=f"Successfully append doordash transactions data in sheet",
        functionName="process_doordash_data"
      )
      return sheet
    except Exception as e:
      print(f"Error while processing Doordash data: {e}")
      create_log_data(
        level="[ERROR]",
        Message=f"Error in processing Doordash transactions",
        messagebody=f'Error {str(e)}',
        functionName="process_doordash_data"
      )
      raise

  @classmethod
  def process_ubereats_data(cls, ubereats_transactions, header_mapping, headers, sheet):
    """
    Processes the UberEats transactions and appends rows directly to the provided sheet.
    """
    try:
      create_log_data(
        level="[INFO]",
        Message=f"In the start of function to process ubereats transactions",
        messagebody=f'Ubereats transactions are {ubereats_transactions}',
        functionName="process_ubereats_data"
      )
      print("------------------")
      for txn in ubereats_transactions:
        row = {}
        platform_payout = 0.00
        for header in headers:
          if header == "Platform":
            row[header] = "UberEats"
          elif header == "Subtotal":
            sales = float(txn.get("Salesexcltax") or 0.00)
            refunds = float(txn.get("Refundsexcltax") or 0.00)
            row[header] = f"{sales + refunds:.2f}"
          elif header == "Tax":
            tax_sales = float(txn.get("Taxonsales") or 0.00)
            tax_refunds = float(txn.get("Taxonrefunds") or 0.00)
            row[header] = f"{tax_sales + tax_refunds:.2f}"
          elif header == "Platform Subtotal":
            platform_sales = float(txn.get("Platform_salesexcltax") or 0.00)
            platform_refunds = float(txn.get("Platform_refundsexcltax") or 0.00)
            row[header] = f"{platform_sales + platform_refunds:.2f}"
          elif header == "Platform Tax":
            platform_sales = float(txn.get("Platform_taxonsales") or 0.00)
            platform_refunds = float(txn.get("Platform_refundsexcltax") or 0.00)
            row[header] = f"{platform_sales + platform_refunds:.2f}"
          elif header == "Transaction Type":
            row[header] = "Order"
          elif header == "Difference":
            payout = float(txn.get("Totalpayout") or 0.00)
            platform_payout = float(txn.get("Platform_totalpayout") or 0.00)
            row[header] = f"{payout - platform_payout:.2f}"
          elif header == 'Reason':
            if platform_payout !=0.00:
              row[header] = cls.get_negative_transaction_text()
            else:
              row[header] = cls.get_no_order_transaction_text()
          else:
            field = header_mapping.get(header)
            value = txn.get(field) if field else 0.00

            if isinstance(value, datetime):
              value = value.strftime('%Y-%m-%d %H:%M:%S')
            elif isinstance(value, (int, float, Decimal)):
              value = f"{float(value):.2f}"
            elif value in [None, ""]:
              value = "0.00"
            row[header] = value

        sheet.append([row.get(header, "0.00") for header in headers])

      create_log_data(
        level="[INFO]",
        Message=f"Successfully append ubereats transactions data in sheet",
        functionName="process_ubereats_data"
      )
      return sheet
    except Exception as e:
      print(f"Error while processing UberEats data: {e}")
      create_log_data(
        level="[ERROR]",
        Message=f"Error in processing ubereats transactions",
        messagebody=f'Error {str(e)}',
        functionName="process_ubereats_data"
      )
      raise

  @classmethod
  def process_grubhub_data(cls, grubhub_transactions, header_mapping, headers, sheet):
    """
    Processes the Grubhub transactions and appends rows directly to the provided sheet.
    """
    try:
      create_log_data(
        level="[INFO]",
        Message=f"In the start of function to process grubhub transactions",
        messagebody=f'Grubhub transactions are {grubhub_transactions}',
        functionName="process_grubhub_data"
      )
      print("------------------")
      for txn in grubhub_transactions:
        row = {}
        fonda_earning = 0.00
        platform_earning = 0.00
        for header in headers:
          if header == "Platform":
            row[header] = "Grubhub"
          elif header == "Fonda Earning":
            restaurant_total = float(txn.get("Restauranttotal") or 0.00)
            commission = float(txn.get("Commission") or 0.00)
            delivery_commission = float(txn.get("Deliverycommission") or 0.00)
            processing_fee = float(txn.get("Processingfee") or 0.00)
            targeted_promotion = float(txn.get("Targetedpromotion") or 0.00)
            fonda_earning = restaurant_total + commission + delivery_commission + processing_fee + targeted_promotion
            row[header] = f"{fonda_earning:.2f}"
          elif header == "Platform Earning":
            platform_restaurant_total = float(txn.get("Platform_restauranttotal") or 0.00)
            platform_commission = float(txn.get("Platform_commission") or 0.00)
            platform_delivery_commission = float(txn.get("Platform_deliverycommission") or 0.00)
            platform_processing_fee = float(txn.get("Platform_processingfee") or 0.00)
            platform_targeted_promotion = float(txn.get("Platform_targetedpromotion") or 0.00)
            platform_earning = platform_restaurant_total + platform_commission + platform_delivery_commission + platform_processing_fee + platform_targeted_promotion
            row[header] = f"{platform_earning:.2f}"
          elif header == "Difference":
            net_earning = fonda_earning - platform_earning
            row[header] = f"{net_earning:.2f}"
          elif header == "Transaction Type":
            row[header] = "Order"
          elif header == 'Reason':
            if platform_earning !=0.00:
              row[header] = cls.get_negative_transaction_text()
            else:
              row[header] = cls.get_no_order_transaction_text()
          else:
            field = header_mapping.get(header)
            value = txn.get(field) if field else 0.00

            if isinstance(value, datetime):
              value = value.strftime('%Y-%m-%d %H:%M:%S')
            elif isinstance(value, (int, float, Decimal)):
              value = f"{float(value):.2f}"
            elif value in [None, ""]:
              value = "0.00"
            row[header] = value

        sheet.append([row.get(header, "0.00") for header in headers])

      create_log_data(
        level="[INFO]",
        Message=f"Successfully append grubhub transactions data in sheet",
        functionName="process_grubhub_data"
      )
      return sheet
    except Exception as e:
      print(f"Error while processing Grubhub data: {e}")
      create_log_data(
        level="[ERROR]",
        Message=f"Error in processing Grubhub transactions",
        messagebody=f'Error {str(e)}',
        functionName="process_grubhub_data"
      )
      raise

  @classmethod
  def process_storefront_data(cls, storefront_transactions, header_mapping, headers, sheet):
    """
    Processes the UberEats transactions and appends rows directly to the provided sheet.
    """
    try:
      create_log_data(
        level="[INFO]",
        Message=f"In the start of function to process storefront transactions",
        messagebody=f'Storefront transactions are {storefront_transactions}',
        functionName="process_storefront_data"
      )
      print("------------------")
      for txn in storefront_transactions:
        row = {}
        platform_earning = 0.00
        fonda_earning = 0.00
        for header in headers:
          if header == "Platform":
            row[header] = "Storefront"
          elif header == "Fonda Earning":
            fonda_earning = float(txn.get("ordertotal") or 0.00)
            row[header] = f"{fonda_earning:.2f}"
          elif header == "Platform Earning":
            stripe_amount = float(txn.get("stripe_amount") or 0.00)
            stripe_fee = float(txn.get("stripe_fee") or 0.00)
            platform_earning = stripe_amount - stripe_fee
            row[header] = f"{platform_earning:.2f}"
          elif header == "Transaction Type":
            row[header] = 'Order'
          elif header == "Promo Discount":
            promodiscount = float(txn.get("promodiscount") or 0.00)
            row[header] = f"{-promodiscount:.2f}"
          elif header == "Difference":
            net_diff = fonda_earning - platform_earning
            row[header] = f"{net_diff:.2f}"
          elif header == 'Reason':
            if platform_earning !=0.00:
              row[header] = cls.get_negative_transaction_text()
            else:
              row[header] = cls.get_no_order_transaction_text()
          else:
            field = header_mapping.get(header)
            value = txn.get(field) if field else 0.00
            if isinstance(value, datetime):
              value = value.strftime('%Y-%m-%d %H:%M:%S')
            elif isinstance(value, (int, float)):
              value = f"{float(value):.2f}"
            elif value in [None, ""]:
              value = "0.00"
            row[header] = value
        sheet.append([row.get(header, "0.00") for header in headers])

      create_log_data(
        level="[INFO]",
        Message=f"Successfully append storefront transactions data in sheet",
        functionName="process_storefront_data"
      )
      return sheet
    except Exception as e:
      print(f"Error while processing Storefront data: {e}")
      create_log_data(
        level="[ERROR]",
        Message=f"Error in processing storefront transactions",
        messagebody=f'Error {str(e)}',
        functionName="process_storefront_data"
      )
      raise
      # return jsonify({"error": f"Error: {e}"}), 500


  @classmethod
  def merge_transactions(cls, negative_transactions, no_order_transactions):
    """
    Merges the negative and no order transactions.
    Returns a tuple of all transactions, handling the case where both are None.
    """
    try:
      if negative_transactions and no_order_transactions:
        return negative_transactions + no_order_transactions
      elif negative_transactions:
        return negative_transactions
      elif no_order_transactions:
        return no_order_transactions
      else:
        return []
    except Exception as e:
      print(f"Error while merging transactions: {e}")
      raise

  @classmethod
  def generate_workbook_sheet(cls,Platform, **kwargs):
    try:
      create_log_data(
        level="[INFO]",
        Message=f"In the start of function to generate sheet for negative and no order transactions. Platform: {Platform}",
        functionName="generate_workbook_sheet"
      )
      import openpyxl
      from app import translate_text
      connection, cursor = get_db_connection()
      cursor.execute("""SELECT * FROM config_master where config_type='Problematic_report_header'""",)
      Headers = cursor.fetchone()
      Headers = Headers['config_value'] if isinstance(Headers['config_value'], list) else eval(Headers['config_value'])

      create_log_data(
        level="[INFO]",
        Message=f"Fetched Problematic report headers {Headers}",
        functionName="generate_workbook_sheet"
      )

      excel_file_name = f"Negative And No Order Report"
      wb = openpyxl.Workbook()
      Negative_no_order_sheet = wb.active
      Negative_no_order_sheet.append(Headers)

      if Platform == 'Doordash' or Platform == 'All':
        Doordash_negative_transactions = kwargs.get('Doordash_negative_transactions')
        Doordash_no_order_transactions = kwargs.get('Doordash_no_order_transactions')
        cursor.execute("""SELECT * FROM config_master where config_type='Doordash_field_mapping'""", )
        header_mapping = cursor.fetchone()
        header_mapping = header_mapping['config_value'] if isinstance(header_mapping['config_value'], dict) else eval(header_mapping['config_value'])

        create_log_data(
          level="[INFO]",
          Message=f"Successfully get doordash header mapping {header_mapping}",
          functionName="generate_workbook_sheet"
        )

        all_transactions = cls.merge_transactions(Doordash_negative_transactions, Doordash_no_order_transactions)
        Negative_no_order_sheet = cls.process_doordash_data(all_transactions, header_mapping, Headers, Negative_no_order_sheet)
      if Platform == 'UberEats' or Platform == 'All':
        Ubereats_negative_transactions = kwargs.get('Ubereats_negative_transactions')
        Ubereats_no_order_transactions = kwargs.get('Ubereats_no_order_transactions')
        cursor.execute("""SELECT * FROM config_master where config_type='UberEATS_fields_mapping'""", )
        header_mapping = cursor.fetchone()
        header_mapping = header_mapping['config_value'] if isinstance(header_mapping['config_value'], dict) else eval(
          header_mapping['config_value'])

        create_log_data(
          level="[INFO]",
          Message=f"Successfully get UberEats header mapping {header_mapping}",
          functionName="generate_workbook_sheet"
        )
        all_transactions = cls.merge_transactions(Ubereats_negative_transactions, Ubereats_no_order_transactions)

        Negative_no_order_sheet = cls.process_ubereats_data(all_transactions, header_mapping, Headers, Negative_no_order_sheet)
      if Platform == 'Grubhub' or Platform == 'All':
        Grubhub_negative_transactions = kwargs.get('Grubhub_negative_transactions')
        Grubhub_no_order_transactions = kwargs.get('Grubhub_no_order_transactions')
        cursor.execute("""SELECT * FROM config_master where config_type='Grubhub_fields_mapping'""", )
        header_mapping = cursor.fetchone()
        header_mapping = header_mapping['config_value'] if isinstance(header_mapping['config_value'], dict) else eval(
          header_mapping['config_value'])
        create_log_data(
          level="[INFO]",
          Message=f"Successfully get Grubhub header mapping {header_mapping}",
          functionName="generate_workbook_sheet"
        )
        all_transactions = cls.merge_transactions(Grubhub_negative_transactions, Grubhub_no_order_transactions)
        Negative_no_order_sheet = cls.process_grubhub_data(all_transactions, header_mapping, Headers,
                                                            Negative_no_order_sheet)
      if Platform == 'Storefront' or Platform == 'All':
        Storefront_negative_transactions = kwargs.get('Storefront_negative_transactions')
        Storefront_no_order_transactions = kwargs.get('Storefront_no_order_transactions')
        cursor.execute("""SELECT * FROM config_master where config_type='Storefront_fields_mapping'""", )
        header_mapping = cursor.fetchone()
        header_mapping = header_mapping['config_value'] if isinstance(header_mapping['config_value'], dict) else eval(
          header_mapping['config_value'])
        create_log_data(
          level="[INFO]",
          Message=f"Successfully get Storefront header mapping {header_mapping}",
          functionName="generate_workbook_sheet"
        )
        all_transactions = cls.merge_transactions(Storefront_negative_transactions, Storefront_no_order_transactions)

        Negative_no_order_sheet = cls.process_storefront_data(all_transactions, header_mapping, Headers,
                                                           Negative_no_order_sheet)
      return wb, excel_file_name
    except Exception as e:
      print(f"Exception in getting transactions {e}")
      create_log_data(
        level="[ERROR]",
        Message=f"Error in generating workbook sheet for transactions",
        messagebody=f"Error: {str(e)}",
        functionName="generate_workbook_sheet"
      )
      raise
      # return jsonify({"error": f"Error: {e}"}), 500

  @classmethod
  def generate_csv_sheet(cls, Platform, **kwargs):
    try:
      import csv
      import os

      connection, cursor = get_db_connection()

      # Fetch headers
      cursor.execute("""SELECT * FROM config_master where config_type='Problematic_report_header'""")
      Headers = cursor.fetchone()
      Headers = Headers['config_value'] if isinstance(Headers['config_value'], list) else eval(Headers['config_value'])

      # Set the CSV file name
      csv_file_name = f"Negative_And_No_Order_Report.csv"

      # Get the current working directory
      current_directory = os.path.dirname(os.path.abspath(__file__))

      # Combine the current directory path with the file name
      csv_file_path = os.path.join(current_directory, csv_file_name)

      # Create and open the CSV file
      with open(csv_file_path, mode='w', newline='', encoding='utf-8') as file:
        csv_writer = csv.writer(file)
        # Write the headers to the CSV file
        csv_writer.writerow(Headers)

        # Initialize the list to hold all rows
        Negative_no_order_sheet = []

        if Platform == 'Doordash' or Platform == 'ALL':
          # Get Doordash transactions
          Doordash_negative_transactions = kwargs.get('Doordash_negative_transactions')
          Doordash_no_order_transactions = kwargs.get('Doordash_no_order_transactions')

          cursor.execute("""SELECT * FROM config_master where config_type='Doordash_field_mapping'""")
          header_mapping = cursor.fetchone()
          header_mapping = header_mapping['config_value'] if isinstance(header_mapping['config_value'], dict) else eval(
            header_mapping['config_value'])

          # Merge Doordash transactions
          all_transactions = cls.merge_transactions(Doordash_negative_transactions, Doordash_no_order_transactions)

          # Process and write the Doordash data to the sheet (Negative_no_order_sheet)
          Negative_no_order_sheet = cls.process_doordash_data(all_transactions, header_mapping, Headers,
                                                              Negative_no_order_sheet)

        elif Platform == 'Ubereats' or Platform == 'ALL':
          # Get UberEats transactions
          Ubereats_negative_transactions = kwargs.get('Ubereats_negative_transactions')
          Ubereats_no_order_transactions = kwargs.get('Ubereats_no_order_transactions')

          cursor.execute("""SELECT * FROM config_master where config_type='UberEATS_fields_mapping'""")
          header_mapping = cursor.fetchone()
          header_mapping = header_mapping['config_value'] if isinstance(header_mapping['config_value'], dict) else eval(
            header_mapping['config_value'])

          # Merge UberEats transactions
          all_transactions = cls.merge_transactions(Ubereats_negative_transactions, Ubereats_no_order_transactions)

          # Process and write the UberEats data to the sheet (Negative_no_order_sheet)
          Negative_no_order_sheet = cls.process_ubereats_data(all_transactions, header_mapping, Headers,
                                                              Negative_no_order_sheet)

        # Write each row to the CSV file from Negative_no_order_sheet
        for row in Negative_no_order_sheet:
          csv_writer.writerow(row)

      # Return the file path of the generated CSV
      return csv_file_path
    except Exception as e:
      print(f"Exception in generating CSV sheet {e}")
      return jsonify({"error": f"Error: {e}"}), 500

  @classmethod
  def get_doordash_negative_no_order_transactions(cls, start_date, end_date, merchant_id):
    try:
      create_log_data(
        level="[INFO]",
        Message=f"Getting negative and no order transactions for platform Doordash",
        messagebody=f"Star Date: {start_date}, End Date: {end_date}, Merchant ID: {merchant_id}",
        functionName="get_doordash_negative_no_order_transactions"
      )
      connection, cursor = get_db_connection()
      end_date_after_one_day = (datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
      where = f''
      if merchant_id != -1:
        where = f' and Merchantid="{merchant_id}"'

      cursor.execute("""
                      SELECT * FROM config_master where config_type='Doordash_fields'""",
                     )
      Doordash_fields_row = cursor.fetchone()
      Doordash_fields_list = json.loads(Doordash_fields_row["config_value"])
      Doordash_fields = ", ".join(Doordash_fields_list)
      print(Doordash_fields)
      create_log_data(
        level="[INFO]",
        Message=f"Getting Doordash columns to Fetch data",
        messagebody=f"Doordash Columns: {Doordash_fields}",
        functionName="get_doordash_negative_no_order_transactions"
      )

      query = f"""SELECT 
                      {Doordash_fields}
                  FROM
                      doordashtransaction join merchants on doordashtransaction.Merchantid = merchants.id
                  WHERE
                      Orderexternalrefenceid IN (SELECT 
                              Orderexternalrefenceid
                          FROM
                              doordashtransaction
                          WHERE
                              Credit > Platform_credit
                                  AND dateandtime BETWEEN '{start_date}' AND '{end_date_after_one_day}'
                                  AND Transactiontype = 'DELIVERY'
                                  AND Platform_subtotal IS NOT NULL
                                  AND Orderexternalrefenceid NOT IN (SELECT 
                                      Orderexternalrefenceid
                                  FROM
                                      doordashtransaction
                                  WHERE
                                      Finalorderstatus IN ('Cancelled -')))
                          AND Transactiontype IN ('DELIVERY')
                          AND dateandtime BETWEEN '{start_date}' AND '{end_date_after_one_day}'
                          AND Platform_subtotal IS NOT NULL {where}"""

      print("Generated SQL Query:")
      print(query)
      cursor.execute(query)
      Doordash_negative_transactions = cursor.fetchall()

      create_log_data(
        level="[INFO]",
        Message=f"Fetched negative transactions for platform Doordash",
        messagebody=f"Negative transactions: {Doordash_negative_transactions}",
        functionName="get_doordash_negative_no_order_transactions"
      )

      cursor.execute(f"""SELECT 
                            {Doordash_fields}
                        FROM
                            doordashtransaction join merchants on doordashtransaction.Merchantid = merchants.id
                        WHERE
                            dateandtime BETWEEN '{start_date}' AND '{end_date_after_one_day}'
                                AND Platform_subtotal IS NULL
                                AND Orderexternalrefenceid NOT IN (SELECT 
                                    Orderexternalrefenceid
                                FROM
                                    doordashtransaction
                                WHERE
                                    dateandtime BETWEEN '{start_date}' AND '{end_date_after_one_day}'
                                        AND Platform_subtotal IS NULL
                                        AND Finalorderstatus = 'Cancelled -') {where}""")
      Doordash_no_order_transactions = cursor.fetchall()

      create_log_data(
        level="[INFO]",
        Message=f"Fetched no order transactions for platform Doordash",
        messagebody=f"No order transactions: {Doordash_no_order_transactions}",
        functionName="get_doordash_negative_no_order_transactions"
      )

      return Doordash_negative_transactions, Doordash_no_order_transactions

    except Exception as e:
      create_log_data(
        level="[ERROR]",
        Message=f"Error in getting negative and no order transactions for platform Doordash",
        messagebody=f"Error: {str(e)}",
        functionName="get_doordash_negative_no_order_transactions"
      )
      print(f"Exception in getting transactions {e}")
      raise

  @classmethod
  def get_ubereats_negative_no_order_transactions(cls, start_date, end_date, merchant_id):
    try:
      create_log_data(
        level="[INFO]",
        Message=f"Getting negative and no order transactions for platform Ubereats",
        messagebody=f"Star Date: {start_date}, End Date: {end_date}, Merchant ID: {merchant_id}",
        functionName="get_ubereats_negative_no_order_transactions"
      )
      connection, cursor = get_db_connection()
      where = f''
      if merchant_id != -1:
        where = f' and Merchantid="{merchant_id}"'

      cursor.execute("""
                        SELECT * FROM config_master where config_type='UberEats_fields'""",
                     )
      UberEats_fields_row = cursor.fetchone()
      UberEats_fields_list = json.loads(UberEats_fields_row["config_value"])
      UberEats_fields = ", ".join(UberEats_fields_list)
      print(UberEats_fields)

      create_log_data(
        level="[INFO]",
        Message=f"Getting Ubereats columns to Fetch data",
        messagebody=f"UberEats Columns: {UberEats_fields}",
        functionName="get_ubereats_negative_no_order_transactions"
      )

      query = f"""select {UberEats_fields} from ubereatstransaction join merchants on ubereatstransaction.Merchantid = merchants.id where Totalpayout>Platform_totalpayout and Transactiondate between '{start_date}' and '{end_date}' and Transactiontype='Completed' {where}
      """

      print("Generated SQL Query:")
      print(query)
      cursor.execute(query)
      Ubereats_negative_transactions = cursor.fetchall()

      create_log_data(
        level="[INFO]",
        Message=f"Fetched negative transactions for platform Ubereats",
        messagebody=f"Negative transactions: {Ubereats_negative_transactions}",
        functionName="get_ubereats_negative_no_order_transactions"
      )


      cursor.execute(f"""select {UberEats_fields} from ubereatstransaction join merchants on ubereatstransaction.Merchantid = merchants.id  where Transactiondate between '{start_date}' and '{end_date}' and Platform_salesexcltax is null and Orderexternalrefenceid not in (select Orderexternalrefenceid from ubereatstransaction where Transactiondate between '{start_date}' and '{end_date}' and Platform_salesexcltax is null and Transactiontype='Refund' and recordfromscript=1) {where};""")
      UberEats_no_order_transactions = cursor.fetchall()

      create_log_data(
        level="[INFO]",
        Message=f"Fetched no order transactions for platform Ubereats",
        messagebody=f"No order transactions: {UberEats_no_order_transactions}",
        functionName="get_ubereats_negative_no_order_transactions"
      )

      return Ubereats_negative_transactions, UberEats_no_order_transactions

    except Exception as e:
      print(f"Exception in getting transactions {e}")
      create_log_data(
        level="[INFO]",
        Message=f"Error in getting negative and no order transactions for platform UberEats",
        messagebody=f"Error: {str(e)}",
        functionName="get_ubereats_negative_no_order_transactions"
      )
      raise

  @classmethod
  def get_grubhub_negative_no_order_transactions(cls, start_date, end_date, merchant_id):
    try:
      create_log_data(
        level="[INFO]",
        Message=f"Getting negative and no order transactions for platform Grubhub",
        messagebody=f"Star Date: {start_date}, End Date: {end_date}, Merchant ID: {merchant_id}",
        functionName="get_grubhub_negative_no_order_transactions"
      )
      connection, cursor = get_db_connection()
      end_date_after_one_day = (datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
      where = f''
      if merchant_id != -1:
        where = f' and Merchantid="{merchant_id}"'

      cursor.execute("""
                          SELECT * FROM config_master where config_type='Grubhub_fields'""",
                     )
      Grubhub_fields_row = cursor.fetchone()
      Grubhub_fields_list = json.loads(Grubhub_fields_row["config_value"])
      Grubhub_fields = ", ".join(Grubhub_fields_list)
      print(Grubhub_fields)
      create_log_data(
        level="[INFO]",
        Message=f"Getting Grubhub columns to Fetch data",
        messagebody=f"Grubhub Columns: {Grubhub_fields}",
        functionName="get_grubhub_negative_no_order_transactions"
      )

      query = f"""select {Grubhub_fields} from ghrubhubtransaction join merchants on ghrubhubtransaction.Merchantid = merchants.id where (Restauranttotal + Commission + Deliverycommission + Processingfee) > (Platform_restauranttotal + Platform_commission + Platform_deliverycommission + Platform_processingfee) and Dateandtime between '{start_date}' and '{end_date_after_one_day}' and Transactiontype='Prepaid order' {where};
        """

      print("Generated SQL Query:")
      print(query)
      cursor.execute(query)
      Grubhub_negative_transactions = cursor.fetchall()

      cursor.execute(
        f"""select {Grubhub_fields} from ghrubhubtransaction join merchants on ghrubhubtransaction.Merchantid = merchants.id where Dateandtime between '{start_date}' and '{end_date_after_one_day}' and Platform_subtotal is null and Transactiontype='Prepaid order' {where}""")
      Grubhub_no_order_transactions = cursor.fetchall()

      create_log_data(
        level="[INFO]",
        Message=f"Fetched no order transactions for platform Grubhub",
        messagebody=f"No order transactions: {Grubhub_no_order_transactions}",
        functionName="get_grubhub_negative_no_order_transactions"
      )

      return Grubhub_negative_transactions, Grubhub_no_order_transactions

    except Exception as e:
      print(f"Exception in getting transactions {e}")
      create_log_data(
        level="[INFO]",
        Message=f"Error in getting negative and no order transactions for platform Grubhub",
        messagebody=f"Error: {str(e)}",
        functionName="get_grubhub_negative_no_order_transactions"
      )
      raise

  @classmethod
  def get_storefront_negative_no_order_transactions(cls, start_date, end_date, merchant_id):
    try:
      create_log_data(
        level="[INFO]",
        Message=f"Getting negative and no order transactions for platform Storefront",
        messagebody=f"Star Date: {start_date}, End Date: {end_date}, Merchant ID: {merchant_id}",
        functionName="get_storefront_negative_no_order_transactions"
      )
      connection, cursor = get_db_connection()
      end_date_after_one_day = (datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
      where = f''
      if merchant_id != -1:
        where = f' and merchantid="{merchant_id}"'

      cursor.execute("""
                            SELECT * FROM config_master where config_type='Storefront_fields'""",
                     )
      Storefront_fields_row = cursor.fetchone()
      Storefront_fields_list = json.loads(Storefront_fields_row["config_value"])
      Storefront_fields = ", ".join(Storefront_fields_list)
      print(Storefront_fields)
      create_log_data(
        level="[INFO]",
        Message=f"Getting Storefront columns to Fetch data",
        messagebody=f"Grubhub Columns: {Storefront_fields}",
        functionName="get_storefront_negative_no_order_transactions"
      )

      query = f"""SELECT 
                    {Storefront_fields}
                  FROM storefronttransaction
                  JOIN merchants
                    ON storefronttransaction.merchantid = merchants.id
                  WHERE 
                    (ordertotal > abs((COALESCE(stripe_amount, 0)) - abs(COALESCE(stripe_fee, 0))) 
                    OR refund_amount > abs((COALESCE(stripe_amount, 0)) - abs(COALESCE(stripe_fee, 0)))) 
                    and orderexternalreference not in (select orderexternalreference from storefronttransaction where Transactiontype="Refund" and dateandtime BETWEEN '{start_date}' AND '{end_date_after_one_day}')
                    AND dateandtime BETWEEN '{start_date}' AND '{end_date_after_one_day}' and stripe_fee is not null
                    AND merchantname != 'Delicio Test Store 2' {where};
                            """

      print("Generated SQL Query:")
      print(query)
      cursor.execute(query)
      Storefront_negative_transactions = cursor.fetchall()

      cursor.execute(
        f"""SELECT 
                {Storefront_fields}
            FROM
                storefronttransaction 
                    JOIN 
                merchants on storefronttransaction.merchantid = merchants.id
            WHERE
                dateandtime BETWEEN '{start_date}' AND '{end_date_after_one_day}'
                    AND stripe_amount IS NULL
                    AND orderexternalreference NOT IN (SELECT 
                        orderexternalreference
                    FROM
                        storefronttransaction
                    WHERE
                        Transactiontype = 'Refund'
                            AND dateandtime BETWEEN '{start_date}' AND '{end_date_after_one_day}'
                            AND merchantname != 'Delicio Test Store 2') {where};""")
      Storefront_no_order_transactions = cursor.fetchall()
      create_log_data(
        level="[INFO]",
        Message=f"Fetched no order transactions for platform Storefront",
        messagebody=f"No order transactions: {Storefront_no_order_transactions}",
        functionName="get_storefront_negative_no_order_transactions"
      )

      return Storefront_negative_transactions, Storefront_no_order_transactions

    except Exception as e:
      print(f"Exception in getting transactions {e}")
      create_log_data(
        level="[INFO]",
        Message=f"Error in getting negative and no order transactions for platform Storefront",
        messagebody=f"Error: {str(e)}",
        functionName="get_storefront_negative_no_order_transactions"
      )
      raise


  @classmethod
  def generate_negative_no_order_report(cls, start_date, end_date, merchant_id, platform_type):
    try:
      print(platform_type)
      create_log_data(
        level="[INFO]",
        Message=f"In the start of function to get negative and no order transaction and make file for those negative and no order report",
        messagebody=f"Star Date: {start_date}, End Date: {end_date}, Merchant ID: {merchant_id}, Platform Type: {platform_type}",
        functionName="generate_negative_no_order_report"
      )
      if platform_type == "Doordash":

        Doordash_negative_transactions, Doordash_no_order_transactions = cls.get_doordash_negative_no_order_transactions(start_date, end_date, merchant_id)
        return cls.generate_workbook_sheet(
            Platform="Doordash",
            Doordash_negative_transactions=Doordash_negative_transactions,
            Doordash_no_order_transactions=Doordash_no_order_transactions
        )
      elif platform_type == "UberEats":
        Ubereats_negative_transactions, Ubereats_no_order_transactions = cls.get_ubereats_negative_no_order_transactions(
          start_date, end_date, merchant_id)
        return cls.generate_workbook_sheet(
          Platform="UberEats",
          Ubereats_negative_transactions=Ubereats_negative_transactions,
          Ubereats_no_order_transactions=Ubereats_no_order_transactions
        )
      elif platform_type == "Grubhub":
        Grubhub_negative_transactions, Grubhub_no_order_transactions = cls.get_grubhub_negative_no_order_transactions(
          start_date, end_date, merchant_id)
        return cls.generate_workbook_sheet(
          Platform="Grubhub",
          Grubhub_negative_transactions=Grubhub_negative_transactions,
          Grubhub_no_order_transactions=Grubhub_no_order_transactions
        )
      elif platform_type == "Storefront":
        Storefront_negative_transactions, Storefront_no_order_transactions = cls.get_storefront_negative_no_order_transactions(
          start_date, end_date, merchant_id)
        return cls.generate_workbook_sheet(
          Platform="Storefront",
          Storefront_negative_transactions=Storefront_negative_transactions,
          Storefront_no_order_transactions=Storefront_no_order_transactions
        )

      elif platform_type == "All":
        Doordash_negative_transactions, Doordash_no_order_transactions = cls.get_doordash_negative_no_order_transactions(
          start_date, end_date, merchant_id)
        Ubereats_negative_transactions, Ubereats_no_order_transactions = cls.get_ubereats_negative_no_order_transactions(
          start_date, end_date, merchant_id)
        Grubhub_negative_transactions, Grubhub_no_order_transactions = cls.get_grubhub_negative_no_order_transactions(
          start_date, end_date, merchant_id)
        Storefront_negative_transactions, Storefront_no_order_transactions = cls.get_storefront_negative_no_order_transactions(
          start_date, end_date, merchant_id)
        return cls.generate_workbook_sheet(
          Platform="All",
          Doordash_negative_transactions=Doordash_negative_transactions,
          Doordash_no_order_transactions=Doordash_no_order_transactions,
          Ubereats_negative_transactions=Ubereats_negative_transactions,
          Ubereats_no_order_transactions=Ubereats_no_order_transactions,
          Grubhub_negative_transactions=Grubhub_negative_transactions,
          Grubhub_no_order_transactions=Grubhub_no_order_transactions,
          Storefront_negative_transactions=Storefront_negative_transactions,
          Storefront_no_order_transactions=Storefront_no_order_transactions
        )

    except Exception as e:
      print("Exception")
      create_log_data(
        level="[ERROR]",
        Message=f"Error in generating negative and no order transactions",
        messagebody=f'Error {str(e)}',
        functionName="generate_negative_no_order_report"
      )
      raise