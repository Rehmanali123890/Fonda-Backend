import copy
import json
import random
import string
import uuid
from dateutil.parser import parse
import boto3
from flask import Flask, render_template, flash, request
from botocore.exceptions import ClientError
from flask.json import jsonify
import datetime
import pytz
from datetime import timedelta
from dateutil.relativedelta import relativedelta
from dateutil.tz import gettz
import calendar
from operator import itemgetter

from models.CategoryServiceAvailability import CategoryServiceAvailability
from models.Stream import Stream
from string import ascii_uppercase, digits
from random import choices
import jwt.utils
import time
import math
import requests
from decimal import Decimal

from models.ActivityLogs import ActivityLogs
# local imports
from models.AuditLogs import AuditLogs
from models.MerchantUsers import MerchantUsers
# from models.Platforms import Platforms
from models.VirtualMerchants import VirtualMerchants
from utilities.helpers import get_db_connection, validateLoginToken, success, is_float, \
  publish_sns_message, open_stripe_connect_account, normalize_string, create_log_data, get_ip_address, \
  update_new_open_stripe_connect_account,send_android_notification_api
from models.Websockets import Websockets
from utilities.errors import unhandled, not_found, invalid
from models.Users import Users
from models.stripe.Stripe import Stripe
import config
import stripe
import re
from botocore.config import Config

s3_apptopus_bucket = config.s3_apptopus_bucket
images_folder = config.s3_images_folder


class Merchants():

  # GET

  @classmethod
  def get_merchants(cls, request, esperDeviceIds: list = None, openingHoursFilter: dict = {"filter": 0},
                    only_merchant=0, logintoken=''):
    try:
      connection, cursor = get_db_connection()

      # get and validate login token
      if only_merchant == 0:
        token = request.args.get("token")
      else:
        token = logintoken
      if not token:
        return not_found(params=["token"])

      userId = validateLoginToken(token)

      user = Users.get_users(conditions=[f"id = '{userId}'"])
      if not user:
        return not_found("User not found")
      user = user[0]

      # limit - offset
      limit = "25"
      if (request.args.get('limit')):
        limit = request.args.get('limit')

      offset = "0"
      if (request.args.get('from')):
        offset = request.args.get('from')

      # form where clause
      conditions = []

      merchantName = request.args.get('merchantName')
      if merchantName:
        conditions.append(f'`merchants`.`merchantname` LIKE "%%{normalize_string(merchantName)}%%"')

      merchantEmail = request.args.get('merchantEmail')
      if merchantEmail:
        conditions.append(f'`merchants`.`email` LIKE "%%{normalize_string(merchantEmail)}%%"')

      merchantStatus = request.args.get('merchantStatus')
      if merchantStatus is not None:
        conditions.append(f'`merchants`.`status` = "{merchantStatus}"')

      marketStatus = request.args.get('marketStatus')
      if marketStatus is not None:
        conditions.append(f'`merchants`.`marketstatus` = "{marketStatus}"')

      parserStatus = request.args.get('parserStatus')
      if parserStatus is not None:
        conditions.append(f'`merchants`.`parserstatus` = "{parserStatus}"')

      subscriptionStatus = request.args.get('subscriptionStatus')
      if subscriptionStatus is not None:
        conditions.append(f'`merchants`.`subscriptionstatus` = "{subscriptionStatus}"')

      onBoardingCompleted = request.args.get('onBoardingCompleted')
      if onBoardingCompleted is not None:
        conditions.append(f'`merchants`.`onBoardingCompleted` = "{onBoardingCompleted}"')

      if esperDeviceIds and len(esperDeviceIds):
        conditions.append(f'`merchants`.`esperdeviceid` IN {tuple(esperDeviceIds)}')

      DDstreamConnFilter = request.args.get('DDstreamConnFilter')
      if DDstreamConnFilter is not None:
          conditions.append(f'`merchants`.`doordashstream` = "{DDstreamConnFilter}"')
      GHstreamConnFilter = request.args.get('GHstreamConnFilter')
      if GHstreamConnFilter is not None:
          conditions.append(f'`merchants`.`grubhubstream` = "{GHstreamConnFilter}"')

      FIELDS = ["`merchants`.*"]
      ORDER_BY = ["`merchants`.`merchantname`"]

      JOINTS = ""
      if user['role'] == 1 or user['role'] == 2:
        pass
      else:
        JOINTS = " INNER JOIN `merchantusers` ON `merchants`.`id` = `merchantusers`.`merchantid` "
        conditions.append(f'`merchantusers`.`userid` = "{userId}"')

      ### check for opening hours filter
      if openingHoursFilter and openingHoursFilter["filter"] == 1:

        # get today's dayname
        today = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc).astimezone(gettz("US/Pacific"))
        today_dayname = calendar.day_name[today.weekday()]

        # append joints
        JOINTS += " LEFT JOIN `merchantopeninghrs` ON `merchants`.`id` = `merchantopeninghrs`.`merchantid` "

        # append fields
        FIELDS.extend(
          ["`merchantopeninghrs`.`opentime` as opentime", "`merchantopeninghrs`.`closetime` as closetime"])

        # append conditions
        conditions.append(f'`merchantopeninghrs`.`day` = "{today_dayname}"')
        conditions.append(f'`merchantopeninghrs`.`closeforbusinessflag` = 0')
        conditions.append(f'`merchantopeninghrs`.`opentime` != ""')

        if openingHoursFilter.get("startTime") and openingHoursFilter.get("endTime"):
          # conditions.append(f'CONVERT(`merchantopeninghrs`.`opentime`, TIME) >= "{openingHoursFilter["startTime"]}" AND CONVERT(`merchantopeninghrs`.`closetime`, TIME) <= "{openingHoursFilter["endTime"]}"')
          conditions.append(
            f'STR_TO_DATE(`merchantopeninghrs`.`opentime`, "%l:%i %p") >= "{openingHoursFilter["startTime"]}" AND STR_TO_DATE(`merchantopeninghrs`.`opentime`, "%l:%i %p") <= "{openingHoursFilter["endTime"]}"')
        else:
          conditions.append(
            f'CONVERT(`merchantopeninghrs`.`opentime`, TIME) IS NOT NULL AND CONVERT(`merchantopeninghrs`.`closetime`, TIME) IS NOT NULL')

        # update order_by
        ORDER_BY = ["STR_TO_DATE(`merchantopeninghrs`.`opentime`, '%l:%i %p')",
                    "`merchantopeninghrs`.`merchantid`"]

      # where clause handling
      WHERE = ' AND '.join(conditions)
      if not WHERE:
        WHERE = "1"
      # checking gmb filter
      GMBConnFilter = request.args.get('GMBConnFilter')
      GMBVerifyFilter = request.args.get('GMBVerifyFilter')
      gmb_join = ''
      if GMBConnFilter is not None:
        if GMBConnFilter == 'connected':
          gmb_join = f""" INNER JOIN `googlelocations` ON `merchants`.`id` = `googlelocations`.`merchantid` """
        elif GMBConnFilter == 'unconnected':
          gmb_join = f""" LEFT JOIN `googlelocations` ON `merchants`.`id` = `googlelocations`.`merchantid` """
          conditions.append(f"`googlelocations`.`merchantid` is NULL")
          WHERE = ' AND '.join(conditions)
        elif GMBConnFilter == '':
          pass

      if GMBVerifyFilter is not None:
        if GMBVerifyFilter == 'verified' and GMBConnFilter != 'unconnected':
          gmb_join = f""" INNER JOIN `googlelocations` ON `merchants`.`id` = `googlelocations`.`merchantid` """
          conditions.append(f"`googlelocations`.status = {3}")
          WHERE = ' AND '.join(conditions)
        elif GMBVerifyFilter == 'unverified' and GMBConnFilter != 'unconnected':
          gmb_join = f""" INNER JOIN `googlelocations` ON `merchants`.`id` = `googlelocations`.`merchantid` """
          conditions.append(f"`googlelocations`.status != {3}")
          WHERE = ' AND '.join(conditions)
        elif GMBVerifyFilter == '' and GMBConnFilter != 'unconnected':
          pass

      JOINTS += gmb_join
      sqlstmt = f"""
          SELECT {','.join(FIELDS)}
          FROM `merchants`
          {JOINTS}
          WHERE {WHERE}
          ORDER BY {','.join(ORDER_BY)}
          """
      # LIMIT {limit}
      # OFFSET {offset}
      cursor.execute(sqlstmt)
      rows = cursor.fetchall()
      print(len(rows))

      count = 0
      merchants = []
      for r in rows:
        print(count)
        where_verification = ""

        cursor.execute("""SELECT status FROM googlelocations where merchantid = %s;""", (r['id']))
        google_verified = cursor.fetchone()

        if openingHoursFilter.get("filter") == 1:
          found = False
          for m in merchants:
            if m["id"] == r["id"]:
              m["openingHours"].append({
                "openTime": r["opentime"],
                "closeTime": r["closetime"]
              })
              found = True
          if found:
            continue

        # get virtual merchants
        virtual_merchants = VirtualMerchants.get_virtual_merchant_str(merchantId=r['id'], activeOnly=1)

        googleconnectivity = 1 if google_verified else 0
        googleverified = google_verified['status'] if google_verified else ''

        merchants.append({
          "address": r['address'],
          "bankAccountNumber": r['bankaccountnumber'],
          "bankAccountRoutingNumber": r['bankaccountroutingnumber'],
          "businessAddressCity": r['businessaddresscity'],
          "businessAddressLine": r['businessaddressline'],
          "businessAddressState": r['businessaddressstate'],
          "businessNumber": r['businessnumber'],
          "businessTaxId": r['businesstaxid'],
          "email": r['email'],
          "firstName": r['firstname'],
          "id": r['id'],
          "lastName": r['lastname'],
          "lat": r['lat'],
          "legalBusinessName": r['legalbusinessname'],
          "longitude": r['longitude'],
          "marketStatus": r['marketstatus'],
          "merchantName": r['merchantname'],
          "merchantStatus": r['status'],
          "merchantTaxRate": r['taxrate'],
          "onBoardingCompleted": r['onBoardingCompleted'],
          "parserStatus": r['parserstatus'],
          "MarketPlacePriceStatus": r['marketplacepricestatus'],
          "fondaparseur": r['parserstatus'],
          "phone": r['phone'],
          "fondaparseur": r['order_creation_permission'],
          "subscriptionStatus": r['subscriptionstatus'],
          "virtualMerchants": virtual_merchants if virtual_merchants else [],
          "acceptSpecialInstructions": r["acceptspecialinstructions"],
          "esperDeviceId": r["esperdeviceid"],
          "openingHours": [{
            "openTime": r["opentime"],
            "closeTime": r["closetime"]
          }] if openingHoursFilter.get("filter") == 1 else [],
          "googleconnectivity": googleconnectivity,
          "googleverified": googleverified,
          "doordashstream": r['doordashstream'],
          "grubhubstream": r['grubhubstream'],
          "is_stream_enabled":r['is_stream_enabled']
        })
        count+=1
      # sort the merchants by opening hour
      # merchants = sorted(merchants, key=lambda d: d.get("openingHours")[0].get("openTime") if len(d.get("openingHours")) else "23:59:59")

      # print(len(merchants))
      merchants = merchants[int(offset):int(limit) + int(offset)]
      if only_merchant == 1:
        return merchants
      return success(jsonify(merchants))
    except Exception as e:
      return unhandled(f"error: {e}")

  @classmethod
  def get_merchant_by_id(cls, merchantId):
    try:
      connection, cursor = get_db_connection()
      cursor.execute("""SELECT * FROM merchants WHERE id=%s""", (merchantId))
      row = cursor.fetchone()

      if row.get('banner'):
        row['banner'] = row.get('banner').replace(config.amazonaws_s3_url, config.cloud_front_url)

      if row.get('logo'):
        row['logo'] = row.get('logo').replace(config.amazonaws_s3_url, config.cloud_front_url)

      return row
    except Exception as e:
      print(str(e))
      return False

  @classmethod
  def get_merchant_by_id(cls, merchantId):
    try:
      connection, cursor = get_db_connection()
      cursor.execute("""SELECT * FROM merchants WHERE id=%s""", (merchantId))
      row = cursor.fetchone()

      if row.get('banner'):
        row['banner'] = row.get('banner').replace(config.amazonaws_s3_url, config.cloud_front_url)

      if row.get('logo'):
        row['logo'] = row.get('logo').replace(config.amazonaws_s3_url, config.cloud_front_url)

      return row
    except Exception as e:
      print(str(e))
      return False
  @classmethod
  def get_users_with_bank_edit_access(cls):
    try:
      connection, cursor = get_db_connection()

      cursor.execute("SELECT config_value FROM config_master WHERE config_type = %s", ('bank_edit_emails_access'))
      row = cursor.fetchone()
      return row.get('config_value') if row else ''

    except Exception as e:
      print("error: ", str(e))
      return ''

  @classmethod
  def scheduler_function(cls, datee):
    try:
      print('---------------scheduler Downtime -------------- ')
      connection, cursor = get_db_connection()
      today_date = datetime.datetime.strptime(datee, "%Y-%m-%d")

      one_day = datetime.timedelta(days=1)
      previous_date = today_date - one_day

      day = previous_date.strftime("%A")
      today = previous_date.date()

      # cursor.execute("SELECT id FROM merchants")
      # merchants = cursor.fetchall()
      merchantcount = 0
      merchants = [{'id': 'b396dfa4-30da-40e9-97cc-2c3373bd08fd'}]
      for merchant in merchants:
        merchantcount = merchantcount + 1
        print("merchant id is ", merchant['id'])
        svaetoDB = False
        merchantId = merchant['id']
        cursor.execute(
          "SELECT * FROM pauseresumetimes WHERE merchantid = %s and DATE(eventdatetime)=%s order by eventdatetime ASC ",
          (merchantId, today))
        response = cursor.fetchall()

        length_of_response = len(response) - 1

        cursor.execute("SELECT * FROM merchantopeninghrs WHERE merchantid = %s and day =%s", (merchantId, day))
        fetch_data = cursor.fetchall()
        resumeMinutes = 0
        pauseMinutes = 0
        total = 0

        for openining_time in fetch_data:
          if openining_time['closetime'] != '' and openining_time['opentime'] != '':
            svaetoDB = True
            closetime = openining_time['closetime']
            closetime_obj = datetime.datetime.strptime(closetime, "%I:%M %p").time()
            closetime = closetime_obj.strftime("%H:%M:%S")

            opentime = openining_time['opentime']
            opentime_obj = datetime.datetime.strptime(opentime, "%I:%M %p").time()
            opentime = opentime_obj.strftime("%H:%M:%S")

            start_time = str(today) + ' ' + opentime
            end_time = str(today) + ' ' + closetime

            total_minutes = (datetime.datetime.strptime(end_time, '%Y-%m-%d %H:%M:%S')) - (
              datetime.datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S'))
            total_minutes = round(total_minutes.seconds / 60)
            total = total + total_minutes

            if opentime > closetime:
              next_day = today_date.strftime("%A")
              next_day_date = today_date.date()
              midnight_time = datetime.time(0, 0, 0)  # Create a time object representing midnight
              next_day_start_datetime = datetime.datetime.combine(today_date, midnight_time)
              midnight_time = datetime.time(0, 0, 0)
              next_day_close_datetime = datetime.datetime.combine(today_date, closetime_obj)
              next_day_end_time = next_day_close_datetime.strftime("%Y-%m-%d %H:%M:%S")
              next_day_start_time = next_day_start_datetime.strftime("%Y-%m-%d %H:%M:%S")
              end_time = next_day_start_time
              if next_day_start_datetime != next_day_close_datetime:
                cursor.execute(
                  "SELECT * FROM pauseresumetimes WHERE merchantid = %s and DATE(eventdatetime) between %s and %s order by eventdatetime ASC ",
                  (merchantId, next_day_start_datetime, next_day_close_datetime))
                next_day_response = cursor.fetchall()
                next_day_length_of_response = len(next_day_response) - 1

                resumeMinutes, pauseMinutes, total = cls.calculate_pause_resume_minutes(merchantId,
                                                                                        next_day_response,
                                                                                        pauseMinutes,
                                                                                        resumeMinutes,
                                                                                        next_day_length_of_response,
                                                                                        next_day_date,
                                                                                        total,
                                                                                        next_day_start_time,
                                                                                        next_day_end_time,
                                                                                        connection,
                                                                                        cursor)

            resumeMinutes, pauseMinutes, total = cls.calculate_pause_resume_minutes(merchantId, response,
                                                                                    pauseMinutes,
                                                                                    resumeMinutes,
                                                                                    length_of_response,
                                                                                    today, total,
                                                                                    start_time, end_time,
                                                                                    connection,
                                                                                    cursor)

        # pauseMinutes = total - resumeMinutes

        if svaetoDB == True:
          id = uuid.uuid4()
          data = (id, merchantId, total, pauseMinutes, resumeMinutes, today)
          cursor.execute("""INSERT INTO downtime
                                 (id, merchantId,totalTime,pauseTime,resumeTime,date)
                                 VALUES (%s,%s,%s,%s,%s,%s)""", data)
          connection.commit()
      return resumeMinutes, pauseMinutes, total
    except Exception as e:
      print("error: ", str(e))
      return False

  @classmethod
  def get_merchant_by_id(cls, merchantId):
    try:
      connection, cursor = get_db_connection()
      cursor.execute(
        """SELECT * FROM merchants WHERE id=%s""", (merchantId))
      row = cursor.fetchone()
      return row
    except Exception as e:
      print(str(e))
      return False

  @classmethod
  def get_merchant_or_virtual_merchant(cls, merchantId):
    try:
      connection, cursor = get_db_connection()
      row = cls.get_merchant_by_id(merchantId)
      if not row:
        cursor.execute(
          "SELECT * FROM virtualmerchants WHERE id = %s", (merchantId))
        row = cursor.fetchone()

        if not row:
          return False

        row["mainMerchant"] = cls.get_merchant_by_id(row["merchantid"])
        row["isVirtual"] = 1

      return row
    except Exception as e:
      print("error: ", str(e))
      return False

  @classmethod
  def scheduler_function(cls, datee):
    try:
      print('---------------scheduler Downtime1 -------------- ')
      connection, cursor = get_db_connection()
      today_date = datetime.datetime.strptime(datee, "%Y-%m-%d")

      one_day = datetime.timedelta(days=1)
      previous_date = today_date - one_day

      day = previous_date.strftime("%A")
      today = previous_date.date()

      cursor.execute("SELECT id FROM merchants")
      merchants = cursor.fetchall()
      merchantcount = 0
      # merchants = [{'id': '672b5857-36d4-492c-9d66-f255741d1ea8'}]
      for merchant in merchants:
        merchantcount = merchantcount + 1
        print("merchant id is ", merchant['id'])

        svaetoDB = False
        merchantId = merchant['id']

        cursor.execute("""SELECT timezone FROM merchants where id=%s""", (merchantId,))
        _mtz = cursor.fetchone()
        _mtz = _mtz['timezone']

        if not _mtz:
          _mtz = "US/Pacific"

        cursor.execute(
          """SELECT pauseresumetimes.id, pauseresumetimes.merchantid, pauseresumetimes.userid, pauseresumetimes.eventtype,
                        CONVERT_TZ(pauseresumetimes.eventdatetime, 'UTC', %s) AS eventdatetime 
                        FROM pauseresumetimes 
                        WHERE merchantid = %s 
                        AND DATE(CONVERT_TZ(pauseresumetimes.eventdatetime, 'UTC', %s)) = %s
                        ORDER BY eventdatetime ASC""",
          (_mtz, merchantId, _mtz, today))
        response = cursor.fetchall()

        length_of_response = len(response) - 1

        cursor.execute(
          "SELECT * FROM merchantopeninghrs WHERE merchantid = %s and day =%s", (merchantId, day))
        fetch_data = cursor.fetchall()
        resumeMinutes = 0
        pauseMinutes = 0
        total = 0

        for openining_time in fetch_data:
          if openining_time['closetime'] != '' and openining_time['opentime'] != '':
            svaetoDB = True
            closetime = openining_time['closetime']
            closetime_obj = datetime.datetime.strptime(
              closetime, "%I:%M %p").time()
            closetime = closetime_obj.strftime("%H:%M:%S")

            opentime = openining_time['opentime']
            opentime_obj = datetime.datetime.strptime(
              opentime, "%I:%M %p").time()
            opentime = opentime_obj.strftime("%H:%M:%S")

            start_time = str(today) + ' ' + opentime
            end_time = str(today) + ' ' + closetime

            total_minutes = (datetime.datetime.strptime(end_time, '%Y-%m-%d %H:%M:%S')) - (
              datetime.datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S'))
            total_minutes = round(total_minutes.seconds / 60)
            total = total + total_minutes

            if opentime > closetime:
              print("--------------------------------------")
              print(opentime)
              print(closetime)
              print("--------------------------------------")
              next_day = today_date.strftime("%A")
              next_day_date = today_date.date()
              # Create a time object representing midnight
              midnight_time = datetime.time(0, 0, 0)
              next_day_start_datetime = datetime.datetime.combine(
                today_date, midnight_time)
              midnight_time = datetime.time(0, 0, 0)
              next_day_close_datetime = datetime.datetime.combine(
                today_date, closetime_obj)
              next_day_end_time = next_day_close_datetime.strftime(
                "%Y-%m-%d %H:%M:%S")
              next_day_start_time = next_day_start_datetime.strftime(
                "%Y-%m-%d %H:%M:%S")
              end_time = next_day_start_time
              if next_day_start_datetime != next_day_close_datetime:
                print("--------------------------------------")
                print(next_day_start_time)
                print(next_day_end_time)
                print("--------------------------------------")
                cursor.execute(
                  """SELECT pauseresumetimes.id, pauseresumetimes.merchantid, pauseresumetimes.userid, pauseresumetimes.eventtype,
                                    CONVERT_TZ(pauseresumetimes.eventdatetime, 'UTC', %s) AS eventdatetime 
                                    FROM pauseresumetimes WHERE merchantid = %s 
                                    AND CONVERT_TZ(pauseresumetimes.eventdatetime, 'UTC', %s) 
                                    BETWEEN %s AND %s 
                                    ORDER BY eventdatetime ASC""",
                  (_mtz, merchantId, _mtz, next_day_start_datetime, next_day_close_datetime)
                )
                next_day_response = cursor.fetchall()
                next_day_length_of_response = len(
                  next_day_response) - 1

                resumeMinutes, pauseMinutes, total = cls.calculate_pause_resume_minutes(merchantId, next_day_response,
                                                                                        pauseMinutes, resumeMinutes,
                                                                                        next_day_length_of_response,
                                                                                        next_day_date, total,
                                                                                        next_day_start_time,
                                                                                        next_day_end_time, connection,
                                                                                        cursor)

            resumeMinutes, pauseMinutes, total = cls.calculate_pause_resume_minutes(merchantId, response,
                                                                                    pauseMinutes, resumeMinutes,
                                                                                    length_of_response, today, total,
                                                                                    start_time, end_time, connection,
                                                                                    cursor)

        # pauseMinutes = total - resumeMinutes

        if svaetoDB == True:
          id = uuid.uuid4()
          data = (id, merchantId, total,
                  pauseMinutes, resumeMinutes, today)
          cursor.execute("""INSERT INTO downtime
                               (id, merchantId,totalTime,pauseTime,resumeTime,date)
                               VALUES (%s,%s,%s,%s,%s,%s)""", data)
          connection.commit()
      return resumeMinutes, pauseMinutes, total
    except Exception as e:
      print("error: ", str(e))
      return False

  @classmethod
  def calculate_pause_resume_minutes(cls, merchantId, response, pauseMinutes, resumeMinutes, length_of_response, today,
                                     total, start_time, end_time, connection, cursor):
    try:
      print(' call calculate_pause_resume_minutes func')

      if len(response) > 0:
        resp = copy.deepcopy(response)

        for row in resp:
          if str(row['eventdatetime']) < start_time:
            row['eventdatetime'] = datetime.datetime.strptime(
              start_time, '%Y-%m-%d %H:%M:%S')
          elif str(row['eventdatetime']) > end_time:
            row['eventdatetime'] = datetime.datetime.strptime(
              end_time, '%Y-%m-%d %H:%M:%S')
          else:
            try:
              # Try parsing with microseconds
              row['eventdatetime'] = datetime.datetime.strptime(
                str(row['eventdatetime']), '%Y-%m-%d %H:%M:%S.%f')
            except ValueError:
              # Fallback to parsing without microseconds if that fails
              row['eventdatetime'] = datetime.datetime.strptime(
                str(row['eventdatetime']), '%Y-%m-%d %H:%M:%S')

        eventdatetime = resp[0]['eventdatetime']
        eventtype = resp[0]['eventtype']
        start_time = datetime.datetime.strptime(
          start_time, '%Y-%m-%d %H:%M:%S')
        end_time = datetime.datetime.strptime(
          end_time, '%Y-%m-%d %H:%M:%S')

        if eventdatetime > start_time:
          if eventtype == "RESUMED":
            time_difference = (
                                  eventdatetime - start_time).total_seconds() / 60
            pauseMinutes = pauseMinutes + time_difference
            print('Resumed')
          else:
            time_difference = (
                                  eventdatetime - start_time).total_seconds() / 60
            resumeMinutes = resumeMinutes + time_difference
            print('paused')

        for row in resp:
          if row['eventtype'] == "RESUMED":
            time_difference = (
                                  row['eventdatetime'] - eventdatetime).total_seconds() / 60
            pauseMinutes = pauseMinutes + time_difference
            print('Resumed')
          else:
            time_difference = (
                                  row['eventdatetime'] - eventdatetime).total_seconds() / 60
            resumeMinutes = resumeMinutes + time_difference
            print('paused')
          eventdatetime = row['eventdatetime']

        if resp[length_of_response]['eventdatetime'] < end_time:
          if resp[length_of_response]['eventtype'] == "RESUMED":
            time_difference = (
                                  end_time - resp[length_of_response]['eventdatetime']).total_seconds() / 60
            resumeMinutes = resumeMinutes + time_difference
          else:
            time_difference = (
                                  end_time - resp[length_of_response]['eventdatetime']).total_seconds() / 60
            pauseMinutes = pauseMinutes + time_difference

        resumeMinutes = round(resumeMinutes)
        pauseMinutes = round(pauseMinutes)
      else:
        # one_day = datetime.timedelta(days=1)
        # yesterday_date = today - one_day

        cursor.execute(
          """SELECT pauseresumetimes.eventtype FROM pauseresumetimes WHERE merchantid = %s 
                    AND CONVERT_TZ(pauseresumetimes.eventdatetime, 'UTC', 
                    (SELECT timezone FROM merchants WHERE id = pauseresumetimes.merchantid)) < %s 
                    order by eventdatetime DESC LIMIT 1 """,
          (merchantId, today))
        previous_day_event = cursor.fetchone()
        if previous_day_event != None:
          svaetoDB = True
          previous_day_event = previous_day_event['eventtype']
          if previous_day_event == "RESUMED":
            resumeMinutes = total
            pauseMinutes = 0
          else:
            pauseMinutes = total
            resumeMinutes = 0
      return resumeMinutes, pauseMinutes, total
    except Exception as e:
      print("error: ", str(e))
      return False

  @classmethod
  def get_merchant_settings_details(cls, merchantId, jsonLen):
    try:
      row = cls.get_merchant_by_id(merchantId)
      if not row:
        return None
      if jsonLen <= 3:
        merchant = {
          'busyMode': row['busymode'],
          'preparationTime': row['preparationtime'],
          'orderDelayTime': row['orderdelaytime']
        }
        return merchant
      merchant = {
        'id': row['id'],
        'language': row['language'],
        'email': row['email'],
        'merchantStatus': row['status'],
        'parserStatus': row['parserstatus'],
        'order_creation_permission': row['order_creation_permission'],
        "MarketPlacePriceStatus": row['marketplacepricestatus'],
        'autoAcceptOrder': row['autoacceptorder'],
        'preparationTime': row['preparationtime'],
        'orderDelayTime': row['orderdelaytime'],
        'busyMode': row['busymode'],
        'onboardingdate': datetime.date.isoformat(row['onboardingdate']) if isinstance(
          row['onboardingdate'], datetime.date) else row['onboardingdate'],
        'acceptSpecialInstructions': row["acceptspecialinstructions"],
        'googleReviewsReply': int(row["googleReviewsReply"]),
        "emailDistributionList": row["emaildistributionlist"],
        "cardfeeType": row["cardfeetype"],
        "pauseStarted_datetime": row["pauseStarted_datetime"].astimezone(gettz('US/Pacific')).strftime(
          "%m-%d-%Y %H:%M (%Z)"),
        "pauseTime_duration": row["pauseTime_duration"],
        "pause_reason": row["pause_reason"],
        "esperDeviceId": row["esperdeviceid"],
        "notificationTextToggle": row["notificationtexttoggle"],
        "notificationText":row["notificationtext"] if row["notificationtext"] else '',
        "is_polling_enabled": int(row["is_polling_enabled"]) if row.get("is_polling_enabled") else 0,
        "polling_frequency": int(row["polling_frequency"]) if row.get("polling_frequency") else 1,
        "is_bogo": int(row["is_bogo"]) if row.get("is_bogo") else 0,
        "platform_price_flag": int(row["platform_price_flag"]) if row.get("platform_price_flag") else 0,
      }
      return merchant
    except Exception as e:
      print(str(e))
      return False

  @classmethod
  def get_merchants_account_detail(cls, merchantId):
    try:
      row = cls.get_merchant_by_id(merchantId)
      if not row:
        return None

      merchant = {
        'id': row['id'],
        'merchantName': row['merchantname'],
        'language': row['language'],
        'firstName': row['firstname'],
        'lastName': row['lastname'],
        'email': row['email'],
        'address': row['address'],
        'phone': row['phone'],
        'businessNumber': row['businessnumber'],
        'legalBusinessName': row['legalbusinessname'],
        'businessTaxId': cls.mask_value(row['businesstaxid'], 2),
        'businessAddressLine': row['businessaddressline'],
        'businessAddressCity': row['businessaddresscity'],
        'businessAddressState': row['businessaddressstate'],
        'bankAccountNumber': cls.mask_value(row['bankaccountnumber'], 4),
        'bankAccountRoutingNumber': row['bankaccountroutingnumber'],
        'stripeAccountId': row['stripeaccountid'],
        'ein': row['ein'],
        'zip': row['zipcode'],
        'pocdob': row['pointofcontactdob'],
        'businessWebsite': row['businessWebsite'],
        'timezone': row.get('timezone'),
        'onBoardingCompleted': row['onBoardingCompleted'],
        'marketStatus': row['marketstatus'],
        "pauseStarted_datetime": row["pauseStarted_datetime"].astimezone(gettz('US/Pacific')).strftime(
          "%m-%d-%Y %H:%M (%Z)"),
        "caller": row["caller"],
        "pauseTime_duration": row["pauseTime_duration"],
        "pause_reason": row["pause_reason"],
        'storeFront': json.loads(row['storefronturls']) if row['storefronturls'] else None,
        'bank_edit_emails_access':cls.get_users_with_bank_edit_access()
      }
      return merchant
    except Exception as e:
      print(str(e))
      return False

  @classmethod
  def mask_value(cls, value: str, visible_digits_count: int) -> str:
    if value is None:
      return None

    # Get the last n characters
    visible_digits = value[-visible_digits_count:]
    # Mask the rest of the characters
    masked_digits = '*' * (len(value) - visible_digits_count)

    return masked_digits + visible_digits

  @classmethod
  def get_merchant_business_info(cls, merchantId):
    try:
      row = cls.get_merchant_by_id(merchantId)
      if not row:
        return None
      merchant = {
        'merchantTaxRate': row['taxrate'],
        'staffTipsRate': float(row['stafftipsrate']),
        'ubereatsCommission': float(row['ubereatscommission']),
        'squareCommission': float(row['squarecommission']),
        'doordashCommission': float(row['doordashcommission']),
        'grubhubCommission': float(row['grubhubcommission']),
        'flipdishCommission': float(row['flipdishcommission']),
        'processingFeePercentage': float(row['processingfeerate']),
        'processingFeeFixed': float(row['processingfeefixed']),
        'marketplaceTaxRate': float(row['marketplacetaxrate']),
        "minimumLifetimeRevenue": row["minimumLifetimeRevenue"],
        "RevenueProcessingThreshold": row["RevenueProcessingThreshold"],
        "RevenueProcessingFeePercent": row["RevenueProcessingFeePercent"],
        "AutoWaivedStatus": row["AutoWaivedStatus"],
        "DownTimeThreshold": row["DownTimeThreshold"],
        "MarketPlacePriceStatus": row['marketplacepricestatus']
      }
      return merchant
    except Exception as e:
      print(str(e))
      return False

  @classmethod
  def get_merchant_by_id_str(cls, merchantId):
    try:
      row = cls.get_merchant_by_id(merchantId)
      if not row:
        return None

      # get stripe account details
      if row['stripeaccountid'] is None or row['stripeaccountid'] == "":
        stripeTransfersStatus = 3
      else:
        stripe_status, stripe_details = Stripe.stripe_get_connected_account(apiKey=config.stripe_api_key,
                                                                            accountId=row['stripeaccountid'])
        if stripe_status == 200:
          stripeTransfersStatus = stripe_details.get(
            "capabilities").get("transfers")
          stripeTransfersStatus2 = stripe_details.get(
            "capabilities").get("platform_payments")
          stripeTransfersStatus = 1 if stripeTransfersStatus == "active" or stripeTransfersStatus2 == "active" else 0
        else:
          stripeTransfersStatus = 2
          print(stripe_details)

      storefrontUrl = ''
      if row["storefrontstatus"] == 1:
        promo = cls.get_default_promo(row['id'])
        if promo:
          promo = promo['promo']
        storefrontUrl = config.app_base_URL_storefront_food + \
                        "/" + row["slug"]

        if promo:
          storefrontUrl = storefrontUrl + "?promo=" + promo

      merchant = {
        'id': row['id'],
        'merchantName': row['merchantname'],
        'language': row['language'],
        'firstName': row['firstname'],
        'lastName': row['lastname'],
        'email': row['email'],
        'address': row['address'],
        'merchantStatus': row['status'],
        'marketStatus': row['marketstatus'],
        'parserStatus': row['parserstatus'],
        'order_creation_permission': row['order_creation_permission'],
        "MarketPlacePriceStatus": row['marketplacepricestatus'],
        'phone': row['phone'],
        'merchantlat': row['lat'],
        'merchantlong': row['longitude'],
        'businessNumber': row['businessnumber'],
        'legalBusinessName': row['legalbusinessname'],
        'businessTaxId': row['businesstaxid'],
        'businessAddressLine': row['businessaddressline'],
        'businessAddressCity': row['businessaddresscity'],
        'businessAddressState': row['businessaddressstate'],
        'bankAccountNumber': row['bankaccountnumber'],
        'bankAccountRoutingNumber': row['bankaccountroutingnumber'],
        'autoAcceptOrder': row['autoacceptorder'],
        'preparationTime': row['preparationtime'],
        'orderDelayTime': row['orderdelaytime'],
        'busyMode': row['busymode'],
        'subscriptionStartDate': datetime.date.isoformat(row['subscriptionstartdate']) if isinstance(
          row['subscriptionstartdate'], datetime.date) else None,
        'onboardingdate': datetime.date.isoformat(row['onboardingdate']) if isinstance(
          row['onboardingdate'], datetime.date) else row['onboardingdate'],
        'nextSubscriptionChargeDate': datetime.date.isoformat(row['nextsubscriptionchargedate']) if row[
          'nextsubscriptionchargedate'] else None,
        'subscriptionAmount': row['subscriptionamount'],
        'subscriptionFrequency': row['subscriptionfrequency'],
        'subscriptionTrialPeriod': row['subscriptiontrialperiod'],
        'subscriptionStatus': row['subscriptionstatus'],
        'stripeAccountId': row['stripeaccountid'],
        'merchantTaxRate': row['taxrate'],
        'staffTipsRate': float(row['stafftipsrate']),
        'ubereatsCommission': float(row['ubereatscommission']),
        'squareCommission': float(row['squarecommission']),
        'doordashCommission': float(row['doordashcommission']),
        'grubhubCommission': float(row['grubhubcommission']),
        'flipdishCommission': float(row['flipdishcommission']),
        'processingFeePercentage': float(row['processingfeerate']),
        'processingFeeFixed': float(row['processingfeefixed']),
        'marketplaceTaxRate': float(row['marketplacetaxrate']),
        'stripeTrasnferStatus': stripeTransfersStatus,
        'zip': row['zipcode'],
        'ein': row['ein'],
        'pocdob': row['pointofcontactdob'],
        'businessWebsite': row['businessWebsite'],
        'timezone': row.get('timezone'),
        'onBoardingCompleted': row['onBoardingCompleted'],
        'storeFront': json.loads(row['storefronturls']) if row['storefronturls'] else None,
        'acceptSpecialInstructions': row["acceptspecialinstructions"],
        'esperDeviceId': row["esperdeviceid"],
        'googleReviewsReply': int(row["googleReviewsReply"]),
        'logo': row["logo"],
        'banner': row["banner"],
        'storefrontStatus': int(row["storefrontstatus"]),
        # 'has_address_error': int(row["has_address_error"]),
        'slug': row["slug"],
        "emailDistributionList": row["emaildistributionlist"],
        "cardfeeType": row["cardfeetype"],
        "trasuaryAuthPhone": row["trasuaryauthphone"],
        "latestOtp": row["latestotp"],
        "trasuaryPhoneValid": row["trasuaryphonevalid"],
        "trasuaryAuthPhoneChanged": row["trasuaryauthphonechanged"],
        "changedPhoneOtp": row["changedphoneotp"],
        "pauseStarted_datetime": row["pauseStarted_datetime"].astimezone(gettz('US/Pacific')).strftime(
          "%m-%d-%Y %H:%M (%Z)"),
        "caller": row["caller"],
        "pauseTime_duration": row["pauseTime_duration"],
        "pause_reason": row["pause_reason"],
        "minimumLifetimeRevenue": row["minimumLifetimeRevenue"],
        "RevenueProcessingThreshold": row["RevenueProcessingThreshold"],
        "RevenueProcessingFeePercent": row["RevenueProcessingFeePercent"],
        "AutoWaivedStatus": row["AutoWaivedStatus"],
        "DownTimeThreshold": row["DownTimeThreshold"],
        "storefrontUrl": storefrontUrl if storefrontUrl else '',
        "notificationTextToggle": row["notificationtexttoggle"],
        "notificationText": row["notificationtext"] if row["notificationtext"] else '',
        "is_polling_enabled": int(row["is_polling_enabled"]) if row.get("is_polling_enabled") else 0,
        "polling_frequency": int(row["polling_frequency"]) if row.get("polling_frequency") else 1,
        "is_bogo": int(row["is_bogo"]) if row.get("is_bogo") else 0,
        "platform_price_flag": int(row["platform_price_flag"]) if row.get("platform_price_flag") else 0,
      }

      return merchant
    except Exception as e:
      print(str(e))
      return False

  # PUT
  @classmethod
  def compare_merchants_dict(cls, merchant_info, merchant_info_updated):
    different_keys = []
    for key in merchant_info.keys():
      if key in merchant_info_updated.keys() and merchant_info[key] != merchant_info_updated[key]:
        different_keys.append(key)
    return different_keys

  # @classmethod
  # def check_update_merchants_field(cls,merchant_info, merchant_info_updated):
  #   try:
  #     updated_fields = []
  #     ignore_fields = ['has_address_error','updated_datetime','updated_by','seqNo']
  #     fields_to_compare = cls.compare_merchants_dict(merchant_info,merchant_info_updated)
  #     for merchant_key in fields_to_compare:
  #         old_value = None
  #         new_value = None
  #         if merchant_info[merchant_key] != merchant_info_updated[merchant_key]:
  #           if merchant_key == 'acceptspecialinstructions':
  #             old_value = 'Yes' if merchant_info[merchant_key]==1 else 'No'
  #             new_value = 'Yes' if merchant_info_updated[merchant_key]==1 else 'No'
  #           elif merchant_key == 'autoacceptorder':
  #             old_value = 'Yes' if merchant_info[merchant_key] == 1 else 'No'
  #             new_value = 'Yes' if merchant_info_updated[merchant_key] == 1 else 'No'
  #           elif merchant_key == 'status':
  #             old_value = 'Active' if merchant_info[merchant_key] == 1 else 'Inactive'
  #             new_value = 'Active' if merchant_info_updated[merchant_key] == 1 else 'Inactive'
  #           elif merchant_key == 'busymode':
  #             old_value = 'Yes' if merchant_info[merchant_key] ==1 else 'No'
  #             new_value = 'Yes' if merchant_info_updated[merchant_key] == 1 else 'No'
  #           elif merchant_key == 'parserstatus':
  #             old_value = 'Resume' if merchant_info[merchant_key] == 1 else 'Pause'
  #             new_value = 'Resume' if merchant_info_updated[merchant_key] == 1 else 'Pause'
  #           elif merchant_key == 'order_creation_permission':
  #             old_value = 'Resume' if merchant_info[merchant_key] == 1 else 'Pause'
  #             new_value = 'Resume' if merchant_info_updated[merchant_key] == 1 else 'Pause'
  #           elif merchant_key == 'cardfeetype':
  #             old_value = 'Yes' if merchant_info[merchant_key] == 1 else 'No'
  #             new_value = 'Yes' if merchant_info_updated[merchant_key] == 1 else 'No'
  #           # elif merchant_key == 'has_address_error':
  #           #   continue
  #           # elif merchant_key == 'updated_datetime':
  #           #   continue
  #           # elif merchant_key == "updated_by":
  #           #   continue
  #           # elif merchant_key == "seqNo":
  #           #   continue
  #           elif merchant_key == "closeForBusinessFlag":
  #             old_value = 'close' if merchant_info[merchant_key] == 1 else 'open'
  #             new_value = 'close' if merchant_info_updated[merchant_key] == 1 else 'open'
  #             format_fields = f'close status: <{old_value}> to <{new_value}>'
  #             updated_fields.append(format_fields)
  #             continue
  #           elif merchant_key in ignore_fields:
  #             continue
  #           else:
  #             old_value, new_value = merchant_info[merchant_key], merchant_info_updated[merchant_key]
  #         if old_value or new_value:
  #           format_fields = f'{merchant_key}: <{old_value}> to <{new_value}>'
  #           updated_fields.append(format_fields)
  #     print(updated_fields)
  #     return ','.join(updated_fields)
  #   except Exception as e:
  #     print(str(e))
  #     return False

  @classmethod
  def check_update_merchants_field(cls, merchant_info, merchant_info_updated):
    try:
      updated_fields = []
      ignore_fields = ['has_address_error',
                       'updated_datetime', 'updated_by', 'seqNo']
      #if merchant_info['marketplacepricestatus'] != merchant_info_updated['marketplacepricestatus']:
      #  if merchant_info_updated['marketplacepricestatus'] == 1:
      #    ignore_fields.extend(['ubereatscommission', 'flipdishcommission', 'doordashcommission', 'grubhubcommission'])
        
      fields_to_compare = cls.compare_merchants_dict(
        merchant_info, merchant_info_updated)
      translation_map = {
        "acceptspecialinstructions": lambda v: "Yes" if v == 1 else "No",
        "status": lambda v: "Active" if v == 1 else "Inactive",
        "busymode": lambda v: "Yes" if v == 1 else "No",
        "parserstatus": lambda v: "Resume" if v == 1 else "Pause",
        "order_creation_permission": lambda v: "Resume" if v == 1 else "Pause",
        "closeForBusinessFlag": lambda v: "closed" if v == 1 else "open"
      }
      # Handle "marketplacepricestatus" first
      if "marketplacepricestatus" in fields_to_compare:
          old_value = 'Yes' if merchant_info["marketplacepricestatus"] == 1 else 'No'
          new_value = 'Yes' if merchant_info_updated["marketplacepricestatus"] == 1 else 'No'
          format_fields = f'Market place price status Changed from: <{old_value}> to <{new_value}>'
          updated_fields.append(format_fields)
      for merchant_key in fields_to_compare:
        if merchant_key == "marketplacepricestatus":
          continue  # Skip since we already processed it
        if merchant_key in translation_map:
          old_value = translation_map.get(
            merchant_key)(merchant_info[merchant_key])
          new_value = translation_map.get(merchant_key)(
            merchant_info_updated[merchant_key])
        elif merchant_key == "cardfeetype":
          old_value = 'Yes' if merchant_info[merchant_key] == 1 else 'No'
          new_value = 'Yes' if merchant_info_updated[merchant_key] == 1 else 'No'
          format_fields = f'Storefront - Charge Card Fees to Customer: <{old_value}> to <{new_value}>'
          updated_fields.append(format_fields)
          continue
        elif merchant_key == "autoacceptorder":
          old_value = 'Yes' if merchant_info[merchant_key] == 1 else 'No'
          new_value = 'Yes' if merchant_info_updated[merchant_key] == 1 else 'No'
          format_fields = f'Uber Eats Auto Accept Order Flag: <{old_value}> to <{new_value}>'
          updated_fields.append(format_fields)
          continue
        elif merchant_key == "AutoWaivedStatus":
          old_value = 'Active' if merchant_info[merchant_key] == 1 else 'Inactive'
          new_value = 'Active' if merchant_info_updated[merchant_key] == 1 else 'Inactive'
          format_fields = f'Auto-Subscription Waiver: <{old_value}> to <{new_value}>'
          updated_fields.append(format_fields)
          continue
        elif merchant_key == "notificationtexttoggle":
          old_value = 'Yes' if merchant_info[merchant_key] == 1 else 'No'
          new_value = 'Yes' if merchant_info_updated[merchant_key] == 1 else 'No'
          format_fields = f'Display notification text on android tablet: <{old_value}> to <{new_value}>'
          updated_fields.append(format_fields)
          continue
        elif merchant_key == "is_bogo":
          old_value = 'Enabled' if merchant_info[merchant_key] == 1 else 'Disabled'
          new_value = 'Enabled' if merchant_info_updated[merchant_key] == 1 else 'Disabled'
          format_fields = f'Merchant Bogo Flag:  <{old_value}> to <{new_value}>'
          updated_fields.append(format_fields)
          continue
        elif merchant_key == "platform_price_flag":
          old_value = 'Enabled' if merchant_info[merchant_key] == 1 else 'Disabled'
          new_value = 'Enabled' if merchant_info_updated[merchant_key] == 1 else 'Disabled'
          format_fields = f'Mecrhnat Platform Price Flag: <{old_value}> to <{new_value}>'
          updated_fields.append(format_fields)
          continue
        elif merchant_key == "is_polling_enabled":
          old_value = 'Enabled' if merchant_info[merchant_key] == 1 else 'Disabled'
          new_value = 'Enabled' if merchant_info_updated[merchant_key] == 1 else 'Disabled'
          format_fields = f'Mecrhnat Polling Flag: <{old_value}> to <{new_value}>'
          updated_fields.append(format_fields)
          continue
        elif merchant_key == "polling_frequency":
          old_value, new_value = merchant_info[merchant_key], merchant_info_updated[merchant_key]
          format_fields = f'Merchant Polling Frequency: <{old_value}> to <{new_value}>'
          updated_fields.append(format_fields)
          continue
        elif merchant_key == "notificationtext":
          old_value, new_value = merchant_info[merchant_key], merchant_info_updated[merchant_key]
          format_fields = f'Android tablet notification text: <{old_value}> to <{new_value}>'
          updated_fields.append(format_fields)
          continue
        elif merchant_key in ignore_fields:
          continue
        else:
          old_value, new_value = merchant_info[merchant_key], merchant_info_updated[merchant_key]
        if old_value or new_value:
          if merchant_key == "doordashcommission":
            format_fields = f'Doordash commission: <{old_value}> to <{new_value}>'
            updated_fields.append(format_fields)
          elif merchant_key == "ubereatscommission":
            format_fields = f'Ubereats commission: <{old_value}> to <{new_value}>'
            updated_fields.append(format_fields)
          elif merchant_key == "grubhubcommission":
            format_fields = f'Grubhub commission: <{old_value}> to <{new_value}>'
            updated_fields.append(format_fields)
          elif merchant_key == "flipdishcommission":
            format_fields = f'Flipdish commission: <{old_value}> to <{new_value}>'
            updated_fields.append(format_fields)
          else:
            format_fields = f"{merchant_key}: <{old_value}> to <{new_value}>"
            updated_fields.append(format_fields)
      print(updated_fields)
      return ', '.join(updated_fields)
    except Exception as e:
      print(str(e))
      return False

  @classmethod
  def put_merchant_Settings(cls, merchantId, merchant, jsonLen, user=None):
    try:

      ip_address = None
      if request:
        ip_address = request.environ.get(
          'HTTP_X_FORWARDED_FOR', request.remote_addr)
      if ip_address:
        ip_address = ip_address.split(',')[0].strip()

      create_log_data(level='[INFO]',
                      Message=f"In the start of updating merchant settings details function, ,IP address:{ip_address}",
                      functionName="put_merchant_Settings", merchantID=merchantId, user=user, request=request)
      connection, cursor = get_db_connection()
      preparation_time = merchant.get("preparationTime")
      delay_time = merchant.get("orderDelayTime")
      if not preparation_time or not delay_time:
        return 'Order Preparation Time and Order Delay Time cannot be 0', True
      merchant_info = cls.get_merchant_by_id(merchantId)
      if jsonLen <= 3:
        busy_mode = merchant.get("busyMode")
        data = (preparation_time, busy_mode, delay_time, merchantId)
        cursor.execute("""
                        UPDATE merchants 
                        SET 
                          preparationtime=COALESCE(%s, preparationtime),
                          busymode=COALESCE(%s, busymode),
                          orderdelaytime=COALESCE(%s, orderdelaytime)
                        WHERE id = %s
                        """, data)
        connection.commit()
      else:
        status = merchant.get('merchantStatus')
        autoAcceptOrder = merchant.get("autoAcceptOrder")
        busy_mode = merchant.get("busyMode")
        _email = merchant.get('email')
        _parserStatus = merchant.get('parserStatus')
        language = merchant.get('language')
        order_creation_permission = merchant.get(
          'order_creation_permission')
        acceptSpecialInstructions = merchant.get(
          "acceptSpecialInstructions")
        googleReviewsReply = int(merchant.get("googleReviewsReply")) if merchant.get(
          "googleReviewsReply") else 1
        emailDistributionList = merchant.get('emailDistributionList')
        if emailDistributionList:
          emailDistributionList = emailDistributionList.split(";")
          if _email not in emailDistributionList:
            emailDistributionList.append(_email)
          emailDistributionList = ";".join(emailDistributionList)
        cardfeetype = merchant.get("cardfeeType")
        onboardingdate = merchant.get("onboardingdate")
        notificationtext = merchant.get("notificationText")
        notificationtexttoggle = merchant.get("notificationTextToggle")
        is_bogo = merchant.get("is_bogo")
        polling_frequency = merchant.get("polling_frequency")
        is_polling_enabled = merchant.get("is_polling_enabled")
        platform_price_flag = merchant.get("platform_price_flag")
        if notificationtexttoggle==0:
          notificationtext=''
        create_log_data(level='[INFO]', Message="Successfully get updated merchant settings details from request",
                        functionName="put_merchant_Settings", merchantID=merchantId, user=user, request=request)
        data = (
        status, _parserStatus, order_creation_permission, user['id'], language, autoAcceptOrder, preparation_time,
        delay_time,
        busy_mode,
        acceptSpecialInstructions, googleReviewsReply, emailDistributionList, cardfeetype,
        onboardingdate,notificationtext,notificationtexttoggle,is_bogo,polling_frequency,is_polling_enabled,platform_price_flag, merchantId)
        cursor.execute("""
                  UPDATE merchants 
                  SET 
                    status=COALESCE(%s, status), 
                    parserstatus=COALESCE(%s, parserstatus), 
                    order_creation_permission=COALESCE(%s, order_creation_permission),
                    updated_by=%s, 
                    language=COALESCE(%s, language),
                    autoacceptorder=COALESCE(%s, autoacceptorder), 
                    preparationtime=COALESCE(%s, preparationtime), 
                    orderdelaytime=COALESCE(%s, orderdelaytime), 
                    busymode=COALESCE(%s, busymode),
                    acceptspecialinstructions=COALESCE(%s, acceptspecialinstructions),
                    googleReviewsReply=COALESCE(%s, googleReviewsReply),
                    emaildistributionlist=COALESCE(%s, emaildistributionlist),
                    cardfeetype=COALESCE(%s, cardfeetype),
                    onboardingdate=COALESCE(%s, onboardingdate),
                    notificationtext=COALESCE(%s, notificationtext),
                    notificationtexttoggle=COALESCE(%s, notificationtexttoggle),
                    is_bogo=COALESCE(%s, is_bogo),
                    polling_frequency=COALESCE(%s, polling_frequency),
                    is_polling_enabled=COALESCE(%s, is_polling_enabled),
                    platform_price_flag=COALESCE(%s, platform_price_flag)
                  WHERE id = %s
                  """, data)
        connection.commit()
      merchant_info_updated = cls.get_merchant_by_id(merchantId)
      if "notificationtexttoggle" in merchant_info_updated and (
          merchant_info_updated.get("notificationtexttoggle") != merchant_info.get(
          "notificationtexttoggle") or merchant_info_updated.get("notificationtext") != merchant_info.get(
          "notificationtext")):
        connections = Websockets.get_connection_by_mid_and_eventname(
          merchantId=merchantId, eventName="android.order")
        print("Android Connection List ", connections)
        if type(connections) is list:
          for connection in connections:
            deviceId = connection.get("connectionId")

            try:
              response = send_android_notification_api(deviceId=deviceId, subject="merchant.text_notification",
                                                       merchantId=merchantId)
              # print(response.text)
              if response.status_code >= 200 and response.status_code < 300:
                create_log_data(level='[INFO]',
                                Message=f"Posted notification to android against merchantId: {merchantId} and deviceid: {deviceId}",
                                messagebody=response,
                                functionName="put_merchant_Settings")
                print(
                  f"Posted notification to android against merchantId: {merchantId} and deviceid: {deviceId}")
              else:
                create_log_data(level='[INFO]',
                                Message=f"Unable to posting notification to android against merchantId: {merchantId} and Device Id:{deviceId}",
                                messagebody=response.text,
                                functionName="put_merchant_Settings")
                print(
                  f"Unable to posting notification to android against merchantId: {merchantId} and Device Id:{deviceId}, Response is {response.text}")
            except Exception as e:
              print("Error: ", str(e))
      messagebody = cls.check_update_merchants_field(
        merchant_info, merchant_info_updated)

      if messagebody:
        messagebody += f" ,IP Address: {ip_address}"
        print("Triggering sns - merchant.update ...")
        sns_msg = {
          "event": "merchant.update",
          "body": {
            "merchantId": merchantId,
            "userId": user['id'],
            "eventName": "merchant.settings_update",
            "eventDetails": messagebody
          }
        }
        logs_sns_resp = publish_sns_message(topic=config.sns_activity_logs, message=str(sns_msg),
                                            subject="merchant.update")
        create_log_data(level='[INFO]', Message="Updated settings details of merchant",
                        functionName="put_merchant_Settings", merchantID=merchantId, user=user, request=request)
      return '', False
    except Exception as e:
      print(str(e))
      create_log_data(level='[ERROR]', Message="Failed to update merchant settings details",
                      messagebody=f"An error occured while updating merchant settings details {str(e)}",
                      functionName="put_merchant_Settings", merchantID=merchantId, user=user, request=request)
      return str(e), True

  @classmethod
  def connect_stripe_account(cls, merchantId, merchant, user=None):
    try:
      create_log_data(level='[INFO]', Message="In the start of connecting stripe account function",
                      functionName="connect_stripe_account", merchantID=merchantId, user=user, request=request)
      connection, cursor = get_db_connection()
      legalBusinessName = merchant.get('legalBusinessName')
      businessTaxId = merchant.get('businessTaxId')
      businessAddressLine = merchant.get('businessAddressLine')
      businessAddressCity = merchant.get('businessAddressCity')
      businessAddressState = merchant.get('businessAddressState')
      bankAccountNumber = merchant.get('bankAccountNumber')
      bankAccountRoutingNumber = merchant.get('bankAccountRoutingNumber')
      businessWebsite = merchant.get('businessWebsite')
      connect_id = merchant.get('stripeAccountId')
      zipcode = merchant.get("zip")
      _fn = merchant.get('firstName')
      _ln = merchant.get('lastName')
      _phone = merchant.get('phone')
      _email = merchant.get('email')
      _address = merchant.get('address')
      is_error_in_account = False
      # check if strip is already connected
      if connect_id is not None and connect_id != "" and connect_id != "null":
        return "Stripe account already created", True

      if not (
          bankAccountRoutingNumber == "" or bankAccountNumber == "" or businessWebsite == "" or legalBusinessName == "" or businessAddressLine == "" or _phone == "" or businessTaxId == "" or businessTaxId == None):

        connect_id, is_error_in_account = open_stripe_connect_account(_fn, _ln, bankAccountRoutingNumber,
                                                                      bankAccountNumber, _email, businessWebsite,
                                                                      businessAddressLine,
                                                                      zipcode,
                                                                      businessAddressState,
                                                                      businessAddressCity, _phone, legalBusinessName,
                                                                      businessTaxId)

        # if error occured while connecting stripe then return
        if is_error_in_account:
          return connect_id, is_error_in_account

      else:
        return "Complete business information is required to open stripe account", True
      create_log_data(level='[INFO]', Message="Successfully open stripe account",
                      messagebody=f'Stripe account of merchant {merchantId} open successfully',
                      functionName="connect_stripe_account", merchantID=merchantId, user=user, request=request)

      data = (connect_id, merchantId)
      cursor.execute("""
              UPDATE merchants 
              SET 
                stripeaccountid=COALESCE(%s, stripeaccountid)
              WHERE id = %s
              """, data)
      connection.commit()
      create_log_data(level='[INFO]', Message="Stripe connect id updated successfully",
                      functionName="connect_stripe_account", merchantID=merchantId, user=user, request=request)
      return connect_id, is_error_in_account
    except Exception as e:
      print(str(e))
      create_log_data(level='[ERROR]', Message="Failed to connect stripe account",
                      messagebody=f"An error occured while connecting stripe {str(e)}",
                      functionName="connect_stripe_account", merchantID=merchantId, user=user, request=request)
      return str(e), True

  @classmethod
  def put_merchant_account(cls, merchantId, merchant, user=None):
    try:

      ip_address = None
      if request:
        ip_address = request.environ.get(
          'HTTP_X_FORWARDED_FOR', request.remote_addr)
      if ip_address:
        ip_address = ip_address.split(',')[0].strip()
      create_log_data(level='[INFO]',
                      Message=f"In the start of updating merchant account details function,  IP Address: {ip_address}",
                      functionName="put_merchant_Account", merchantID=merchantId, user=user, request=request)
      connection, cursor = get_db_connection()
      merchant_info = cls.get_merchant_by_id(merchantId)
      _mn = merchant.get('merchantName')
      _fn = merchant.get('firstName')
      _ln = merchant.get('lastName')
      _phone = merchant.get('phone')
      _address = merchant.get('address')
      pocdob = merchant.get('pocdob')
      zipcode = merchant.get("zip")
      businessWebsite = merchant.get('businessWebsite')
      businessNumber = merchant.get('businessNumber')
      legalBusinessName = merchant.get('legalBusinessName')
      businessTaxId = merchant.get('businessTaxId')
      businessAddressLine = merchant.get('businessAddressLine')
      businessAddressCity = merchant.get('businessAddressCity')
      businessAddressState = merchant.get('businessAddressState')
      bankAccountNumber = merchant.get('bankAccountNumber')
      bankAccountRoutingNumber = merchant.get('bankAccountRoutingNumber')

      slug = cls.generate_slug(merchantId, merchant.get(
        "slug")) if "slug" in merchant else merchant_info['slug']
      store_front = None
      if "storeFront" in merchant:
        store_front = merchant.get("storeFront")
        store_front = json.dumps(store_front)
      if businessAddressLine == '':
        has_address_error = 1
      else:
        response = cls.checkpickupaddress(store_front=merchant)
        if response:
          has_address_error = 1
        else:
          has_address_error = 0
      create_log_data(level='[INFO]',
                      Message=f"Successfully get updated merchant account details from request,  IP Address: {ip_address}",
                      functionName="put_merchant_Account", merchantID=merchantId, user=user, request=request)
      if businessTaxId is not None:
        if "*" in businessTaxId:
          businessTaxId = None
      if bankAccountNumber is not None:
        if "*" in bankAccountNumber:
          bankAccountNumber = None
        elif bankAccountNumber != merchant_info['bankaccountnumber'] and merchant_info.get('stripeaccountid'):
          if bankAccountNumber=='' or bankAccountRoutingNumber =='':
            return 'The account number or routing number is missing.' , True
          resp, is_error = update_new_open_stripe_connect_account(_fn, _ln, bankAccountRoutingNumber, bankAccountNumber,
                                                                  merchant_info.get('stripeaccountid'))
          if is_error:
            return resp, True

      data = (_mn, _fn, _ln, _address, _phone, user['id'],
              businessNumber,
              legalBusinessName, businessTaxId, businessAddressLine, businessAddressCity, businessAddressState,
              bankAccountNumber, bankAccountRoutingNumber, pocdob, businessWebsite, store_front,
              has_address_error, zipcode, slug, merchantId)
      cursor.execute("""
              UPDATE merchants 
              SET 
                merchantname=COALESCE(%s, merchantname), 
                firstname=COALESCE(%s, firstname), 
                lastname=COALESCE(%s, lastname), 
                address=COALESCE(%s, address),
                phone=COALESCE(%s, phone), 
                updated_by=%s, 
                businessnumber=COALESCE(%s, businessnumber), 
                legalbusinessname=COALESCE(%s, legalbusinessname), 
                businesstaxid=COALESCE(%s, businesstaxid), 
                businessaddressline=COALESCE(%s, businessaddressline), 
                businessaddresscity=COALESCE(%s, businessaddresscity), 
                businessaddressstate=COALESCE(%s, businessaddressstate), 
                bankaccountnumber=COALESCE(%s, bankaccountnumber), 
                bankaccountroutingnumber=COALESCE(%s, bankaccountroutingnumber),
                pointofcontactdob=COALESCE(%s, pointofcontactdob), 
                businessWebsite=COALESCE(%s, businessWebsite),
                storefronturls=COALESCE(%s, storefronturls),
                has_address_error=COALESCE(%s, has_address_error),
                zipcode=COALESCE(%s, zipcode),
                slug=COALESCE(%s, slug)
                WHERE id = %s
                """, data)
      connection.commit()
      create_log_data(level='[INFO]', Message=f"Updated account details of merchant,  IP Address: {ip_address}",
                      functionName="put_merchant_Account", merchantID=merchantId, user=user, request=request)
      merchant_info_updated = cls.get_merchant_by_id(merchantId)
      messagebody = cls.check_update_merchants_field(
        merchant_info, merchant_info_updated)

      if messagebody:
        messagebody += f", IP Address: {ip_address}"
        sns_msg = {
          "event": "merchant.update",
          "body": {
            "merchantId": merchantId,
            "userId": user['id'],
            "eventName": "merchant.accountdetails_update",
            "eventDetails": messagebody

          }
        }
        logs_sns_resp = publish_sns_message(topic=config.sns_activity_logs, message=str(sns_msg),
                                            subject="merchant.update")
      return '', False

    except Exception as e:
      print(str(e))
      create_log_data(level='[ERROR]', Message="Failed to update merchant record",
                      messagebody=f"An error occured while updating merchant record {str(e)},  IP Address: {ip_address}",
                      functionName="put_merchant_Account", merchantID=merchantId, user=user, request=request)
      return str(e), True

  @classmethod
  def put_storefront_slug(cls, slug, merchantId, user, userId=None, ip_address=None):
      connection, cursor = get_db_connection()
      merchant_info = cls.get_merchant_by_id(merchantId)
      slug = cls.generate_slug(merchantId, slug) if slug else merchant_info['slug']
      try:
          data = (slug, merchantId)
          cursor.execute(""" 
            UPDATE merchants 
            SET 
                slug = COALESCE(%s, slug)
            WHERE id = %s
        """, data)
          connection.commit()

          if merchant_info['slug'] != slug:
              sns_msg = {
                  "event": "storefront.slug_url",
                  "body": {
                      "merchantId": merchantId,
                      "userId": userId,
                      "eventDetails": f"URL Slug changed from {merchant_info['slug']} to {slug} ,IP address {ip_address}"
                  }
              }
              create_log_data(level='[INFO]',
                              Message=f"Publish sns message of slug changed from {merchant_info['slug']} to {slug}",
                              functionName="put_merchant", merchantID=merchantId, user=user, request=request)
              logs_sns_resp = publish_sns_message(topic=config.sns_activity_logs, message=str(sns_msg),
                                                  subject="storefront.slug_url")

          return True
      except Exception as e:
          print(e)
          return False

  # storefront status
  @classmethod
  def put_storefront_status(cls, storefrontstatus, merchantId, user, userId=None, ip_address=None):
      connection, cursor = get_db_connection()
      merchant_info = cls.get_merchant_by_id(merchantId)

      try:
          data = (storefrontstatus, merchantId)
          cursor.execute(""" 
              UPDATE merchants 
              SET 
                  storefrontstatus=COALESCE(%s, storefrontstatus)
              WHERE id = %s
          """, data)
          connection.commit()

          if merchant_info['storefrontstatus'] != storefrontstatus:
              if storefrontstatus == 1:
                  print("Triggering sns - storefront.enabled ...")
                  sns_msg = {
                      "event": "storefront.enabled",
                      "body": {
                          "merchantId": merchantId,
                          "userId": userId,
                          "ipAddr": ip_address,
                      }
                  }
                  create_log_data(level='[INFO]', Message=f"Publish sns of storefornt enabled, IP address {ip_address}",
                                  functionName="put_merchant", merchantID=merchantId, user=user, request=request)
                  logs_sns_resp = publish_sns_message(topic=config.sns_activity_logs, message=str(sns_msg),
                                                      subject="storefront.enabled")
              if storefrontstatus == 0:
                  print("Triggering sns - storefront.disabled ...")
                  sns_msg = {
                      "event": "storefront.disabled",
                      "body": {
                          "merchantId": merchantId,
                          "userId": userId,
                        "ipAddr": ip_address,
                      }
                  }
                  create_log_data(level='[INFO]', Message=f"Publish sns of storefornt disabled",
                                  functionName="put_merchant", merchantID=merchantId, user=user, request=request)
                  logs_sns_resp = publish_sns_message(topic=config.sns_activity_logs, message=str(sns_msg),
                                                      subject="storefront.disabled")

          return True
      except Exception as e:
          print(e)
          return False

  @classmethod
  def put_merchant(cls, merchantId, merchant, user, userId=None, ip_address=None):
    try:
      create_log_data(level='[INFO]',
                      Message=f"In the start of updating merchant details function, , IP address:{ip_address}",
                      functionName="put_merchant", merchantID=merchantId, user=user, request=request)
      connection, cursor = get_db_connection()
      merchant_info = cls.get_merchant_by_id(merchantId)
      current_busymode_status = merchant_info['busymode']

      _mn = merchant.get('merchantName')
      _fn = merchant.get('firstName')
      _ln = merchant.get('lastName')
      _phone = merchant.get('phone')
      _address = merchant.get('address')
      _status = merchant.get('merchantStatus')
      _parserStatus = merchant.get('parserStatus')
      order_creation_permission = merchant.get(
        'order_creation_permission')
      _lat = merchant.get('merchantlat')
      _long = merchant.get('merchantlong')
      businessNumber = merchant.get('businessNumber')
      legalBusinessName = merchant.get('legalBusinessName')
      language = merchant.get('language')
      businessTaxId = merchant.get('businessTaxId')
      businessAddressLine = merchant.get('businessAddressLine')
      businessAddressCity = merchant.get('businessAddressCity')
      businessAddressState = merchant.get('businessAddressState')
      bankAccountNumber = merchant.get('bankAccountNumber')
      bankAccountRoutingNumber = merchant.get('bankAccountRoutingNumber')
      autoAcceptOrder = merchant.get("autoAcceptOrder")
      busy_mode = merchant.get("busyMode")
      preparation_time = merchant.get("preparationTime")
      delay_time = merchant.get("orderDelayTime")
      zipcode = merchant.get("zip")
      pocdob = merchant.get('pocdob')
      _email = merchant.get('email')
      businessWebsite = merchant.get('businessWebsite')
      connect_id = merchant.get('stripeAccountId')
      merchantTimezone = merchant.get("timezone")
      store_front = ''
      has_address_error = merchant.get("has_address_error") if merchant.get(
        "has_address_error") else 0
      # if isinstance(merchant.get('storefrontStatus'), bool) and merchant.get('storefrontStatus') == True:
      if businessAddressLine == '':
        has_address_error = 1
      else:
        response = cls.checkpickupaddress(store_front=merchant)
        if response:
          has_address_error = 1
        else:
          has_address_error = 0
      connection, cursor = get_db_connection()
      acceptSpecialInstructions = merchant.get("acceptSpecialInstructions") if merchant.get(
        "acceptSpecialInstructions") and int(merchant.get("acceptSpecialInstructions")) in (0, 1) else None
      googleReviewsReply = int(merchant.get("googleReviewsReply")) if merchant.get(
        "googleReviewsReply") else 1
      storefrontstatus = int(merchant.get(
        "storefrontStatus")) if "storefrontStatus" in merchant else merchant_info['storefrontstatus']
      slug = cls.generate_slug(merchantId, merchant.get(
        "slug")) if "slug" in merchant else merchant_info['slug']
      emailDistributionList = merchant_info.get('emaildistributionlist')
      if merchant.get("emailDistributionList"):
        emailDistributionList = merchant.get("emailDistributionList")
        emailDistributionList = emailDistributionList.split(";")
        if _email not in emailDistributionList:
          emailDistributionList.append(_email)
        emailDistributionList = ";".join(emailDistributionList)
      cardfeetype = merchant.get("cardfeeType")
      onboardingdate = merchant.get("onboardingdate")

      if "storeFront" in merchant:
        store_front = merchant.get("storeFront")
        store_front = json.dumps(store_front)

      create_log_data(level='[INFO]', Message="Successfully get updated merchant details from request",
                      functionName="put_merchant", merchantID=merchantId, user=user, request=request)
      # open stripe account
      is_error_in_account = False
      if merchant.get("openStripeConnectAccount"):

        # check if strip is already connected
        if connect_id is not None and connect_id != "" and connect_id != "null":
          return "Stripe account already created", True

        if not (
            bankAccountRoutingNumber == "" or bankAccountNumber == "" or businessWebsite == "" or legalBusinessName == "" or businessAddressLine == "" or _phone == "" or businessTaxId == "" or businessTaxId == None):

          connect_id, is_error_in_account = open_stripe_connect_account(_fn, _ln, bankAccountRoutingNumber,
                                                                        bankAccountNumber, _email, businessWebsite,
                                                                        businessAddressLine,
                                                                        zipcode,
                                                                        businessAddressState,
                                                                        businessAddressCity, _phone, legalBusinessName,
                                                                        businessTaxId)

          # if error occured while connecting stripe then return
          if is_error_in_account:
            create_log_data(level='[INFO]', Message=f"Unable to open stripe account, IP address:{ip_address}",
                            messagebody=f'An error occured {connect_id}',
                            functionName="put_merchant", merchantID=merchantId, user=user, request=request)
            return connect_id, is_error_in_account
          create_log_data(level='[INFO]', Message=f"Successfully open stripe account, IP address:{ip_address}",
                          messagebody=f'Stripe account of merchant {merchantId} open successfully',
                          functionName="put_merchant", merchantID=merchantId, user=user, request=request)

        else:
          return "Complete business information is required to open stripe account", True

      # update merchant
      data = (_mn, _fn, _ln, _address, _status, _parserStatus, order_creation_permission, _phone, userId, _lat, _long,
              businessNumber,
              legalBusinessName, language, businessTaxId, businessAddressLine, businessAddressCity,
              businessAddressState,
              bankAccountNumber, bankAccountRoutingNumber, autoAcceptOrder, preparation_time, delay_time,
              busy_mode, zipcode, pocdob, businessWebsite, connect_id, merchantTimezone, store_front,
              acceptSpecialInstructions, googleReviewsReply, slug, storefrontstatus, emailDistributionList, cardfeetype,
              onboardingdate, has_address_error, merchantId)
      cursor.execute("""
        UPDATE merchants 
        SET 
          merchantname=COALESCE(%s, merchantname), 
          firstname=COALESCE(%s, firstname), 
          lastname=COALESCE(%s, lastname), 
          address=COALESCE(%s, address), 
          status=COALESCE(%s, status), 
          parserstatus=COALESCE(%s, parserstatus), 
          order_creation_permission=COALESCE(%s, order_creation_permission), 
          phone=COALESCE(%s, phone), 
          updated_by=%s, 
          lat=COALESCE(%s, lat), 
          longitude=COALESCE(%s, longitude), 
          businessnumber=COALESCE(%s, businessnumber), 
          legalbusinessname=COALESCE(%s, legalbusinessname), 
          language=COALESCE(%s, language),
          businesstaxid=COALESCE(%s, businesstaxid), 
          businessaddressline=COALESCE(%s, businessaddressline), 
          businessaddresscity=COALESCE(%s, businessaddresscity), 
          businessaddressstate=COALESCE(%s, businessaddressstate), 
          bankaccountnumber=COALESCE(%s, bankaccountnumber), 
          bankaccountroutingnumber=COALESCE(%s, bankaccountroutingnumber), 
          autoacceptorder=COALESCE(%s, autoacceptorder), 
          preparationtime=COALESCE(%s, preparationtime), 
          orderdelaytime=COALESCE(%s, orderdelaytime), 
          busymode=COALESCE(%s, busymode), 
          zipcode=COALESCE(%s, zipcode), 
          pointofcontactdob=COALESCE(%s, pointofcontactdob), 
          businessWebsite=COALESCE(%s, businessWebsite), 
          stripeaccountid=COALESCE(%s, stripeaccountid), 
          timezone=COALESCE(%s, timezone),
          storefronturls=COALESCE(%s, storefronturls),
          acceptspecialinstructions=COALESCE(%s, acceptspecialinstructions),
          googleReviewsReply=COALESCE(%s, googleReviewsReply),
          slug=COALESCE(%s, slug),
          storefrontstatus=COALESCE(%s, storefrontstatus),
          emaildistributionlist=COALESCE(%s, emaildistributionlist),
          cardfeetype=COALESCE(%s, cardfeetype),
          onboardingdate=COALESCE(%s, onboardingdate),
          has_address_error=COALESCE(%s, has_address_error)
        WHERE id = %s
        """, data)
      connection.commit()
      create_log_data(level='[INFO]', Message="Updated details of merchant",
                      functionName="put_merchant", merchantID=merchantId, user=user, request=request)

      if current_busymode_status != int(busy_mode):
        if int(busy_mode) == 1:
          message = "Busy mode is turned on"
        else:
          message = "Busy mode is turned off"
        message += f",IP Address: {ip_address}"
        print("Triggering sns - busymode.update ...")
        sns_msg = {
          "event": "busymode.update",
          "body": {
            "merchantId": merchantId,
            "userId": userId,
            "message": message,
            "ipAddr": ip_address,
          }
        }
        create_log_data(level='[INFO]', Message=f"Publish sns of {message},IP:{ip_address}",
                        functionName="put_merchant", merchantID=merchantId, user=user, request=request)
        logs_sns_resp = publish_sns_message(topic=config.sns_activity_logs, message=str(sns_msg),
                                            subject="busymode.update")

      if merchant_info['storefrontstatus'] != storefrontstatus:

        if storefrontstatus == 1:
          print("Triggering sns - storefront.enabled ...")
          sns_msg = {
            "event": "storefront.enabled",
            "body": {
              "merchantId": merchantId,
              "userId": userId,
              "ipAddr": ip_address,
            }
          }
          create_log_data(level='[INFO]', Message=f"Publish sns of storefornt enabled , IP address:{ip_address}",
                          functionName="put_merchant", merchantID=merchantId, user=user, request=request)
          logs_sns_resp = publish_sns_message(topic=config.sns_activity_logs, message=str(sns_msg),
                                              subject="storefront.enabled")
        if storefrontstatus == 0:
          print("Triggering sns - storefront.disabled ...")
          sns_msg = {
            "event": "storefront.disabled",
            "body": {
              "merchantId": merchantId,
              "userId": userId,
              "ipAddr": ip_address,
            }
          }
          create_log_data(level='[INFO]', Message=f"Publish sns of storefornt disabled",
                          functionName="put_merchant", merchantID=merchantId, user=user, request=request)
          logs_sns_resp = publish_sns_message(topic=config.sns_activity_logs, message=str(sns_msg),
                                              subject="storefront.disabled")
      if merchant_info['slug'] != slug:
        sns_msg = {
          "event": "storefront.slug_url",
          "body": {
            "merchantId": merchantId,
            "userId": userId,
            "eventDetails": f"URL Slug changed from {merchant_info['slug']} to {slug}, IP address:{ip_address}"
          }
        }
        create_log_data(level='[INFO]',
                        Message=f"Publish sns message of slug changed from {merchant_info['slug']} to {slug}, IP address:{ip_address}",
                        functionName="put_merchant", merchantID=merchantId, user=user, request=request)
        logs_sns_resp = publish_sns_message(topic=config.sns_activity_logs, message=str(sns_msg),
                                            subject="storefront.slug_url")

      return connect_id, is_error_in_account

    except Exception as e:
      print(str(e))
      create_log_data(level='[ERROR]', Message=f"Failed to update merchnat details, IP address:{ip_address}",
                      messagebody=f"An error occured {str(e)}",
                      functionName="put_merchant", merchantID=merchantId, user=user, request=request)
      return str(e), True

  @classmethod
  def update_merchant_business_info(cls, merchantId, merchant, user, userId=None):
    try:

      ip_address = get_ip_address(request)

      connection, cursor = get_db_connection()
      create_log_data(level='[INFO]',
                      Message=f"Inside update merchant business details function.",
                      merchantID=merchantId, functionName="update_merchant_business_info", user=user, request=request)
      merchant_details = cls.get_merchant_by_id(merchantId)

      TaxRate = merchant.get("merchantTaxRate") if is_float(
        merchant.get("merchantTaxRate")) else None
      StaffTipsRate = merchant.get("staffTipsRate") if is_float(
        merchant.get("staffTipsRate")) else None
      UbereatsCommission = merchant.get("ubereatsCommission") if is_float(
        merchant.get("ubereatsCommission")) else None
      DoordashCommission = merchant.get("doordashCommission") if is_float(
        merchant.get("doordashCommission")) else None
      GrubhubCommission = merchant.get("grubhubCommission") if is_float(
        merchant.get("grubhubCommission")) else None
      FlipdishCommission = merchant.get("flipdishCommission") if is_float(
        merchant.get("flipdishCommission")) else None
      processingFeeRate = merchant.get("processingFeePercentage") if is_float(
        merchant.get("processingFeePercentage")) else None
      processingFeeFixed = merchant.get("processingFeeFixed") if is_float(
        merchant.get("processingFeeFixed")) else None
      marketplaceTaxRate = merchant.get("marketplaceTaxRate") if is_float(
        merchant.get("marketplaceTaxRate")) else None

      minimumLifetimeRevenue = merchant.get("minimumLifetimeRevenue") if is_float(
        merchant.get("minimumLifetimeRevenue")) else None
      RevenueProcessingThreshold = merchant.get("RevenueProcessingThreshold") if is_float(
        merchant.get("RevenueProcessingThreshold")) else None
      RevenueProcessingFeePercent = merchant.get("RevenueProcessingFeePercent") if is_float(
        merchant.get("RevenueProcessingFeePercent")) else None
      SquareCommission = merchant.get("squareCommission") if is_float(
        merchant.get("squareCommission")) else None
      AutoWaivedStatus = merchant.get("AutoWaivedStatus") if is_float(
        merchant.get("AutoWaivedStatus")) else False
      DownTimeThreshold = merchant.get("DownTimeThreshold") if is_float(
        merchant.get("DownTimeThreshold")) else None

      marketplacepricestatus = merchant.get(
        "MarketPlacePriceStatus") if "MarketPlacePriceStatus" in merchant else 0

      subscriptionamount = merchant.get("subscriptionAmount") if is_float(
        merchant.get("subscriptionAmount")) else None
      marketplacepricestatus = merchant.get(
        "MarketPlacePriceStatus") if "MarketPlacePriceStatus" in merchant else 0

      create_log_data(level='[INFO]', Message="Successfully get updated merchant business details from request.",
                      functionName="update_merchant_business_info", user=user, request=request)
      message_list = []


      data = (userId,
              TaxRate, StaffTipsRate, UbereatsCommission, SquareCommission, DoordashCommission, GrubhubCommission,
              FlipdishCommission,
              processingFeeRate, processingFeeFixed, marketplaceTaxRate, minimumLifetimeRevenue,
              RevenueProcessingThreshold,

              RevenueProcessingFeePercent, AutoWaivedStatus, DownTimeThreshold, marketplacepricestatus,
              subscriptionamount,

              merchantId)
      cursor.execute("""
        UPDATE merchants
          SET updated_by=%s, updated_datetime=CURRENT_TIMESTAMP,
             taxrate=COALESCE(%s, taxrate), stafftipsrate=COALESCE(%s, stafftipsrate),
             ubereatscommission=COALESCE(%s, ubereatscommission), squarecommission=COALESCE(%s, squarecommission), 
             doordashcommission=COALESCE(%s, doordashcommission), grubhubcommission=COALESCE(%s, grubhubcommission), 
             flipdishcommission=COALESCE(%s, flipdishcommission),
             processingfeerate=COALESCE(%s, processingfeerate), processingfeefixed=COALESCE(%s, processingfeefixed), 
             marketplacetaxrate=COALESCE(%s, marketplacetaxrate), minimumLifetimeRevenue=COALESCE(%s, minimumLifetimeRevenue),
             RevenueProcessingThreshold=COALESCE(%s, RevenueProcessingThreshold), 
             RevenueProcessingFeePercent=COALESCE(%s, RevenueProcessingFeePercent),

             AutoWaivedStatus=COALESCE(%s, AutoWaivedStatus), DownTimeThreshold=COALESCE(%s, DownTimeThreshold),

              marketplacepricestatus=COALESCE(%s, marketplacepricestatus) , subscriptionamount=COALESCE(%s, subscriptionamount)

            WHERE id = %s""", data)
      connection.commit()
      create_log_data(level='[INFO]', Message="Uodated business details of merchant ",
                      functionName="update_merchant_business_info", user=user, request=request)

      

      # Triggering SNS - merchant.update_business_info
      # sns_msg = {
      #   "event": "merchant.update_business_info",
      #   "body": {
      #     "merchantId": merchantId,
      #     "userId": userId,
      #     "old_merchant_details": merchant_details
      #   }
      # }
      # logs_sns_resp = publish_sns_message(topic=config.sns_audit_logs, message=str(sns_msg),
      #                                     subject="merchant.update_business_info")
      merchant_info_updated = cls.get_merchant_by_id(merchantId)
      messagebody = cls.check_update_merchants_field(
        merchant_details, merchant_info_updated)

      if messagebody:
        messagebody += f", IP Address: {ip_address}"
        print("Triggering sns - merchant.update ...")
        sns_msg = {
          "event": "merchant.update",
          "body": {
            "merchantId": merchantId,
            "userId": userId,
            "eventName": "merchant.finance_update",
            "eventDetails": messagebody
          }
        }
        logs_sns_resp = publish_sns_message(topic=config.sns_activity_logs, message=str(sns_msg),
                                            subject="merchant.update")
      return True
    except Exception as e:
      print(str(e))
      if e.args[0] == 1062:
        return 'Duplicate Entry'
      return False

  def post_PauseResumeTime(userid, merchantid, eventtype):
    try:

      connection, cursor = get_db_connection()
      id = uuid.uuid4()
      data = (id, userid, merchantid, eventtype)
      cursor.execute("""INSERT INTO pauseresumetimes 
         (id, userid,merchantid,eventtype)
         VALUES (%s,%s,%s,%s)""", data)
      connection.commit()

      return id
    except Exception as e:
      print("Error: ", str(e))
      return False

  @classmethod
  def update_marketplace_status(cls, merchantId, marketStatus, pause_reason, pauseTime_duration, caller, userId=None):
    try:
        connection, cursor = get_db_connection()

        data = (marketStatus, userId, caller,
                pauseTime_duration, pause_reason, merchantId)
        cursor.execute(
            "UPDATE merchants SET marketstatus=%s, updated_by=%s,caller=%s, pauseTime_duration=%s,pause_reason=%s, pauseStarted_datetime=CURRENT_TIMESTAMP,updated_datetime=CURRENT_TIMESTAMP WHERE id=%s",
            data)
        
        connection.commit()
        return True
    except Exception as e:
        print(str(e))
        return False
  @classmethod
  def update_stream_platform_status(cls, merchantId, Status, platform, userId=None , is_main_merchant=None , VmerchantId=None):
    try:
        connection, cursor = get_db_connection()


        if is_main_merchant:
          data = (Status, userId, merchantId)
          cursor.execute(
              f"UPDATE merchants SET {platform}=%s, updated_by=%s,updated_datetime=CURRENT_TIMESTAMP WHERE id=%s",
              data)
        else:
          data = (Status, userId, VmerchantId)
          cursor.execute(
            f"UPDATE virtualmerchants SET {platform}=%s, updated_by=%s,updated_datetime=CURRENT_TIMESTAMP WHERE id=%s",
            data)
        connection.commit()
        return True
    except Exception as e:
        print(str(e))
        return False

  @classmethod
  def update_subscription_status(cls, merchantId, subscriptionStatus, subscriptionTrialPeriod, subscriptionFrequency,
                                 subscriptionStartDate, userId=None):
    try:
      connection, cursor = get_db_connection()
      create_log_data(level='[INFO]', Message=f"Triggering updating subscription status method.",
                      messagebody=f"",
                      functionName="update_subscription_status", request=request)
      next_charge_date = None
      if subscriptionStatus == 1:
        merchant_details = cls.get_merchant_by_id(merchantId)
        create_log_data(level='[INFO]', Message=f"Successfully retrieved merchant details",
                        messagebody=f"{merchant_details}",
                        functionName="update_subscription_status", request=request)
        next_charge_date = merchant_details['nextsubscriptionchargedate'] if isinstance(
          merchant_details['nextsubscriptionchargedate'], datetime.date) else None

        if next_charge_date is None:
          # this condition is important in order to avoid issue of selecting incorrect date and then not able to
          # select back the past date
          pass
        elif subscriptionStartDate < next_charge_date:

          cursor.execute("""SELECT * FROM subscriptions WHERE merchantId=%s ORDER BY `date` DESC LIMIT 1""",
                         (merchantId))
          row = cursor.fetchone()
          date = relativedelta(months=row['frequency'])
          subscription_date_check = None
          if row:
            if row['frequency'] == 4:
              subscription_date_check = row['date'] + \
                                        timedelta(weeks=1)
            else:
              subscription_date_check = row['date'] + \
                                        relativedelta(months=row['frequency'])
          if row and subscription_date_check > subscriptionStartDate:
            if row['frequency'] == 4:
              # Adds 1 week if frequency is 4
              subscriptionStartDate = row['date'] + \
                                      timedelta(weeks=1, days=1)
            else:
              subscriptionStartDate = row['date'] + relativedelta(months=row['frequency'],
                                                                  days=1)  # Adds months for other cases
            next_charge_date = subscriptionStartDate
          else:
            next_charge_date = subscriptionStartDate

          # subscriptionStartDate = next_charge_date
        else:
          next_charge_date = subscriptionStartDate
      else:
        subscriptionStartDate = None

      data = (subscriptionStatus, subscriptionStatus, subscriptionTrialPeriod, subscriptionFrequency,
              subscriptionStartDate, next_charge_date, userId, merchantId)
      cursor.execute("""UPDATE merchants SET 
        status=%s, subscriptionstatus=%s, subscriptiontrialperiod=%s, 
        subscriptionfrequency=%s, subscriptionstartdate=%s, nextsubscriptionchargedate=COALESCE(%s, nextsubscriptionchargedate),
        updated_by=%s, updated_datetime=CURRENT_TIMESTAMP WHERE id=%s""", data)
      connection.commit()
      create_log_data(level='[INFO]',
                      Message=f"Update subscription status and subscription details for merchant {merchantId}",
                      functionName="update_subscription_status", request=request)
      return True
    except Exception as e:
      print(str(e))
      create_log_data(level='[ERROR]',
                      Message=f"Failed to update subscription status and subscription details for merchant {merchantId}",
                      messagebody=str(e),
                      functionName="update_subscription_status", request=request)
      return False

  # POST

  @classmethod
  def post_merchant(cls, merchant, token_user, userId=None, onBoard=False):
    try:
      connection, cursor = get_db_connection()

      _mn = merchant['merchantName']
      businessWebsite = merchant['businessWebsite']
      _email = merchant['email']
      if onBoard:
        getMerchant, errorMsg = cls.check_email_and_merchantName(
          _email, _mn)
        if getMerchant:
          create_log_data(level='[ERROR]', Message=f"Merchant {_mn} with email {_email} can not be onboarded",
                          messagebody=errorMsg,
                          user=token_user,
                          functionName="post_merchant", request=request)
          return errorMsg, True
        if not getMerchant:
          _phone = merchant['businessNumber']
          zipcode = merchant['zip']

          first_name = merchant['firstName']
          last_name = merchant['lastName']
          pocdob = merchant['pocdob']
          ein = merchant['ein']

          _taxRate = merchant.get("merchantTaxRate")
          businessNumber = merchant.get('businessNumber')
          legalBusinessName = merchant.get('legalBusinessName')
          businessTaxId = merchant.get('businessTaxId')
          businessAddressLine = merchant.get('businessAddressLine')
          businessAddressCity = merchant.get('businessAddressCity')
          businessAddressState = merchant.get('businessAddressState')
          bankAccountNumber = merchant.get('bankAccountNumber')
          bankAccountRoutingNumber = merchant.get(
            'bankAccountRoutingNumber')
          autoAcceptOrder = merchant.get("autoAcceptOrder")
          account = None
          merchantTimezone = merchant.get("timezone")
          merchantGUID = uuid.uuid4()

          RevenueProcessingThreshold = merchant.get(
            'RevenueProcessingThreshold') if merchant.get('RevenueProcessingThreshold') else 0
          RevenueProcessingFeePercent = merchant.get(
            'RevenueProcessingFeePercent') if merchant.get('RevenueProcessingFeePercent') else 0
          minimumLifetimeRevenue = merchant.get(
            'minimumLifetimeRevenue') if merchant.get('minimumLifetimeRevenue') else 0
          DownTimeThreshold = merchant.get(
            'DownTimeThreshold') if merchant.get('DownTimeThreshold') else 0
          subscriptionamount = merchant.get(
            'subscriptionAmount') if merchant.get('subscriptionAmount') else 0
          onboardingdate = merchant.get('onboardingdate') if merchant.get(
            'onboardingdate') else '0000-00-00'
          subscriptionstartdate = merchant.get('subscriptionStartDate') if merchant.get(
            'subscriptionStartDate') else '0000-00-00'
          subscriptiontrialperiod = merchant.get(
            "subscriptionTrialPeriod") if merchant.get('subscriptionTrialPeriod') else 0
          AutoWaivedStatus = 1 if merchant.get(
            'AutoWaivedStatus') else 0
          slug = cls.generate_slug(merchantGUID, _mn)

          connect_id = None
          is_error_in_account = False

          if not (
              bankAccountRoutingNumber == "" or bankAccountNumber == "" or businessWebsite == "" or businessAddressLine == "" or _phone == "" or legalBusinessName == "" or businessTaxId == "" or businessTaxId == None):
            connect_id, is_error_in_account = open_stripe_connect_account(first_name, last_name,
                                                                          bankAccountRoutingNumber, bankAccountNumber,
                                                                          _email, businessWebsite, businessAddressLine,
                                                                          zipcode,
                                                                          businessAddressState, businessAddressCity,
                                                                          _phone, legalBusinessName, businessTaxId)
            if not is_error_in_account:
              create_log_data(level='[INFO]', Message="Successfully created stripe account id",
                              messagebody=f"stripe account created for merchant {_mn} with id {connect_id}",
                              user=token_user,
                              functionName="post_merchant", request=request)

          if is_error_in_account:
            create_log_data(level='[ERROR]', Message="Unable to create stripe account",
                            messagebody=f"Failed to create stripe account for merchant {_mn} due to {connect_id}",
                            user=token_user,
                            functionName="post_merchant", request=request)
            return connect_id, is_error_in_account

          data = (
            merchantGUID, _mn, first_name, last_name, _email, businessAddressLine, 0, _phone, userId, " ", " ",
            _taxRate,
            businessNumber,
            legalBusinessName, businessTaxId, businessAddressLine, businessAddressCity, businessAddressState,
            bankAccountNumber, bankAccountRoutingNumber, autoAcceptOrder, zipcode, pocdob, ein,
            connect_id, 0, 0, 0, businessWebsite, merchantTimezone, slug, RevenueProcessingThreshold,
            RevenueProcessingFeePercent,
            minimumLifetimeRevenue, DownTimeThreshold, subscriptionamount, onboardingdate, subscriptionstartdate,
            subscriptiontrialperiod, AutoWaivedStatus)
          cursor.execute(
            "INSERT INTO merchants (id, merchantname, firstname,lastname,email,address,status,phone,created_by, lat, longitude, taxrate, businessnumber ,legalbusinessname ,businesstaxid ,businessaddressline ,businessaddresscity ,businessaddressstate ,bankaccountnumber ,bankaccountroutingnumber, autoacceptorder, zipcode, pointofcontactdob,ein,stripeaccountid,marketstatus,parserstatus,subscriptionstatus,businessWebsite,timezone, slug,RevenueProcessingThreshold,RevenueProcessingFeePercent,minimumLifetimeRevenue,DownTimeThreshold,subscriptionamount,onboardingdate,subscriptionstartdate,subscriptiontrialperiod,AutoWaivedStatus) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            data)
          connection.commit()
          create_log_data(level='[INFO]', Message=f"Successfully created record of merchant {_mn}",
                          user=token_user,
                          functionName="post_merchant", request=request)

          if subscriptionstartdate != '0000-00-00':
            resp = Merchants.update_subscription_status(merchantGUID, 1, subscriptiontrialperiod,
                                                        1, subscriptionstartdate)
          onboard_update_resp = Merchants.update_email_distribution_list(
            merchantId=merchantGUID, emails=_email)
          create_log_data(level='[INFO]', Message=f"Updated email distribution list of merchant {_mn}",
                          messagebody=f"",
                          user=token_user,
                          functionName="post_merchant", request=request)

          for hours in merchant.get('openinghours'):
            seqNo = hours['seqNo']
            day = hours['day']
            opentime = hours['openTime']
            closetime = hours['closeTime']
            closeforbusinessflag = hours['closeForBusinessFlag']
            merchantopeninghrGUID = uuid.uuid4()
            data = (merchantopeninghrGUID, merchantGUID, seqNo,
                    day, opentime, closetime, closeforbusinessflag)
            cursor.execute(
              "INSERT INTO merchantopeninghrs (id, merchantid, daynumber, day, opentime, closetime, closeforbusinessflag) VALUES (%s, %s, %s,%s, %s, %s,%s)",
              data)
            connection.commit()
          create_log_data(level='[INFO]', Message=f"Created record of merchant {_mn} opening hours",
                          messagebody=f"",
                          user=token_user,
                          functionName="post_merchant", request=request)

          password = cls.generate_random_password()
          print(password)
          getuser = cls.check_email_for_user(_email)
          if not getuser:
            userGUID = uuid.uuid4()

            # insert user
            fields = (userGUID, first_name, last_name, _email,
                      _email, "address", 1, "", 3, password, "")
            cursor.execute("""INSERT INTO users
                            (id, firstname, lastname, username, email, address, status, phone, role, password, created_by)
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""", fields)
            connection.commit()
            create_log_data(level='[INFO]', Message=f"Created user record of merchant {_mn}",
                            messagebody=f"",
                            user=token_user,
                            functionName="post_merchant", request=request)
          else:
            userGUID = getuser['id']

          data = (merchantGUID, userGUID, "")
          cursor.execute(
            "INSERT INTO merchantusers (merchantid, userid, created_by) VALUES (%s, %s, %s)", data)
          connection.commit()
          create_log_data(level='[INFO]',
                          Message=f"Inserted merchant id {merchantGUID} and user id {userGUID} of merchant {_mn}",
                          messagebody=f"",
                          user=token_user,
                          functionName="post_merchant", request=request)
          cls.send_onboard_email(
            _email, password, first_name + " " + last_name, merchantGUID)
          create_log_data(level='[INFO]',
                          Message=f"Successfully send onboarding email",
                          messagebody=f"",
                          user=token_user,
                          functionName="post_merchant", request=request)

          return merchantGUID, False

    except Exception as e:
      create_log_data(level='[ERROR]',
                      Message=f"Unable to do onboarding of merchant {merchant['merchantName']}",
                      messagebody=str(e),
                      functionName="post_merchant", request=request)
      print(str(e))
      return str(e), True

  @classmethod
  def post_merchant_user(cls, merchant, userId=None, onBoard=False):
    try:
      connection, cursor = get_db_connection()
      _mn = merchant['merchantName']
      businessWebsite = merchant.get(
        "businessWebsite") if merchant.get('businessWebsite') else ''
      _email = merchant['email']

      if onBoard:
        getMerchant, errorMsg = cls.check_email_and_merchantName(
          _email, _mn)
        if getMerchant:
          create_log_data(level='[ERROR]', Message=f"Merchant {_mn} with email {_email} can not be onboarded",
                          messagebody=errorMsg,
                          functionName="post_merchant", request=request)
          return errorMsg, True
        if not getMerchant:
          _phone = merchant.get("businessNumber") if merchant.get(
            'businessNumber') else ''
          zipcode = merchant.get(
            "zip") if merchant.get('zip') else ''

          first_name = merchant.get(
            "firstName") if merchant.get('firstName') else ''
          last_name = merchant.get(
            "lastName") if merchant.get('lastName') else ''
          pocdob = merchant.get(
            "pocdob") if merchant.get('pocdob') else ''
          ein = merchant.get("ein") if merchant.get('ein') else ''

          _taxRate = 0
          if merchant.get('zip') :
            try:
                
                api_url = config.ninja_api_url
                response = requests.get(api_url, headers={'X-Api-Key': config.ninja_api_key}, params={"zip_code": merchant.get('zip')})
                
                total_rate = 0
                if response.status_code == 200:
                    data = response.json()
                    if isinstance(data, dict):
                        total_rate = Decimal(data.get("total_rate", 0))
                        _taxRate = total_rate * Decimal(100) 
                        create_log_data(level='[INFO]',
                        Message=f"Ninja api response total rate : ${total_rate},",
                        functionName="post_merchant_user", request=request)
                    elif isinstance(data, list) and len(data) > 0 and isinstance(data[0], dict):
                        total_rate = Decimal(data[0].get("total_rate", 0))
                        print(f"Total Sales Tax Rate for ZIP code {merchant.get('zip')}: {total_rate}")
                        _taxRate = total_rate * Decimal(100) 
                        create_log_data(level='[INFO]',
                        Message=f"Ninja api response total rate : ${total_rate},",
                        functionName="post_merchant_user", request=request)
                    else:
                        print(f"Unexpected list structure: {data}")   
                        create_log_data(level='[ERROR]',
                      Message=f"Error in fetching total rate from ninja api : ${data},",
                      functionName="post_merchant_user", request=request)                 
                else:
                    _taxRate = 0
                    create_log_data(level='[ERROR]',
                      Message=f"Error in getting total rate from ninja api : ${response.text},",
                      functionName="post_merchant_user", request=request)
            except Exception as e:
                _taxRate = 0
                create_log_data(level='[ERROR]',
                      Message=f"Issue in get rate from ninja api",
                      functionName="post_merchant_user", request=request)
          else:
            _taxRate = merchant.get("merchantTaxRate") if merchant.get(
            'merchantTaxRate') else 0

          businessNumber = merchant.get(
            'businessNumber') if merchant.get('businessNumber') else ''
          legalBusinessName = merchant.get(
            'legalBusinessName') if merchant.get('legalBusinessName') else ''
          businessTaxId = merchant.get(
            'businessTaxId') if merchant.get('businessTaxId') else ''
          businessAddressLine = merchant.get(
            'businessAddressLine') if merchant.get('businessAddressLine') else ''
          businessAddressCity = merchant.get(
            'businessAddressCity') if merchant.get('businessAddressCity') else ''
          businessAddressState = merchant.get(
            'businessAddressState') if merchant.get('businessAddressState') else ''
          bankAccountNumber = merchant.get(
            'bankAccountNumber') if merchant.get('bankAccountNumber') else ''
          bankAccountRoutingNumber = merchant.get(
            'bankAccountRoutingNumber') if merchant.get('bankAccountRoutingNumber') else ''
          account = None
          emaildistributionlist = merchant['email']

          defaultTimezone = 'US/Pacific'
          if merchant.get('zip') :
            try:
              timezone_api_url = config.googleMap_timezone_api_url
              geocode_api_url = config.googleMap_geocode_api_url
              googleMap_api_key = config.googleMap_api_key
              response = requests.get(geocode_api_url, params={"address": merchant.get('zip'), "key": googleMap_api_key})
              if response.status_code == 200:
                    data = response.json()
                    if data.get('results'):
                      latt = data['results'][0].get('geometry').get('location').get('lat')
                      long = data['results'][0].get('geometry').get('location').get('lng')
                      timestamp = int(time.time())
                      if latt and long :
                        location =  str(latt) + ',' + str(long)
                        response1 = requests.get(timezone_api_url, params={"location": location,"timestamp": timestamp, "key": googleMap_api_key})
                        if response1.status_code == 200:
                          timeData = response1.json()
                          if timeData:
                            defaultTimezone = timeData.get('timeZoneId')
                          else:
                            create_log_data(level='[ERROR]',
                            Message=f"Issue with timezone api : {timeData}",
                            functionName="post_merchant_user", request=request)
                      else:
                        create_log_data(level='[ERROR]',
                    Message=f"Not able to get lat long based on zipcode :{data}",
                    functionName="post_merchant_user", request=request)
                    else:
                       create_log_data(level='[ERROR]',
                    Message=f"Error in get lat long based on zipcode: {data}",
                    functionName="post_merchant_user", request=request)
              else:
                create_log_data(level='[ERROR]',
                    Message=f"Issue in get lat long based on zipcode: {response}",
                    functionName="post_merchant_user", request=request)
            except Exception as e:
              create_log_data(level='[ERROR]',
                    Message=f"Error: {str(e)}",
                    functionName="post_merchant_user", request=request)

          merchantTimezone = merchant.get("timezone") if merchant.get(
            'timezone') else defaultTimezone
          merchantGUID = uuid.uuid4()

          RevenueProcessingThreshold = merchant.get(
            'RevenueProcessingThreshold') if merchant.get('RevenueProcessingThreshold') else 0
          RevenueProcessingFeePercent = merchant.get(
            'RevenueProcessingFeePercent') if merchant.get('RevenueProcessingFeePercent') else 0
          minimumLifetimeRevenue = merchant.get(
            'minimumLifetimeRevenue') if merchant.get('minimumLifetimeRevenue') else 0
          DownTimeThreshold = merchant.get(
            'DownTimeThreshold') if merchant.get('DownTimeThreshold') else 0
          subscriptionamount = merchant.get(
            'subscriptionAmount') if merchant.get('subscriptionAmount') else 0
          onboardingdate = merchant.get('onboardingdate') if merchant.get(
            'onboardingdate') else '0000-00-00'
          subscriptionstartdate = merchant.get('subscriptionStartDate') if merchant.get(
            'subscriptionStartDate') else '0000-00-00'
          subscriptiontrialperiod = merchant.get(
            "subscriptionTrialPeriod") if merchant.get('subscriptionTrialPeriod') else 0
          AutoWaivedStatus = 1 if merchant.get(
            'AutoWaivedStatus') else 0
          slug = cls.generate_slug(merchantGUID, _mn)

          connect_id = None
          is_error_in_account = False

          if not (businessWebsite == "" or _phone == ""):
            # connect_id, is_error_in_account = open_stripe_connect_account(first_name, last_name, bankAccountRoutingNumber, bankAccountNumber, _email, businessWebsite, businessAddressLine, zipcode,
            # businessAddressState, businessAddressCity, _phone, legalBusinessName, businessTaxId)
            if not is_error_in_account:
              create_log_data(level='[INFO]', Message="Successfully created stripe account id",
                              messagebody=f"stripe account created for merchant {_mn} with id {connect_id}",

                              functionName="post_merchant", request=request)

          if is_error_in_account:
            create_log_data(level='[ERROR]', Message="Unable to create stripe account",
                            messagebody=f"Failed to create stripe account for merchant {_mn} due to {connect_id}",

                            functionName="post_merchant", request=request)
            return connect_id, is_error_in_account

          data = (
            merchantGUID, _mn, first_name, last_name, _email, businessAddressLine, 1, _phone, userId, " ", " ",
            _taxRate,
            businessNumber,
            legalBusinessName, businessTaxId, businessAddressLine, businessAddressCity, businessAddressState,
            bankAccountNumber, bankAccountRoutingNumber, zipcode, pocdob, ein,
            connect_id, 0, 0, 0, businessWebsite, merchantTimezone, slug, RevenueProcessingThreshold,
            RevenueProcessingFeePercent,
            minimumLifetimeRevenue, DownTimeThreshold, subscriptionamount, onboardingdate, subscriptionstartdate,
            subscriptiontrialperiod, AutoWaivedStatus,emaildistributionlist)

          cursor.execute(
            "INSERT INTO merchants (id, merchantname, firstname,lastname,email,address,status,phone,created_by, lat, longitude, taxrate, businessnumber ,legalbusinessname ,businesstaxid ,businessaddressline ,businessaddresscity ,businessaddressstate ,bankaccountnumber ,bankaccountroutingnumber, zipcode, pointofcontactdob,ein,stripeaccountid,marketstatus,parserstatus,subscriptionstatus,businessWebsite,timezone, slug,RevenueProcessingThreshold,RevenueProcessingFeePercent,minimumLifetimeRevenue,DownTimeThreshold,subscriptionamount,onboardingdate,subscriptionstartdate,subscriptiontrialperiod,AutoWaivedStatus,emaildistributionlist) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            data)
          connection.commit()
          data = (merchantGUID, userId, "")
          cursor.execute(
            "INSERT INTO merchantusers (merchantid, userid, created_by) VALUES (%s, %s, %s)", data)
          connection.commit()
          
          user_data = Users.get_users(conditions=[f"id = '{userId}'"])
          newuserId = uuid.uuid4()
          if user_data:
            user = user_data[0]
            user_email = user.get('email')
            user_name, domain = user_email.split("@", 1)
            new_user_email = f"{user_name}+1@{domain}"
            rows = Users.get_users(conditions=[f"email='{new_user_email}'"])
            rows1 = Users.get_users(conditions=[f"username='{new_user_email}'"])
            if not rows and not rows1:             
              fields = (newuserId, user.get('firstname'), user.get('lastname'), new_user_email, new_user_email, user.get('address'), user.get('status'), user.get('phone'), 4, user.get('password'), user.get('createUserId'))
              cursor.execute("""INSERT INTO users
                (id, firstname, lastname, username, email, address, status, phone, role, password, created_by) 
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""", fields)
              connection.commit()
              data1 = (merchantGUID, newuserId, "")
              cursor.execute(
                "INSERT INTO merchantusers (merchantid, userid, created_by) VALUES (%s, %s, %s)", data1)
              connection.commit()
         
          create_log_data(level='[INFO]', Message=f"Successfully created record of merchant {_mn}",

                          functionName="post_merchant", request=request)

          if subscriptionstartdate != '0000-00-00':
            resp = Merchants.update_subscription_status(merchantGUID, 1, subscriptiontrialperiod,
                                                        1, subscriptionstartdate)
          onboard_update_resp = Merchants.update_email_distribution_list(
            merchantId=merchantGUID, emails=_email)
          create_log_data(level='[INFO]', Message=f"Updated email distribution list of merchant {_mn}",
                          messagebody=f"",
                          functionName="post_merchant", request=request)

          if (merchant.get('openinghours')):
            for hours in merchant.get('openinghours'):
              seqNo = hours['seqNo']
              day = hours['day']
              opentime = hours['openTime']
              closetime = hours['closeTime']
              closeforbusinessflag = hours['closeForBusinessFlag']
              merchantopeninghrGUID = uuid.uuid4()
              data = (merchantopeninghrGUID, merchantGUID, seqNo,
                      day, opentime, closetime, closeforbusinessflag)
              cursor.execute(
                "INSERT INTO merchantopeninghrs (id, merchantid, daynumber, day, opentime, closetime, closeforbusinessflag) VALUES (%s, %s, %s,%s, %s, %s,%s)",
                data)
              connection.commit()
            create_log_data(level='[INFO]', Message=f"Created record of merchant {_mn} opening hours",
                            messagebody=f"",
                            functionName="post_merchant", request=request)

          return merchantGUID, False

    except Exception as e:
      create_log_data(level='[ERROR]',
                      Message=f"Unable to do onboarding of merchant {merchant['merchantName']}",
                      messagebody=str(e),
                      functionName="post_merchant", request=request)
      print(str(e))
      return str(e), True

  @classmethod
  def update_time_merchant(cls, merchantId, status):
    try:
      connection, cursor = get_db_connection()
      data = (int(status), merchantId)
      cursor.execute(
        "update merchants set busy_mode = %s WHERE id=%s", data)
      return connection.commit()
    except Exception as e:
      print(str(e))
      return False

  @classmethod
  def check_email(cls, email, status):
    try:
      connection, cursor = get_db_connection()
      cursor.execute("""
        SELECT *
        FROM merchants 
        WHERE email=%s and onBoardingCompleted=%s
        """, (email, status))
      rows = cursor.fetchone()

      return rows
    except Exception as e:
      print("Error: ", str(e))
      return False

  @classmethod
  def check_email_and_merchantName(cls, email, merchantname):
    try:
      connection, cursor = get_db_connection()
      cursor.execute("""
         SELECT *
         FROM merchants 
         WHERE  merchantname=%s
         """, (merchantname))
      merchant = cursor.fetchone()
      if merchant:
        if merchant['email'] == email:
          return True, "Merchant with same email already exists"
        return True, "Merchant with same name already exist"

      return False, ''
    except Exception as e:
      print("Error: ", str(e))
      return False

  @classmethod
  def check_email_for_user(cls, email):
    try:
      connection, cursor = get_db_connection()
      cursor.execute("""
          SELECT *
          FROM users 
          WHERE email=%s 
          """, (email))
      rows = cursor.fetchone()

      return rows
    except Exception as e:
      print("Error: ", str(e))
      return False

  @classmethod
  def send_onboard_email(cls, merchent_email, password, pocname, merchantId, complete=False):
    print(password)
    print(pocname)

    row = cls.get_merchant_by_id(merchantId)
    # send email to the user
    SENDER = f"[NoReply] Fonda <{config.ses_sender_email}>"
    RECIPIENT = merchent_email
    SUBJECT = f"Complete Your Fonda Online Sign up"
    BODY_TEXT = (
      f"")
    app_login_url = config.app_base_URL + "/accounts/login"
    continue_url = config.app_base_URL + \
                   "/onBoarding/merchant-onBoarding/" + str(row['id'])

    data = {
      "pocname": pocname,
      "app_login_url": app_login_url,
      "continue_url": continue_url,
      "merchent_email": merchent_email,
      "password": password
    }

    if complete:
      BODY_HTML = render_template(
        'emails/merchant-onboard-completed.html', data=data)
    else:
      BODY_HTML = render_template(
        'emails/merchant-onboard.html', data=data)

    CHARSET = "UTF-8"
    ses_client = boto3.client('ses')

    try:
      # Provide the contents of the email.
      response = ses_client.send_email(
        Destination={
          'ToAddresses': [
            RECIPIENT,
          ],
        },
        Message={
          'Body': {
            'Html': {
              'Charset': CHARSET,
              'Data': BODY_HTML,
            },
            'Text': {
              'Charset': CHARSET,
              'Data': BODY_TEXT,
            },
          },
          'Subject': {
            'Charset': CHARSET,
            'Data': SUBJECT,
          },
        },
        Source=SENDER,
      )
    # Display an error if something goes wrong.
    except ClientError as e:
      print(e.response['Error']['Message'])
      return success(jsonify(e.response['Error']['Message']))
    else:
      print("Email sent! Message ID:"),
      print(response['MessageId'])

  print("Done")

  @classmethod
  def generate_random_password(cls):
    characters = list(string.ascii_letters + string.digits + "!@")
    # length of password from the user
    length = 8

    # shuffling the characters
    random.shuffle(characters)

    # picking random characters from the list
    password = []
    for i in range(length):
      password.append(random.choice(characters))

    # shuffling the resultant password
    random.shuffle(password)

    # converting the list to string
    # printing the list
    print("".join(password))

    return "".join(password)

  @classmethod
  def update_merchant_payment(cls, merchantId, chargeId):
    connection, cursor = get_db_connection()

    merchant = cls.get_merchant_by_id(merchantId)
    data = (chargeId, merchantId)
    cursor.execute(
      "UPDATE merchants set charge_id=%s, onBoardingCompleted=1 where id=%s",
      data)
    connection.commit()

    cursor.execute(
      "Select *  from merchantusers where merchantid=%s", merchantId)
    row = cursor.fetchone()

    cursor.execute("Select *  from users where id=%s", row['userid'])
    user = cursor.fetchone()

    cls.send_onboard_email(user['email'], user['password'],
                           user['firstname'] + " " + user['lastname'], merchantId, True)

    AuditLogs.post_audit_logs(
      userid='',
      username='system',
      merchantid=merchantId,
      merchantname=merchant['merchantname'],
      eventname="onboarding.complete",
      eventdetails=merchant['merchantname'] + " on boarding completed and charged amount is $" + str(
        config.stripe_onboard_fee_total / 100)
    )

  @classmethod
  def reminder_email(cls, merchantId):
    connection, cursor = get_db_connection()

    cursor.execute(
      "Select *  from merchantusers where merchantid=%s", merchantId)
    row = cursor.fetchone()

    cursor.execute("Select *  from users where id=%s", row['userid'])
    user = cursor.fetchone()

    cls.send_onboard_email(user['email'], user['password'],
                           user['firstname'] + " " + user['lastname'], merchantId)

  @classmethod
  def connect_disconnect_esper_device(cls, merchantId, esperDeviceId, disconnect):
    try:
      connection, cursor = get_db_connection()

      if disconnect == 0:
        cursor.execute(
          """SELECT * FROM merchants WHERE esperdeviceid = %s""", (esperDeviceId))
        row = cursor.fetchone()
        if row:
          return invalid("esper device is already attached with another merchant")

        cursor.execute(
          """UPDATE merchants SET esperdeviceid = %s WHERE id = %s""", (esperDeviceId, merchantId))

      else:
        cursor.execute(
          """SELECT * FROM merchants WHERE id = %s AND esperdeviceid = %s""", (merchantId, esperDeviceId))
        row = cursor.fetchone()
        if not row:
          return invalid("invalid merchantId and deviceId")

        cursor.execute(
          """UPDATE merchants SET esperdeviceid = NULL WHERE id = %s""", (merchantId))

      connection.commit()
      return success()

    except Exception as e:
      return unhandled(f"error: {e}")

  @classmethod
  def get_merchant_opening_hours_by_id(cls, merchantId):
    try:
      connection, cursor = get_db_connection()
      cursor.execute(
        """SELECT * FROM merchantopeninghrs WHERE merchantid=%s and opentime is not null""", (merchantId))
      return cursor.fetchall()

    except Exception as e:
      print(str(e))
      return False

  @classmethod
  def storefront_logo_update(cls, merchantId, logo):
    try:
      connection, cursor = get_db_connection()
      cursor.execute(
        "SELECT logo FROM merchants WHERE id=%s", (merchantId))
      row = cursor.fetchone()

      logoUrl = row['logo']
      if logo:
        logoUrl = cls.upload_media(logo, logoUrl)

      # put new image url to the items table
      cursor.execute(
        """UPDATE merchants SET logo=%s WHERE id=%s""", (logoUrl, merchantId))
      connection.commit()

    except Exception as e:
      print(str(e))
      return False

  @classmethod
  def storefront_banner_update(cls, merchantId, banner=None, uploaded_url=None):
    try:
      if uploaded_url == "1":
        s3_apptopus_bucket = config.s3_apptopus_bucket
        storefront_data = "storefront"
        s3_client = boto3.client("s3")

        resp = s3_client.list_objects(
          Bucket=s3_apptopus_bucket, Prefix=storefront_data, Delimiter='/', MaxKeys=1)
        if not 'CommonPrefixes' in resp:
          resp = s3_client.put_object(
            Bucket=s3_apptopus_bucket, Key=(storefront_data + "/"))

        contentType = None
        if ":-_-:" in banner:
          fn = banner.split(":-_-:")
          fileName, contentType = fn[0], fn[1]

        fileName = banner
        fileName = fileName.replace(" ", "")

        Fields = {"acl": "public-read"}
        Conditions = [{"acl": "public-read"}, ]

        if contentType:
          Fields["Content-Type"] = contentType
          Conditions.append({"Content-Type": contentType})

        postUrl = s3_client.generate_presigned_post(
          Bucket=s3_apptopus_bucket,
          Key=f"{storefront_data}/{merchantId}/{fileName}",
          Fields=Fields,
          Conditions=Conditions,
          ExpiresIn=3600
        )

        return {
          "fileName": fileName,
          "presignedUrl": postUrl
        }
      else:
        connection, cursor = get_db_connection()
        cursor.execute(
          """UPDATE merchants SET banner=%s WHERE id=%s""", (uploaded_url, merchantId))
        connection.commit()

        return "uploaded successfully"

    except Exception as e:
      print(str(e))
      return False

  @classmethod
  def upload_media(cls, image, oldimage=None):
    client = boto3.client("s3")
    ext = image.filename.split(".")[-1]
    imageName = str(uuid.uuid4()) + "." + ext

    if oldimage is not None:
      print("Deleting old image...")
      oldImageName = oldimage.split("/")[-1]
      client.delete_object(Bucket=s3_apptopus_bucket,
                           Key=f"{images_folder}/{oldImageName}")
      print("Old image delete from s3")

    # boto3 upload image to s3

    print("Creating New Image...")
    client.upload_fileobj(
      image,
      s3_apptopus_bucket,
      f"{images_folder}/{imageName}",
      ExtraArgs={
        "ACL": "public-read",
        "ContentType": image.content_type
      }
    )

    imageUrl = client.generate_presigned_url(
      ClientMethod='get_object',
      Params={
        'Bucket': s3_apptopus_bucket,
        'Key': f"{images_folder}/{imageName}"
      }
    )
    imageUrl = imageUrl.split("?")[0]
    return imageUrl

  @classmethod
  def delete_aws_media(cls, oldimage):
    client = boto3.client("s3")
    if oldimage is not None:
      print("Deleting old image...")
      oldImageName = oldimage.split("/")[-1]
      client.delete_object(Bucket=s3_apptopus_bucket,
                           Key=f"{images_folder}/{oldImageName}")
      print("Old image delete from s3")

  @classmethod
  def generate_slug(cls, merchantId, slug):
    connection, cursor = get_db_connection()
    merchant = cls.get_merchant_by_id(merchantId)
    slug = re.sub("\s*(\W)\s*", r"\1", slug)
    slug = slug.replace(" ", "")

    if merchant:
      if merchant['slug'] == slug:
        return slug

    for i in range(0, 10):
      cursor.execute(
        "SELECT slug FROM merchants WHERE slug=%s and id!=%s", (slug, merchantId))
      row = cursor.fetchone()
      if not row:
        return slug

      slug = slug + str(i)

  @classmethod
  def update_email_distribution_list(cls, merchantId, emails):
    try:
      connection, cursor = get_db_connection()
      if isinstance(emails, list):
        emails = ";".join(emails)
      cursor.execute(
        "UPDATE merchants SET emaildistributionlist=%s WHERE id=%s", (emails, merchantId))
      connection.commit()
      return True
    except Exception as e:
      print(f"error: {e}")
      return False

  @classmethod
  def update_square_tax(cls, merchantId, tax):
    try:
      connection, cursor = get_db_connection()
      data = (tax, merchantId)
      cursor.execute(
        "UPDATE merchants SET taxrate=%s WHERE id=%s", data)
      connection.commit()

    except Exception as e:
      print(f"error: {e}")
      return False

  @classmethod
  def update_trasuary_auth_phone(cls, merchantId, phoneNumber):
    try:
      connection, cursor = get_db_connection()
      data = (phoneNumber, merchantId)
      cursor.execute(
        "UPDATE merchants SET trasuaryauthphone=%s WHERE id=%s", data)
      connection.commit()
      return True
    except Exception as e:
      print(f"error: {e}")
      return False

  @classmethod
  def update_trasuary_auth_changed_phone(cls, merchantId, phoneNumber):
    try:
      connection, cursor = get_db_connection()
      data = (phoneNumber, merchantId)
      cursor.execute(
        "UPDATE merchants SET trasuaryauthphonechanged=%s WHERE id=%s", data)
      connection.commit()
      return True
    except Exception as e:
      print(f"error: {e}")
      return False

  @classmethod
  def update_trasuary_auth_changed_phone_otp(cls, merchantId, otp):
    try:
      connection, cursor = get_db_connection()
      data = (otp, merchantId)
      cursor.execute(
        "UPDATE merchants SET changedphoneotp=%s WHERE id=%s", data)
      connection.commit()
      return True
    except Exception as e:
      print(f"error: {e}")
      return False

  @classmethod
  def update_trasuary_auth_otp(cls, merchantId, otp):
    try:
      connection, cursor = get_db_connection()
      data = (otp, merchantId)
      cursor.execute(
        "UPDATE merchants SET latestotp=%s WHERE id=%s", data)
      connection.commit()
      return True
    except Exception as e:
      print(f"error: {e}")
      return False

  @classmethod
  def change_trasuary_auth_phone_validation_status(cls, merchantId, status):
    try:
      connection, cursor = get_db_connection()
      data = (status, merchantId)
      cursor.execute(
        "UPDATE merchants SET trasuaryphonevalid=%s WHERE id=%s", data)
      connection.commit()
      return True
    except Exception as e:
      print(f"error: {e}")
      return False

  @classmethod
  def onboardnew_merchant(cls, merchant, userId=None, onBoard=False):
    try:
      connection, cursor = get_db_connection()

      _mn = merchant['merchantName']
      first_name = merchant['firstName']
      last_name = merchant['lastName']
      _phone = merchant['phone']
      businessWebsite = merchant['businessWebsite']
      _email = merchant['email']
      password = merchant['password']
      businessAddressLine = merchant.get('businessAddressLine')
      businessAddressCity = merchant.get('businessAddressCity')
      zipcode = merchant['zip']
      business_phone = merchant['businessNumber']
      logo = merchant['logo'] if merchant['logo'] else ''
      banner = merchant['banner'] if merchant['banner'] else ''

      merchantGUID = uuid.uuid4()
      slug = cls.generate_slug(merchantGUID, _mn)

      getMerchant = cls.check_email(_email, 0)
      if cls.check_email(_email, 0):
        return "Merchant with same email already exists", True

      if not getMerchant:
        data = (
          merchantGUID, _mn, first_name, last_name, _email, businessAddressLine, _phone, userId, business_phone,
          businessAddressLine, businessAddressCity,
          zipcode, businessWebsite, slug, logo, banner)
        cursor.execute(
          "INSERT INTO merchants (id, merchantname, firstname,lastname,email,address,phone,created_by, businessnumber,businessaddressline ,businessaddresscity,zipcode,businessWebsite, slug, logo, banner) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
          data)

        connection.commit()

        for hours in merchant.get('openinghours'):
          seqNo = hours['seqNo']
          day = hours['day']
          opentime = hours['openTime']
          closetime = hours['closeTime']
          closeforbusinessflag = hours['closeForBusinessFlag']
          merchantopeninghrGUID = uuid.uuid4()
          data = (merchantopeninghrGUID, merchantGUID, seqNo,
                  day, opentime, closetime, closeforbusinessflag)
          cursor.execute(
            "INSERT INTO merchantopeninghrs (id, merchantid, daynumber, day, opentime, closetime, closeforbusinessflag) VALUES (%s, %s, %s,%s, %s, %s,%s)",
            data)
          connection.commit()

        userGUID = uuid.uuid4()
        print(password)

        fields = (userGUID, first_name, last_name, _email,
                  _email, "address", 1, "", 3, password, "")
        cursor.execute("""INSERT INTO users
                     (id, firstname, lastname, username, email, address, status, phone, role, password, created_by)
                     VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""", fields)
        connection.commit()

        data = (merchantGUID, userGUID, "")

        cursor.execute(
          "INSERT INTO merchantusers (merchantid, userid, created_by) VALUES (%s, %s, %s)", data)
        connection.commit()

        cls.send_onboard_email(
          _email, password, first_name + " " + last_name, merchantGUID, True)

        return merchantGUID, False
      else:
        return "Merchant with same name already exists!", True
    except Exception as e:
      print(str(e))
      return False

  @classmethod
  def get_default_promo(cls, merchantid):
    connection, cursor = get_db_connection()
    cursor.execute("""SELECT * FROM promo WHERE merchantid=%s and printinreceipt=1 and status=1""",
                   (merchantid))
    return cursor.fetchone()

  @classmethod
  def get_use_promo_on_order(cls, promoid):
    connection, cursor = get_db_connection()
    cursor.execute("""SELECT promo FROM promo WHERE id=%s""",
                   (promoid))
    return cursor.fetchone()

  @classmethod
  def checkpickupaddress(cls, store_front):
    try:
      quote_id = ascii_uppercase + digits
      quote_id = str.join('', choices(quote_id, k=10))
      token = cls.get_doordash_jwt()
      merchant_address = str(store_front.get('businessAddressLine', '')) + " " + \
                         str(store_front.get('businessAddressCity', '')) + " " + \
                         str(store_front.get('businessAddressState', '')) + " " + \
                         str(store_front.get('zip', ''))
      if merchant_address.strip() == '':
        print("")
      import requests
      import json

      url = config.DOOR_DASH_DRIVE + "drive/v2/quotes"

      payload = json.dumps({
        "external_delivery_id": quote_id,
        "pickup_address": merchant_address,
        "pickup_business_name": store_front['merchantName'],
        "pickup_phone_number": store_front['businessNumber'],
        "dropoff_address": "",
        "dropoff_phone_number": "",
      })
      headers = {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer ' + token

      }

      response = requests.request(
        "POST", url, headers=headers, data=payload)
      response = json.loads(response.text)
      errors_fields_dict = response['field_errors']
      has_address_errors = any(error['field'] in {
        'pickup_address', 'pickup_phone_number'} for error in errors_fields_dict)
      return has_address_errors

    except Exception as e:
      print(str(e))
      return False

  @classmethod
  def get_merchant_by_slug(cls, slug):
    try:
      connection, cursor = get_db_connection()
      cursor.execute("""SELECT * FROM merchants WHERE slug=%s""", (slug))
      row = cursor.fetchone()
      return row
    except Exception as e:
      print(str(e))
      return False

  @classmethod
  def get_merchants_names_by_shop_ids(cls, ids: set):
    if not ids:
      return []

    try:
      connection, cursor = get_db_connection()

      # Prepare placeholders (%s, %s, %s, …) dynamically
      placeholders = ','.join(['%s'] * len(ids))
      query = f"SELECT id, merchantname , status FROM merchants WHERE id IN ({placeholders})"

      cursor.execute(query, tuple(ids))
      rows = cursor.fetchall()
      if rows:
        # Extract found IDs
        found_ids = {row["id"] for row in rows}
        ids = list(ids - found_ids)
      return rows , ids  # list of dicts (id + merchantname)
    except Exception as e:
      print(str(e))
      return []

  @classmethod
  def get_merchant_name_by_id_storename_doordash(cls, store_names_and_ids_doordash):
    results = []
    try:
      connection, cursor = get_db_connection()

      # Step 0: preload all mappings from DB
      cursor.execute("""
              SELECT 
                  mpm.dd_storeid,
                  m.merchantname,
                  m.id,
                  m.status
              FROM merchantstoreplatformmapping mpm
              JOIN merchants m ON mpm.merchantid = m.id
          """)
      platform_mappings = cursor.fetchall()  # list of dicts with dd_storeid, merchantname, id

      cursor.execute("SELECT merchantname, id , status FROM merchants")
      merchant_records = cursor.fetchall()

      # Convert mappings into lookup structures
      dd_storeid_to_merchant = {
        row["dd_storeid"]: (row["merchantname"], row["id"] ,row["status"] )
        for row in platform_mappings if row["dd_storeid"]
      }

      all_merchants = [(row["merchantname"], row["id"], row["status"]) for row in merchant_records]

      for store_name, store_id in store_names_and_ids_doordash:
        matched_name, matched_id , merchant_status = None, None , None


        # Step 1: Match by store_id
        if store_id in dd_storeid_to_merchant:
          matched_name, matched_id , merchant_status = dd_storeid_to_merchant[store_id]

        # Step 2: fallback to merchants by store_name
        if not matched_name:
          store_name_norm = re.sub(r'\s+', ' ', store_name.strip().lower())
          store_name_tokens = set(store_name_norm.split())

          # First try: exact match
          merchant_dict = {m.lower().strip(): (m, mid , mstatus) for m, mid , mstatus in all_merchants}
          if store_name_norm in merchant_dict:
            matched_name, matched_id , merchant_status = merchant_dict[store_name_norm]

          # Second try: word-set match
          if not matched_name:
            for merchant, mid , mstatus in all_merchants:
              merchant_norm = re.sub(r'\s+', ' ', merchant.strip().lower())
              if set(merchant_norm.split()) == store_name_tokens:
                matched_name, matched_id , merchant_status = merchant, mid , mstatus
                break

          # Third try: partial substring match
          if not matched_name:
            for merchant, mid , mstatus in all_merchants:
              merchant_norm = merchant.lower()
              if (merchant_norm in store_name_norm) or (store_name_norm in merchant_norm):
                matched_name, matched_id , merchant_status= merchant, mid , mstatus
                break

        if matched_name and matched_id:
          results.append({
            "id": matched_id,
            "merchantname": matched_name,
            "doordash_name": store_name,
            "merchant_status":merchant_status
          })

      return results

    except Exception as e:
      print("Exception in get_merchant_name_by_id_storename_doordash:", str(e))
      return results

  @classmethod
  def get_merchant_name_storename_grubhub(cls, store_names_grubhub):
    results = []
    try:
      connection, cursor = get_db_connection()

      # Step 0: preload all mappings from DB (Grubhub)
      cursor.execute("""
              SELECT 
                  mpm.grubhubname,
                  m.merchantname,
                  m.id,
                  m.status
              FROM merchantstoreplatformmapping mpm
              JOIN merchants m ON mpm.merchantid = m.id
              WHERE mpm.grubhubname IS NOT NULL
          """)
      gh_platform_mappings = cursor.fetchall()

      cursor.execute("SELECT merchantname, id , status FROM merchants")
      merchant_records = cursor.fetchall()

      # Convert mappings into lookup structures
      gh_name_to_merchant = {
        row["grubhubname"].lower(): {"merchantname": row["merchantname"], "id": row["id"], "merchant_status": row["status"]}
        for row in gh_platform_mappings if row["grubhubname"]
      }

      merchant_dict_exact = {
        row["merchantname"].lower().strip(): {"merchantname": row["merchantname"], "id": row["id"], "merchant_status": row["status"]}
        for row in merchant_records
      }

      all_merchants = merchant_records  # keep full rows

      # Loop over input grubhub store names
      for store_name in store_names_grubhub:
        matched = None

        # Step 1: Match by grubhubname from preloaded mappings
        if store_name and store_name.lower() in gh_name_to_merchant:
          matched = gh_name_to_merchant[store_name.lower()]

        # Step 2: fallback to merchants by store_name
        if not matched:
          store_name_norm = re.sub(r'\s+', ' ', store_name.strip().lower())
          store_name_tokens = set(store_name_norm.split())

          # First try: exact match
          if store_name_norm in merchant_dict_exact:
            matched = merchant_dict_exact[store_name_norm]

          # Second try: word-set match
          if not matched:
            for merchant in all_merchants:
              merchant_norm = re.sub(r'\s+', ' ', merchant["merchantname"].strip().lower())
              if set(merchant_norm.split()) == store_name_tokens:
                matched = {"merchantname": merchant["merchantname"], "id": merchant["id"], "merchant_status": merchant["status"]}
                break

          # Third try: partial substring match
          if not matched:
            for merchant in all_merchants:
              merchant_norm = merchant["merchantname"].lower()
              if (merchant_norm in store_name_norm) or (store_name_norm in merchant_norm):
                matched = {"merchantname": merchant["merchantname"], "id": merchant["id"], "merchant_status": merchant["status"]}
                break

        if matched:
          results.append({
            "merchantname": matched["merchantname"],
            "gh_name": store_name,
            "id": matched["id"],
            "merchant_status":matched["merchant_status"]
          })

      return results
    except Exception as e:
      print(str(e))
      return results

  @classmethod
  def get_merchant_name_storename_ubereats(cls, store_names_ubereats):
    results = []
    try:
      connection, cursor = get_db_connection()

      # Step 0: preload UberEats mappings
      cursor.execute("""
            SELECT 
                mpm.ubereats_name,
                m.id AS merchantid,
                m.merchantname,
                m.status
            FROM merchantstoreplatformmapping mpm
            JOIN merchants m ON mpm.merchantid = m.id
            WHERE mpm.ubereats_name IS NOT NULL
        """)
      ue_platform_mappings = cursor.fetchall()  # list of dicts with ubereats_name, merchantid, merchantname

      cursor.execute("SELECT id AS merchantid, merchantname , status FROM merchants")
      merchant_records = cursor.fetchall()

      # Convert mappings into lookup structures
      ue_name_to_merchant = {
        row["ubereats_name"].lower(): {
          "merchantid": row["merchantid"],
          "merchantname": row["merchantname"],
          "merchant_status": row["status"]
        }
        for row in ue_platform_mappings if row["ubereats_name"]
      }

      # Dict for fast exact lookups
      merchant_name_dict = {
        row["merchantname"].strip().lower(): {
          "merchantid": row["merchantid"],
          "merchantname": row["merchantname"],
          "merchant_status": row["status"]
        }
        for row in merchant_records
      }

      # Step 1 & 2: match each incoming store_name
      for store_name in store_names_ubereats:
        matched = None
        if not store_name:
          continue

        store_name_norm = re.sub(r'\s+', ' ', store_name.strip().lower())

        # 1. Exact UberEats mapping match
        if store_name_norm in ue_name_to_merchant:
          matched = ue_name_to_merchant[store_name_norm]

        # 2. Exact merchant name match
        if not matched and store_name_norm in merchant_name_dict:
          matched = merchant_name_dict[store_name_norm]

        # 3. Word-set match
        if not matched:
          store_name_tokens = set(store_name_norm.split())
          for merchant in merchant_records:
            merchant_norm = re.sub(r'\s+', ' ', merchant["merchantname"].strip().lower())
            if set(merchant_norm.split()) == store_name_tokens:
              matched = {
                "merchantid": merchant["merchantid"],
                "merchantname": merchant["merchantname"],
                "merchant_status": merchant["status"]
              }
              break

        # 4. Partial substring match
        if not matched:
          for merchant in merchant_records:
            merchant_norm = merchant["merchantname"].lower()
            if (merchant_norm in store_name_norm) or (store_name_norm in merchant_norm):
              matched = {
                "merchantid": merchant["merchantid"],
                "merchantname": merchant["merchantname"],
                "merchant_status": merchant["status"]
              }
              break

        # Add to results
        if matched:
          results.append({
            "fonda_id": matched["merchantid"],
            "fonda_name": matched["merchantname"],
            "ue_name": store_name,
            "merchant_status": matched["merchant_status"]
          })

      return results

    except Exception as e:
      print("Error in get_merchant_name_storename_ubereats:", str(e))
      return results
  @classmethod
  def get_doordash_jwt(cls):

    return jwt.encode(
      {
        "aud": "doordash",
        "iss": config.developer_id,
        "kid": config.key_id,
        "exp": str(math.floor(time.time() + 1800)),
        "iat": str(math.floor(time.time())),
      },
      jwt.utils.base64url_decode(config.signing_secret),
      algorithm="HS256",
      headers={"dd-ver": "DD-JWT-V1"})

  @classmethod
  def create_auto_resume_merchant_scheduler(cls, merchantId,timezone):
    try:
      connection, cursor = get_db_connection()

      sql = (
        "SELECT moh.day, moh.opentime "
        "FROM merchantopeninghrs moh "
        "WHERE moh.merchantid = %s "
        "  AND moh.day <> DAYNAME(CONVERT_TZ(NOW(), 'UTC', %s)) "
        "  AND moh.closeforbusinessflag = 0 "
        "ORDER BY ( "
        "   (FIELD(moh.day, 'Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday') "
        "    - FIELD(DAYNAME(CONVERT_TZ(NOW(), 'UTC', %s)), "
        "            'Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday') "
        "    + 7) %% 7 "
        ") "
        "LIMIT 1;"
      )

      cursor.execute(sql, (merchantId, timezone, timezone))
      row = cursor.fetchone()
      if row and row['opentime']:
        # Convert to 24-hour format
        dt = datetime.datetime.strptime(row['opentime'], "%I:%M %p")
        dt_minus_5 = dt - datetime.timedelta(minutes=5)
        opentime = dt_minus_5.strftime("%H:%M")
        openSplit = opentime.split(":")
        weekdayList = {
          "Monday": "MON",
          "Tuesday": "TUES",
          "Wednesday": "WED",
          "Thursday": "THU",
          "Friday": "FRI",
          "Saturday": "SAT",
          "Sunday": "SUN"
        }
        cron_day = weekdayList.get(row['day'])
        cron_enable = f"cron({openSplit[1]} {openSplit[0]} ? * {cron_day} *)"

        # Create schedulers
        lambda_arn = config.auto_resume_lambda_arn
        enable_schedule_id = f"m-{merchantId}-e-{cron_day}"
        enable_schedule , msg = cls.create_merchant_scheduler(enable_schedule_id, cron_enable, lambda_arn,
                                                                        "Active", row['day'], timezone,
                                                                       merchantId)
        if not enable_schedule:
          return False , f"Error: {msg} , name : {enable_schedule_id} , cron :{cron_enable}"
        else:
          return True , f"Scheduler name :{enable_schedule_id} , cron : {cron_enable} , timezone : {timezone}"
      return False, f"Error: Open time not found"
    except Exception as e:
      print(str(e))
      return False , f"Error: {str(e)}"

  @classmethod
  def create_merchant_scheduler(cls,schedule_name, cron_expression, target_arn, status,weekDay,timezone,merchantId):
    print("create scheduler")
    try:
      uri = "/schedules/" + schedule_name
      payload = json.dumps({
          "ClientToken": str(uuid.uuid4()),
          "FlexibleTimeWindow": {"Mode": "OFF"},
          "ScheduleExpression": f"{cron_expression}",
          "ScheduleExpressionTimezone": f"{timezone}",
          "State": "ENABLED",
          "Target": {
                  "Arn": target_arn,
                  "RoleArn": "arn:aws:iam::677331532364:role/service-role/Amazon_EventBridge_Scheduler_LAMBDA_a69ad19f74",
                  "Input": json.dumps({
                    "merchantId": merchantId,
                    "scheduleName":schedule_name
                  })
                  }
      })

      response = CategoryServiceAvailability.aws_signed_request("POST", uri, payload)
      print(response.json(),response.status_code)
      if response.status_code == 200:
        print("Scheduler created successfully.")
        return True , 'Scheduler created successfully'
      else:
        return False , response.text
    except Exception as e:
      print({"error" : str(e)})
      return False , str(e)

  @classmethod
  def delete_merchant_scheduler(cls,schedule_name):
    try:
      uri = f"/schedules/{schedule_name}"
      payload = json.dumps({
        "ClientToken": str(uuid.uuid4())  # Generates a unique token for the request
      })
      response = CategoryServiceAvailability.aws_signed_request("DELETE", uri, payload)
      if response.status_code == 200:
        return True , "Scheduler deleted successfully."
      else:
        return False , response.text
    except Exception as e:
      return False , str(e)