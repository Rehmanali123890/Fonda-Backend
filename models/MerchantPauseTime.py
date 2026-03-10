from flask import jsonify

from utilities.errors import unhandled
from utilities.helpers import get_db_connection, success, convert_minutes_to_time


class MerchantPauseTime():

  @classmethod
  def get_down_time_analytics(cls, merchantId, startDate, endDate):
    try:
      connection, cursor = get_db_connection()

      cursor.execute("""
        SELECT
          merchantid,
          COALESCE(SUM(totaltime), 0) totaltime,
          COALESCE(SUM(activetime), 0) activetime,
          COALESCE(SUM(pausetime), 0) pausetime
        FROM 
          merchantpausetime
        WHERE 
          merchantid = %s AND
          entrydate BETWEEN %s AND %s
      """, (merchantId, startDate, endDate))
      row = cursor.fetchone()
      print(row)

      data = {
        "merchantId": row["merchantid"],
        "totalTime": int(row["totaltime"]),
        "activeTime": int(row["activetime"]),
        "pauseTime": int(row["pausetime"])
      }

      days, hours, minutes = convert_minutes_to_time(int(row["totaltime"]))
      data["totalTimeFormatted"] = {
        "days": days,
        "hours": hours,
        "minutes": minutes
      }

      days, hours, minutes = convert_minutes_to_time(int(row["activetime"]))
      data["activeTimeFormatted"] = {
        "days": days,
        "hours": hours,
        "minutes": minutes
      }

      days, hours, minutes = convert_minutes_to_time(int(row["pausetime"]))
      data["pauseTimeFormatted"] = {
        "days": days,
        "hours": hours,
        "minutes": minutes
      }

      return success(jsonify({
        "message": "success",
        "status": 200,
        "data": data
      }))
    except Exception as e:
      return unhandled(f"error: {e}")