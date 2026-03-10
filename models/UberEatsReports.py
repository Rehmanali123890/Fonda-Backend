

from utilities.helpers import get_db_connection


class UberEatsReports():

  @classmethod
  def post(cls, jobid, reporttype, startdate, enddate):
    try:
      connection, cursor = get_db_connection()

      cursor.execute("""
        INSERT INTO ubereatsreports (jobid, reporttype, startdate, enddate, isprocessed)
        VALUES (%s,%s,%s,%s,%s)
      """, (jobid, reporttype, startdate, enddate, 0))
      connection.commit()

      return True
    except Exception as e:
      print("error: ", str(e))
      return False
  

  @classmethod
  def get(cls, id=None, jobid=None):
    try:
      connection, cursor = get_db_connection()

      if id is not None:
        cursor.execute("SELECT * FROM ubereatsreports WHERE id = %s", (id))
      elif jobid is not None:
        cursor.execute("SELECT * FROM ubereatsreports WHERE jobid = %s ORDER BY created_datetime DESC LIMIT 1", (jobid))
      
      row = cursor.fetchone()
      return row
    except Exception as e:
      print("error: ", str(e))
      return False
  
  
  @classmethod
  def update(cls, id, webhookdata=None, isprocessed=None):
    try:
      connection, cursor = get_db_connection()

      cursor.execute("""
        UPDATE ubereatsreports
        SET
          webhookdata = COALESCE(%s, webhookdata),
          isprocessed = COALESCE(%s, isprocessed),
          updated_datetime = CURRENT_TIMESTAMP
        WHERE id = %s
      """, (webhookdata, isprocessed, id))
      connection.commit()

      return True
    except Exception as e:
      print("error: ", str(e))
      return False