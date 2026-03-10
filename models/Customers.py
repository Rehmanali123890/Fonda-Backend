import uuid
import json

# local imports
from utilities.helpers import get_db_connection, create_log_data


class Customers():

  ############################################### GET

  @classmethod
  def get_customer(cls, id=None, first_name=None, last_name=None):
    try:
      connection, cursor = get_db_connection()
      if first_name is not None and last_name is not None:
        cursor.execute("""SELECT * FROM customers WHERE firstname =%s and lastname =%s""", (first_name, last_name))
      row = cursor.fetchone()
      return row
    except Exception as e:
      print(str(e))
      return False

  ############################################### POST

  @classmethod
  def post_customer(cls, merchantId, first_name, last_name, email, address, phone):
    try:
      connection, cursor = get_db_connection()

      id = uuid.uuid4()
      data = (id, merchantId, first_name, last_name, email, address, phone)
      cursor.execute("""
        INSERT INTO customers
        (id, merchantid, firstname, lastname, email, address, phone)
        VALUES (%s,%s,%s,%s,%s,%s,%s)""", data)
      connection.commit()
      return id
    except Exception as e:
      print(str(e))
      return False
    
  ############################################### POST

  @classmethod
  def post_customer_ratings(cls, merchantId, first_name, email, phone, order_ratings,services_ratings, utm_source, comments):
    try:
      connection, cursor = get_db_connection()

      id = uuid.uuid4()
      data = (id, merchantId, first_name, email, phone, order_ratings,services_ratings, utm_source, comments)
      cursor.execute("""
        INSERT INTO customers
        (id, merchantid, firstname, email, phone, order_ratings,service_ratings, utm_source, comments)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""", data)
      connection.commit()
      return id
    except Exception as e:
      print(str(e))
      create_log_data(level='[error]',
                        Message=str(e),functionName="post_customer_ratings")
      return False

  ############################################### OTHER

  @classmethod
  def get_add_customer_by_name(cls, merchantId, first_name, last_name, email, address, phone):
    connection, cursor = get_db_connection()
    customer = cls.get_customer(first_name=first_name, last_name=last_name)
    if customer:
      print("customer exist in db")
      return customer['id']

    customer_id = cls.post_customer(merchantId, first_name, last_name, email, address, phone)
    return customer_id