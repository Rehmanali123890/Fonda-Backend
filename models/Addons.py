import uuid
from flask import jsonify

# local imports
from utilities.helpers import get_db_connection, success, publish_sns_message
from models.AddonsOptions import AddonsOptions
from utilities.errors import invalid
import config


class Addons():

  ############################################### GET

  @classmethod
  def get_addon_by_id(cls, addonId):
    try:
      connection, cursor = get_db_connection()
      cursor.execute("SELECT * from addons WHERE id=%s", addonId)
      row = cursor.fetchone() 
      return row
    except Exception as e:
      print("Error: ", str(e))
      return False
  

  @classmethod
  def get_addon_by_id_str(cls, addonId):
    row = cls.get_addon_by_id(addonId)
    if row:
      message = { 
        'id': row['id'],
        'addonName': row['addonname'],
        'posName': row['posname'],
        'addonDescription': row['addondescription'],
        'minPermitted': row['minpermitted'],
        'maxPermitted': row['maxpermitted'],
        'status': row['status']
      }
      return message
    return False
  

  @classmethod
  def get_addon_by_id_with_options_str(cls, addonId):
    try:
      row = cls.get_addon_by_id(addonId)
      if (row):
        options = AddonsOptions.get_addonOptions(addonId)
        items=   AddonsOptions.get_addonItems(addonId)
        message = { 
          'id': row['id'],
          'addonName': row['addonname'],
          'posName': row['posname'],
          'addonDescription': row['addondescription'],
          'minPermitted': row['minpermitted'],
          'maxPermitted': row['maxpermitted'],
          'addonOptions': options,
          'addonItems':items,
          'status':row['status']
        }
        return message
      return False
    except Exception as e:
      print("Error: ", str(e))
      return False


  @classmethod
  def get_all_addons_with_options_str(cls, merchantId, limit=None, _from=None, addonName=None, platformType=None):
    try:
      connection, cursor = get_db_connection()

      conditions = []

      if addonName:
        conditions.append(f'`addons`.`addonname` LIKE "%%{addonName}%%"')
      
      where = ' AND '.join(conditions)
      if not where:
        where = "1"

      if platformType==None:
        cursor.execute(f"""
          SELECT addons.id addonId, addons.addonname addonName, addons.posname addonPosName, addons.addondescription addonDescription, 
            addons.minpermitted minPermitted, addons.maxpermitted maxPermitted,addons.status status,
            addonsoptions.sortid sortId,
            items.id addonOptionId, items.itemname addonOptionName, items.posname posName, items.shortname shortName, 
            items.itemdescription addonOptionDescription, itemsku addonOptionSKU, convert(itemprice, CHAR) addonOptionPrice, 
            items.status addonOptionStatus ,
            (select count(*) from productsaddons where addonid=addons.id ) as itemcount
  
          FROM addons
          LEFT JOIN addonsoptions ON addons.id=addonsoptions.addonid
          LEFT JOIN items ON addonsoptions.itemid=items.id
          WHERE {where} AND addons.merchantid=%s
          ORDER BY addons.addonname ASC, addonsoptions.sortid ASC
          """, (merchantId))
      else:
        cursor.execute(f"""
        select distinct addons.id addonId, addons.addonname addonName, addons.posname addonPosName, addons.addondescription addonDescription, 
          addons.minpermitted minPermitted, addons.maxpermitted maxPermitted,
          addonsoptions.sortid sortId,
          addonItems.id addonOptionId, addonItems.itemname addonOptionName, addonItems.posname posName, addonItems.shortname shortName, 
          addonItems.itemdescription addonOptionDescription,addonItems.itemsku addonOptionSKU,
          convert(addonItems.itemprice, CHAR) addonOptionPrice, 
          addonItems.status addonOptionStatus ,menucategories.menuid 
          from addons
          left join addonsoptions on addonsoptions.addonid=addons.id
          left join items addonItems on addonItems.id=addonsoptions.itemid
          left join productsaddons on productsaddons.addonid=addonsoptions.addonid
           left join items on items .id= productsaddons.productid
            left JOIN productscategories ON productscategories.productid=items.id
         LEFT JOIN categories ON productscategories.categoryid = categories.id
         LEFT JOIN menucategories ON categories.id=menucategories.categoryid
         left join menumappings on menumappings.menuid=menucategories.menuid

          where addons.merchantid=%s and  menumappings.platformtype=%s
          """, (merchantId,platformType))
      rows = cursor.fetchall()

      all_addons = list()

      for row in rows:
        
        exists = False
        for addon in all_addons:
          if addon['id'] == row['addonId']:

            # code here
            addon['addonOptions'].append({
              'id': row['addonOptionId'],
              'addonOptionName': row['addonOptionName'],
              'addonOptionDescription': row['addonOptionDescription'],
              'addonOptionPrice': row['addonOptionPrice'],
              'addonOptionSKU': row['addonOptionSKU'],
              'addonOptionStatus': row['addonOptionStatus'],
              'posName': row['posName'],
              'shortName': row['shortName'],
              'sortId': row['sortId']
            })

            exists = True
            break
        
        
        if not exists:
          all_addons.append({
            'id': row['addonId'],
            'addonName': row['addonName'],
            'posName': row['addonPosName'],
            'addonDescription': row['addonDescription'],
            'minPermitted': row['minPermitted'],
            'maxPermitted': row['maxPermitted'],
            'itemcount': row['itemcount'] if 'itemcount' in row else 0,
            'status': row['status'] if 'status' in row else 0,
            'addonOptions': [
              {
                'id': row['addonOptionId'],
                'addonOptionName': row['addonOptionName'],
                'addonOptionDescription': row['addonOptionDescription'],
                'addonOptionPrice': row['addonOptionPrice'],
                'addonOptionSKU': row['addonOptionSKU'],
                'addonOptionStatus': row['addonOptionStatus'],
                'posName': row['posName'],
                'shortName': row['shortName'],
                'sortId': row['sortId']
              }
            ] if row['addonOptionId'] is not None else []

          })


      return all_addons
    except Exception as e:
      print("Error: ", str(e))
      return False

  @classmethod
  def get_all_addons_without_options_str(cls, merchantId, limit=None, _from=None, addonName=None, platformType=None,withitemcount=0):
    try:
      connection, cursor = get_db_connection()
      if withitemcount==1:
        cursor.execute(f"""
                  SELECT addons.id addonId, addons.addonname addonName, addons.posname addonPosName, addons.addondescription addonDescription, 
                    addons.minpermitted minPermitted, addons.maxpermitted maxPermitted,addons.status status
                  FROM addons
                  WHERE addons.merchantid=%s
                  ORDER BY addons.addonname ASC
                  """, (merchantId))
      else:
        cursor.execute(f"""
           SELECT addons.id addonId, addons.addonname addonName, addons.posname addonPosName, addons.addondescription addonDescription, 
             addons.minpermitted minPermitted, addons.maxpermitted maxPermitted,addons.status status,
             (select count(*) from productsaddons where addonid=addons.id ) as itemcount,
             (select count(*) from addonsoptions where addonid=addons.id ) as addoncount
           FROM addons
           WHERE addons.merchantid=%s
           ORDER BY addons.addonname ASC
           """, (merchantId))
      rows = cursor.fetchall()

      all_addons = list()

      for row in rows:
          all_addons.append({
            'id': row['addonId'],
            'addonName': row['addonName'],
            'posName': row['addonPosName'],
            'addonDescription': row['addonDescription'],
            'minPermitted': row['minPermitted'],
            'maxPermitted': row['maxPermitted'],
            'itemcount': row['itemcount'] if 'itemcount' in row else 0,
            'addoncount': row['addoncount'] if 'addoncount' in row else 0,
            'status': row['status'] if 'status' in row else 0,
          })

      return all_addons
    except Exception as e:
      print("Error: ", str(e))
      return False

  ############################################### POST

  @classmethod
  def post_addon(cls, merchantId, addon, userId=None , ip_address= None):
    try:
      if not addon:
        return invalid("invalid request")

      _an = addon.get('addonName')
      _posName = addon.get('posName')
      _desc = addon.get('addonDescription')
      _minPermitted = addon.get('minPermitted')
      _maxPermitted = addon.get('maxPermitted')
      status = addon["status"] if 'status' in addon else 1

      if _minPermitted and _maxPermitted and _maxPermitted < _minPermitted:
        return invalid("minPermitted is greater than maxPermitted")
      
      connection, cursor = get_db_connection()

      addonGUID = str(uuid.uuid4())
      data = (addonGUID, merchantId, _an, _posName, _desc, _minPermitted, _maxPermitted, userId , status)
      cursor.execute("""
        INSERT INTO addons 
          (id, merchantid, addonname, posname, addondescription, minpermitted, maxpermitted, created_by,status) 
          VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""", data)
      connection.commit()

      # Triggering Addon SNS
      subject = "addon.create"
      sns_msg = {
        "event": subject,
        "body": {
          "merchantId": merchantId,
          "addonId": addonGUID,
          "userId": userId,
          "ipAddr": ip_address
        }
      }
      sns_resp = publish_sns_message(topic=config.sns_addon_notification, message=str(sns_msg), subject=subject)
      publish_sns_message(topic=config.sns_activity_logs, message=str(sns_msg),
                                subject="addon.create")
      resp = cls.get_addon_by_id_with_options_str(addonGUID)
      return success(jsonify(resp))
    
    except Exception as e:
      print("error: ", str(e))
      return False

  ############################################### PUT
  
  @classmethod
  def put_addon(cls, addonId, addon, userId=None):
    try:
      connection, cursor = get_db_connection()
      
      addonName = addon["addonName"]
      posName = addon.get('posName')
      addonDescription = addon["addonDescription"]
      minPermitted = addon["minPermitted"]
      maxPermitted = addon["maxPermitted"]
      status= addon["status"] if 'status' in addon else 1

      if minPermitted and maxPermitted and maxPermitted < minPermitted:
        return invalid("minPermitted is greater than maxPermitted")

      data = (addonName, posName, addonDescription, minPermitted, maxPermitted, status, userId, addonId)
      cursor.execute("""UPDATE addons 
        SET addonname=%s, posname=%s, addondescription=%s, minpermitted=%s, maxpermitted=%s,status=%s, updated_by=%s, updated_datetime=CURRENT_TIMESTAMP
        WHERE id=%s""", data)
      connection.commit()
      return True
    except Exception as e:
      print("Error: ", str(e))
      return False
  
  ############################################### DELETE
  
  @classmethod
  def delete_addon_by_id(cls, addonId):
    try:
      connection, cursor = get_db_connection()
      
      cursor.execute("DELETE FROM addons WHERE id=%s", addonId)
      cursor.execute("DELETE FROM productsaddons WHERE addonid=%s", addonId)
      cursor.execute("DELETE FROM addonsoptions WHERE addonid=%s", addonId)

      connection.commit()
      return True
    except Exception as e:
      print("Error: ", str(e))
      return False
  

