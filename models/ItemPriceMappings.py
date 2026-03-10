import uuid
import json

# local imports
import config
from utilities.helpers import get_db_connection



class ItemPriceMappings():

  @classmethod
  def get_itemPriceMappings(cls, itemId , merchantId=None , fromtab=None):
    try:

      connection, cursor = get_db_connection()
      mappings = []
      if not fromtab:
        query = """
                  SELECT platformtype, platformitemprice
                  FROM itempricemappings
                  WHERE itemid = %s
              """
        params = [itemId]
        cursor.execute(query, params)
        rows = cursor.fetchall()

        for row in rows:
          mappings.append({
            "platformType": row["platformtype"],
            "platformItemPrice": format(row["platformitemprice"])
          })
        return mappings


      platform_types = get_flagged_platform_data(merchantId)
      if platform_types:
        query = """
            SELECT platformtype, platformitemprice
            FROM itempricemappings
            WHERE itemid = %s
        """
        params = [itemId]

        placeholders = ', '.join(['%s'] * len(platform_types))
        query += f" AND platformtype IN ({placeholders})"
        params.extend(platform_types)


        cursor.execute(query, params)
        rows = cursor.fetchall()


        for row in rows:
          mappings.append({
            "platformType": row["platformtype"],
            "platformItemPrice": format(row["platformitemprice"])
          })

      # ✅ fromtab=True → Initialize defaults first
      item_query = """
                SELECT itemprice
                FROM items
                WHERE id = %s
            """

      cursor.execute(item_query, [itemId])
      itemprice = cursor.fetchone()
      default_price = 0.0
      if itemprice:
        default_price=itemprice.get('itemprice')
      required_platforms = platform_types
      # ✅ Collect existing platform types from mappings
      existing_platforms = {m["platformType"] for m in mappings}

      # ✅ Add missing ones with default price
      for p_type in required_platforms:
        if p_type not in existing_platforms:
          mappings.append({
            "platformType": p_type,
            "platformItemPrice": format(default_price)
          })
      return mappings
    except Exception as e:
      print(str(e))
      return False
  
 

def get_flagged_platform_data(merchantId):
    connection, cursor = get_db_connection()
    cursor.execute('''
        SELECT 
            m.id,
            m.doordashstream,
            m.grubhubstream
        FROM merchants m 
        WHERE m.platform_price_flag = 1 and m.id = %s
    ''', (merchantId))
    merchant_row = cursor.fetchone()
    
    platform_types = []

    if merchant_row:
        if merchant_row.get('doordashstream') == 1:
            platform_types.append(6)
        if merchant_row.get('grubhubstream') == 1:
            platform_types.append(5)

    cursor.execute('''
        SELECT 
            p.platformtype as platformType,
            m.platform_price_flag
        FROM platforms p 
        JOIN merchants m ON m.id = p.merchantid
        WHERE p.platformtype = 3 AND m.platform_price_flag = 1 and m.id = %s;
    ''', (merchantId))
    platform_row = cursor.fetchone()

    if platform_row:
        platform_types.append(3)

    platform_types = list(set(platform_types))
    return platform_types