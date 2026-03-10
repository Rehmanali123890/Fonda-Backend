import requests


class Stripe():

  @classmethod
  def stripe_get_connected_account(cls, apiKey, accountId):
    try:
      url = f"https://api.stripe.com/v1/accounts/{accountId}"

      payload = {}
      headers = {
        'Authorization': f'Bearer {apiKey}'
      }

      response = requests.request("GET", url, headers=headers, data=payload)
      # print(response.text)

      status_code = response.status_code
      response_json = response.json()

      return status_code, response_json
    except Exception as e:
      print("Error: ", str(e))
      return False, ""
 
  @classmethod
  def stripe_get_balance(cls, apiKey):
    try:
      url = f"https://api.stripe.com/v1/balance"

      payload = {}
      headers = {
        'Authorization': f'Bearer {apiKey}'
      }

      response = requests.request("GET", url, headers=headers, data=payload)
      # print(response.text)

      status_code = response.status_code
      response_json = response.json()

      return status_code, response_json
    except Exception as e:
      print("Error: ", str(e))
      return False, ""
  @classmethod
  def stripe_create_a_transfer(cls, apiKey, payload):
    try:
      url = "https://api.stripe.com/v1/transfers"

      headers = {
        'Authorization': f'Bearer {apiKey}',
        'Content-Type': 'application/x-www-form-urlencoded'
      }

      response = requests.request("POST", url, headers=headers, data=payload)
      print(response.text)

      status_code = response.status_code
      response_json = response.json()

      return status_code, response_json

    except Exception as e:
      print("Error: ", str(e))
      return 500, str(e)

  @classmethod
  def stripe_create_a_transfer_reversal(cls, apiKey, transferId, amount):
    try:
      url = f"https://api.stripe.com/v1/transfers/{transferId}/reversals"

      headers = {
        'Authorization': f'Bearer {apiKey}',
        'Content-Type': 'application/x-www-form-urlencoded'
      }
      payload = f"amount={amount}"

      response = requests.request("POST", url, headers=headers, data=payload)
      print(response.text)

      status_code = response.status_code
      response_json = response.json()

      return status_code, response_json

    except Exception as e:
      print("Error: ", str(e))
      return 500, str(e)

  @classmethod
  def list_external_accounts(cls, apiKey, connect_id):

    import requests

    url = "https://api.stripe.com/v1/accounts/" + connect_id + "/external_accounts"

    payload = 'object=bank_account&limit=3'

    headers = {
      'Authorization': f'Bearer '+apiKey,
      'Content-Type': 'application/x-www-form-urlencoded'
    }

    response = requests.request("GET", url, headers=headers, data=payload)

    print(response.text)

    status_code = response.status_code
    response_json = response.json()

    return status_code, response_json

  @classmethod
  def create_transfer_to_bank_from_connect_account(cls, apiKey, to_bank_account, amount, connect_account_id):
    import requests

    url = "https://api.stripe.com/v1/payouts"

    payload = 'amount=' + str(amount) + '&currency=usd&destination=' + to_bank_account + '&description=Fonda%20Technologies%20payout'
    headers = {
      'Authorization': f'Bearer '+apiKey,
      'Stripe-Account': connect_account_id,
      'Content-Type': 'application/x-www-form-urlencoded'
    }

    response = requests.request("POST", url, headers=headers, data=payload)

    print(response.text)
    status_code = response.status_code
    response_json = response.json()

    return status_code, response_json
