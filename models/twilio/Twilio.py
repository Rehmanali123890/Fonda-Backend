import requests
from requests.auth import HTTPBasicAuth

import config


class Twilio():


    @classmethod
    def send_message(cls, _from, _to, _message,data_encoded=True):
        try:
            url = f"https://api.twilio.com/2010-04-01/Accounts/{config.twilio_account_sid}/Messages.json"

            headers = {
                'Content-Type': 'application/x-www-form-urlencoded',
            }

            data = f"Body={_message}&From={_from}&To={_to}"

            if data_encoded:
                data = data.encode('utf-8')
            else:
                data = data
            auth = HTTPBasicAuth(config.twilio_account_sid, config.twilio_auth_token)

            response = requests.post(url=url, data=data, headers=headers, auth=auth)

            return response.status_code, response.json()

        except Exception as e:
            return False, str(e)