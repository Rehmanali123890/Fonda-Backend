import requests

import config


class Esper():

    @classmethod
    def get_all_devices_in_an_enterprise(cls, limit: int = 500, state: str = "",status=0,serialNumber='' , esperId=''):
        try:
            url = f"{config.esper_base_url}/enterprise/{config.esper_enterprise_id}/device?limit={limit}&state={state}&serial={serialNumber}&name={esperId}"
            
            # state can be (1-active-devices or 20-removed-devices)

            headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {config.esper_api_key}"
            }
            
            devices = list()

            while True:

                response = requests.get(url, headers=headers)

                if response.status_code == 200:
                    response = response.json()

                    # extend results
                    results = response.get("results")
                    if status==1:
                        return results
                    for result in results:
                        devices.append({
                            "id": result.get("id"),
                            "device_name": result.get("device_name"),
                            "alias_name": result.get("alias_name"),
                            "state": result.get("state"),
                            "status": result.get("status"),  # 1-online, 60-offline
                        })


                    next_url = response.get("next")
                    if next_url and next_url != "null":
                        url = next_url
                    else:
                        break
                else:
                    print(response.text)
                    break
            
            print(len(devices))
            return devices 
        except Exception as e:
            print("error: ", str(e))
            return False
    

    @classmethod
    def get_esper_device_by_uuid(cls, device_uuid):
        try:
            url = f"{config.esper_base_url}/enterprise/{config.esper_enterprise_id}/device/{device_uuid}"

            headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {config.esper_api_key}"
            }

            response = requests.request("GET", url, headers=headers, data={})
            
            if response.status_code == 200:
                return response.json()
            else:
                print(response.text)
                return False

        except Exception as e:
            print("error: ", str(e))
            return False