import json
import requests

def lambda_handler(event, context):
    # print(event)
    
    # CONSTANTS
    DASHBOARD_URL = "https://xrdtnomt8c.execute-api.us-east-2.amazonaws.com/dev"
    API_KEY = "rFcKIRsWyg5EJ0C0o6zjN9gMym8TVILd5K13kPYB"
    
    # get route and connectionId
    route = event.get("requestContext").get("routeKey")
    connectionId = event.get("requestContext").get("connectionId")
    
    print("route: ", route)
    print("connection Id: ", connectionId)
    
    # $connect, $disconnect, $default
    if (route == "$connect"):
        
        # get query string parameters
        queryStringParameters = event.get("queryStringParameters")
        print("Request Data: ", queryStringParameters)
        
        if queryStringParameters:
            token = queryStringParameters.get("token")
            eventName = queryStringParameters.get("eventName")
            
            # store websocket details in database
            if token and eventName:
                payload = json.dumps({
                    "token": token,
                    "connectionId": connectionId,
                    "eventName": eventName
                })
            
                headers = {
                    'x-api-key': API_KEY,
                    'Content-Type': 'application/json'
                }
            
                response = requests.request("POST", DASHBOARD_URL+"/websocket", headers=headers, data=payload)
                
                if response.status_code >= 200 and response.status_code <= 300:
                    print("Connect route triggered")
                    return {
                        'statusCode': 200,
                        'body': json.dumps('Route Connected')
                    }
                else:
                    print(response)
            else:
                print("Insufficient data in request query")
                return {
                    "statusCode": 400,
                    "body": json.dumps("Insufficient data in request query")
                }

    elif (route == "$disconnect"):
        
        url = f"{DASHBOARD_URL}/websocket/{connectionId}"
        headers = {
            'x-api-key': API_KEY
        }
        response = requests.request("DELETE", url, headers=headers)
        
        if response.status_code >= 200 and response.status_code <= 300:
            print("Route Disconnected")
            return {
                'statusCode': 200,
                'body': json.dumps('Route Disconnected')
            }
        
    else:
        print("Invalid Route")
        '''
        currently i am using default for handling connection refrest.
        Later-on there will be $refresh route for handling this request.
        So, format must be following:
        {"action": "refresh"}
        Response:-> {
            'statusCode': 200,
            'body': json.dumps({
                "status": "connected"
            })
        '''
        return {
            'statusCode': 200,
            'body': json.dumps({
                "status": "connected"
            })
        }
    
    return {
        'statusCode': 500,
        'body': json.dumps('Unexpected Error')
    }
