import requests
import json
import datetime
from dateutil.tz import gettz
from utilities.helpers import  publish_sns_message
import config


def send_error_message_to_slack_webhook(webhook_url, merchantName, username, errorDateTime, errorName, errorSource, errorDetails, orderexternalreference=None):
	try:
		headers = {
			'Content-Type': 'application/json'
		}

		payload = json.dumps({
			"text": errorDetails,
			"blocks": [
				{
					"type": "header",
					"text": {
						"type": "plain_text",
						"text": f"Merchant <{merchantName}>",
						"emoji": True
					}
				},
				{
					"type": "section",
					"fields": [
						{
							"type": "mrkdwn",
							"text": "*Error Name*\n{}".format(errorName)
						},
						{
							"type": "mrkdwn",
							"text": "*Error Source*\n{}".format(errorSource)
						}
					]
				},
				{
					"type": "section",
					"fields": [
						{
							"type": "mrkdwn",
							"text": "*Order External Refrence Id*\n{}".format(orderexternalreference)
						}
					]},
				{
					"type": "section",
					"fields": [
						{
							"type": "mrkdwn",
							"text": "*DateTime*\n{}".format(errorDateTime)
						},
						{
							"type": "mrkdwn",
							"text": "*Username*\n{}".format(username)
						}
					]
				},
				{
					"type": "section",
					"text": {
						"type": "mrkdwn",
						"text": "*Error Details*\n {}".format(errorDetails)
					}
				},
				{
					"type": "divider"
				},
				{
					"type": "divider"
				}
			]
		})

		response = requests.request("POST", webhook_url, headers=headers, data=payload)
		print(response.text)
		data = True if response and response.status_code >= 200 and response.status_code < 300 else False
		return data
	except Exception as e:
		print("error: ", str(e))
		return False



def send_menu_update_message_to_slack_webhook(webhook_url, merchantName, username, eventName, eventDetails, eventDateTime,source,is_merchant=True):
	try:
		headers = {
			'Content-Type': 'application/json'
		}

		payload = json.dumps({
			"text": eventDetails,
			"blocks": [
				{
					"type": "header",
					"text": {
						"type": "plain_text",
						 "text": f"Merchant <{merchantName}>" if is_merchant else f"{merchantName}",
						"emoji": True
					}
				},
				{
					"type": "section",
					"fields": [
						{
							"type": "mrkdwn",
							"text": "*Event Name*\n{}".format(eventName)
						},
						{
							"type": "mrkdwn",
							"text": "*Source*\n{}".format(source)
						}
					]
				},
				{
					"type": "section",
					"fields": [
						{
							"type": "mrkdwn",
							"text": "*Username*\n{}".format(username)
						},
						{
							"type": "mrkdwn",
							"text": "*DateTime*\n{}".format(eventDateTime)
						}
					]
				},
				{
					"type": "section",
					"text": {
						"type": "mrkdwn",
						"text": "*Event Details*\n {}".format(eventDetails)
					}
				},
				{
					"type": "divider"
				},
				{
					"type": "divider"
				}
			]
		})

		response = requests.request("POST", webhook_url, headers=headers, data=payload)
		print(response.text)
		data = True if response and response.status_code >= 200 and response.status_code < 300 else False
		return data
	except Exception as e:
		print("error: ", str(e))
		return False


def send_merchant_status_message_to_slack_webhook(webhook_url, merchantName, username, eventDetails):
	try:
		currentDateTime = datetime.datetime.now(datetime.timezone.utc).astimezone(gettz('US/Pacific')).strftime("%m-%d-%Y %H:%M (%Z)")
		headers = {
			'Content-Type': 'application/json'
		}

		payload = json.dumps({
			"text": eventDetails,
			"blocks": [
				{
					"type": "header",
					"text": {
						"type": "plain_text",
						"text": f"Merchant <{merchantName}>",
						"emoji": True
					}
				},
				{
					"type": "section",
					"fields": [
						{
							"type": "mrkdwn",
							"text": "*Username*\n{}".format(username)
						},
						{
							"type": "mrkdwn",
							"text": "*DateTime*\n{}".format(currentDateTime)
						}
					]
				},
				{
					"type": "section",
					"text": {
						"type": "mrkdwn",
						"text": "*Event Details*\n {}".format(eventDetails)
					}
				},
				{ "type": "divider" },
				{ "type": "divider" }
			]
		})

		response = requests.request("POST", webhook_url, headers=headers, data=payload)
		print(response.text)
		data = True if response and response.status_code >= 200 and response.status_code < 300 else False
		return data
	except Exception as e:
		print("error: ", str(e))
		return False

def send_slack_google_mybusiness_review_webhook (webhook_url, merchantName, username, eventDetails):
	try:
		currentDateTime = datetime.datetime.now(datetime.timezone.utc).astimezone(gettz('US/Pacific')).strftime("%m-%d-%Y %H:%M (%Z)")
		headers = {
			'Content-Type': 'application/json'
		}

		payload = json.dumps({
			"text": eventDetails,
			"blocks": [
				{
					"type": "header",
					"text": {
						"type": "plain_text",
						"text": f"Merchant <{merchantName}>",
						"emoji": True
					}
				},
				{
					"type": "section",
					"fields": [
						{
							"type": "mrkdwn",
							"text": "*Username*\n{}".format(username)
						},
						{
							"type": "mrkdwn",
							"text": "*DateTime*\n{}".format(currentDateTime)
						}
					]
				},
				{
					"type": "section",
					"text": {
						"type": "mrkdwn",
						"text": "*Event Details*\n {}".format(eventDetails)
					}
				},
				{ "type": "divider" },
				{ "type": "divider" }
			]
		})

		response = requests.request("POST", webhook_url, headers=headers, data=payload)
		print(response.text)
		data = True if response and response.status_code >= 200 and response.status_code < 300 else False
		return data
	except Exception as e:
		print("error: ", str(e))
		return False

def send_slack_error_notification(userId ,merchantId , errorName , errorSource , errorStatus , errorDetails , orderExternalReference ):
		print("Triggering SNS For send_slack_error_notification")
		sns_msg = {
			"event": "error_logs.entry",
			"body": {
				"userId": userId,
				"merchantId": merchantId,
				"errorName": errorName,
				"errorSource":errorSource,
				"errorStatus": errorStatus,
				"errorDetails": errorDetails,
				"orderExternalReference": orderExternalReference
			}
		}
		error_logs_sns_resp = publish_sns_message(topic=config.sns_error_logs, message=str(sns_msg),
																							subject="error_logs.entry")
  
  
# send reconciliation error message on slack   
def send_reconciliation_report_to_slack(webhook_url, start_period, end_period, error_message):
    try:
        currentDateTime = datetime.datetime.now(datetime.timezone.utc)\
            .astimezone(gettz('US/Pacific')).strftime("%m-%d-%Y %H:%M (%Z)")

        headers = { 'Content-Type': 'application/json' }

        payload = json.dumps({
            "text": f"Payout Reconciliation Report - {start_period} to {end_period}",
            "blocks": [
                {
                    "type": "header",
                    "text": {
                        "type": "plain_text",
                        "text": "Payout Reconciliation Report",
                        "emoji": True
                    }
                },
                {
                    "type": "section",
                    "fields": [
                        {
                            "type": "mrkdwn",
                            "text": "*Error Name*\nError on creating reconciliation report for period {} - {}".format(start_period, end_period)
                        },
                        {
                            "type": "mrkdwn",
                            "text": "*Error Source*\ndashboard"
                        }
                    ]
                },
                {
                    "type": "section",
                    "fields": [
                        {
                            "type": "mrkdwn",
                            "text": "*DateTime*\n{}".format(currentDateTime)
                         }
                        # {
                        #     "type": "mrkdwn",
                        #     "text": "*Username*\n{}".format(username)
                        # }
                    ]
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": "*Error Details*\n{}".format(error_message)
                    }
                },
                { "type": "divider" }
            ]
        })

        response = requests.post(webhook_url, headers=headers, data=payload)
        print(response.text)
        return True if response.status_code >= 200 and response.status_code < 300 else False

    except Exception as e:
        print("error: ", str(e))
        return False