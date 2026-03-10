import os
import sys
import uuid
import logging
import requests
import traceback

# Ensure the script can find utilities/helpers
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, ROOT_DIR)

# Local imports
from utilities.helpers import openDbconnection
from config import slack_error_logs_channel_webhook  # Import Slack webhook URL

# Set up logging
logging.basicConfig(
    level=logging.INFO,  # Change to DEBUG for more details
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),  # Logs to console
        logging.FileHandler("calculate_revenue.log", mode="a")  # Logs to a file
    ]
)


def send_slack_alert(error_message, function_name):
    """Send an error alert to Slack."""
    if not slack_error_logs_channel_webhook:
        logging.error("Slack webhook URL is missing. Cannot send alert.")
        return

    message = f":x: *Error in {function_name}* :x:\n```{error_message}```"
    try:
        response = requests.post(slack_error_logs_channel_webhook, json={"text": message})
        response.raise_for_status()
        logging.info("Slack alert sent successfully.")
    except requests.RequestException as e:
        logging.error(f"Failed to send Slack alert: {e}")


def calculate_revenue_by_day_handler():
    logging.info("---------- Starting Revenue Calculation Script ----------")

    try:
        # Establish DB connection
        connection, cursor = openDbconnection()
        logging.info("Database connection established.")
    except Exception as e:
        error_message = f"Failed to establish DB connection: {str(e)}"
        logging.error(error_message)
        send_slack_alert(error_message, "openDbconnection")
        return

    try:
        # Get all merchants
        cursor.execute("SELECT * FROM merchants WHERE status=1")
        merchants = cursor.fetchall()
        total_merchants = len(merchants)
        logging.info(f"Found {total_merchants} merchants to process.")
    except Exception as e:
        error_message = f"Failed to fetch merchants: {str(e)}"
        logging.error(error_message)
        send_slack_alert(error_message, "fetch_merchants")
        connection.close()
        return

    # Dictionary of weekdays in MySQL (0 = Monday, ..., 6 = Sunday)
    weekdays = {0: 'monday', 1: 'tuesday', 2: 'wednesday', 3: 'thursday', 4: 'friday', 5: 'saturday', 6: 'sunday'}

    # Process each merchant
    for index, merchant in enumerate(merchants, start=1):
        try:
            logging.info(f"Processing merchant {index}/{total_merchants} (ID: {merchant['id']}, Timezone: {merchant['timezone']})...")

            cursor.execute(f"""
                SELECT 
                    COALESCE(AVG(dailyRevenue), 0) as revenue, weekday
                FROM (
                    SELECT 
                        COALESCE(SUM(ordertotal), 0) dailyRevenue, 
                        @orderDate := DATE(convert_tz(orderdatetime, '+00:00', '{merchant["timezone"]}')) orderDate,
                        WEEKDAY(@orderDate) weekday
                    FROM ordersCombined WHERE merchantid = %s
                    GROUP BY @orderDate
                ) as result1
                GROUP BY weekday;
            """, (merchant["id"],))

            rows = cursor.fetchall()
            daily_revenue = {i: 0.00 for i in range(7)}

            # Update revenue dictionary
            for row in rows:
                if row['weekday'] is not None:
                    daily_revenue[int(row['weekday'])] = row['revenue']

            # Check if merchant row exists
            cursor.execute("SELECT id FROM lifetimerevenue WHERE merchantid=%s", (merchant['id'],))
            row = cursor.fetchone()

            if row:
                cursor.execute("""
                    UPDATE lifetimerevenue
                    SET monday=%s, tuesday=%s, wednesday=%s, thursday=%s, friday=%s, saturday=%s, sunday=%s, updated_datetime=CURRENT_TIMESTAMP
                    WHERE merchantid=%s
                """, (daily_revenue[0], daily_revenue[1], daily_revenue[2], daily_revenue[3], daily_revenue[4], daily_revenue[5], daily_revenue[6], merchant['id']))
            else:
                cursor.execute("""
                    INSERT INTO lifetimerevenue 
                    (id, merchantid, monday, tuesday, wednesday, thursday, friday, saturday, sunday, updated_datetime)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,CURRENT_TIMESTAMP)
                """, (str(uuid.uuid4()), merchant['id'], daily_revenue[0], daily_revenue[1], daily_revenue[2], daily_revenue[3], daily_revenue[4], daily_revenue[5], daily_revenue[6]))

            # Commit changes after each merchant
            connection.commit()

            # Log progress update
            progress_percentage = (index / total_merchants) * 100
            logging.info(f"✅ Completed {index}/{total_merchants} merchants ({progress_percentage:.2f}% done)")

        except Exception as e:
            error_message = f"Error processing merchant ID {merchant['id']}: {traceback.format_exc()}"
            logging.error(error_message)
            send_slack_alert(error_message, "process_merchant")
            continue  # Continue processing other merchants

    # Close DB connection
    try:
        connection.close()
        logging.info("Database connection closed.")
    except Exception as e:
        error_message = f"Failed to close DB connection: {str(e)}"
        logging.error(error_message)
        send_slack_alert(error_message, "close_db_connection")

    logging.info("---------- Revenue Calculation Script Finished ----------")


# Run as a standalone script
if __name__ == "__main__":
    calculate_revenue_by_day_handler()
