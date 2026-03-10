from flask import jsonify, request, g
from controllers.Middleware import validate_token_middleware
from utilities.errors import unauthorised, unhandled, invalid
from utilities.helpers import *
import io
from models.NegativeAndNoOrderReport import NegativeAndNoOrderReport

@validate_token_middleware
def generate_negative_and_no_order_report():
    try:
        request_data = request.get_json()
        print("Request data:", request_data)

        create_log_data(
            level="[INFO]",
            Message=f"In the start of function to generate negative and no order report",
            functionName="generate_negative_and_no_order_report",
            request=request,
        )

        merchant_id = request_data['merchantId']
        if merchant_id == "-1":
            merchant_id = int(merchant_id)
        start_date = request_data['startDate']
        end_date = request_data['endDate']
        platform_type = request_data['platform_type']
        negative_no_order_report, excel_name = NegativeAndNoOrderReport.generate_negative_no_order_report(start_date, end_date, merchant_id, platform_type)
        output = io.BytesIO()
        negative_no_order_report.save(output)
        output.seek(0)  # Reset the pointer to the beginning of the file
        # Construct the response
        response = Response(
            output.getvalue(),  # Get the binary data from BytesIO
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
        response.headers["Content-Disposition"] = f"attachment; filename=\"{excel_name}.xlsx\"".encode("ascii",
                                                                                                       "ignore").decode()
        response.headers["Content-Length"] = len(output.getvalue())

        create_log_data(
            level="[INFO]",
            Message=f"Successfully generate negative and no order report",
            functionName="generate_negative_and_no_order_report",
            request=request,
        )

        return response

    except Exception as e:
        print("Error:", str(e))
        create_log_data(
            level="[ERROR]",
            Message=f"Error in generating negative and no order resport",
            messagebody=f'Error {str(e)}',
            functionName="generate_negative_and_no_order_report"
        )
        return unhandled(f"Unhandled Exception {str(e)}",500,str(e))
