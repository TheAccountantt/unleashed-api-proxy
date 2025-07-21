import logging
import json
import hmac
import hashlib
import base64
import os
import requests
import time
import azure.functions as func

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

# --- Your working function routes ---
@app.route(route="UnleashedStockOnHand")
def unleashed_stock_on_hand(req: func.HttpRequest) -> func.HttpResponse:
    return call_unleashed_api(req, "StockOnHand")

@app.route(route="UnleashedCustomers")
def unleashed_customers(req: func.HttpRequest) -> func.HttpResponse:
    return call_unleashed_api(req, "Customers")

@app.route(route="UnleashedInvoices")
def unleashed_invoices(req: func.HttpRequest) -> func.HttpResponse:
    return call_unleashed_api(req, "Invoices")

@app.route(route="UnleashedProducts")
def unleashed_products(req: func.HttpRequest) -> func.HttpResponse:
    return call_unleashed_api(req, "Products")

@app.route(route="UnleashedSalesOrders")
def unleashed_sales_orders(req: func.HttpRequest) -> func.HttpResponse:
    return call_unleashed_api(req, "SalesOrders")

@app.route(route="UnleashedCreditNotes")
def unleashed_credit_notes(req: func.HttpRequest) -> func.HttpResponse:
    return call_unleashed_api(req, "CreditNotes")

@app.route(route="UnleashedPurchaseOrders")
def unleashed_purchase_orders(req: func.HttpRequest) -> func.HttpResponse:
    return call_unleashed_api(req, "PurchaseOrders")

def generate_signature(api_key, query_params):
    """Generates the HMAC-SHA256 signature required by the Unleashed API."""
    message = query_params.encode('utf-8')
    secret = api_key.encode('utf-8')
    signature = hmac.new(secret, message, hashlib.sha256).digest()
    return base64.b64encode(signature).decode()

def flatten_sales_orders(orders):
    """
    Processes a list of sales orders to flatten the SalesOrderLines,
    similar to the user's reference script.
    """
    logging.info("Flattening SalesOrders data...")
    processed_lines = []
    for order in orders:
        # Create a base record with all parent-level order info, excluding the lines
        order_info = {k: v for k, v in order.items() if k != 'SalesOrderLines'}
        
        for line in order.get("SalesOrderLines", []):
            # For each line, create a new record combining the parent info and the line info
            flat_line_record = order_info.copy()
            flat_line_record.update(line) # Add all keys from the line item
            processed_lines.append(flat_line_record)
    
    logging.info(f"Flattened {len(orders)} orders into {len(processed_lines)} lines.")
    return processed_lines

def call_unleashed_api(req: func.HttpRequest, endpoint: str) -> func.HttpResponse:
    """
    Fetches all pages for an endpoint and flattens the result if necessary.
    """
    logging.info(f"Starting full data fetch for endpoint: {endpoint}")
    api_id = os.environ.get('UNLEASHED_API_ID')
    api_key = os.environ.get('UNLEASHED_API_KEY')
    
    if not api_id or not api_key:
        return func.HttpResponse("API credentials not configured.", status_code=400)

    try:
        all_items = []
        page_number = 1
        base_url = f"https://api.unleashedsoftware.com/{endpoint}"
        
        filter_params = req.params.copy()
        filter_params.pop('code', None)
        
        if 'pageSize' not in filter_params:
            filter_params['pageSize'] = '1000' # Use a larger page size for efficiency

        query_string_for_signature = '&'.join([f"{k}={v}" for k, v in sorted(filter_params.items())])

        while True:
            logging.info(f"Fetching page {page_number} for {endpoint}...")
            signature = generate_signature(api_key, query_string_for_signature)
            headers = {
                'Content-Type': 'application/json',
                'Accept': 'application/json',
                'api-auth-id': api_id,
                'api-auth-signature': signature
            }
            paginated_url = f"{base_url}/{page_number}"

            response = requests.get(paginated_url, headers=headers, params=filter_params, timeout=180)

            if response.status_code != 200:
                logging.error(f"API call failed on page {page_number}: {response.status_code} - {response.text}")
                return func.HttpResponse(f"Failed on page {page_number}. Error: {response.text}", status_code=response.status_code)

            data = response.json()
            items = data.get("Items", [])
            
            if not items:
                logging.info(f"No more items found. Fetch complete.")
                break

            all_items.extend(items)
            
            if len(items) < int(filter_params['pageSize']):
                logging.info("Received fewer items than page size. Fetch complete.")
                break

            page_number += 1
            time.sleep(0.1)

        final_data = all_items
        # If the endpoint was SalesOrders, flatten the result before returning
        if endpoint == "SalesOrders":
            final_data = flatten_sales_orders(all_items)

        logging.info(f"Successfully processed {len(final_data)} total items for {endpoint}.")
        return func.HttpResponse(json.dumps({"Items": final_data}), status_code=200, headers={'Content-Type': 'application/json'})

    except Exception as e:
        logging.error(f"A critical error occurred: {str(e)}", exc_info=True)
        return func.HttpResponse(f"A critical error occurred in the Azure Function: {str(e)}", status_code=500)
