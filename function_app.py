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
    # The signature is based on the query parameters ONLY.
    message = query_params.encode('utf-8')
    secret = api_key.encode('utf-8')
    signature = hmac.new(secret, message, hashlib.sha256).digest()
    return base64.b64encode(signature).decode()

def call_unleashed_api(req: func.HttpRequest, endpoint: str) -> func.HttpResponse:
    """
    Fetches all pages for a given endpoint in a single execution,
    based on the logic from your reference script.
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
        
        # Extract filter parameters from the request, excluding the function code
        # These will be the same for every page we fetch.
        filter_params = req.params.copy()
        filter_params.pop('code', None)
        
        # Add a default page size, but allow it to be overridden
        if 'pageSize' not in filter_params:
            filter_params['pageSize'] = '200' # A common page size

        # Convert the filter params to a query string once. This is used for the signature.
        query_string_for_signature = '&'.join([f"{k}={v}" for k, v in sorted(filter_params.items())])

        while True:
            logging.info(f"Fetching page {page_number} for {endpoint}...")
            
            # Generate the signature for this request
            signature = generate_signature(api_key, query_string_for_signature)
            headers = {
                'Content-Type': 'application/json',
                'Accept': 'application/json',
                'api-auth-id': api_id,
                'api-auth-signature': signature
            }

            # Construct the URL with the page number in the path, as per your script
            # The requests library will handle adding the filter_params correctly.
            paginated_url = f"{base_url}/{page_number}"

            response = requests.get(paginated_url, headers=headers, params=filter_params, timeout=120)

            if response.status_code != 200:
                logging.error(f"API call failed on page {page_number}: {response.status_code} - {response.text}")
                # Return what we have so far, along with the error
                return func.HttpResponse(f"Failed on page {page_number}. Partial data might be available. Error: {response.text}", status_code=response.status_code)

            data = response.json()
            items = data.get("Items", [])
            
            if not items:
                logging.info(f"No more items found on page {page_number}. Fetch complete.")
                break # Exit the loop if the page is empty

            all_items.extend(items)
            
            # Check if we've received less than the requested page size, indicating the last page
            if len(items) < int(filter_params['pageSize']):
                logging.info("Received fewer items than page size. Fetch complete.")
                break

            page_number += 1
            time.sleep(0.1) # Be kind to the API

        logging.info(f"Successfully fetched {len(all_items)} total items for {endpoint}.")
        # Return the complete list of items in a structure Power BI can easily handle
        return func.HttpResponse(json.dumps({"Items": all_items}), status_code=200, headers={'Content-Type': 'application/json'})

    except Exception as e:
        logging.error(f"A critical error occurred: {str(e)}", exc_info=True)
        return func.HttpResponse(f"A critical error occurred in the Azure Function: {str(e)}", status_code=500)
