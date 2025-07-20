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
    return call_unleashed_api_chunked(req, "StockOnHand")

@app.route(route="UnleashedCustomers")
def unleashed_customers(req: func.HttpRequest) -> func.HttpResponse:
    return call_unleashed_api_chunked(req, "Customers")

@app.route(route="UnleashedInvoices")
def unleashed_invoices(req: func.HttpRequest) -> func.HttpResponse:
    return call_unleashed_api_chunked(req, "Invoices")

@app.route(route="UnleashedProducts")
def unleashed_products(req: func.HttpRequest) -> func.HttpResponse:
    return call_unleashed_api_chunked(req, "Products")

@app.route(route="UnleashedSalesOrders")
def unleashed_sales_orders(req: func.HttpRequest) -> func.HttpResponse:
    return call_unleashed_api_chunked(req, "SalesOrders")

@app.route(route="UnleashedCreditNotes")
def unleashed_credit_notes(req: func.HttpRequest) -> func.HttpResponse:
    return call_unleashed_api_chunked(req, "CreditNotes")

@app.route(route="UnleashedPurchaseOrders")
def unleashed_purchase_orders(req: func.HttpRequest) -> func.HttpResponse:
    return call_unleashed_api_chunked(req, "PurchaseOrders")

# --- Corrected Logic for Handling API Calls ---

def call_unleashed_api_chunked(req: func.HttpRequest, endpoint: str) -> func.HttpResponse:
    """Parses the incoming request and calls the page-fetching function."""
    logging.info(f'Calling Unleashed {endpoint} API with chunked pagination')
    api_id = os.environ.get('UNLEASHED_API_ID')
    api_key = os.environ.get('UNLEASHED_API_KEY')
    
    if not api_id or not api_key:
        return func.HttpResponse("API credentials not configured.", status_code=400)

    try:
        params = req.params.copy()
        params.pop('code', None) # Remove Azure-specific key

        start_page = int(params.pop('startPage', 1))
        
        endpoint_defaults = {'SalesOrders': 15, 'StockOnHand': 15, 'Products': 10, 'Invoices': 25, 'Customers': 30, 'CreditNotes': 20, 'PurchaseOrders': 20}
        default_chunk_size = endpoint_defaults.get(endpoint, 15)
        chunk_size = int(params.pop('chunkSize', default_chunk_size))

        # The remaining 'params' are the user's filters.
        logging.info(f"Endpoint: {endpoint}, Start Page: {start_page}, Chunk Size: {chunk_size}, Filters: {params}")
        return get_chunked_pages(endpoint, params, api_id, api_key, start_page, chunk_size)
        
    except Exception as e:
        logging.error(f"Error in call_unleashed_api_chunked: {str(e)}", exc_info=True)
        return func.HttpResponse(f"Error processing request: {str(e)}", status_code=500)

def get_chunked_pages(endpoint: str, filter_params: dict, api_id: str, api_key: str, start_page: int, chunk_size: int) -> func.HttpResponse:
    """Gets a chunk of pages. This version uses the robust 'params' argument for requests."""
    try:
        all_items = []
        base_url = f"https://api.unleashedsoftware.com/{endpoint}"
        
        # --- Get Metadata First ---
        meta_params = filter_params.copy()
        meta_params['pageSize'] = 1
        
        # The requests library will correctly URL-encode this query string
        meta_query_string_for_signature = '&'.join([f"{k}={v}" for k, v in sorted(meta_params.items())])
        meta_signature = generate_signature(meta_query_string_for_signature, api_key)
        meta_headers = {'Accept': 'application/json', 'api-auth-id': api_id, 'api-auth-signature': meta_signature}
        
        meta_response = requests.get(base_url, headers=meta_headers, params=meta_params, timeout=120)
        if meta_response.status_code != 200:
            return func.HttpResponse(f"Failed to get dataset metadata: {meta_response.status_code} - {meta_response.text}", status_code=meta_response.status_code)
        
        pagination_info = meta_response.json().get('Pagination', {})
        total_pages = pagination_info.get('NumberOfPages', 0)
        
        if total_pages == 0:
             return func.HttpResponse(json.dumps({'Items': [], 'ChunkInfo': {'Message': 'No data found for the specified filters.'}}), status_code=200, headers={'Content-Type': 'application/json'})

        # --- Fetch Pages for this Chunk ---
        end_page = min(start_page + chunk_size - 1, total_pages)
        
        for page_number in range(start_page, end_page + 1):
            current_page_params = filter_params.copy()
            current_page_params['pageSize'] = 1000
            current_page_params['pageNumber'] = page_number
            
            # Build the query string for the signature ONLY, in alphabetical order as per some API best practices
            query_string_for_signature = '&'.join([f"{k}={v}" for k, v in sorted(current_page_params.items())])
            signature = generate_signature(query_string_for_signature, api_key)
            headers = {'Accept': 'application/json', 'api-auth-id': api_id, 'api-auth-signature': signature}
            
            logging.info(f"Requesting page {page_number} of {total_pages}...")
            
            try:
                # Pass the parameters as a dictionary to requests, which is the correct and safe method.
                response = requests.get(base_url, headers=headers, params=current_page_params, timeout=120)
                if response.status_code != 200:
                    logging.error(f"API call failed for page {page_number}: {response.text}")
                    break
                
                items = response.json().get('Items', [])
                if not items: break
                all_items.extend(items)

            except requests.exceptions.Timeout:
                logging.error(f"Request timeout on page {page_number}")
                break
        
        # --- Prepare Response ---
        result = {
            'Items': all_items,
            'ChunkInfo': {
                'StartPage': start_page, 'EndPage': end_page, 'RequestedChunkSize': chunk_size,
                'TotalPagesAvailable': pagination_info.get('NumberOfPages', 0),
                'TotalItemsAvailable': pagination_info.get('NumberOfItems', 0),
                'HasMorePages': end_page < total_pages,
                'NextStartPage': end_page + 1 if end_page < total_pages else None
            }
        }
        return func.HttpResponse(json.dumps(result), status_code=200, headers={'Content-Type': 'application/json'})
        
    except Exception as e:
        logging.error(f"Critical error in get_chunked_pages: {str(e)}", exc_info=True)
        return func.HttpResponse(f"A critical error occurred: {str(e)}", status_code=500)

def generate_signature(query_string, api_key):
    """Generates the HMAC-SHA256 signature required by the Unleashed API."""
    key_bytes = api_key.encode('utf-8')
    query_bytes = query_string.encode('utf-8')
    signature = hmac.new(key_bytes, query_bytes, hashlib.sha256)
    return base64.b64encode(signature.digest()).decode('utf-8')
