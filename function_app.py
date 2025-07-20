import logging
import json
import hmac
import hashlib
import base64
import os
import requests
import time
from urllib.parse import urlparse, parse_qs
import azure.functions as func

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

# Define all your routes to use the single, robust chunking function
@app.route(route="UnleashedStockOnHand")
@app.route(route="UnleashedCustomers")
@app.route(route="UnleashedInvoices")
@app.route(route="UnleashedProducts")
@app.route(route="UnleashedSalesOrders")
@app.route(route="UnleashedCreditNotes")
@app.route(route="UnleashedPurchaseOrders")
def handle_request(req: func.HttpRequest) -> func.HttpResponse:
    """A single route handler for all Unleashed endpoints."""
    # Extract the endpoint name from the route that was called
    # e.g., "/api/UnleashedSalesOrders" -> "SalesOrders"
    endpoint = req.route_params.get('route').replace('Unleashed', '')
    if not endpoint:
        # Fallback for older function versions or direct calls
        endpoint_from_url = req.url.split('/')[-1].split('?')[0].replace('Unleashed', '')
        endpoint = endpoint_from_url if endpoint_from_url else "SalesOrders"

    return call_unleashed_api_chunked(req, endpoint)


def call_unleashed_api_chunked(req: func.HttpRequest, endpoint: str) -> func.HttpResponse:
    """Chunked pagination for very large datasets - allows multiple function calls"""
    logging.info(f'Calling Unleashed {endpoint} API with chunked pagination')

    # Get API credentials from environment variables
    api_id = os.environ.get('UNLEASHED_API_ID')
    api_key = os.environ.get('UNLEASHED_API_KEY')
    
    if not api_id or not api_key:
        return func.HttpResponse(
            "API credentials not configured. Please set UNLEASHED_API_ID and UNLEASHED_API_KEY environment variables.",
            status_code=400
        )

    try:
        # Get query string from request, excluding the function-specific 'code' parameter
        params = req.params.copy()
        params.pop('code', None) # Safely remove the 'code' key if it exists

        # Extract chunk parameters
        start_page = int(params.pop('startPage', 1))
        
        # Endpoint-specific default chunk sizes
        endpoint_defaults = {
            'SalesOrders': 15, 'StockOnHand': 15, 'Products': 10,
            'Invoices': 25, 'Customers': 30, 'CreditNotes': 20, 'PurchaseOrders': 20
        }
        default_chunk_size = endpoint_defaults.get(endpoint, 15)
        chunk_size = int(params.pop('chunkSize', default_chunk_size))

        # The remaining params are the user's filters (e.g., orderDate, orderStatus)
        filter_query_string = '&'.join([f"{k}={v}" for k, v in params.items()])
        
        logging.info(f"Endpoint: {endpoint}, Start Page: {start_page}, Chunk Size: {chunk_size}, Filters: '{filter_query_string}'")
        return get_chunked_pages(endpoint, filter_query_string, api_id, api_key, start_page, chunk_size)
        
    except Exception as e:
        error_msg = f"Error processing chunked {endpoint} request: {str(e)}"
        logging.error(error_msg, exc_info=True)
        return func.HttpResponse(error_msg, status_code=500)

def get_chunked_pages(endpoint: str, query_string: str, api_id: str, api_key: str, start_page: int, chunk_size: int) -> func.HttpResponse:
    """Get a chunk of pages for very large datasets with conservative timeout management"""
    try:
        all_items = []
        requested_pages_in_chunk = [] # DEBUG: list to store page numbers we request
        start_time = time.time()
        
        base_url = f"https://api.unleashedsoftware.com/{endpoint}"
        
        # --- Get Metadata First ---
        # We need the total number of pages to calculate the correct range for this chunk.
        # Always add pageSize=1 to the metadata call to minimize data transfer.
        meta_query = f"{query_string}&pageSize=1" if query_string else "pageSize=1"
        meta_signature = generate_signature(meta_query, api_key)
        meta_headers = {'Accept': 'application/json', 'api-auth-id': api_id, 'api-auth-signature': meta_signature}
        
        meta_response = requests.get(f"{base_url}?{meta_query}", headers=meta_headers, timeout=60)
        if meta_response.status_code != 200:
            return func.HttpResponse(f"Failed to get dataset metadata: {meta_response.text}", status_code=meta_response.status_code)
        
        pagination_info = meta_response.json().get('Pagination', {})
        total_pages = pagination_info.get('NumberOfPages', 0)
        total_items_available = pagination_info.get('NumberOfItems', 0)
        
        if total_pages == 0: # No data to fetch
             return func.HttpResponse(json.dumps({'Items': [], 'ChunkInfo': {'Message': 'No data found for the specified filters.'}}), status_code=200)

        # --- Fetch Pages for this Chunk ---
        # Calculate the real end page for this chunk, ensuring it doesn't exceed the total pages available.
        end_page = min(start_page + chunk_size - 1, total_pages)
        
        for page_number in range(start_page, end_page + 1):
            # Always include pageSize=1000 for maximum efficiency on actual data calls
            page_query_parts = [query_string] if query_string else []
            page_query_parts.append(f"pageSize=1000")
            page_query_parts.append(f"pageNumber={page_number}")
            
            page_query = '&'.join(filter(None, page_query_parts))
            
            signature = generate_signature(page_query, api_key)
            headers = {'Accept': 'application/json', 'api-auth-id': api_id, 'api-auth-signature': signature}
            
            logging.info(f"Requesting page {page_number} of {total_pages}...")
            requested_pages_in_chunk.append(page_number) # DEBUG: Record the page we are requesting

            try:
                response = requests.get(f"{base_url}?{page_query}", headers=headers, timeout=90)
                if response.status_code != 200:
                    logging.error(f"API call failed for page {page_number}: {response.text}")
                    break # Stop this chunk on failure
                
                items = response.json().get('Items', [])
                if not items:
                    break # No more items, end of data
                all_items.extend(items)

            except requests.exceptions.Timeout:
                logging.error(f"Request timeout on page {page_number}")
                break
            except Exception as e:
                logging.error(f"An unexpected error occurred on page {page_number}: {str(e)}")
                break
        
        # --- Prepare Response ---
        has_more_pages = end_page < total_pages
        next_start_page = end_page + 1 if has_more_pages else None
        
        result = {
            'Items': all_items,
            'ChunkInfo': {
                'StartPage': start_page,
                'EndPage': end_page,
                'PagesRequestedInChunk': requested_pages_in_chunk, # DEBUG: The list of pages we actually requested
                'TotalPagesAvailable': total_pages,
                'TotalItemsAvailable': total_items_available,
                'HasMorePages': has_more_pages,
                'NextStartPage': next_start_page
            }
        }
        
        return func.HttpResponse(json.dumps(result), status_code=200, headers={'Content-Type': 'application/json'})
        
    except Exception as e:
        logging.error(f"Critical error in get_chunked_pages: {str(e)}", exc_info=True)
        return func.HttpResponse(f"A critical error occurred: {str(e)}", status_code=500)

def generate_signature(query_string, api_key):
    """Generate HMAC-SHA256 signature for Unleashed API"""
    key_bytes = api_key.encode('utf-8')
    query_bytes = query_string.encode('utf-8')
    signature = hmac.new(key_bytes, query_bytes, hashlib.sha256)
    return base64.b64encode(signature.digest()).decode('utf-8')
