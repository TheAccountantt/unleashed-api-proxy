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

@app.route(route="UnleashedStockOnHand")
def unleashed_stock_on_hand(req: func.HttpRequest) -> func.HttpResponse:
    """Get stock on hand in chunks - recommended for large datasets"""
    return call_unleashed_api_chunked(req, "StockOnHand")

@app.route(route="UnleashedCustomers")
def unleashed_customers(req: func.HttpRequest) -> func.HttpResponse:
    """Get customers in chunks - recommended for large datasets"""
    return call_unleashed_api_chunked(req, "Customers")

@app.route(route="UnleashedInvoices")
def unleashed_invoices(req: func.HttpRequest) -> func.HttpResponse:
    """Get invoices in chunks - recommended for large datasets"""
    return call_unleashed_api_chunked(req, "Invoices")

@app.route(route="UnleashedProducts")
def unleashed_products(req: func.HttpRequest) -> func.HttpResponse:
    """Get products in chunks - recommended for large datasets"""
    return call_unleashed_api_chunked(req, "Products")

@app.route(route="UnleashedSalesOrders")
def unleashed_sales_orders(req: func.HttpRequest) -> func.HttpResponse:
    """Get sales orders in chunks - use startPage parameter for large datasets"""
    return call_unleashed_api_chunked(req, "SalesOrders")

@app.route(route="UnleashedCreditNotes")
def unleashed_credit_notes(req: func.HttpRequest) -> func.HttpResponse:
    """Get credit notes in chunks - recommended for large datasets"""
    return call_unleashed_api_chunked(req, "CreditNotes")

@app.route(route="UnleashedPurchaseOrders")
def unleashed_purchase_orders(req: func.HttpRequest) -> func.HttpResponse:
    """Get purchase orders in chunks - recommended for large datasets"""
    return call_unleashed_api_chunked(req, "PurchaseOrders")

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
        # Get query string from request
        query_string = req.url.split('?', 1)[1] if '?' in req.url else ""
        
        # Remove function-specific parameters
        if 'code=' in query_string:
            params = []
            for param in query_string.split('&'):
                if not param.startswith('code='):
                    params.append(param)
            query_string = '&'.join(params)
        
        # Extract chunk parameters
        start_page = 1
        
        # Endpoint-specific default chunk sizes - optimized for Power BI and gateway timeout avoidance
        endpoint_defaults = {
            'SalesOrders': 15,     # 15,000 records per chunk (15 pages * 1000) - optimized for Power BI reliability
            'StockOnHand': 15,      # Increased from 3 to 15 for consistency, can be adjusted if timeouts occur
            'Products': 10,         # 10,000 records per chunk (slower endpoint)
            'Invoices': 25,         # 25,000 records per chunk
            'Customers': 30,        # 30,000 records per chunk (typically faster)
            'CreditNotes': 20,      # 20,000 records per chunk
            'PurchaseOrders': 20    # 20,000 records per chunk
        }
        
        chunk_size = endpoint_defaults.get(endpoint, 15)  # Default to 15 pages if endpoint not found
        
        # Parse startPage parameter from query string
        if 'startPage=' in query_string:
            for param in query_string.split('&'):
                if param.startswith('startPage='):
                    start_page = int(param.split('=')[1])
                    break
            # Remove startPage from query string for API call
            query_string = '&'.join([p for p in query_string.split('&') if not p.startswith('startPage=')])
        
        # Allow override of default chunk size via query string
        if 'chunkSize=' in query_string:
            for param in query_string.split('&'):
                if param.startswith('chunkSize='):
                    chunk_size = int(param.split('=')[1])
                    break
            # Remove chunkSize from query string for API call
            query_string = '&'.join([p for p in query_string.split('&') if not p.startswith('chunkSize=')])
        
        logging.info(f"Using chunk size: {chunk_size} pages ({chunk_size * 1000} records) for {endpoint}")
        return get_chunked_pages(endpoint, query_string, api_id, api_key, start_page, chunk_size)
        
    except Exception as e:
        error_msg = f"Error processing chunked {endpoint} request: {str(e)}"
        logging.error(error_msg)
        return func.HttpResponse(error_msg, status_code=500)

def get_chunked_pages(endpoint: str, query_string: str, api_id: str, api_key: str, start_page: int, chunk_size: int) -> func.HttpResponse:
    """Get a chunk of pages for very large datasets with conservative timeout management"""
    try:
        all_items = []
        total_items = 0
        start_time = time.time()
        
        # Conservative timeout management for chunked requests
        CHUNK_TIMEOUT = 480  # 8 minutes
        gateway_timeout_protection = 420  # 7 minutes - hard stop to avoid gateway timeouts
        
        # Ensure we use 1000 records per page for maximum efficiency
        if 'pageSize=' not in query_string:
            if query_string:
                query_string += "&pageSize=1000"
            else:
                query_string = "pageSize=1000"
        
        # Calculate the exact page range for this chunk
        end_page = start_page + chunk_size - 1
        
        logging.info(f"Starting chunked pagination for {endpoint}: pages {start_page}-{end_page} (chunk size: {chunk_size})")
        
        # Define base URL for all requests
        base_url = f"https://api.unleashedsoftware.com/{endpoint}"
        
        total_pages = None
        total_available = None
        
        # Get total pages info first to manage pagination loop effectively
        # This call also helps verify the query and credentials before starting the main loop
        info_query = f"{query_string}&pageNumber=1" if query_string else "pageSize=1000&pageNumber=1"
        info_signature = generate_signature(info_query, api_key)
        info_headers = {
            'Accept': 'application/json', 'api-auth-id': api_id,
            'api-auth-signature': info_signature, 'client-type': 'PowerBI/Integration'
        }
        info_url = f"{base_url}?{info_query}"
        
        try:
            info_response = requests.get(info_url, headers=info_headers, timeout=60)
            if info_response.status_code != 200:
                return func.HttpResponse(
                    f"Failed to get dataset info: {info_response.status_code} - {info_response.text}",
                    status_code=info_response.status_code
                )
            info_data = info_response.json()
            pagination = info_data.get('Pagination', {})
            total_pages = pagination.get('NumberOfPages', 1)
            total_available = pagination.get('NumberOfItems', 0)
            logging.info(f"Dataset info: {total_available} items across {total_pages} pages")
        except requests.exceptions.Timeout:
            return func.HttpResponse("Timeout when getting dataset info. The API may be slow.", status_code=408)
        except json.JSONDecodeError as e:
            return func.HttpResponse(f"Invalid JSON response when getting dataset info: {str(e)}", status_code=500)

        # The actual last page we should fetch in this chunk
        actual_end_page = min(end_page, total_pages)
        
        headers = {'Accept': 'application/json', 'api-auth-id': api_id, 'api-auth-signature': '', 'client-type': 'PowerBI/Integration'}
        
        # Fetch each page in the requested range
        for page_number in range(start_page, actual_end_page + 1):
            elapsed_time = time.time() - start_time
            if elapsed_time > gateway_timeout_protection:
                logging.warning(f"Gateway timeout protection triggered after {elapsed_time:.1f}s at page {page_number-1}")
                break
            
            page_query = f"{query_string}&pageNumber={page_number}" if query_string else f"pageSize=1000&pageNumber={page_number}"
            signature = generate_signature(page_query, api_key)
            headers['api-auth-signature'] = signature
            
            full_url = f"{base_url}?{page_query}"
            
            request_timeout = max(30, min(90, (CHUNK_TIMEOUT - elapsed_time) / 2))
            
            logging.info(f"Fetching page {page_number}/{actual_end_page} (timeout: {request_timeout:.0f}s, elapsed: {elapsed_time:.1f}s)")
            
            try:
                response = requests.get(full_url, headers=headers, timeout=request_timeout)
                if response.status_code != 200:
                    logging.error(f"Page {page_number} failed: {response.status_code} - {response.text}")
                    break
                
                page_data = response.json()
                items = page_data.get('Items', [])
                if not items:
                    logging.info(f"No items on page {page_number}, stopping chunk.")
                    break
                
                all_items.extend(items)
                total_items += len(items)
                
            except requests.exceptions.Timeout:
                logging.error(f"Request timeout on page {page_number} after {request_timeout:.0f}s")
                break
            except json.JSONDecodeError as e:
                logging.error(f"Invalid JSON on page {page_number}: {str(e)}")
                break
            
            # Dynamic delay to be kind to the API
            time.sleep(0.1)
        
        final_elapsed = time.time() - start_time
        pages_retrieved = len(all_items) > 0 and (page_number - start_page + 1) or 0
        has_more_pages = actual_end_page < total_pages
        next_start_page = actual_end_page + 1 if has_more_pages else None
        
        result = {
            'Items': all_items,
            'ChunkInfo': {
                'StartPage': start_page,
                'EndPage': page_number,
                'RequestedChunkSize': chunk_size,
                'PagesRetrievedInChunk': pages_retrieved,
                'ItemsInChunk': len(all_items),
                'HasMorePages': has_more_pages,
                'NextStartPage': next_start_page,
                'TotalPagesAvailable': total_pages,
                'TotalItemsAvailable': total_available,
                'Performance': {
                    'ElapsedTime': f"{final_elapsed:.1f}s",
                    'ChunkTimeoutLimit': f"{CHUNK_TIMEOUT}s"
                }
            },
            'Pagination': { # Included for compatibility with Power BI if it expects this structure
                'NumberOfItems': len(all_items),
                'PageSize': len(all_items),
                'PageNumber': 1,
                'NumberOfPages': 1,
                'ChunkedMode': True
            }
        }
        
        status_msg = f"Chunk complete for {endpoint}: pages {start_page}-{page_number}, {len(all_items)} items in {final_elapsed:.1f}s"
        logging.info(status_msg)
        
        return func.HttpResponse(json.dumps(result), status_code=200, headers={'Content-Type': 'application/json'})
        
    except Exception as e:
        error_msg = f"Chunked pagination error for {endpoint}: {str(e)}"
        logging.error(error_msg, exc_info=True)
        return func.HttpResponse(error_msg, status_code=500)

def generate_signature(query_string, api_key):
    """Generate HMAC-SHA256 signature for Unleashed API"""
    try:
        key_bytes = api_key.encode('utf-8')
        query_bytes = query_string.encode('utf-8')
        signature = hmac.new(key_bytes, query_bytes, hashlib.sha256)
        return base64.b64encode(signature.digest()).decode('utf-8')
    except Exception as e:
        logging.error(f"Error generating signature: {str(e)}")
        raise