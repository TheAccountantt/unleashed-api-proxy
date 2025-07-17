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
    return call_unleashed_api(req, "StockOnHand")

@app.route(route="UnleashedStockOnHandChunked")
def unleashed_stock_on_hand_chunked(req: func.HttpRequest) -> func.HttpResponse:
    """Get stock on hand in chunks - recommended for large datasets"""
    return call_unleashed_api_chunked(req, "StockOnHand")

@app.route(route="UnleashedCustomers")
def unleashed_customers(req: func.HttpRequest) -> func.HttpResponse:
    return call_unleashed_api(req, "Customers")

@app.route(route="UnleashedCustomersChunked")
def unleashed_customers_chunked(req: func.HttpRequest) -> func.HttpResponse:
    """Get customers in chunks - recommended for large datasets"""
    return call_unleashed_api_chunked(req, "Customers")

@app.route(route="UnleashedInvoices")
def unleashed_invoices(req: func.HttpRequest) -> func.HttpResponse:
    return call_unleashed_api(req, "Invoices")

@app.route(route="UnleashedInvoicesChunked")
def unleashed_invoices_chunked(req: func.HttpRequest) -> func.HttpResponse:
    """Get invoices in chunks - recommended for large datasets"""
    return call_unleashed_api_chunked(req, "Invoices")

@app.route(route="UnleashedProducts")
def unleashed_products(req: func.HttpRequest) -> func.HttpResponse:
    return call_unleashed_api(req, "Products")

@app.route(route="UnleashedProductsChunked")
def unleashed_products_chunked(req: func.HttpRequest) -> func.HttpResponse:
    """Get products in chunks - recommended for large datasets"""
    return call_unleashed_api_chunked(req, "Products")

@app.route(route="UnleashedSalesOrders")
def unleashed_sales_orders(req: func.HttpRequest) -> func.HttpResponse:
    return call_unleashed_api(req, "SalesOrders")

@app.route(route="UnleashedSalesOrdersChunked")
def unleashed_sales_orders_chunked(req: func.HttpRequest) -> func.HttpResponse:
    """Get sales orders in chunks - use startPage parameter for large datasets"""
    return call_unleashed_api_chunked(req, "SalesOrders")

@app.route(route="UnleashedCreditNotes")
def unleashed_credit_notes(req: func.HttpRequest) -> func.HttpResponse:
    return call_unleashed_api(req, "CreditNotes")

@app.route(route="UnleashedCreditNotesChunked")
def unleashed_credit_notes_chunked(req: func.HttpRequest) -> func.HttpResponse:
    """Get credit notes in chunks - recommended for large datasets"""
    return call_unleashed_api_chunked(req, "CreditNotes")

@app.route(route="UnleashedPurchaseOrders")
def unleashed_purchase_orders(req: func.HttpRequest) -> func.HttpResponse:
    return call_unleashed_api(req, "PurchaseOrders")

@app.route(route="UnleashedPurchaseOrdersChunked")
def unleashed_purchase_orders_chunked(req: func.HttpRequest) -> func.HttpResponse:
    """Get purchase orders in chunks - recommended for large datasets"""
    return call_unleashed_api_chunked(req, "PurchaseOrders")

@app.route(route="UnleashedDatasetInfo")
def unleashed_dataset_info(req: func.HttpRequest) -> func.HttpResponse:
    """Get information about dataset sizes and recommended chunking strategies"""
    
    # Get the endpoint parameter
    endpoint = req.params.get('endpoint', 'SalesOrders')
    
    # Endpoint information and recommendations
    endpoint_info = {
        'SalesOrders': {
            'description': 'Sales orders dataset',
            'recommendedChunkSize': 15,
            'estimatedItemsPerChunk': 15000,
            'chunkedEndpoint': '/UnleashedSalesOrdersChunked',
            'usage': [
                '1. First call: /UnleashedSalesOrdersChunked',
                '2. Check ChunkInfo.HasMorePages in response',
                '3. If true, call: /UnleashedSalesOrdersChunked?startPage={NextStartPage}',
                '4. Repeat until HasMorePages = false'
            ]
        },
        'StockOnHand': {
            'description': 'Stock on hand dataset (slower API)',
            'recommendedChunkSize': 15,
            'estimatedItemsPerChunk': 15000,
            'chunkedEndpoint': '/UnleashedStockOnHandChunked',
            'usage': [
                '1. First call: /UnleashedStockOnHandChunked',
                '2. Check ChunkInfo.HasMorePages in response',
                '3. If true, call: /UnleashedStockOnHandChunked?startPage={NextStartPage}',
                '4. Repeat until HasMorePages = false'
            ]
        },
        'Products': {
            'description': 'Products dataset (slower API)',
            'recommendedChunkSize': 10,
            'estimatedItemsPerChunk': 10000,
            'chunkedEndpoint': '/UnleashedProductsChunked',
            'usage': [
                '1. First call: /UnleashedProductsChunked',
                '2. Check ChunkInfo.HasMorePages in response',
                '3. If true, call: /UnleashedProductsChunked?startPage={NextStartPage}',
                '4. Repeat until HasMorePages = false'
            ]
        },
        'Customers': {
            'description': 'Customers dataset',
            'recommendedChunkSize': 30,
            'estimatedItemsPerChunk': 30000,
            'chunkedEndpoint': '/UnleashedCustomersChunked',
            'usage': [
                '1. First call: /UnleashedCustomersChunked',
                '2. Check ChunkInfo.HasMorePages in response',
                '3. If true, call: /UnleashedCustomersChunked?startPage={NextStartPage}',
                '4. Repeat until HasMorePages = false'
            ]
        },
        'Invoices': {
            'description': 'Invoices dataset',
            'recommendedChunkSize': 25,
            'estimatedItemsPerChunk': 25000,
            'chunkedEndpoint': '/UnleashedInvoicesChunked',
            'usage': [
                '1. First call: /UnleashedInvoicesChunked',
                '2. Check ChunkInfo.HasMorePages in response',
                '3. If true, call: /UnleashedInvoicesChunked?startPage={NextStartPage}',
                '4. Repeat until HasMorePages = false'
            ]
        },
        'CreditNotes': {
            'description': 'Credit notes dataset',
            'recommendedChunkSize': 20,
            'estimatedItemsPerChunk': 20000,
            'chunkedEndpoint': '/UnleashedCreditNotesChunked',
            'usage': [
                '1. First call: /UnleashedCreditNotesChunked',
                '2. Check ChunkInfo.HasMorePages in response',
                '3. If true, call: /UnleashedCreditNotesChunked?startPage={NextStartPage}',
                '4. Repeat until HasMorePages = false'
            ]
        },
        'PurchaseOrders': {
            'description': 'Purchase orders dataset',
            'recommendedChunkSize': 20,
            'estimatedItemsPerChunk': 20000,
            'chunkedEndpoint': '/UnleashedPurchaseOrdersChunked',
            'usage': [
                '1. First call: /UnleashedPurchaseOrdersChunked',
                '2. Check ChunkInfo.HasMorePages in response',
                '3. If true, call: /UnleashedPurchaseOrdersChunked?startPage={NextStartPage}',
                '4. Repeat until HasMorePages = false'
            ]
        }
    }
    
    info = endpoint_info.get(endpoint, {
        'description': f'{endpoint} dataset',
        'recommendedChunkSize': 15,
        'estimatedItemsPerChunk': 15000,
        'chunkedEndpoint': f'/Unleashed{endpoint}Chunked',
        'usage': [
            f'1. First call: /Unleashed{endpoint}Chunked',
            '2. Check ChunkInfo.HasMorePages in response',
            f'3. If true, call: /Unleashed{endpoint}Chunked?startPage={{NextStartPage}}',
            '4. Repeat until HasMorePages = false'
        ]
    })
    
    result = {
        'endpoint': endpoint,
        'recommendation': 'Use chunked endpoints for complete datasets',
        'info': info,
        'generalGuidance': {
            'forCompleteDatasets': 'Always use *Chunked endpoints for datasets > 50,000 records',
            'chunkSizeCustomization': 'Add ?chunkSize=N to override default chunk sizes',
            'dateFiltering': 'Use modifiedSince or date filters to reduce dataset size when possible',
            'monitoringLogs': 'Check Azure Function logs for timeout warnings and truncation info'
        },
        'exampleWorkflow': {
            'step1': f'GET {info["chunkedEndpoint"]}',
            'step2': 'Parse response.ChunkInfo.HasMorePages',
            'step3': f'If true: GET {info["chunkedEndpoint"]}?startPage={{response.ChunkInfo.NextStartPage}}',
            'step4': 'Repeat step 3 until HasMorePages = false',
            'step5': 'Combine all Items arrays from each response'
        }
    }
    
    return func.HttpResponse(
        json.dumps(result, indent=2),
        status_code=200,
        headers={'Content-Type': 'application/json'}
    )

def call_unleashed_api(req: func.HttpRequest, endpoint: str) -> func.HttpResponse:
    """Auto-paginating function that gets all data for any Unleashed API endpoint"""
    logging.info(f'Calling Unleashed {endpoint} API with auto-pagination')

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
        
        # Remove function-specific parameters (like Azure's 'code' parameter)
        if 'code=' in query_string:
            params = []
            for param in query_string.split('&'):
                if not param.startswith('code='):
                    params.append(param)
            query_string = '&'.join(params)
        
        # Check for single page request (opt-out of auto-pagination)
        single_page = 'singlePage=true' in query_string
        if single_page:
            query_string = query_string.replace('singlePage=true&', '').replace('&singlePage=true', '').replace('singlePage=true', '')
            return get_single_page(endpoint, query_string, api_id, api_key)
        
        # Default to auto-pagination for all requests
        return get_all_pages(endpoint, query_string, api_id, api_key)
        
    except Exception as e:
        error_msg = f"Error processing {endpoint} request: {str(e)}"
        logging.error(error_msg)
        return func.HttpResponse(error_msg, status_code=500)

def get_single_page(endpoint: str, query_string: str, api_id: str, api_key: str) -> func.HttpResponse:
    """Get single page of data (for testing or when specifically requested)"""
    try:
        # Default to 1000 records if no pageSize specified
        if not query_string:
            query_string = "pageSize=1000"
        elif 'pageSize=' not in query_string:
            query_string += "&pageSize=1000"
        
        signature = generate_signature(query_string, api_key)
        
        base_url = f"https://api.unleashedsoftware.com/{endpoint}"
        full_url = f"{base_url}?{query_string}"
        
        logging.info(f"Single page request: {full_url}")
        
        headers = {
            'Accept': 'application/json',
            'api-auth-id': api_id,
            'api-auth-signature': signature,
            'client-type': 'PowerBI/Integration'
        }
        
        response = requests.get(full_url, headers=headers, timeout=60)
        
        if response.status_code == 200:
            return func.HttpResponse(
                response.text,
                status_code=200,
                headers={'Content-Type': 'application/json'}
            )
        else:
            error_msg = f"API call failed: {response.status_code} - {response.text}"
            logging.error(error_msg)
            return func.HttpResponse(error_msg, status_code=response.status_code)
            
    except Exception as e:
        error_msg = f"Single page error: {str(e)}"
        logging.error(error_msg)
        return func.HttpResponse(error_msg, status_code=500)

def get_all_pages(endpoint: str, query_string: str, api_id: str, api_key: str) -> func.HttpResponse:
    """Get all pages of data with automatic pagination and intelligent timeout management"""
    try:
        all_items = []
        page_number = 1
        total_items = 0
        start_time = time.time()
        
        # Azure Functions timeout limits (in seconds)
        # Consumption plan: 5 minutes (300s) default, 10 minutes (600s) max
        # Premium plan: 30 minutes (1800s) max
        AZURE_TIMEOUT_BUFFER = 30  # Keep 30 seconds buffer
        FUNCTION_TIMEOUT = 570  # 9.5 minutes (600s - 30s buffer)
        
        # Ensure we use 1000 records per page for maximum efficiency
        if 'pageSize=' not in query_string:
            if query_string:
                query_string += "&pageSize=1000"
            else:
                query_string = "pageSize=1000"
        
        logging.info(f"Starting auto-pagination for {endpoint} with query: {query_string}")
        logging.info(f"Function timeout limit: {FUNCTION_TIMEOUT}s")
        
        # Track performance metrics
        total_api_time = 0
        
        while True:  # No artificial page limit - let the API or timeout decide
            # Check if we're approaching the function timeout
            elapsed_time = time.time() - start_time
            remaining_time = FUNCTION_TIMEOUT - elapsed_time
            
            if remaining_time < 60:  # Less than 1 minute remaining
                logging.warning(f"Approaching timeout limit. Elapsed: {elapsed_time:.1f}s, Remaining: {remaining_time:.1f}s")
                logging.warning(f"Stopping pagination at page {page_number-1} to prevent timeout")
                break
            
            # Build query with pagination
            if query_string:
                page_query = f"{query_string}&pageNumber={page_number}"
            else:
                page_query = f"pageSize=1000&pageNumber={page_number}"
            
            signature = generate_signature(page_query, api_key)
            
            base_url = f"https://api.unleashedsoftware.com/{endpoint}"
            full_url = f"{base_url}?{page_query}"
            
            headers = {
                'Accept': 'application/json',
                'api-auth-id': api_id,
                'api-auth-signature': signature,
                'client-type': 'PowerBI/Integration'
            }
            
            # Calculate dynamic timeout for this request
            # Give each request up to 1/3 of remaining time, but min 30s, max 120s
            request_timeout = max(30, min(120, remaining_time / 3))
            
            logging.info(f"Fetching page {page_number}: timeout={request_timeout:.0f}s, remaining_func_time={remaining_time:.1f}s")
            
            api_start = time.time()
            
            try:
                response = requests.get(full_url, headers=headers, timeout=request_timeout)
                api_time = time.time() - api_start
                total_api_time += api_time
                
            except requests.exceptions.Timeout:
                logging.error(f"Request timeout on page {page_number} after {request_timeout}s")
                if page_number == 1:
                    return func.HttpResponse(
                        f"API request timeout on first page. The API may be slow or overloaded.",
                        status_code=408
                    )
                else:
                    logging.warning(f"Stopping pagination at page {page_number-1} due to request timeout")
                    break
            
            if response.status_code != 200:
                error_msg = f"Page {page_number} failed: {response.status_code} - {response.text}"
                logging.error(error_msg)
                
                # If it's the first page, return the error
                if page_number == 1:
                    return func.HttpResponse(error_msg, status_code=response.status_code)
                else:
                    # If later pages fail, return what we have so far
                    logging.warning(f"Stopping pagination at page {page_number-1} due to API error")
                    break
            
            try:
                page_data = response.json()
            except json.JSONDecodeError as e:
                error_msg = f"Invalid JSON response on page {page_number}: {str(e)}"
                logging.error(error_msg)
                if page_number == 1:
                    return func.HttpResponse(error_msg, status_code=500)
                else:
                    logging.warning(f"Stopping pagination at page {page_number-1} due to JSON error")
                    break
            
            items = page_data.get('Items', [])
            
            if not items:
                logging.info(f"No items found on page {page_number}, pagination complete")
                break
            
            all_items.extend(items)
            current_page_count = len(items)
            total_items += current_page_count
            
            # Calculate performance stats
            avg_api_time = total_api_time / page_number
            estimated_time_per_page = avg_api_time + 0.2  # Add processing overhead
            
            logging.info(f"Page {page_number}: {current_page_count} items (Total: {total_items}) - "
                        f"API time: {api_time:.1f}s, Avg: {avg_api_time:.1f}s, Elapsed: {elapsed_time:.1f}s")
            
            # Check if we've got all pages according to API
            pagination = page_data.get('Pagination', {})
            total_pages = pagination.get('NumberOfPages', 1)
            
            # Log pagination info on first page
            if page_number == 1:
                total_available = pagination.get('NumberOfItems', 0)
                estimated_total_time = total_pages * estimated_time_per_page
                
                logging.info(f"Dataset info: {total_available} items across {total_pages} pages")
                logging.info(f"Estimated completion time: {estimated_total_time:.1f}s")
                
                if estimated_total_time > FUNCTION_TIMEOUT:
                    logging.warning(f"Estimated time ({estimated_total_time:.1f}s) exceeds timeout limit ({FUNCTION_TIMEOUT}s)")
                    logging.warning("Will fetch as many pages as possible within timeout")
            
            if page_number >= total_pages:
                logging.info(f"Pagination complete: {page_number}/{total_pages} pages retrieved")
                break
            
            page_number += 1
            
            # Dynamic delay based on API performance
            # Faster APIs get shorter delays, slower APIs get longer delays
            if avg_api_time < 1.0:
                delay = 0.1
            elif avg_api_time < 3.0:
                delay = 0.2
            else:
                delay = 0.5
                
            time.sleep(delay)
        
        # Calculate final statistics
        final_elapsed = time.time() - start_time
        was_truncated = page_number < total_pages if 'total_pages' in locals() else False
        
        # Return combined results with comprehensive metadata
        result = {
            'Items': all_items,
            'Pagination': {
                'NumberOfItems': len(all_items),
                'PageSize': len(all_items),
                'PageNumber': 1,
                'NumberOfPages': 1,
                'AutoPaginated': True,
                'PagesRetrieved': page_number - 1,
                'TotalPagesAvailable': total_pages if 'total_pages' in locals() else page_number - 1,
                'Truncated': was_truncated,
                'Performance': {
                    'ElapsedTime': f"{final_elapsed:.1f}s",
                    'TotalApiTime': f"{total_api_time:.1f}s",
                    'AverageApiTimePerPage': f"{total_api_time / max(1, page_number - 1):.1f}s",
                    'FunctionTimeoutLimit': f"{FUNCTION_TIMEOUT}s"
                }
            }
        }
        
        status_msg = f"Auto-pagination for {endpoint}: {len(all_items)} items retrieved in {final_elapsed:.1f}s"
        if was_truncated:
            status_msg += f" (TRUNCATED: {page_number-1}/{total_pages} pages due to timeout protection)"
        
        logging.info(status_msg)
        
        return func.HttpResponse(
            json.dumps(result),
            status_code=200,
            headers={'Content-Type': 'application/json'}
        )
        
    except Exception as e:
        error_msg = f"Auto-pagination error for {endpoint}: {str(e)}"
        logging.error(error_msg)
        return func.HttpResponse(error_msg, status_code=500)

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
            'StockOnHand': 3,      # 3,000 records per chunk (slower endpoint)
            'Products': 2,         # 2,000 records per chunk (slower endpoint)
            'Invoices': 3,         # 3,000 records per chunk
            'Customers': 2,        # 2,000 records per chunk (typically faster)
            'CreditNotes': 2,      # 2,000 records per chunk
            'PurchaseOrders': 2     # 2,000 records per chunk
        }
        
        chunk_size = endpoint_defaults.get(endpoint, 15)  # Default to 15 pages if endpoint not found
        
        # Parse startPage parameter
        if 'startPage=' in query_string:
            for param in query_string.split('&'):
                if param.startswith('startPage='):
                    start_page = int(param.split('=')[1])
                    break
            # Remove startPage from query string
            query_string = '&'.join([p for p in query_string.split('&') if not p.startswith('startPage=')])
        
        # Parse chunkSize parameter (allow override of defaults)
        if 'chunkSize=' in query_string:
            for param in query_string.split('&'):
                if param.startswith('chunkSize='):
                    chunk_size = int(param.split('=')[1])
                    break
            # Remove chunkSize from query string
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
        
        # Gateway timeout protection warning
        if chunk_size > 20 and endpoint in ['SalesOrders', 'StockOnHand', 'Products']:
            logging.warning(f"Large chunk size ({chunk_size}) for {endpoint} - may cause gateway timeout")
        
        # Get total pages info from page 1 (for metadata only)
        info_query = f"{query_string}&pageNumber=1" if query_string else "pageSize=1000&pageNumber=1"
        signature = generate_signature(info_query, api_key)
        
        base_url = f"https://api.unleashedsoftware.com/{endpoint}"
        full_url = f"{base_url}?{info_query}"
        
        headers = {
            'Accept': 'application/json',
            'api-auth-id': api_id,
            'api-auth-signature': signature,
            'client-type': 'PowerBI/Integration'
        }
        
        response = requests.get(full_url, headers=headers, timeout=60)
        
        if response.status_code != 200:
            return func.HttpResponse(
                f"Failed to get dataset info: {response.status_code} - {response.text}",
                status_code=response.status_code
            )
        
        try:
            info_data = response.json()
            pagination = info_data.get('Pagination', {})
            total_pages = pagination.get('NumberOfPages', 1)
            total_available = pagination.get('NumberOfItems', 0)
            logging.info(f"Dataset info: {total_available} items across {total_pages} pages")
        except json.JSONDecodeError as e:
            return func.HttpResponse(f"Invalid JSON response: {str(e)}", status_code=500)
        
        # Adjust end_page if it exceeds total pages
        actual_end_page = min(end_page, total_pages)
        
        # Now fetch the actual pages for this chunk
        headers = {
            'Accept': 'application/json',
            'api-auth-id': api_id,
            'api-auth-signature': '',  # Will be set per request
            'client-type': 'PowerBI/Integration'
        }
        
        # Fetch each page in the requested range
        for page_number in range(start_page, actual_end_page + 1):
            # Check timeouts
            elapsed_time = time.time() - start_time
            if elapsed_time > CHUNK_TIMEOUT:
                logging.warning(f"Chunk timeout reached after {elapsed_time:.1f}s at page {page_number-1}")
                break
            
            if elapsed_time > gateway_timeout_protection:
                logging.warning(f"Gateway timeout protection triggered after {elapsed_time:.1f}s at page {page_number-1}")
                break
            
            # Build the query for this specific page
            page_query = f"{query_string}&pageNumber={page_number}" if query_string else f"pageSize=1000&pageNumber={page_number}"
            signature = generate_signature(page_query, api_key)
            headers['api-auth-signature'] = signature
            
            full_url = f"{base_url}?{page_query}"
            
            # Calculate timeout for this request
            remaining_time = CHUNK_TIMEOUT - elapsed_time
            request_timeout = max(30, min(90, remaining_time / 3))
            
            logging.info(f"Fetching page {page_number}/{actual_end_page} (timeout: {request_timeout:.0f}s, elapsed: {elapsed_time:.1f}s)")
            
            try:
                response = requests.get(full_url, headers=headers, timeout=request_timeout)
            except requests.exceptions.Timeout:
                logging.error(f"Request timeout on page {page_number} after {request_timeout}s")
                break
            
            if response.status_code != 200:
                error_msg = f"Page {page_number} failed: {response.status_code} - {response.text}"
                logging.error(error_msg)
                break
            
            try:
                page_data = response.json()
            except json.JSONDecodeError as e:
                logging.error(f"Invalid JSON on page {page_number}: {str(e)}")
                break
            
            items = page_data.get('Items', [])
            if not items:
                logging.info(f"No items on page {page_number}, stopping")
                break
            
            all_items.extend(items)
            total_items += len(items)
            logging.info(f"Page {page_number}: {len(items)} items (Total: {total_items})")
            
            # Endpoint-specific delays
            if endpoint in ['StockOnHand', 'Products']:
                time.sleep(0.2)
            elif endpoint in ['SalesOrders']:
                time.sleep(0.05)
            else:
                time.sleep(0.1)
        
        # Calculate final chunk information
        pages_retrieved = len(range(start_page, min(page_number, actual_end_page + 1)))
        has_more_pages = actual_end_page < total_pages
        next_start_page = actual_end_page + 1 if has_more_pages else None
        final_elapsed = time.time() - start_time
        
        # Return chunk results
        result = {
            'Items': all_items,
            'ChunkInfo': {
                'StartPage': start_page,
                'EndPage': min(page_number - 1, actual_end_page) if 'page_number' in locals() else actual_end_page,
                'RequestedChunkSize': chunk_size,
                'PagesRetrieved': pages_retrieved,
                'ItemsInChunk': len(all_items),
                'HasMorePages': has_more_pages,
                'NextStartPage': next_start_page,
                'TotalPages': total_pages,
                'TotalItemsAvailable': total_available,
                'Performance': {
                    'ElapsedTime': f"{final_elapsed:.1f}s",
                    'ChunkTimeoutLimit': f"{CHUNK_TIMEOUT}s",
                    'GatewayTimeoutProtection': f"{gateway_timeout_protection}s",
                    'TimedOut': final_elapsed > CHUNK_TIMEOUT,
                    'GatewayTimeoutProtectionTriggered': final_elapsed > gateway_timeout_protection
                }
            },
            'Pagination': {
                'NumberOfItems': len(all_items),
                'PageSize': len(all_items),
                'PageNumber': 1,
                'NumberOfPages': 1,
                'AutoPaginated': True,
                'ChunkedMode': True
            }
        }
        
        status_msg = f"Chunk complete for {endpoint}: pages {start_page}-{result['ChunkInfo']['EndPage']}, {len(all_items)} items in {final_elapsed:.1f}s"
        if has_more_pages:
            status_msg += f" (MORE DATA AVAILABLE - use startPage={next_start_page})"
        
        logging.info(status_msg)
        
        return func.HttpResponse(
            json.dumps(result),
            status_code=200,
            headers={'Content-Type': 'application/json'}
        )
        
    except Exception as e:
        error_msg = f"Chunked pagination error for {endpoint}: {str(e)}"
        logging.error(error_msg)
        return func.HttpResponse(error_msg, status_code=500)

def generate_signature(query_string, api_key):
    """Generate HMAC-SHA256 signature for Unleashed API"""
    try:
        # Convert strings to bytes
        key_bytes = api_key.encode('utf-8')
        query_bytes = query_string.encode('utf-8')
        
        # Generate HMAC-SHA256
        signature = hmac.new(key_bytes, query_bytes, hashlib.sha256)
        
        # Return base64 encoded signature
        return base64.b64encode(signature.digest()).decode('utf-8')
        
    except Exception as e:
        logging.error(f"Error generating signature: {str(e)}")
        raise