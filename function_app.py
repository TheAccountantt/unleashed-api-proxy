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
    """Get all pages of data with automatic pagination"""
    try:
        all_items = []
        page_number = 1
        max_pages = 500  # Safety limit (500,000 records max at 1000 per page)
        total_items = 0
        
        # Ensure we use 1000 records per page for efficiency
        if 'pageSize=' not in query_string:
            if query_string:
                query_string += "&pageSize=1000"
            else:
                query_string = "pageSize=1000"
        
        logging.info(f"Starting auto-pagination for {endpoint} with query: {query_string}")
        
        while page_number <= max_pages:
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
            
            logging.info(f"Fetching page {page_number}: {full_url}")
            
            # Adaptive timeout based on page number
            request_timeout = min(60 + (page_number * 2), 120)  # 60-120 seconds
            
            response = requests.get(full_url, headers=headers, timeout=request_timeout)
            
            if response.status_code != 200:
                error_msg = f"Page {page_number} failed: {response.status_code} - {response.text}"
                logging.error(error_msg)
                
                # If it's the first page, return the error
                if page_number == 1:
                    return func.HttpResponse(error_msg, status_code=response.status_code)
                else:
                    # If later pages fail, return what we have so far
                    logging.warning(f"Stopping pagination at page {page_number-1} due to error")
                    break
            
            try:
                page_data = response.json()
            except json.JSONDecodeError as e:
                error_msg = f"Invalid JSON response on page {page_number}: {str(e)}"
                logging.error(error_msg)
                if page_number == 1:
                    return func.HttpResponse(error_msg, status_code=500)
                else:
                    break
            
            items = page_data.get('Items', [])
            
            if not items:
                logging.info(f"No items found on page {page_number}, stopping pagination")
                break
            
            all_items.extend(items)
            current_page_count = len(items)
            total_items += current_page_count
            
            logging.info(f"Page {page_number}: Retrieved {current_page_count} items (Total: {total_items})")
            
            # Check if we've got all pages
            pagination = page_data.get('Pagination', {})
            total_pages = pagination.get('NumberOfPages', 1)
            
            # Log pagination info on first page
            if page_number == 1:
                total_available = pagination.get('NumberOfItems', 0)
                logging.info(f"Total items available: {total_available}, Total pages: {total_pages}")
            
            if page_number >= total_pages:
                logging.info(f"Completed pagination: {page_number}/{total_pages} pages")
                break
            
            page_number += 1
            
            # Add small delay to avoid overwhelming the API
            time.sleep(0.1)
        
        # Return combined results
        result = {
            'Items': all_items,
            'Pagination': {
                'NumberOfItems': len(all_items),
                'PageSize': len(all_items),
                'PageNumber': 1,
                'NumberOfPages': 1,
                'AutoPaginated': True,
                'TotalPagesRetrieved': page_number - 1
            }
        }
        
        logging.info(f"Auto-pagination complete for {endpoint}: {len(all_items)} total items retrieved")
        
        return func.HttpResponse(
            json.dumps(result),
            status_code=200,
            headers={'Content-Type': 'application/json'}
        )
        
    except Exception as e:
        error_msg = f"Auto-pagination error for {endpoint}: {str(e)}"
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