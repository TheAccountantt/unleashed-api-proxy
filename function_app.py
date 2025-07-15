import logging
import json
import hmac
import hashlib
import base64
import os
import requests
from urllib.parse import urlparse, parse_qs
import azure.functions as func

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

@app.route(route="UnleashedStockOnHand")
def unleashed_stock_on_hand(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

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
        
        # Remove any function-specific parameters (like 'code=')
        if 'code=' in query_string:
            params = []
            for param in query_string.split('&'):
                if not param.startswith('code='):
                    params.append(param)
            query_string = '&'.join(params)
        
        # Generate HMAC-SHA256 signature
        signature = generate_signature(query_string, api_key)
        
        # Build request URL
        base_url = "https://api.unleashedsoftware.com/StockOnHand"
        full_url = f"{base_url}?{query_string}" if query_string else base_url
        
        logging.info(f"Calling Unleashed API: {full_url}")
        logging.info(f"Signature: {signature}")
        
        # Prepare headers
        headers = {
            'Accept': 'application/json',
            'api-auth-id': api_id,
            'api-auth-signature': signature,
            'client-type': 'PowerBI/Integration'
        }
        
        # Make request to Unleashed API
        response = requests.get(full_url, headers=headers, timeout=30)
        
        if response.status_code == 200:
            return func.HttpResponse(
                response.text,
                status_code=200,
                headers={'Content-Type': 'application/json'}
            )
        else:
            error_msg = f"API call failed: {response.status_code} - {response.text}"
            logging.error(error_msg)
            return func.HttpResponse(
                error_msg,
                status_code=response.status_code
            )
            
    except Exception as e:
        error_msg = f"Error processing request: {str(e)}"
        logging.error(error_msg)
        return func.HttpResponse(
            error_msg,
            status_code=500
        )

def generate_signature(query_string, api_key):
    """Generate HMAC-SHA256 signature for Unleashed API"""
    # Convert strings to bytes
    key_bytes = api_key.encode('utf-8')
    query_bytes = query_string.encode('utf-8')
    
    # Generate HMAC-SHA256
    signature = hmac.new(key_bytes, query_bytes, hashlib.sha256)
    
    # Return base64 encoded signature
    return base64.b64encode(signature.digest()).decode('utf-8')
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

def call_unleashed_api(req: func.HttpRequest, endpoint: str) -> func.HttpResponse:
    """Generic function to call any Unleashed API endpoint"""
    logging.info(f'Calling Unleashed {endpoint} API')

    # Get API credentials from environment variables
    api_id = os.environ.get('UNLEASHED_API_ID')
    api_key = os.environ.get('UNLEASHED_API_KEY')
    
    if not api_id or not api_key:
        return func.HttpResponse(
            "API credentials not configured.",
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
        
        # Generate signature
        signature = generate_signature(query_string, api_key)
        
        # Build URL
        base_url = f"https://api.unleashedsoftware.com/{endpoint}"
        full_url = f"{base_url}?{query_string}" if query_string else base_url
        
        logging.info(f"Calling: {full_url}")
        
        # Headers
        headers = {
            'Accept': 'application/json',
            'api-auth-id': api_id,
            'api-auth-signature': signature,
            'client-type': 'PowerBI/Integration'
        }
        
        # Make request
        response = requests.get(full_url, headers=headers, timeout=30)
        
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
        error_msg = f"Error: {str(e)}"
        logging.error(error_msg)
        return func.HttpResponse(error_msg, status_code=500)