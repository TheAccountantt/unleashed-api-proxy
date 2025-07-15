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