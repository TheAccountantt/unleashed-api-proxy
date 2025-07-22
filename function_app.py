import logging
import json
import hmac
import hashlib
import base64
import os
import requests
import time
import azure.functions as func

# Create the FunctionApp with Function-level auth (requires ?code=…)
app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

# Whitelisted query-string filters for the large endpoints
VALID_FILTERS = {
    "SalesOrders": {
        "startDate", "endDate",
        "completedAfter", "completedBefore",
        "modifiedSince",
        "customerCode", "customerId",
        "orderNumber", "orderStatus",
        "serialBatch", "warehouseCode",
        "sourceId", "pageSize"
    },
    "Invoices": {
        "customerCode", "startDate", "endDate",
        "modifiedSince",
        "invoiceNumber", "invoiceStatus",
        "orderNumber", "serialBatch",
        "pageSize"
    }
}

# Define your HTTP-triggered routes
@app.route(route="UnleashedStockOnHand")
def unleashed_stock_on_hand(req: func.HttpRequest) -> func.HttpResponse:
    return call_unleashed_api(req, "StockOnHand")

@app.route(route="UnleashedCustomers")
def unleashed_customers(req: func.HttpRequest) -> func.HttpResponse:
    return call_unleashed_api(req, "Customers")

@app.route(route="UnleashedProducts")
def unleashed_products(req: func.HttpRequest) -> func.HttpResponse:
    return call_unleashed_api(req, "Products")

@app.route(route="UnleashedSalesOrders")
def unleashed_sales_orders(req: func.HttpRequest) -> func.HttpResponse:
    return call_unleashed_api(req, "SalesOrders")

@app.route(route="UnleashedInvoices")
def unleashed_invoices(req: func.HttpRequest) -> func.HttpResponse:
    return call_unleashed_api(req, "Invoices")

@app.route(route="UnleashedCreditNotes")
def unleashed_credit_notes(req: func.HttpRequest) -> func.HttpResponse:
    return call_unleashed_api(req, "CreditNotes")

@app.route(route="UnleashedPurchaseOrders")
def unleashed_purchase_orders(req: func.HttpRequest) -> func.HttpResponse:
    return call_unleashed_api(req, "PurchaseOrders")


def generate_signature(api_key: str, query_string: str) -> str:
    """Generate the HMAC-SHA256 signature per Unleashed API spec."""
    message = query_string.encode("utf-8")
    secret = api_key.encode("utf-8")
    digest = hmac.new(secret, message, hashlib.sha256).digest()
    return base64.b64encode(digest).decode()


def flatten_sales_orders(orders: list) -> list:
    """Flatten SalesOrders into one record per SalesOrderLine."""
    flat = []
    for order in orders:
        header = {k: v for k, v in order.items() if k != "SalesOrderLines"}
        for line in order.get("SalesOrderLines", []):
            rec = header.copy()
            rec.update(line)
            flat.append(rec)
    logging.info(f"Flattened {len(orders)} orders → {len(flat)} lines.")
    return flat


def flatten_sales_invoices(invoices: list) -> list:
    """Flatten Invoices into one record per InvoiceLine."""
    flat = []
    for inv in invoices:
        header = {k: v for k, v in inv.items() if k != "InvoiceLines"}
        for line in inv.get("InvoiceLines", []):
            rec = header.copy()
            rec.update(line)
            flat.append(rec)
    logging.info(f"Flattened {len(invoices)} invoices → {len(flat)} lines.")
    return flat


def call_unleashed_api(req: func.HttpRequest, endpoint: str) -> func.HttpResponse:
    """
    1. Strip Azure ?code=… param
    2. Whitelist only valid filters
    3. Page through every /{page} of the Unleashed API
    4. Flatten SalesOrders & Invoices
    5. Return JSON {"Items": […]}
    """
    logging.info(f"Entering call_unleashed_api for {endpoint}")
    api_id = os.getenv("UNLEASHED_API_ID")
    api_key = os.getenv("UNLEASHED_API_KEY")
    if not api_id or not api_key:
        return func.HttpResponse(
            "Missing UNLEASHED_API_ID or UNLEASHED_API_KEY",
            status_code=400
        )

    # 1. Prepare filters
    raw = req.params.copy()
    raw.pop("code", None)  # drop Azure function key
    filters = {
        k: v
        for k, v in raw.items()
        if endpoint not in VALID_FILTERS or k in VALID_FILTERS[endpoint]
    }
    # always request max pageSize
    if "pageSize" not in filters:
        filters["pageSize"] = "1000"

    # Build the sorted query string for signing
    qs = "&".join(f"{k}={filters[k]}" for k in sorted(filters))

    # 2. Pagination loop
    all_items = []
    page = 1
    base_url = f"https://api.unleashedsoftware.com/{endpoint}"
    while True:
        sig = generate_signature(api_key, qs)
        headers = {
            "api-auth-id": api_id,
            "api-auth-signature": sig,
            "Accept": "application/json"
        }
        url = f"{base_url}/{page}" if page > 1 else base_url
        resp = requests.get(url, headers=headers, params=filters, timeout=180)
        if resp.status_code != 200:
            logging.error(f"{endpoint} page {page} failed: {resp.status_code} {resp.text}")
            return func.HttpResponse(
                f"Error fetching {endpoint} page {page}: {resp.text}",
                status_code=resp.status_code
            )
        data = resp.json()
        items = data.get("Items", [])
        if not items:
            break
        all_items.extend(items)
        if len(items) < int(filters["pageSize"]):
            break
        page += 1
        time.sleep(0.1)

    # 3. Flatten large collections
    result = all_items
    if endpoint == "SalesOrders":
        result = flatten_sales_orders(all_items)
    elif endpoint == "Invoices":
        result = flatten_sales_invoices(all_items)

    # 4. Return
    payload = {"Items": result}
    return func.HttpResponse(
        json.dumps(payload),
        status_code=200,
        headers={"Content-Type": "application/json"}
    )
