# Unleashed API Proxy

An Azure Function App that sits between the Unleashed Software API
and Power BI, handling pagination and caching so reports pull large
datasets without timing out.

## Why

Unleashed's API returns paged results. Power BI's native web
connector handles this badly on large endpoints, so this proxy walks
the pages server-side, assembles the full response, and caches it in
blob storage.

## What it does

- Signs requests using Unleashed's HMAC-SHA256 scheme
- Walks all pages for an endpoint, or returns a single page on request
- Caches responses in Azure blob storage with a configurable TTL
- Supports a second set of credentials for an EU region

## Configuration

Set as application settings in the Function App:

| Setting | Purpose |
|---|---|
| `UNLEASHED_API_ID` | Unleashed API ID |
| `UNLEASHED_API_KEY` | Unleashed API key |
| `UNLEASHED_API_ID_EU` | API ID for the EU region |
| `UNLEASHED_API_KEY_EU` | API key for the EU region |
| `AzureWebJobsStorage` | Storage connection string for the cache |
| `CACHE_CONTAINER` | Blob container name (default: `cache`) |
| `CACHE_TTL_SECONDS` | Cache lifetime (default: `3600`) |

No credentials are stored in this repository.

## Requirements

Python 3.9+. See `requirements.txt`.
