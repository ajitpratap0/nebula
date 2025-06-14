# Google Ads Source Connector

## Overview

Extracts data from Google Ads using the Google Ads API with GAQL (Google Ads Query Language) support.

## Configuration

```yaml
source:
  type: google_ads
  properties:
    # Authentication (Required)
    developer_token: "YOUR_DEVELOPER_TOKEN"
    client_id: "YOUR_OAUTH_CLIENT_ID" 
    client_secret: "YOUR_OAUTH_CLIENT_SECRET"
    refresh_token: "YOUR_OAUTH_REFRESH_TOKEN"
    
    # Customer accounts
    customer_ids: ["1234567890", "0987654321"]
    
    # GAQL query
    query: |
      SELECT campaign.id, campaign.name, metrics.impressions, metrics.clicks
      FROM campaign
      WHERE segments.date DURING LAST_30_DAYS
```

## Authentication Setup

1. Create a Google Ads API application in Google Cloud Console
2. Generate OAuth2 credentials (client ID and secret)
3. Obtain a developer token from Google Ads Manager
4. Generate a refresh token using OAuth2 flow

## Features

- **GAQL Support**: Full Google Ads Query Language
- **Multi-Customer**: Process multiple accounts concurrently  
- **Rate Limiting**: Automatic API quota management
- **Streaming**: Memory-efficient for large datasets

## Data Types

The connector extracts campaign, ad group, keyword, and performance data based on your GAQL query. All standard Google Ads API fields are supported.

## Notes

- Requires approved Google Ads API access
- Rate limits apply based on your API access level
- Historical data availability depends on account settings