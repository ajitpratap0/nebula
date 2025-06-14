# Meta Ads Source Connector

## Overview

Extracts advertising data from Meta's platform (Facebook, Instagram) using the Meta Marketing API.

## Configuration

```yaml
source:
  type: meta_ads
  properties:
    # Authentication (Required)
    access_token: "YOUR_META_ACCESS_TOKEN"
    account_id: "act_XXXXXXXXX"
    
    # Data selection
    object_type: "insights"  # insights, campaigns, adsets, ads
    level: "ad"              # account, campaign, adset, ad
    date_preset: "last_30d"  # yesterday, last_7d, last_30d, etc.
    
    # Optional: Custom date range
    time_range:
      since: "2024-01-01"
      until: "2024-01-31"
    
    # Optional: Specific fields
    fields:
      - "campaign_name"
      - "impressions" 
      - "clicks"
      - "spend"
```

## Authentication Setup

1. Create a Meta App in Meta for Developers
2. Request `ads_read` permission  
3. Generate a long-lived access token
4. Get your Ad Account ID from Meta Ads Manager

## Object Types

- **insights**: Performance metrics and analytics
- **campaigns**: Campaign information and settings
- **adsets**: Ad set configuration and targeting
- **ads**: Individual ad creative data

## Features

- **Async Reports**: Efficient handling of large datasets
- **Rate Limiting**: Automatic API quota management
- **Batch Operations**: Optimized API calls
- **Incremental Sync**: Date-based incremental updates

## Notes

- Requires approved Meta App with ads_read permission
- Access tokens expire and need regular refresh
- Rate limits vary by app review status and usage