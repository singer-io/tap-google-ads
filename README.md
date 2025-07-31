# tap_google_ads

This is a [Singer](https://singer.io) tap that produces JSON-formatted
data from the Google Ads API following the [Singer
spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap: 

- Pulls data from the [Google Ads API](https://developers.google.com/google-ads/api/docs/start).
- Extracts the following resources from Google Ads
  - [Accessible Bidding Strategies](https://developers.google.com/google-ads/api/reference/rpc/v10/AccessibleBiddingStrategy)
  - [Accounts](https://developers.google.com/google-ads/api/reference/rpc/v10/Customer)
  - [Ad Groups](https://developers.google.com/google-ads/api/reference/rpc/v10/AdGroup)
  - [Ads](https://developers.google.com/google-ads/api/reference/rpc/v10/Ad)
  - [Assets](https://developers.google.com/google-ads/api/reference/rpc/v20/Asset)
  - [Bidding Strategies](https://developers.google.com/google-ads/api/reference/rpc/v10/BiddingStrategy)
  - [Call Details](https://developers.google.com/google-ads/api/reference/rpc/v10/CallView)
  - [Campaigns](https://developers.google.com/google-ads/api/reference/rpc/v10/Campaign)
  - [Campaign Budgets](https://developers.google.com/google-ads/api/reference/rpc/v10/CampaignBudget)
  - [Campaign Labels](https://developers.google.com/google-ads/api/reference/rpc/v10/CampaignLabel)
  - [Labels](https://developers.google.com/google-ads/api/reference/rpc/v10/Label)
  - [Reporting](https://developers.google.com/google-ads/api/docs/reporting/overview)
    - [Account Performance Report](https://developers.google.com/google-ads/api/fields/v10/customer)
    - [Ad Group Performance Report](https://developers.google.com/google-ads/api/fields/v10/ad_group)
    - [Ad Group Audience Performance Report](https://developers.google.com/google-ads/api/fields/v10/ad_group_audience_view)
    - [Ad Performance Report](https://developers.google.com/google-ads/api/fields/v10/ad_group_ad)
    - [Age Range Performance Report](https://developers.google.com/google-ads/api/fields/v10/age_range_view)
    - [Campaign Performance Report](https://developers.google.com/google-ads/api/fields/v10/campaign)
    - [Campaign Audience Performance Report](https://developers.google.com/google-ads/api/fields/v10/campaign_audience_view)
    - [Call Metrics Call Details Report](https://developers.google.com/google-ads/api/fields/v10/call_view)
    - [Click Performance Report](https://developers.google.com/google-ads/api/fields/v10/click_view)
    - [Display Keyword Performance Report](https://developers.google.com/google-ads/api/fields/v10/display_keyword_view)
    - [Display Topics Performance Report](https://developers.google.com/google-ads/api/fields/v10/topic_view)
    - [Expanded Landing Page Report](https://developers.google.com/google-ads/api/fields/v10/expanded_landing_page_view)
    - [Gender Performance Report](https://developers.google.com/google-ads/api/fields/v10/gender_view)
    - [Geo Performance Report](https://developers.google.com/google-ads/api/fields/v10/geographic_view)
    - [Keywordless Query Report](https://developers.google.com/google-ads/api/fields/v10/dynamic_search_ads_search_term_view)
    - [Keywords Performance Report](https://developers.google.com/google-ads/api/fields/v10/keyword_view)
    - [Landing Page Report](https://developers.google.com/google-ads/api/fields/v10/landing_page_view)
    - [Placeholder Feed Item Report](https://developers.google.com/google-ads/api/fields/v10/feed_item)
    - [Placeholder Report](https://developers.google.com/google-ads/api/fields/v10/feed_placeholder_view)
    - [Placement Performance Report](https://developers.google.com/google-ads/api/fields/v10/managed_placement_view)
    - [Search Query Performance Report](https://developers.google.com/google-ads/api/fields/v10/search_term_view)
    - [Shopping Performance Report](https://developers.google.com/google-ads/api/fields/v10/shopping_performance_view)
    - [User Location Performance Report](https://developers.google.com/google-ads/api/fields/v10/user_location_view)
    - [UserLocation Performance Report](https://developers.google.com/google-ads/api/fields/v10/user_location_view)
    - [Video Performance Report](https://developers.google.com/google-ads/api/fields/v10/video)

## Bookmarking Strategy

The Google Ads API supports the `start_date` and `end_date` parameters that limits the records which filters the analytics records in the given time period.

## Configuration

This tap requires a `config.json` which specifies details regarding [OAuth 2.0](https://developers.google.com/google-ads/api/docs/oauth/overview) authentication and a cutoff date for syncing historical data. See [config.sample.json](config.sample.json) for an example.

To run the discover mode of `tap-google-ads` with the configuration file, use this command:

```bash
$ tap-google-ads -c my-config.json -d
```

To run the sync mode of `tap-google-ads` with the catalog file, use the command:

```bash
$ tap-google-ads -c my-config.json --catalog catalog.json
```
---

Copyright &copy; 2021 Stitch
