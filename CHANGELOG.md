# Changelog

## v2.0.0
  * Updates API version to 20
  * Add new stream `assets`
  * Remove feed-related streams
  * Updates pkg version to 27.0.0
  * Updates `singer-python` to 6.1.1
  * [#97](https://github.com/singer-io/tap-google-ads/pull/97)

## v1.10.0
  * Fail the connection once every 3 days to ensure customers are aware of the version deprecation. [#100](https://github.com/singer-io/tap-google-ads/pull/100)

## v1.9.1
  * Bump dependency versions for twistlock compliance
  * Update circleci config to handle linting errors
  * [#96](https://github.com/singer-io/tap-google-ads/pull/96)

## v1.9.0
  * Updates API version to 17
  * [#95](https://github.com/singer-io/tap-google-ads/pull/95)

## v1.8.0
  * Updates API version to 17
  * Updates pkg version to 25.0.0
  * [#93](https://github.com/singer-io/tap-google-ads/pull/93)

## v1.7.0
  * Run on python 3.11.7 [#88](https://github.com/singer-io/tap-google-ads/pull/88)

## v1.6.0
  * Updates API version to 15
  * Updates pkg version to 22.1.0
  * [#86](https://github.com/singer-io/tap-google-ads/pull/86)

## v1.5.0
  * Updates API version to 13
  * Updates pkg version to 21.0.0
  * [#82](https://github.com/singer-io/tap-google-ads/pull/82)


## v1.4.0
  * Updates API version to 12
  * Updates pkg version to 19.0.0
  * Removes `gmail_ad` fields from `ad_performance_report` as they are no longer available after API version bump.
  * [#76](https://github.com/singer-io/tap-google-ads/pull/76)

## v1.3.4
  * Updates API Version to 11
  * Updates pkg version to 17.0.0
  * [#79](https://github.com/singer-io/tap-google-ads/pull/79)

## v1.3.3
  * Update applicable core streams to use limit clause. Updates tests [#68](https://github.com/singer-io/tap-google-ads/pull/68)

## v1.3.2
  * Add timeout parameter to Google Ads search requests
  * Allow for request_timeout config parameter to be provided [#64](https://github.com/singer-io/tap-google-ads/pull/64)

## v1.3.1
  * Handle uncaught exceptions [#61](https://github.com/singer-io/tap-google-ads/pull/61)
  * Implement interruptible full table streams [#60](https://github.com/singer-io/tap-google-ads/pull/60)

## v1.3.0 [#58](https://github.com/singer-io/tap-google-ads/pull/58)
  * Adds several new core streams including ad_group_criterion, campaign_criterion, and their attributed resources.
  * Adds new subclass UserInterestStream to handle stream specific name transformations.
  * Renames obj and corresponding variables in all transform_keys functions.

## v1.2.0
  * Renames `REPORTS` variable to `STREAMS` and updates corresponding variables similarly. Removes unused `add_extra_fields` function [#56](https://github.com/singer-io/tap-google-ads/pull/56) 
  * Adds `automatic_keys` to metadata for streams, including reports. Updates tests [#55](https://github.com/singer-io/tap-google-ads/pull/55)

## v1.1.0
  * Fixes a bug with currently_syncing and adds tests around the bug fix [#54](https://github.com/singer-io/tap-google-ads/pull/54)
  * Adds `campaign_labels` and `labels` core streams; adds "campaign.labels" field to reports where relevant [#53](https://github.com/singer-io/tap-google-ads/pull/53)
  * Adds `call_details` core stream and removes segmenting resources from core streams [#49](https://github.com/singer-io/tap-google-ads/pull/49)

## v1.0.0
  * Version bump for GA release
  * Adds fields to click_view report definition [#44](https://github.com/singer-io/tap-google-ads/pull/44)
  * Adds date ranges to tests for faster test runs [#43](https://github.com/singer-io/tap-google-ads/pull/43)
  * Adds more tests around primary key hashing [#42](https://github.com/singer-io/tap-google-ads/pull/42)

## v0.3.0
  * Removes unused code
  * Adds a behavior to "_sdc_record_hash"
  * Fixed field exclusion for segments and attributes
  * Adds tests for field exclusion
  * Updates the "type_" field to "type" Transform type_ to type [#36](https://github.com/singer-io/tap-google-ads/pull/36)
  * Updates fields in the report streams to include the Google Ads resource name Report streams prefix resource names [#37](https://github.com/singer-io/tap-google-ads/pull/37)
  * Updates the generate_hash function to be explicit about the order of the fields getting hashed Change _sdc_record_hash to sorted list of tuples [#38](https://github.com/singer-io/tap-google-ads/pull/38)

## v0.2.0 [#31](https://github.com/singer-io/tap-google-ads/pull/31)
  * Add ability for the tap to use `currently_syncing` [#24](https://github.com/singer-io/tap-google-ads/pull/24)
  * Add `end_date` as a configurable property to end a sync at a certain date [#28](https://github.com/singer-io/tap-google-ads/pull/28)
  * Fix a field exculsion bug introduced in `v0.1.0` around metric compatibility [#29](https://github.com/singer-io/tap-google-ads/pull/29)

## v0.1.0 [#23](https://github.com/singer-io/tap-google-ads/pull/23)
  * Update bookmarks to only be written with a midnight time value
  * Fix error logging to more concise
  * Add retry logic to sync queries [#22](https://github.com/singer-io/tap-google-ads/pull/22)
  * Fix field exclusion bug: Metrics can exclude attributes
  * Rename:
    * `adgroup_performance_report` to `ad_group_performance_report`
    * `audience_performance_report` to `ad_group_audience_performance_report`
  * Add `campaign_audience_performance_report`

## v0.0.1
  * Alpha Release [#13](https://github.com/singer-io/tap-google-ads/pull/13)
