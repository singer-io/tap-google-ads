# Changelog

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
