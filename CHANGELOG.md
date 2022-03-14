# Changelog

## v0.2.0 [#31][PR-31]
  * Add ability for the tap to use `currently_syncing` [#24][PR-24]
  * Add `end_date` as a configurable property to end a sync at a certain date [#28][PR-28]
  * Fix a field exculsion bug introduced in `v0.1.0` around metric compatibility [#29][PR-29]


[PR-24]: https://github.com/singer-io/tap-google-ads/pull/24
[PR-28]: https://github.com/singer-io/tap-google-ads/pull/28
[PR-29]: https://github.com/singer-io/tap-google-ads/pull/29
[PR-31]: https://github.com/singer-io/tap-google-ads/pull/31

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
