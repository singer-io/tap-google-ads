from base_google_ads_field_exclusion import FieldExclusionGoogleAdsBase


class FieldExlusionCoverage(FieldExclusionGoogleAdsBase):

    checking_coverage = True

    @staticmethod
    def name():
        return "tt_google_ads_exclusion_coverage"

    def test_run(self):
        """
        Ensure all report streams are covered for the field exlusion test cases.
        The report streams are spread out across several test classes for parallelism. This extra
        step is required as we hardcode the streams under test in each of the four classes. 
        """
        report_streams = {stream for stream in self.expected_streams()
                          if self.is_report(stream)
                          and stream != "click_performance_report"}

        from test_google_ads_field_exclusion_1 import FieldExclusion1
        from test_google_ads_field_exclusion_2 import FieldExclusion2
        from test_google_ads_field_exclusion_3 import FieldExclusion3
        from test_google_ads_field_exclusion_4 import FieldExclusion4

        f1 = FieldExclusion1()
        f2 = FieldExclusion2()
        f3 = FieldExclusion3()
        f4 = FieldExclusion4()

        streams_under_test = f1.streams_to_test | f2.streams_to_test | f3.streams_to_test | f4.streams_to_test

        self.assertSetEqual(report_streams, streams_under_test)
        print("ALL REPORT STREAMS UNDER TEST")
        print(f"Streams: {streams_under_test}")
