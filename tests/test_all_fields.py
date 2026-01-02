from base import mailjetBaseTest
from tap_tester.base_suite_tests.all_fields_test import AllFieldsTest

KNOWN_MISSING_FIELDS = {

}


class mailjetAllFields(AllFieldsTest, mailjetBaseTest):
    """Ensure running the tap with all streams and fields selected results in
    the replication of all fields."""

    @staticmethod
    def name():
        return "tap_tester_mailjet_all_fields_test"

    def streams_to_test(self):
        # Exclude streams with no test data available in the test account
        streams_to_exclude = {
            'geo_statistics',
            'top_link_clicked'
        }
        return self.expected_stream_names().difference(streams_to_exclude)

