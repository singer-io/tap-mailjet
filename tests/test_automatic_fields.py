"""Test that with no fields selected for a stream automatic fields are still
replicated."""
from base import mailjetBaseTest
from tap_tester.base_suite_tests.automatic_fields_test import MinimumSelectionTest


class mailjetAutomaticFields(MinimumSelectionTest, mailjetBaseTest):
    """Test that with no fields selected for a stream automatic fields are
    still replicated."""

    @staticmethod
    def name():
        return "tap_tester_mailjet_automatic_fields_test"

    def streams_to_test(self):
        streams_to_exclude = {
            "geo_statistics",  # No automatic fields
            "top_link_clicked",  # No test data available
        }
        return self.expected_stream_names().difference(streams_to_exclude)

