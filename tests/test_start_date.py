from base import mailjetBaseTest
from tap_tester.base_suite_tests.start_date_test import StartDateTest



class mailjetStartDateTest(StartDateTest, mailjetBaseTest):
    """Instantiate start date according to the desired data set and run the
    test."""

    @staticmethod
    def name():
        return "tap_tester_mailjet_start_date_test"

    def streams_to_test(self):
        streams_to_exclude = {
            # Unsupported Full-Table Streams
            'contacts',
            'contacts_list',
            'list_recipient',
            'template',
            'geo_statistics',
            'top_link_clicked',
            'campaign_overview'
        }
        return self.expected_stream_names().difference(streams_to_exclude)

    @property
    def start_date_1(self):
        return "2025-11-01T00:00:00Z"
    @property
    def start_date_2(self):
        return "2025-12-01T00:00:00Z"

