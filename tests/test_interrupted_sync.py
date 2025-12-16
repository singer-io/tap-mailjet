
from base import mailjetBaseTest
from tap_tester.base_suite_tests.interrupted_sync_test import InterruptedSyncTest


class mailjetInterruptedSyncTest(InterruptedSyncTest, mailjetBaseTest):
    """Test tap sets a bookmark and respects it for the next sync of a
    stream."""

    @staticmethod
    def name():
        return "tap_tester_mailjet_interrupted_sync_test"

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


    def manipulate_state(self):
        return {
            "currently_syncing": "messages",
            "bookmarks": {
                "messages": { "ArrivedAt" : "2020-01-01T00:00:00Z"},
                "campaigns": { "CreatedAt" : "2020-01-01T00:00:00Z"},
                "message_information": { "CreatedAt" : "2020-01-01T00:00:00Z"},
                "click_statistics": { "ClickedAt" : "2020-01-01T00:00:00Z"},
            }
        }

