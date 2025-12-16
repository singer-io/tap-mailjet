from base import mailjetBaseTest
from tap_tester.base_suite_tests.bookmark_test import BookmarkTest


class mailjetBookMarkTest(BookmarkTest, mailjetBaseTest):
    """Test tap sets a bookmark and respects it for the next sync of a
    stream."""
    bookmark_format = "%Y-%m-%dT%H:%M:%S.%fZ"
    initial_bookmarks = {
        "bookmarks": {
            "messages": { "ArrivedAt" : "2020-01-01T00:00:00Z"},
            "campaigns": { "CreatedAt" : "2020-01-01T00:00:00Z"},
            "message_information": { "CreatedAt" : "2020-01-01T00:00:00Z"},
            "click_statistics": { "ClickedAt" : "2020-01-01T00:00:00Z"},
        }
    }
    @staticmethod
    def name():
        return "tap_tester_mailjet_bookmark_test"

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

    def calculate_new_bookmarks(self):
        """Calculates new bookmarks by looking through sync 1 data to determine
        a bookmark that will sync 2 records in sync 2 (plus any necessary look
        back data)"""
        new_bookmarks = {
            "messages": { "ArrivedAt" : "2024-01-01T00:00:00Z"},
            "campaigns": { "CreatedAt" : "2024-01-01T00:00:00Z"},
            "message_information": { "CreatedAt" : "2024-01-01T00:00:00Z"},
            "click_statistics": { "ClickedAt" : "2024-01-01T00:00:00Z"},
        }

        return new_bookmarks

