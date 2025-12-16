from base import mailjetBaseTest
from tap_tester.base_suite_tests.bookmark_test import BookmarkTest


class mailjetBookMarkTest(BookmarkTest, mailjetBaseTest):
    """Test tap sets a bookmark and respects it for the next sync of a
    stream."""
    bookmark_format = "%Y-%m-%dT%H:%M:%S.%fZ"
    initial_bookmarks = {
        "bookmarks": {
            "messages": { "A" : "2020-01-01T00:00:00Z"},
            "campaigns": { "C" : "2020-01-01T00:00:00Z"},
            "message_information": { "C" : "2020-01-01T00:00:00Z"},
            "click_statistics": { "C" : "2020-01-01T00:00:00Z"},
        }
    }
    @staticmethod
    def name():
        return "tap_tester_mailjet_bookmark_test"

    def streams_to_test(self):
        streams_to_exclude = {}
        return self.expected_stream_names().difference(streams_to_exclude)

