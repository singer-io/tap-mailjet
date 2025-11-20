
from base import mailjetBaseTest
from tap_tester.base_suite_tests.interrupted_sync_test import InterruptedSyncTest


class mailjetInterruptedSyncTest(mailjetBaseTest):
    """Test tap sets a bookmark and respects it for the next sync of a
    stream."""

    @staticmethod
    def name():
        return "tap_tester_mailjet_interrupted_sync_test"

    def streams_to_test(self):
        return self.expected_stream_names()


    def manipulate_state(self):
        return {
            "currently_syncing": "prospects",
            "bookmarks": {
        }
    }

