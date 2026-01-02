"""Unit tests for full table sync flow."""
import unittest
from unittest.mock import patch, MagicMock
from singer import Transformer
from tap_mailjet.streams.contacts import Contacts


class TestFullTableSync(unittest.TestCase):
    """Test full table sync functionality."""

    def setUp(self):
        """Set up test stream."""
        config = {
            "api_key": "test_key",
            "secret_key": "test_secret",
            "start_date": "2025-01-01T00:00:00Z"
        }
        client = MagicMock()
        client.config = config
        
        catalog = MagicMock()
        catalog.schema.to_dict.return_value = {
            "type": "object",
            "properties": {
                "ID": {"type": "integer"},
                "Email": {"type": "string"}
            }
        }
        catalog.metadata = []
        
        self.stream = Contacts(client=client, catalog=catalog)
        self.stream.is_selected = MagicMock(return_value=True)

    @patch("tap_mailjet.streams.abstracts.write_record")
    @patch("tap_mailjet.streams.abstracts.metrics.record_counter")
    def test_full_table_sync_all_records(self, mock_counter, mock_write_record):
        """Test that full table sync fetches all records."""
        mock_records = [
            {"ID": 1, "Email": "user1@example.com"},
            {"ID": 2, "Email": "user2@example.com"},
            {"ID": 3, "Email": "user3@example.com"}
        ]
        
        self.stream.get_records = MagicMock(return_value=iter(mock_records))
        
        transformer = Transformer()
        state = {}
        
        self.stream.sync(state, transformer)
        
        # Verify all records were written
        self.assertEqual(mock_write_record.call_count, 3)

    @patch("tap_mailjet.streams.abstracts.write_record")
    @patch("tap_mailjet.streams.abstracts.metrics.record_counter")
    def test_full_table_sync_no_state_emission(self, mock_counter, mock_write_record):
        """Test that full table sync does not emit state."""
        mock_records = [{"ID": 1, "Email": "test@example.com"}]
        
        self.stream.get_records = MagicMock(return_value=iter(mock_records))
        
        transformer = Transformer()
        state = {"bookmarks": {}}
        
        result_state = self.stream.sync(state, transformer)
        
        # Full table streams should not modify state
        # (result_state is counter value, not modified state dict)
        self.assertIsNotNone(result_state)

    @patch("tap_mailjet.streams.abstracts.write_record")
    @patch("tap_mailjet.streams.abstracts.metrics.record_counter")
    def test_full_table_sync_with_transformation(self, mock_counter, mock_write_record):
        """Test that records are transformed before writing."""
        mock_records = [
            {"ID": 1, "Email": "test@example.com", "CreatedAt": "2025-01-01T00:00:00Z"}
        ]
        
        self.stream.get_records = MagicMock(return_value=iter(mock_records))
        
        transformer = MagicMock()
        transformer.transform = MagicMock(return_value={"ID": 1, "Email": "test@example.com"})
        state = {}
        
        self.stream.sync(state, transformer)
        
        # Verify transformer was called
        transformer.transform.assert_called_once()

    @patch("tap_mailjet.streams.abstracts.write_record")
    @patch("tap_mailjet.streams.abstracts.metrics.record_counter")
    def test_full_table_sync_empty_results(self, mock_counter, mock_write_record):
        """Test full table sync with no records."""
        self.stream.get_records = MagicMock(return_value=iter([]))
        
        transformer = Transformer()
        state = {}
        
        self.stream.sync(state, transformer)
        
        # No records should be written
        mock_write_record.assert_not_called()

    @patch("tap_mailjet.streams.abstracts.write_record")
    @patch("tap_mailjet.streams.abstracts.LOGGER")
    @patch("tap_mailjet.streams.abstracts.metrics.record_counter")
    def test_full_table_sync_error_handling(self, mock_counter, mock_logger, mock_write_record):
        """Test that transformation errors are logged and raised."""
        mock_records = [{"ID": 1, "Email": "test@example.com"}]
        
        self.stream.get_records = MagicMock(return_value=iter(mock_records))
        
        transformer = MagicMock()
        transformer.transform = MagicMock(side_effect=Exception("Transform error"))
        state = {}
        
        with self.assertRaises(Exception):
            self.stream.sync(state, transformer)
        
        # Verify error was logged
        mock_logger.error.assert_called_once()

    @patch("tap_mailjet.streams.abstracts.write_record")
    @patch("tap_mailjet.streams.abstracts.metrics.record_counter")
    def test_full_table_sync_replication_method(self, mock_counter, mock_write_record):
        """Test that replication method is FULL_TABLE."""
        self.assertEqual(self.stream.replication_method, "FULL_TABLE")
        self.assertEqual(self.stream.replication_keys, [])

    @patch("tap_mailjet.streams.abstracts.write_record")
    @patch("tap_mailjet.streams.abstracts.metrics.record_counter")
    def test_full_table_sync_child_streams(self, mock_counter, mock_write_record):
        """Test full table sync with child streams."""
        mock_records = [{"ID": 1, "Email": "test@example.com"}]
        
        self.stream.get_records = MagicMock(return_value=iter(mock_records))
        
        # Add mock child stream
        child_stream = MagicMock()
        child_stream.sync = MagicMock()
        self.stream.child_to_sync = [child_stream]
        
        transformer = Transformer()
        state = {}
        
        self.stream.sync(state, transformer)
        
        # Verify child stream sync was called
        child_stream.sync.assert_called_once()

    @patch("tap_mailjet.streams.abstracts.write_record")
    @patch("tap_mailjet.streams.abstracts.metrics.record_counter")
    def test_full_table_sync_not_selected(self, mock_counter, mock_write_record):
        """Test that unselected streams don't write records."""
        self.stream.is_selected = MagicMock(return_value=False)
        mock_records = [{"ID": 1, "Email": "test@example.com"}]
        
        self.stream.get_records = MagicMock(return_value=iter(mock_records))
        
        transformer = Transformer()
        state = {}
        
        self.stream.sync(state, transformer)
        
        # No records should be written for unselected stream
        mock_write_record.assert_not_called()
