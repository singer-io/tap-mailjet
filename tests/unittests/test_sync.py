import unittest
from unittest.mock import patch, MagicMock, call
from tap_mailjet.sync import sync, update_currently_syncing, write_schema
from singer import Catalog, CatalogEntry, Schema, metadata as singer_metadata

class TestSync(unittest.TestCase):
    """Test cases for sync functionality."""

    def setUp(self):
        """Set up test fixtures."""
        self.config = {
            'api_key': 'test_key',
            'api_secret': 'test_secret',
            'start_date': '2024-01-01T00:00:00Z'
        }
        self.state = {}

    @patch('tap_mailjet.sync.sync_stream')
    @patch('tap_mailjet.streams.Messages')
    @patch('tap_mailjet.streams.MessageInformation')
    def test_sync_child_selected(self, mock_message_info_class, mock_messages_class, mock_sync_stream):
        """Test sync when child stream is selected."""
        # Create mock stream instances
        mock_messages_instance = MagicMock()
        mock_messages_instance.tap_stream_id = 'messages'
        mock_messages_instance.is_selected.return_value = True
        mock_messages_class.return_value = mock_messages_instance
        
        mock_message_info_instance = MagicMock()
        mock_message_info_instance.tap_stream_id = 'message_information'
        mock_message_info_instance.parent = 'messages'
        mock_message_info_instance.is_selected.return_value = True
        mock_message_info_class.return_value = mock_message_info_instance
        
        # Create mock schema
        schema_dict = {
            'type': 'object',
            'properties': {
                'ID': {'type': 'integer'},
                'ArrivedAt': {'type': ['null', 'string'], 'format': 'date-time'}
            }
        }
        
        # Create catalog with parent and child streams
        parent_metadata = singer_metadata.get_standard_metadata(
            schema=schema_dict,
            key_properties=['ID'],
            replication_method='INCREMENTAL',
            valid_replication_keys=['ArrivedAt']
        )
        parent_metadata = singer_metadata.to_list(parent_metadata)
        parent_metadata[0]['metadata']['selected'] = True
        
        parent_stream = CatalogEntry(
            tap_stream_id='messages',
            stream='messages',
            schema=Schema.from_dict(schema_dict),
            key_properties=['ID'],
            metadata=parent_metadata
        )
        
        child_metadata = singer_metadata.get_standard_metadata(
            schema=schema_dict,
            key_properties=['ID'],
            replication_method='INCREMENTAL',
            valid_replication_keys=['ArrivedAt']
        )
        child_metadata = singer_metadata.to_list(child_metadata)
        child_metadata[0]['metadata']['selected'] = True
        
        child_stream = CatalogEntry(
            tap_stream_id='message_information',
            stream='message_information',
            schema=Schema.from_dict(schema_dict),
            key_properties=['ID'],
            metadata=child_metadata
        )
        
        catalog = Catalog(streams=[parent_stream, child_stream])
        
        # Run sync
        sync.sync(self.config, self.state, catalog)
        
        # Verify sync_stream was called
        # The actual number of calls depends on implementation
        self.assertGreaterEqual(mock_sync_stream.call_count, 1)

    def test_write_schema_only_parent_selected(self):
        mock_stream = MagicMock()
        mock_stream.is_selected.return_value = True
        mock_stream.children = ["list_recipient", "template"]
        mock_stream.child_to_sync = []

        client = MagicMock()
        catalog = MagicMock()
        catalog.get_stream.return_value = MagicMock()

        write_schema(mock_stream, client, [], catalog)

        mock_stream.write_schema.assert_called_once()
        self.assertEqual(len(mock_stream.child_to_sync), 0)

    def test_write_schema_parent_child_both_selected(self):
        mock_stream = MagicMock()
        mock_stream.is_selected.return_value = True
        mock_stream.children = ["list_recipient", "template"]
        mock_stream.child_to_sync = []

        client = MagicMock()
        catalog = MagicMock()
        catalog.get_stream.return_value = MagicMock()

        write_schema(mock_stream, client, ["list_recipient"], catalog)

        mock_stream.write_schema.assert_called_once()
        self.assertEqual(len(mock_stream.child_to_sync), 1)

    def test_write_schema_child_selected(self):
        mock_stream = MagicMock()
        mock_stream.is_selected.return_value = False
        mock_stream.children = ["list_recipient", "template"]
        mock_stream.child_to_sync = []

        client = MagicMock()
        catalog = MagicMock()
        catalog.get_stream.return_value = MagicMock()

        write_schema(mock_stream, client, ["list_recipient", "template"], catalog)

        self.assertEqual(mock_stream.write_schema.call_count, 0)
        self.assertEqual(len(mock_stream.child_to_sync), 2)

    @patch("singer.write_schema")
    @patch("singer.get_currently_syncing")
    @patch("singer.Transformer")
    @patch("singer.write_state")
    @patch("tap_mailjet.streams.abstracts.IncrementalStream.sync")
    def test_sync_stream1_called(self, mock_sync, mock_write_state, mock_transformer, mock_get_currently_syncing, mock_write_schema):
        mock_catalog = MagicMock()
        campaigns_stream = MagicMock()
        campaigns_stream.stream = "campaigns"
        messages_stream = MagicMock()
        messages_stream.stream = "messages"
        mock_catalog.get_selected_streams.return_value = [
            campaigns_stream,
            messages_stream
        ]
        state = {}

        client = MagicMock()
        config = {}

        sync(client, config, mock_catalog, state)

        self.assertEqual(mock_sync.call_count, 2)

    @patch("singer.write_schema")
    @patch("singer.get_currently_syncing")
    @patch("singer.Transformer")
    @patch("singer.write_state")
    def test_sync_child_selected(self, mock_write_state, mock_transformer, mock_get_currently_syncing, mock_write_schema):
        # Create a mock catalog where list_recipient has contacts as its parent
        mock_catalog = MagicMock()
        contacts_stream = MagicMock()
        contacts_stream.stream = "contacts"
        list_recipient_stream = MagicMock()
        list_recipient_stream.stream = "list_recipient"
        mock_catalog.get_selected_streams.return_value = [
            contacts_stream,
            list_recipient_stream
        ]
        state = {}

        client = MagicMock()
        config = {}

        # Mock the STREAMS dictionary to return stream instances with parent relationship
        parent_stream_instance = MagicMock()
        parent_stream_instance.parent = ""
        parent_stream_instance.sync.return_value = 0
        
        child_stream_instance = MagicMock()
        child_stream_instance.parent = "contacts"
        
        mock_streams_dict = {
            "contacts": MagicMock(return_value=parent_stream_instance),
            "list_recipient": MagicMock(return_value=child_stream_instance),
        }
        
        with patch.dict("tap_mailjet.sync.STREAMS", mock_streams_dict, clear=False):
            sync(client, config, mock_catalog, state)

            # Only parent stream should be synced, child is skipped due to parent relationship
            self.assertEqual(parent_stream_instance.sync.call_count, 1)
            self.assertEqual(child_stream_instance.sync.call_count, 0)

    @patch("singer.get_currently_syncing")
    @patch("singer.set_currently_syncing")
    @patch("singer.write_state")
    def test_remove_currently_syncing(self, mock_write_state, mock_set_currently_syncing, mock_get_currently_syncing):
        mock_get_currently_syncing.return_value = "some_stream"
        state = {"currently_syncing": "some_stream"}

        update_currently_syncing(state, None)

        mock_get_currently_syncing.assert_called_once_with(state)
        mock_set_currently_syncing.assert_not_called()
        mock_write_state.assert_called_once_with(state)
        self.assertNotIn("currently_syncing", state) 

    @patch("singer.get_currently_syncing")
    @patch("singer.set_currently_syncing")
    @patch("singer.write_state")
    def test_set_currently_syncing(self, mock_write_state, mock_set_currently_syncing, mock_get_currently_syncing):
        mock_get_currently_syncing.return_value = None
        state = {}

        update_currently_syncing(state, "new_stream")

        mock_get_currently_syncing.assert_not_called()
        mock_set_currently_syncing.assert_called_once_with(state, "new_stream")
        mock_write_state.assert_called_once_with(state)
        self.assertNotIn("currently_syncing", state)
