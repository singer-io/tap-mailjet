import unittest
from unittest.mock import patch, MagicMock
from tap_mailjet.sync import write_schema, sync, update_currently_syncing

class TestSync(unittest.TestCase):

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
