import unittest
from unittest.mock import Mock, patch, MagicMock
from tap_mailjet.client import Client
from tap_mailjet.streams.message import Message
from tap_mailjet.streams.campaign import Campaign
from tap_mailjet.streams.contact import Contact
from tap_mailjet.streams.contactslist import Contactslist
from tap_mailjet.streams.listrecipient import Listrecipient
from singer import Transformer


class TestAPIIntegration(unittest.TestCase):
    """Integration tests for API calls and stream synchronization."""

    def setUp(self):
        """Set up test client and configurations."""
        self.config = {
            "api_access": "Bearer test_token",
            "start_date": "2024-01-01T00:00:00Z",
            "request_timeout": 30
        }
        self.client = Client(self.config)

    def create_mock_catalog(self, stream_name):
        """Create a mock catalog for testing."""
        mock_catalog = Mock()
        mock_catalog.schema = Mock()
        mock_catalog.schema.to_dict = Mock(return_value={
            "type": "object",
            "properties": {
                "ID": {"type": ["integer", "null"]},
                "Name": {"type": ["string", "null"]},
                "Email": {"type": ["string", "null"]}
            }
        })
        mock_catalog.metadata = []
        return mock_catalog

    @patch('tap_mailjet.client.session')
    def test_message_stream_full_sync(self, mock_session_class):
        """Test full sync of message stream with API."""
        # Mock the session and response
        mock_session = MagicMock()
        mock_session_class.return_value = mock_session
        
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "Data": [
                {
                    "ID": 1,
                    "ArrivedAt": "2024-01-15T10:30:00Z",
                    "ContactID": 100,
                    "Status": "sent"
                },
                {
                    "ID": 2,
                    "ArrivedAt": "2024-01-15T11:00:00Z",
                    "ContactID": 101,
                    "Status": "delivered"
                }
            ]
        }
        mock_session.request.return_value = mock_response
        
        # Create stream and sync
        catalog = self.create_mock_catalog("message")
        stream = Message(self.client, catalog)
        
        records = list(stream.get_records())
        
        # Verify results
        self.assertEqual(len(records), 2)
        self.assertEqual(records[0]["ID"], 1)
        self.assertEqual(records[1]["Status"], "delivered")
        
        # Verify API call was made correctly
        mock_session.request.assert_called_once()
        call_args = mock_session.request.call_args
        self.assertEqual(call_args[0][0], "GET")
        self.assertIn("/v3/REST/message", call_args[0][1])

    @patch('tap_mailjet.client.session')
    def test_campaign_stream_pagination(self, mock_session_class):
        """Test campaign stream with pagination."""
        mock_session = MagicMock()
        mock_session_class.return_value = mock_session
        
        # First page response (full page)
        page1_response = MagicMock()
        page1_response.status_code = 200
        page1_response.json.return_value = {
            "Data": [{"ID": i, "Name": f"Campaign {i}"} for i in range(1, 11)]
        }
        
        # Second page response (partial page)
        page2_response = MagicMock()
        page2_response.status_code = 200
        page2_response.json.return_value = {
            "Data": [{"ID": 11, "Name": "Campaign 11"}]
        }
        
        mock_session.request.side_effect = [page1_response, page2_response]
        
        # Create stream with small page size
        catalog = self.create_mock_catalog("campaign")
        stream = Campaign(self.client, catalog)
        stream.page_size = 10
        
        records = list(stream.get_records())
        
        # Verify results
        self.assertEqual(len(records), 11)
        self.assertEqual(records[0]["ID"], 1)
        self.assertEqual(records[10]["ID"], 11)
        
        # Verify pagination calls
        self.assertEqual(mock_session.request.call_count, 2)
        
        # Check first call has Offset=0
        first_call = mock_session.request.call_args_list[0]
        self.assertEqual(first_call[1]["params"]["Offset"], 0)
        
        # Check second call has Offset=10
        second_call = mock_session.request.call_args_list[1]
        self.assertEqual(second_call[1]["params"]["Offset"], 10)

    @patch('tap_mailjet.client.session')
    def test_contact_stream_empty_response(self, mock_session_class):
        """Test contact stream handles empty response."""
        mock_session = MagicMock()
        mock_session_class.return_value = mock_session
        
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"Data": []}
        mock_session.request.return_value = mock_response
        
        catalog = self.create_mock_catalog("contact")
        stream = Contact(self.client, catalog)
        
        records = list(stream.get_records())
        
        # Verify no records returned
        self.assertEqual(len(records), 0)
        
        # Verify only one API call was made
        mock_session.request.assert_called_once()

    @patch('tap_mailjet.client.session')
    def test_contactslist_stream_with_filters(self, mock_session_class):
        """Test contactslist stream with filter parameters."""
        mock_session = MagicMock()
        mock_session_class.return_value = mock_session
        
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "Data": [
                {"ID": 1, "Name": "List 1", "SubscriberCount": 100},
                {"ID": 2, "Name": "List 2", "SubscriberCount": 200}
            ]
        }
        mock_session.request.return_value = mock_response
        
        catalog = self.create_mock_catalog("contactslist")
        stream = Contactslist(self.client, catalog)
        
        # Add custom filter parameters
        stream.update_params(IsDeleted=False)
        
        records = list(stream.get_records())
        
        # Verify results
        self.assertEqual(len(records), 2)
        
        # Verify filter was included in API call
        call_args = mock_session.request.call_args
        self.assertEqual(call_args[1]["params"]["IsDeleted"], False)

    @patch('tap_mailjet.client.session')
    def test_listrecipient_stream_api_authentication(self, mock_session_class):
        """Test that API authentication is applied correctly."""
        mock_session = MagicMock()
        mock_session_class.return_value = mock_session
        
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"Data": []}
        mock_session.request.return_value = mock_response
        
        catalog = self.create_mock_catalog("listrecipient")
        stream = Listrecipient(self.client, catalog)
        
        list(stream.get_records())
        
        # Verify Authorization header was set
        call_args = mock_session.request.call_args
        headers = call_args[1]["headers"]
        self.assertEqual(headers["Authorization"], "Bearer test_token")

    @patch('tap_mailjet.client.session')
    def test_multiple_streams_sequential_sync(self, mock_session_class):
        """Test syncing multiple streams sequentially."""
        mock_session = MagicMock()
        mock_session_class.return_value = mock_session
        
        # Mock different responses for different endpoints
        def mock_request_side_effect(method, url, **kwargs):
            response = MagicMock()
            response.status_code = 200
            
            if "/message" in url:
                response.json.return_value = {
                    "Data": [{"ID": 1, "Status": "sent"}]
                }
            elif "/campaign" in url:
                response.json.return_value = {
                    "Data": [{"ID": 2, "Name": "Campaign 1"}]
                }
            else:
                response.json.return_value = {"Data": []}
            
            return response
        
        mock_session.request.side_effect = mock_request_side_effect
        
        # Sync message stream
        message_catalog = self.create_mock_catalog("message")
        message_stream = Message(self.client, message_catalog)
        message_records = list(message_stream.get_records())
        
        # Sync campaign stream
        campaign_catalog = self.create_mock_catalog("campaign")
        campaign_stream = Campaign(self.client, campaign_catalog)
        campaign_records = list(campaign_stream.get_records())
        
        # Verify results
        self.assertEqual(len(message_records), 1)
        self.assertEqual(message_records[0]["ID"], 1)
        
        self.assertEqual(len(campaign_records), 1)
        self.assertEqual(campaign_records[0]["ID"], 2)
        
        # Verify both API calls were made
        self.assertEqual(mock_session.request.call_count, 2)

    @patch('tap_mailjet.client.session')
    def test_stream_handles_api_timeout(self, mock_session_class):
        """Test that streams properly handle API timeouts with retry."""
        mock_session = MagicMock()
        mock_session_class.return_value = mock_session
        
        from requests.exceptions import Timeout
        
        # First 4 calls timeout, 5th succeeds
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"Data": [{"ID": 1}]}
        
        mock_session.request.side_effect = [
            Timeout("Request timed out"),
            Timeout("Request timed out"),
            Timeout("Request timed out"),
            Timeout("Request timed out"),
            mock_response
        ]
        
        catalog = self.create_mock_catalog("message")
        stream = Message(self.client, catalog)
        
        with patch('time.sleep'):  # Skip actual sleep delays
            records = list(stream.get_records())
        
        # Verify the request eventually succeeded
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["ID"], 1)
        
        # Verify retry logic kicked in
        self.assertEqual(mock_session.request.call_count, 5)

    @patch('tap_mailjet.client.session')
    def test_stream_url_endpoint_construction(self, mock_session_class):
        """Test that streams construct proper URL endpoints."""
        mock_session = MagicMock()
        mock_session_class.return_value = mock_session
        
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"Data": []}
        mock_session.request.return_value = mock_response
        
        # Test each stream's endpoint
        streams_endpoints = [
            (Message, "/v3/REST/message"),
            (Campaign, "/v3/REST/campaign"),
            (Contact, "/v3/REST/contact"),
            (Contactslist, "/v3/REST/contactslist"),
            (Listrecipient, "/v3/REST/listrecipient")
        ]
        
        for stream_class, expected_path in streams_endpoints:
            catalog = self.create_mock_catalog(stream_class.__name__.lower())
            stream = stream_class(self.client, catalog)
            
            # Get URL endpoint
            url = stream.get_url_endpoint()
            
            # Verify correct URL construction
            self.assertEqual(url, f"https://api.mailjet.com{expected_path}")


if __name__ == "__main__":
    unittest.main()
