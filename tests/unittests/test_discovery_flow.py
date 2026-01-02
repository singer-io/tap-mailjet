"""Unit tests for discovery flow."""
import unittest
from unittest.mock import patch, MagicMock
from tap_mailjet import discover
from singer import Catalog, metadata as singer_metadata


class TestDiscoveryFlow(unittest.TestCase):
    """Test cases for discovery flow."""

    @patch('tap_mailjet.discover.get_schemas')
    def test_discover_returns_catalog(self, mock_get_schemas):
        """Test that discover returns a valid Catalog object."""
        mock_schemas = {
            'messages': {
                'type': 'object',
                'properties': {}
            }
        }
        mock_metadata = {
            'messages': singer_metadata.get_standard_metadata(
                schema=mock_schemas['messages'],
                key_properties=['ID'],
                replication_method='INCREMENTAL',
                valid_replication_keys=['ArrivedAt']
            )
        }
        mock_get_schemas.return_value = (mock_schemas, mock_metadata)
        
        catalog = discover()
        self.assertIsInstance(catalog, Catalog)

    @patch('tap_mailjet.discover.get_schemas')
    def test_discover_all_streams(self, mock_get_schemas):
        """Test that discover returns all available streams."""
        mock_schemas = {
            'messages': {
                'type': 'object',
                'properties': {}
            },
            'contacts': {
                'type': 'object',
                'properties': {}
            }
        }
        mock_metadata = {
            'messages': singer_metadata.get_standard_metadata(
                schema=mock_schemas['messages'],
                key_properties=['ID'],
                replication_method='INCREMENTAL',
                valid_replication_keys=['ArrivedAt']
            ),
            'contacts': singer_metadata.get_standard_metadata(
                schema=mock_schemas['contacts'],
                key_properties=['ID'],
                replication_method='FULL_TABLE'
            )
        }
        mock_get_schemas.return_value = (mock_schemas, mock_metadata)
        
        catalog = discover()
        self.assertEqual(len(catalog.streams), 2)

    @patch('tap_mailjet.discover.get_schemas')
    def test_discover_catalog_entry_attributes(self, mock_get_schemas):
        """Test that CatalogEntry has all required attributes."""
        mock_schemas = {
            'messages': {
                'type': 'object',
                'properties': {}
            }
        }
        mock_metadata = {
            'messages': singer_metadata.get_standard_metadata(
                schema=mock_schemas['messages'],
                key_properties=['ID'],
                replication_method='INCREMENTAL',
                valid_replication_keys=['ArrivedAt']
            )
        }
        mock_get_schemas.return_value = (mock_schemas, mock_metadata)
        
        catalog = discover()
        entry = catalog.streams[0]
        
        self.assertTrue(hasattr(entry, 'tap_stream_id'))
        self.assertTrue(hasattr(entry, 'schema'))
        self.assertTrue(hasattr(entry, 'key_properties'))
        self.assertTrue(hasattr(entry, 'metadata'))

    @patch('tap_mailjet.discover.get_schemas')
    def test_discover_empty_schemas(self, mock_get_schemas):
        """Test discovery with no schemas."""
        mock_get_schemas.return_value = ({}, {})
        
        catalog = discover()
        self.assertEqual(len(catalog.streams), 0)

    @patch('tap_mailjet.discover.get_schemas')
    def test_discover_key_properties(self, mock_get_schemas):
        """Test that key properties are correctly set."""
        mock_schemas = {
            'messages': {
                'type': 'object',
                'properties': {}
            }
        }
        mock_metadata = {
            'messages': singer_metadata.get_standard_metadata(
                schema=mock_schemas['messages'],
                key_properties=['ID', 'MessageID'],
                replication_method='INCREMENTAL',
                valid_replication_keys=['ArrivedAt']
            )
        }
        mock_get_schemas.return_value = (mock_schemas, mock_metadata)
        
        catalog = discover()
        entry = catalog.streams[0]
        
        self.assertEqual(entry.key_properties, ['ID', 'MessageID'])

    @patch('tap_mailjet.discover.get_schemas')
    def test_discover_metadata_structure(self, mock_get_schemas):
        """Test that metadata is properly structured."""
        mock_schemas = {
            'messages': {
                'type': 'object',
                'properties': {}
            }
        }
        mock_metadata = {
            'messages': singer_metadata.get_standard_metadata(
                schema=mock_schemas['messages'],
                key_properties=['ID'],
                replication_method='INCREMENTAL',
                valid_replication_keys=['ArrivedAt']
            )
        }
        mock_get_schemas.return_value = (mock_schemas, mock_metadata)
        
        catalog = discover()
        entry = catalog.streams[0]
        
        self.assertIsNotNone(entry.metadata)
        self.assertIsInstance(entry.metadata, list)

    @patch('tap_mailjet.discover.get_schemas')
    def test_discover_schema_validation(self, mock_get_schemas):
        """Test that schemas are properly validated during discovery."""
        mock_schemas = {
            'messages': {
                'type': 'object',
                'properties': {
                    'ID': {'type': 'integer'},
                    'ArrivedAt': {'type': 'string', 'format': 'date-time'}
                }
            }
        }
        mock_metadata = {
            'messages': singer_metadata.get_standard_metadata(
                schema=mock_schemas['messages'],
                key_properties=['ID'],
                replication_method='INCREMENTAL',
                valid_replication_keys=['ArrivedAt']
            )
        }
        mock_get_schemas.return_value = (mock_schemas, mock_metadata)
        
        catalog = discover()
        entry = catalog.streams[0]
        
        self.assertIn('properties', entry.schema.to_dict())
        self.assertIn('ID', entry.schema.to_dict()['properties'])

    @patch('tap_mailjet.discover.get_schemas')
    def test_discover_handles_schema_error(self, mock_get_schemas):
        """Test that discovery handles schema errors properly."""
        mock_get_schemas.side_effect = Exception("Schema loading failed")
        
        with self.assertRaises(Exception) as context:
            discover()
        
        self.assertIn("Schema loading failed", str(context.exception))
