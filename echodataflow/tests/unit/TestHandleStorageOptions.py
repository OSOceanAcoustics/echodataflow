import pytest
from unittest.mock import Mock, patch
from echodataflow.utils.filesystem_utils import handle_storage_options
from echodataflow.models.datastore import StorageOptions
from prefect_aws import AwsCredentials

# Adjust Mocks setup
Block = Mock(return_value = AwsCredentials(aws_access_key_id="test", aws_secret_access_key='password'))
MockStorageOptions = Mock(return_value = StorageOptions())
load_block = Mock()

@pytest.fixture
def setup_blocks():
    block = Block()
    storage_options = MockStorageOptions()
    return block, storage_options

class TestHandleStorageOptions:
    def test_none(self):
        """Test handling with no parameters."""
        assert handle_storage_options() == {}

    def test_empty_dict(self):
        """Test handling with an empty dictionary."""
        assert handle_storage_options({}) == {}

    def test_anon_dict(self):
        """Test handling with anonymous dictionary."""
        assert handle_storage_options({'anon': True}) == {'anon': True}

    def test_block(self, setup_blocks):
        block, _ = setup_blocks
        expected_dict = {'key': 'test', 'secret': 'password'}
        assert handle_storage_options(block) == expected_dict        

    def test_anonymous_storage_options(self, setup_blocks):
        _, storage_options = setup_blocks
        storage_options.anon = True
        assert handle_storage_options(storage_options) == {"anon": True}

    @patch('echodataflow.utils.filesystem_utils.load_block')
    def test_dict_with_block_name(self, mock_load_block):
        storage_dict = {'block_name': 'echoflow-aws-credentials', 'type': 'AWS'}
        expected_dict = {'key': 'test', 'secret': 'password'}
        block = AwsCredentials(aws_access_key_id="test", aws_secret_access_key='password')
        mock_load_block.return_value = block
        assert handle_storage_options(storage_dict) == expected_dict
        mock_load_block.assert_called_with(name="echoflow-aws-credentials", type="AWS")
    
    @patch('echodataflow.utils.filesystem_utils.load_block')
    def test_storage_options(self, mock_load_block, setup_blocks):
        _, storage_options = setup_blocks
        storage_options.anon = False
        storage_options.block_name = "echoflow-aws-credentials"
        storage_options.type = "AWS"
        expected_dict = {'key': 'test', 'secret': 'password'}
        block = AwsCredentials(aws_access_key_id="test", aws_secret_access_key='password')
        mock_load_block.return_value = block
        assert handle_storage_options(storage_options) == expected_dict
        mock_load_block.assert_called_with(name="echoflow-aws-credentials", type="AWS")