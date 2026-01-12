from moto.core.decorator import mock_aws
import pytest
from moto import mock_aws
from unittest.mock import patch, MagicMock
from botocore.exceptions import ClientError
import base64
import hashlib
from app.replicator import replicate_file
from app.config import s3_client 

@pytest.fixture
def mock_gcs_blob():
    mock_blob = MagicMock()
    mock_blob.exists.return_value = False
    mock_blob.reload = MagicMock()
    mock_blob.upload_from_file = MagicMock()
    mock_blob.delete = MagicMock()
    mock_blob.metadata = {}
    mock_blob.patch = MagicMock()
    return mock_blob

#  Test: Successful replication of new file

@mock_aws
def test_replicate_new_file(mock_gcs_blob):
   
    conn = s3_client 
    conn.create_bucket(Bucket='test-bucket', CreateBucketConfiguration={'LocationConstraint': 'eu-north-1'})
    test_data = b'test data'
    conn.put_object(Bucket='test-bucket', Key='file.csv', Body=test_data)
    
    
    with patch('app.replicator.gcs_bucket.blob', return_value=mock_gcs_blob):
        result = replicate_file('test-bucket', 'file.csv')
    
    assert "Successfully replicated" in result
    mock_gcs_blob.upload_from_file.assert_called()
    
    mock_gcs_blob.patch.assert_called()


 #Test: Idempotent skip for single-part file with matching hash

@mock_aws
def test_idempotent_skip_single_part(mock_gcs_blob):
    conn = s3_client
    conn.create_bucket(Bucket='test-bucket', CreateBucketConfiguration={'LocationConstraint': 'eu-north-1'})
    test_data = b'test data'
    conn.put_object(Bucket='test-bucket', Key='file.csv', Body=test_data)
    
    
    expected_md5_hex = hashlib.md5(test_data).hexdigest()
    expected_gcs_md5_base64 = base64.b64encode(hashlib.md5(test_data).digest()).decode()
    
    
    mock_gcs_blob.exists.return_value = True
    mock_gcs_blob.md5_hash = expected_gcs_md5_base64
    
    with patch('app.replicator.gcs_bucket.blob', return_value=mock_gcs_blob):
        result = replicate_file('test-bucket', 'file.csv')
    
    assert "idempotent skip" in result
    mock_gcs_blob.upload_from_file.assert_not_called()

  # Test: Idempotent skip logic for multipart ETag

@mock_aws
def test_idempotent_skip_multipart_etag(mock_gcs_blob):
    conn = s3_client
    conn.create_bucket(Bucket='test-bucket', CreateBucketConfiguration={'LocationConstraint': 'eu-north-1'})
   
    conn.put_object(Bucket='test-bucket', Key='file.csv', Body=b'test data', Metadata={'etag': 'fake-md5-2'})
    
   
    mock_gcs_blob.exists.return_value = True
    mock_gcs_blob.md5_hash = None 
    
    with patch('app.replicator.gcs_bucket.blob', return_value=mock_gcs_blob):
        with patch('app.replicator.s3_client.head_object', return_value={'ETag': '"fake-md5-2"'}):
            result = replicate_file('test-bucket', 'file.csv')
    
  


  # Test: Checksum mismatch triggers deletion

@mock_aws   
def test_checksum_mismatch(mock_gcs_blob):
    conn = s3_client
    conn.create_bucket(Bucket='test-bucket', CreateBucketConfiguration={'LocationConstraint': 'eu-north-1'})
    test_data = b'test data'
    conn.put_object(Bucket='test-bucket', Key='file.csv', Body=test_data)
    
    # Mock upload but force mismatch
    def fake_upload(*args, **kwargs):
        pass  # Simulate upload
    
    mock_gcs_blob.upload_from_file = fake_upload
    
    with patch('app.replicator.gcs_bucket.blob', return_value=mock_gcs_blob):
        with patch('app.replicator.hashlib.md5', return_value=MagicMock(hexdigest=lambda: 'mismatch')):  
            with pytest.raises(ValueError, match="Checksum mismatch"):
                replicate_file('test-bucket', 'file.csv')
    
    mock_gcs_blob.delete.assert_called()

#  Test: Checksum mismatch triggers deletion

@mock_aws
def test_retry_on_transient_error(mock_gcs_blob):
    conn = s3_client
    conn.create_bucket(Bucket='test-bucket', CreateBucketConfiguration={'LocationConstraint': 'eu-north-1'})
    conn.put_object(Bucket='test-bucket', Key='file.csv', Body=b'test data')
    
    # Mock transient error on get_object, then success
    side_effects = [ClientError({'Error': {'Code': '500'}}, 'get_object'), {'Body': MagicMock(read=MagicMock(side_effect=[b'test data', b'']))}]
    with patch('app.replicator.s3_client.get_object', side_effect=side_effects):
        with patch('app.replicator.gcs_bucket.blob', return_value=mock_gcs_blob):
            result = replicate_file('test-bucket', 'file.csv')
    
    assert "Successfully" in result

 #  Test: S3 object not found triggers error

@mock_aws
def test_s3_not_found():
    conn = s3_client
    conn.create_bucket(Bucket='test-bucket', CreateBucketConfiguration={'LocationConstraint': 'eu-north-1'})
    
    with pytest.raises(ValueError, match="S3 object not found"):
        replicate_file('test-bucket', 'missing.csv')
