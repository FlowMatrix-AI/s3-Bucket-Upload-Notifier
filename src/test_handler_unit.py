"""
Unit tests for S3 Upload Notifier Lambda Handler

This module contains unit tests for specific functionality including
content type detection, error handling, and edge cases.
"""

import json
import os
import pytest
from unittest.mock import Mock, patch, MagicMock
from botocore.exceptions import ClientError, BotoCoreError

from handler import get_content_type, send_notification, validate_environment, lambda_handler, process_s3_record


class TestContentTypeDetection:
    """Unit tests for content type detection functionality."""

    @patch('handler.boto3.client')
    def test_get_content_type_various_file_types(self, mock_boto3_client):
        """
        Test content type detection for various file types and extensions.
        
        Requirements: 2.3, 2.5
        """
        # Mock S3 client
        mock_s3_client = Mock()
        mock_boto3_client.return_value = mock_s3_client
        
        # Test cases for different file types
        test_cases = [
            # (object_key, expected_content_type)
            ("document.pdf", "application/pdf"),
            ("image.jpg", "image/jpeg"),
            ("image.jpeg", "image/jpeg"),
            ("image.png", "image/png"),
            ("image.gif", "image/gif"),
            ("video.mp4", "video/mp4"),
            ("audio.mp3", "audio/mpeg"),
            ("text.txt", "text/plain"),
            ("data.json", "application/json"),
            ("style.css", "text/css"),
            ("script.js", "application/javascript"),
            ("archive.zip", "application/zip"),
            ("spreadsheet.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
            ("presentation.pptx", "application/vnd.openxmlformats-officedocument.presentationml.presentation"),
            ("document.docx", "application/vnd.openxmlformats-officedocument.wordprocessingml.document"),
        ]
        
        for object_key, expected_content_type in test_cases:
            # Configure mock response
            mock_s3_client.head_object.return_value = {
                'ContentType': expected_content_type,
                'ContentLength': 1024,
                'LastModified': '2024-01-01T12:00:00Z'
            }
            
            # Call function
            result = get_content_type("test-bucket", object_key)
            
            # Verify result
            assert result == expected_content_type, f"Expected {expected_content_type} for {object_key}, got {result}"
            
            # Verify S3 client was called correctly
            mock_s3_client.head_object.assert_called_with(Bucket="test-bucket", Key=object_key)
            
            # Reset mock for next iteration
            mock_s3_client.reset_mock()

    @patch('handler.boto3.client')
    def test_get_content_type_missing_content_type(self, mock_boto3_client):
        """
        Test content type detection when S3 response doesn't include ContentType.
        
        Requirements: 2.3, 2.5
        """
        # Mock S3 client
        mock_s3_client = Mock()
        mock_boto3_client.return_value = mock_s3_client
        
        # Configure mock response without ContentType
        mock_s3_client.head_object.return_value = {
            'ContentLength': 1024,
            'LastModified': '2024-01-01T12:00:00Z'
            # No ContentType field
        }
        
        # Call function
        result = get_content_type("test-bucket", "unknown-file.xyz")
        
        # Should return default content type
        assert result == "application/octet-stream"
        
        # Verify S3 client was called correctly
        mock_s3_client.head_object.assert_called_with(Bucket="test-bucket", Key="unknown-file.xyz")

    @patch('handler.boto3.client')
    def test_get_content_type_empty_content_type(self, mock_boto3_client):
        """
        Test content type detection when S3 response has empty ContentType.
        
        Requirements: 2.3, 2.5
        """
        # Mock S3 client
        mock_s3_client = Mock()
        mock_boto3_client.return_value = mock_s3_client
        
        # Configure mock response with empty ContentType
        mock_s3_client.head_object.return_value = {
            'ContentType': '',  # Empty content type
            'ContentLength': 1024,
            'LastModified': '2024-01-01T12:00:00Z'
        }
        
        # Call function
        result = get_content_type("test-bucket", "empty-content-type.file")
        
        # Should return default content type
        assert result == "application/octet-stream"

    @patch('handler.boto3.client')
    def test_get_content_type_none_content_type(self, mock_boto3_client):
        """
        Test content type detection when S3 response has None ContentType.
        
        Requirements: 2.3, 2.5
        """
        # Mock S3 client
        mock_s3_client = Mock()
        mock_boto3_client.return_value = mock_s3_client
        
        # Configure mock response with None ContentType
        mock_s3_client.head_object.return_value = {
            'ContentType': None,  # None content type
            'ContentLength': 1024,
            'LastModified': '2024-01-01T12:00:00Z'
        }
        
        # Call function
        result = get_content_type("test-bucket", "none-content-type.file")
        
        # Should return default content type
        assert result == "application/octet-stream"

    @patch('handler.boto3.client')
    def test_get_content_type_no_such_key_error(self, mock_boto3_client):
        """
        Test error handling when S3 object doesn't exist (NoSuchKey).
        
        Requirements: 2.3, 2.5
        """
        # Mock S3 client
        mock_s3_client = Mock()
        mock_boto3_client.return_value = mock_s3_client
        
        # Configure mock to raise NoSuchKey error
        error_response = {
            'Error': {
                'Code': 'NoSuchKey',
                'Message': 'The specified key does not exist.'
            }
        }
        mock_s3_client.head_object.side_effect = ClientError(error_response, 'HeadObject')
        
        # Call function
        result = get_content_type("test-bucket", "nonexistent-file.txt")
        
        # Should return default content type
        assert result == "application/octet-stream"
        
        # Verify S3 client was called correctly
        mock_s3_client.head_object.assert_called_with(Bucket="test-bucket", Key="nonexistent-file.txt")

    @patch('handler.boto3.client')
    def test_get_content_type_no_such_bucket_error(self, mock_boto3_client):
        """
        Test error handling when S3 bucket doesn't exist (NoSuchBucket).
        
        Requirements: 2.3, 2.5
        """
        # Mock S3 client
        mock_s3_client = Mock()
        mock_boto3_client.return_value = mock_s3_client
        
        # Configure mock to raise NoSuchBucket error
        error_response = {
            'Error': {
                'Code': 'NoSuchBucket',
                'Message': 'The specified bucket does not exist.'
            }
        }
        mock_s3_client.head_object.side_effect = ClientError(error_response, 'HeadObject')
        
        # Call function
        result = get_content_type("nonexistent-bucket", "test-file.txt")
        
        # Should return default content type
        assert result == "application/octet-stream"

    @patch('handler.boto3.client')
    def test_get_content_type_access_denied_error(self, mock_boto3_client):
        """
        Test error handling when access is denied to S3 object (AccessDenied).
        
        Requirements: 2.3, 2.5
        """
        # Mock S3 client
        mock_s3_client = Mock()
        mock_boto3_client.return_value = mock_s3_client
        
        # Configure mock to raise AccessDenied error
        error_response = {
            'Error': {
                'Code': 'AccessDenied',
                'Message': 'Access Denied'
            }
        }
        mock_s3_client.head_object.side_effect = ClientError(error_response, 'HeadObject')
        
        # Call function
        result = get_content_type("restricted-bucket", "restricted-file.txt")
        
        # Should return default content type
        assert result == "application/octet-stream"

    @patch('handler.boto3.client')
    def test_get_content_type_generic_client_error(self, mock_boto3_client):
        """
        Test error handling for generic S3 client errors.
        
        Requirements: 2.3, 2.5
        """
        # Mock S3 client
        mock_s3_client = Mock()
        mock_boto3_client.return_value = mock_s3_client
        
        # Configure mock to raise generic client error
        error_response = {
            'Error': {
                'Code': 'InternalError',
                'Message': 'We encountered an internal error. Please try again.'
            }
        }
        mock_s3_client.head_object.side_effect = ClientError(error_response, 'HeadObject')
        
        # Call function
        result = get_content_type("test-bucket", "test-file.txt")
        
        # Should return default content type
        assert result == "application/octet-stream"

    @patch('handler.boto3.client')
    def test_get_content_type_botocore_error(self, mock_boto3_client):
        """
        Test error handling for BotoCore errors (network issues, etc.).
        
        Requirements: 2.3, 2.5
        """
        # Mock S3 client
        mock_s3_client = Mock()
        mock_boto3_client.return_value = mock_s3_client
        
        # Configure mock to raise BotoCoreError
        mock_s3_client.head_object.side_effect = BotoCoreError()
        
        # Call function
        result = get_content_type("test-bucket", "test-file.txt")
        
        # Should return default content type
        assert result == "application/octet-stream"

    @patch('handler.boto3.client')
    def test_get_content_type_unexpected_error(self, mock_boto3_client):
        """
        Test error handling for unexpected exceptions.
        
        Requirements: 2.3, 2.5
        """
        # Mock S3 client
        mock_s3_client = Mock()
        mock_boto3_client.return_value = mock_s3_client
        
        # Configure mock to raise unexpected error
        mock_s3_client.head_object.side_effect = Exception("Unexpected error occurred")
        
        # Call function
        result = get_content_type("test-bucket", "test-file.txt")
        
        # Should return default content type
        assert result == "application/octet-stream"

    @patch('handler.boto3.client')
    def test_get_content_type_special_characters_in_key(self, mock_boto3_client):
        """
        Test content type detection with special characters in object key.
        
        Requirements: 2.3, 2.5
        """
        # Mock S3 client
        mock_s3_client = Mock()
        mock_boto3_client.return_value = mock_s3_client
        
        # Configure mock response
        mock_s3_client.head_object.return_value = {
            'ContentType': 'text/plain',
            'ContentLength': 1024,
            'LastModified': '2024-01-01T12:00:00Z'
        }
        
        # Test with special characters in object key
        special_keys = [
            "files with spaces.txt",
            "files-with-dashes.txt",
            "files_with_underscores.txt",
            "files.with.dots.txt",
            "files(with)parentheses.txt",
            "files[with]brackets.txt",
            "files{with}braces.txt",
            "files@with@symbols.txt",
            "files#with#hash.txt",
            "files%20with%20encoding.txt",
            "files+with+plus.txt",
            "files&with&ampersand.txt",
            "files=with=equals.txt",
            "files?with?question.txt",
            "files:with:colon.txt",
            "files;with;semicolon.txt",
            "files,with,comma.txt",
            "files'with'quotes.txt",
            "files\"with\"doublequotes.txt",
            "files<with>angles.txt",
            "files|with|pipes.txt",
            "files\\with\\backslash.txt",
            "files/with/forward/slash.txt",
            "files~with~tilde.txt",
            "files`with`backtick.txt",
            "files!with!exclamation.txt",
            "files$with$dollar.txt",
            "files^with^caret.txt",
            "files*with*asterisk.txt",
        ]
        
        for object_key in special_keys:
            # Call function
            result = get_content_type("test-bucket", object_key)
            
            # Should return the content type from S3
            assert result == "text/plain", f"Failed for object key: {object_key}"
            
            # Verify S3 client was called correctly
            mock_s3_client.head_object.assert_called_with(Bucket="test-bucket", Key=object_key)
            
            # Reset mock for next iteration
            mock_s3_client.reset_mock()

    @patch('handler.boto3.client')
    def test_get_content_type_unicode_characters_in_key(self, mock_boto3_client):
        """
        Test content type detection with Unicode characters in object key.
        
        Requirements: 2.3, 2.5
        """
        # Mock S3 client
        mock_s3_client = Mock()
        mock_boto3_client.return_value = mock_s3_client
        
        # Configure mock response
        mock_s3_client.head_object.return_value = {
            'ContentType': 'text/plain',
            'ContentLength': 1024,
            'LastModified': '2024-01-01T12:00:00Z'
        }
        
        # Test with Unicode characters in object key
        unicode_keys = [
            "файл.txt",  # Cyrillic
            "文件.txt",   # Chinese
            "ファイル.txt", # Japanese
            "파일.txt",   # Korean
            "αρχείο.txt", # Greek
            "ملف.txt",    # Arabic
            "קובץ.txt",   # Hebrew
            "फ़ाइल.txt",   # Hindi
            "dosya.txt",  # Turkish (with special characters)
            "tệp.txt",    # Vietnamese
            "файл_с_пробелами и символами.txt",  # Mixed
            "émojis_🎉_file.txt",  # Emojis
        ]
        
        for object_key in unicode_keys:
            # Call function
            result = get_content_type("test-bucket", object_key)
            
            # Should return the content type from S3
            assert result == "text/plain", f"Failed for Unicode object key: {object_key}"
            
            # Verify S3 client was called correctly
            mock_s3_client.head_object.assert_called_with(Bucket="test-bucket", Key=object_key)
            
            # Reset mock for next iteration
            mock_s3_client.reset_mock()

    @patch('handler.boto3.client')
    def test_get_content_type_very_long_key(self, mock_boto3_client):
        """
        Test content type detection with very long object key.
        
        Requirements: 2.3, 2.5
        """
        # Mock S3 client
        mock_s3_client = Mock()
        mock_boto3_client.return_value = mock_s3_client
        
        # Configure mock response
        mock_s3_client.head_object.return_value = {
            'ContentType': 'application/pdf',
            'ContentLength': 1024,
            'LastModified': '2024-01-01T12:00:00Z'
        }
        
        # Create a very long object key (close to S3's 1024 character limit)
        long_key = "very/long/path/" + "a" * 900 + "/document.pdf"
        
        # Call function
        result = get_content_type("test-bucket", long_key)
        
        # Should return the content type from S3
        assert result == "application/pdf"
        
        # Verify S3 client was called correctly
        mock_s3_client.head_object.assert_called_with(Bucket="test-bucket", Key=long_key)

    @patch('handler.boto3.client')
    def test_get_content_type_empty_key(self, mock_boto3_client):
        """
        Test content type detection with empty object key.
        
        Requirements: 2.3, 2.5
        """
        # Mock S3 client
        mock_s3_client = Mock()
        mock_boto3_client.return_value = mock_s3_client
        
        # Configure mock to raise error for empty key (S3 would reject this)
        error_response = {
            'Error': {
                'Code': 'InvalidRequest',
                'Message': 'Invalid request'
            }
        }
        mock_s3_client.head_object.side_effect = ClientError(error_response, 'HeadObject')
        
        # Call function with empty key
        result = get_content_type("test-bucket", "")
        
        # Should return default content type
        assert result == "application/octet-stream"

    @patch('handler.boto3.client')
    def test_get_content_type_case_sensitivity(self, mock_boto3_client):
        """
        Test that content type detection preserves case sensitivity of object keys.
        
        Requirements: 2.3, 2.5
        """
        # Mock S3 client
        mock_s3_client = Mock()
        mock_boto3_client.return_value = mock_s3_client
        
        # Configure mock response
        mock_s3_client.head_object.return_value = {
            'ContentType': 'text/plain',
            'ContentLength': 1024,
            'LastModified': '2024-01-01T12:00:00Z'
        }
        
        # Test case-sensitive object keys
        case_sensitive_keys = [
            "File.TXT",
            "FILE.txt",
            "file.TXT",
            "CamelCase.pdf",
            "UPPERCASE.PDF",
            "lowercase.pdf",
            "MiXeD_CaSe.DoC",
        ]
        
        for object_key in case_sensitive_keys:
            # Call function
            result = get_content_type("test-bucket", object_key)
            
            # Should return the content type from S3
            assert result == "text/plain", f"Failed for case-sensitive key: {object_key}"
            
            # Verify S3 client was called with exact key (preserving case)
            mock_s3_client.head_object.assert_called_with(Bucket="test-bucket", Key=object_key)
            
            # Reset mock for next iteration
            mock_s3_client.reset_mock()


class TestSNSPublishingErrorScenarios:
    """Unit tests for SNS publishing failures and retry logic."""

    @patch('handler.boto3.client')
    @patch('handler.time.sleep')  # Mock sleep to speed up tests
    def test_sns_publishing_failure_with_retry_success(self, mock_sleep, mock_boto3_client):
        """
        Test SNS publishing fails once then succeeds on retry.
        
        Requirements: 4.3, 5.1
        """
        # Mock SNS client
        mock_sns_client = Mock()
        mock_boto3_client.return_value = mock_sns_client
        
        # Configure mock to fail first, succeed second
        error_response = {
            'Error': {
                'Code': 'Throttling',
                'Message': 'Rate exceeded'
            }
        }
        mock_sns_client.publish.side_effect = [
            ClientError(error_response, 'Publish'),  # First call fails
            {'MessageId': 'test-message-id-123'}     # Second call succeeds
        ]
        
        # Test file info
        file_info = {
            'file_name': 'test.txt',
            'file_size': 1024,
            'bucket_name': 'test-bucket',
            'object_key': 'test.txt',
            'event_time': '2024-01-01T12:00:00Z',
            'aws_region': 'us-east-1'
        }
        
        # Call function - should succeed after retry
        send_notification(file_info, 'arn:aws:sns:us-east-1:123456789012:test-topic')
        
        # Verify retry logic was called
        assert mock_sns_client.publish.call_count == 2
        mock_sleep.assert_called_once_with(1)  # Verify backoff delay

    @patch('handler.boto3.client')
    @patch('handler.time.sleep')
    def test_sns_publishing_failure_exhausts_retries(self, mock_sleep, mock_boto3_client):
        """
        Test SNS publishing fails on all retry attempts.
        
        Requirements: 4.3, 5.1
        """
        # Mock SNS client
        mock_sns_client = Mock()
        mock_boto3_client.return_value = mock_sns_client
        
        # Configure mock to always fail
        error_response = {
            'Error': {
                'Code': 'InvalidTopicArn',
                'Message': 'Invalid topic ARN'
            }
        }
        mock_sns_client.publish.side_effect = ClientError(error_response, 'Publish')
        
        # Test file info
        file_info = {
            'file_name': 'test.txt',
            'file_size': 1024,
            'bucket_name': 'test-bucket',
            'object_key': 'test.txt',
            'event_time': '2024-01-01T12:00:00Z',
            'aws_region': 'us-east-1'
        }
        
        # Call function - should raise exception after retries
        with pytest.raises(ClientError):
            send_notification(file_info, 'arn:aws:sns:us-east-1:123456789012:invalid-topic')
        
        # Verify retry logic was attempted (original + 1 retry = 2 calls)
        assert mock_sns_client.publish.call_count == 2
        mock_sleep.assert_called_once_with(1)

    @patch('handler.boto3.client')
    @patch('handler.time.sleep')
    def test_sns_publishing_unexpected_error_with_retry(self, mock_sleep, mock_boto3_client):
        """
        Test SNS publishing with unexpected errors and retry logic.
        
        Requirements: 4.3, 5.1
        """
        # Mock SNS client
        mock_sns_client = Mock()
        mock_boto3_client.return_value = mock_sns_client
        
        # Configure mock to raise unexpected error
        mock_sns_client.publish.side_effect = Exception("Network timeout")
        
        # Test file info
        file_info = {
            'file_name': 'test.txt',
            'file_size': 1024,
            'bucket_name': 'test-bucket',
            'object_key': 'test.txt',
            'event_time': '2024-01-01T12:00:00Z',
            'aws_region': 'us-east-1'
        }
        
        # Call function - should raise exception after retries
        with pytest.raises(Exception):
            send_notification(file_info, 'arn:aws:sns:us-east-1:123456789012:test-topic')
        
        # Verify retry logic was attempted
        assert mock_sns_client.publish.call_count == 2
        mock_sleep.assert_called_once_with(1)

    def test_sns_notification_missing_required_fields(self):
        """
        Test SNS notification with missing required fields in file_info.
        
        Requirements: 5.1, 5.4
        """
        # Test cases with missing required fields
        incomplete_file_infos = [
            {},  # Empty dict
            {'file_name': 'test.txt'},  # Missing other fields
            {'file_name': 'test.txt', 'file_size': 1024},  # Missing bucket_name, etc.
            {
                'file_name': 'test.txt',
                'file_size': 1024,
                'bucket_name': 'test-bucket'
                # Missing object_key, event_time, aws_region
            }
        ]
        
        for file_info in incomplete_file_infos:
            with pytest.raises(ValueError, match="Missing required field"):
                send_notification(file_info, 'arn:aws:sns:us-east-1:123456789012:test-topic')


class TestMalformedS3EventHandling:
    """Unit tests for handling malformed S3 events."""

    def test_process_s3_record_non_s3_event(self):
        """
        Test processing non-S3 event records.
        
        Requirements: 5.1, 5.4
        """
        # Test with non-S3 event
        non_s3_record = {
            'eventSource': 'aws:dynamodb',
            'eventName': 'INSERT',
            'dynamodb': {'Keys': {'id': {'S': 'test'}}}
        }
        
        result = process_s3_record(non_s3_record)
        assert result is None

    def test_process_s3_record_missing_s3_field(self):
        """
        Test processing S3 record with missing 's3' field.
        
        Requirements: 5.1, 5.4
        """
        malformed_record = {
            'eventSource': 'aws:s3',
            'eventName': 'ObjectCreated:Put',
            'eventTime': '2024-01-01T12:00:00Z',
            # Missing 's3' field
        }
        
        result = process_s3_record(malformed_record)
        assert result is None

    def test_process_s3_record_missing_bucket_info(self):
        """
        Test processing S3 record with missing bucket information.
        
        Requirements: 5.1, 5.4
        """
        malformed_record = {
            'eventSource': 'aws:s3',
            'eventName': 'ObjectCreated:Put',
            'eventTime': '2024-01-01T12:00:00Z',
            's3': {
                # Missing 'bucket' field
                'object': {
                    'key': 'test.txt',
                    'size': 1024
                }
            }
        }
        
        result = process_s3_record(malformed_record)
        assert result is None

    def test_process_s3_record_missing_object_info(self):
        """
        Test processing S3 record with missing object information.
        
        Requirements: 5.1, 5.4
        """
        malformed_record = {
            'eventSource': 'aws:s3',
            'eventName': 'ObjectCreated:Put',
            'eventTime': '2024-01-01T12:00:00Z',
            's3': {
                'bucket': {'name': 'test-bucket'},
                # Missing 'object' field
            }
        }
        
        result = process_s3_record(malformed_record)
        assert result is None

    def test_process_s3_record_missing_object_key(self):
        """
        Test processing S3 record with missing object key.
        
        Requirements: 5.1, 5.4
        """
        malformed_record = {
            'eventSource': 'aws:s3',
            'eventName': 'ObjectCreated:Put',
            'eventTime': '2024-01-01T12:00:00Z',
            's3': {
                'bucket': {'name': 'test-bucket'},
                'object': {
                    # Missing 'key' field
                    'size': 1024
                }
            }
        }
        
        result = process_s3_record(malformed_record)
        assert result is None

    def test_process_s3_record_missing_file_size(self):
        """
        Test processing S3 record with missing file size.
        
        Requirements: 5.1, 5.4
        """
        malformed_record = {
            'eventSource': 'aws:s3',
            'eventName': 'ObjectCreated:Put',
            'eventTime': '2024-01-01T12:00:00Z',
            's3': {
                'bucket': {'name': 'test-bucket'},
                'object': {
                    'key': 'test.txt'
                    # Missing 'size' field
                }
            }
        }
        
        result = process_s3_record(malformed_record)
        assert result is None

    def test_process_s3_record_invalid_file_size(self):
        """
        Test processing S3 record with invalid file size format.
        
        Requirements: 5.1, 5.4
        """
        malformed_record = {
            'eventSource': 'aws:s3',
            'eventName': 'ObjectCreated:Put',
            'eventTime': '2024-01-01T12:00:00Z',
            's3': {
                'bucket': {'name': 'test-bucket'},
                'object': {
                    'key': 'test.txt',
                    'size': 'invalid-size'  # Invalid size format
                }
            }
        }
        
        result = process_s3_record(malformed_record)
        assert result is None

    def test_process_s3_record_missing_event_time(self):
        """
        Test processing S3 record with missing event time.
        
        Requirements: 5.1, 5.4
        """
        malformed_record = {
            'eventSource': 'aws:s3',
            'eventName': 'ObjectCreated:Put',
            # Missing 'eventTime' field
            's3': {
                'bucket': {'name': 'test-bucket'},
                'object': {
                    'key': 'test.txt',
                    'size': 1024
                }
            }
        }
        
        result = process_s3_record(malformed_record)
        assert result is None

    def test_process_s3_record_url_decoding_failure(self):
        """
        Test processing S3 record with URL decoding failure.
        
        Requirements: 5.1, 5.4
        """
        # Create a record with an object key that might cause URL decoding issues
        record_with_encoding_issues = {
            'eventSource': 'aws:s3',
            'eventName': 'ObjectCreated:Put',
            'eventTime': '2024-01-01T12:00:00Z',
            'awsRegion': 'us-east-1',
            's3': {
                'bucket': {'name': 'test-bucket'},
                'object': {
                    'key': 'test%ZZ%invalid.txt',  # Invalid URL encoding
                    'size': 1024
                }
            }
        }
        
        # Should handle gracefully and return metadata with original key
        result = process_s3_record(record_with_encoding_issues)
        assert result is not None
        assert result['object_key'] == 'test%ZZ%invalid.txt'  # Should use original key
        assert result['file_name'] == 'test%ZZ%invalid.txt'

    def test_process_s3_record_unexpected_exception(self):
        """
        Test processing S3 record with unexpected exception during processing.
        
        Requirements: 5.1, 5.4
        """
        # Create a malformed record that will cause an exception during processing
        # This record has a structure that will pass initial validation but fail later
        malformed_record = {
            'eventSource': 'aws:s3',
            'eventName': 'ObjectCreated:Put',
            'eventTime': '2024-01-01T12:00:00Z',
            'awsRegion': 'us-east-1',
            's3': {
                'bucket': {'name': 'test-bucket'},
                'object': {
                    'key': 'test.txt',
                    'size': None  # This will cause an exception when trying to convert to int
                }
            }
        }
        
        result = process_s3_record(malformed_record)
        assert result is None


class TestEnvironmentVariableHandling:
    """Unit tests for missing environment variables."""

    def test_validate_environment_missing_sns_topic_arn(self):
        """
        Test validation when SNS_TOPIC_ARN environment variable is missing.
        
        Requirements: 5.1, 5.3
        """
        # Ensure SNS_TOPIC_ARN is not set
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(ValueError, match="SNS_TOPIC_ARN environment variable is required"):
                validate_environment()

    def test_validate_environment_empty_sns_topic_arn(self):
        """
        Test validation when SNS_TOPIC_ARN environment variable is empty.
        
        Requirements: 5.1, 5.3
        """
        # Set empty SNS_TOPIC_ARN
        with patch.dict(os.environ, {'SNS_TOPIC_ARN': ''}):
            with pytest.raises(ValueError, match="SNS_TOPIC_ARN environment variable is required"):
                validate_environment()

    def test_validate_environment_invalid_sns_arn_format(self):
        """
        Test validation when SNS_TOPIC_ARN has invalid format.
        
        Requirements: 5.1, 5.3
        """
        # Set invalid SNS ARN format
        invalid_arns = [
            'invalid-arn',
            'arn:aws:s3:::bucket-name',  # S3 ARN instead of SNS
            'arn:aws:sns:',  # Incomplete ARN
            'not-an-arn-at-all',
            'arn:aws:lambda:us-east-1:123456789012:function:test'  # Lambda ARN
        ]
        
        for invalid_arn in invalid_arns:
            with patch.dict(os.environ, {'SNS_TOPIC_ARN': invalid_arn}):
                with pytest.raises(ValueError, match="Invalid SNS topic ARN format"):
                    validate_environment()

    def test_validate_environment_valid_sns_arn(self):
        """
        Test validation with valid SNS topic ARN.
        
        Requirements: 5.3
        """
        valid_arn = 'arn:aws:sns:us-east-1:123456789012:test-topic'
        
        with patch.dict(os.environ, {'SNS_TOPIC_ARN': valid_arn}):
            result = validate_environment()
            assert result == valid_arn

    @patch('handler.validate_environment')
    @patch('handler.setup_logging')
    def test_lambda_handler_missing_environment_variables(self, mock_setup_logging, mock_validate_env):
        """
        Test Lambda handler behavior when environment variables are missing.
        
        Requirements: 4.3, 5.1, 5.3
        """
        # Mock logger
        mock_logger = Mock()
        mock_setup_logging.return_value = mock_logger
        
        # Mock context
        mock_context = Mock()
        mock_context.aws_request_id = 'test-request-id'
        
        # Configure validate_environment to raise ValueError
        mock_validate_env.side_effect = ValueError("SNS_TOPIC_ARN environment variable is required")
        
        # Test event
        test_event = {
            'Records': [
                {
                    'eventSource': 'aws:s3',
                    'eventName': 'ObjectCreated:Put',
                    'eventTime': '2024-01-01T12:00:00Z',
                    'awsRegion': 'us-east-1',
                    's3': {
                        'bucket': {'name': 'test-bucket'},
                        'object': {'key': 'test.txt', 'size': 1024}
                    }
                }
            ]
        }
        
        # Call lambda handler
        response = lambda_handler(test_event, mock_context)
        
        # Should return error response
        assert response['statusCode'] == 500
        body = json.loads(response['body'])
        assert body['error'] == 'Configuration error'
        assert 'SNS_TOPIC_ARN' in body['message']

    @patch('handler.validate_environment')
    @patch('handler.setup_logging')
    def test_lambda_handler_unexpected_error(self, mock_setup_logging, mock_validate_env):
        """
        Test Lambda handler behavior with unexpected errors.
        
        Requirements: 5.1
        """
        # Mock logger
        mock_logger = Mock()
        mock_setup_logging.return_value = mock_logger
        
        # Mock context
        mock_context = Mock()
        mock_context.aws_request_id = 'test-request-id'
        
        # Configure validate_environment to raise unexpected error
        mock_validate_env.side_effect = Exception("Unexpected system error")
        
        # Test event
        test_event = {
            'Records': [
                {
                    'eventSource': 'aws:s3',
                    'eventName': 'ObjectCreated:Put',
                    'eventTime': '2024-01-01T12:00:00Z',
                    'awsRegion': 'us-east-1',
                    's3': {
                        'bucket': {'name': 'test-bucket'},
                        'object': {'key': 'test.txt', 'size': 1024}
                    }
                }
            ]
        }
        
        # Call lambda handler
        response = lambda_handler(test_event, mock_context)
        
        # Should return error response
        assert response['statusCode'] == 500
        body = json.loads(response['body'])
        assert body['error'] == 'Internal server error'
        assert 'Unexpected system error' in body['message']

    @patch('handler.validate_environment')
    @patch('handler.setup_logging')
    def test_lambda_handler_empty_records(self, mock_setup_logging, mock_validate_env):
        """
        Test Lambda handler behavior with empty records.
        
        Requirements: 5.1, 5.4
        """
        # Mock logger
        mock_logger = Mock()
        mock_setup_logging.return_value = mock_logger
        
        # Mock context
        mock_context = Mock()
        mock_context.aws_request_id = 'test-request-id'
        
        # Configure validate_environment to return valid ARN
        mock_validate_env.return_value = 'arn:aws:sns:us-east-1:123456789012:test-topic'
        
        # Test event with empty records
        test_event = {'Records': []}
        
        # Call lambda handler
        response = lambda_handler(test_event, mock_context)
        
        # Should return success response with no processing
        assert response['statusCode'] == 200
        body = json.loads(response['body'])
        assert body['message'] == 'No records to process'
        assert body['processed_count'] == 0
        assert body['processed_files'] == []
        assert body['errors'] == []

    @patch('handler.validate_environment')
    @patch('handler.setup_logging')
    def test_lambda_handler_no_records_field(self, mock_setup_logging, mock_validate_env):
        """
        Test Lambda handler behavior when event has no Records field.
        
        Requirements: 5.1, 5.4
        """
        # Mock logger
        mock_logger = Mock()
        mock_setup_logging.return_value = mock_logger
        
        # Mock context
        mock_context = Mock()
        mock_context.aws_request_id = 'test-request-id'
        
        # Configure validate_environment to return valid ARN
        mock_validate_env.return_value = 'arn:aws:sns:us-east-1:123456789012:test-topic'
        
        # Test event without Records field
        test_event = {}
        
        # Call lambda handler
        response = lambda_handler(test_event, mock_context)
        
        # Should return success response with no processing
        assert response['statusCode'] == 200
        body = json.loads(response['body'])
        assert body['message'] == 'No records to process'
        assert body['processed_count'] == 0