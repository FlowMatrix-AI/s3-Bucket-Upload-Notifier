"""
Integration tests for S3 Upload Notifier Lambda Handler

This module contains integration tests that validate the complete event processing flow,
batch processing with multiple files, and error scenarios with recovery.
"""

import json
import os
import pytest
from unittest.mock import Mock, patch, MagicMock
from botocore.exceptions import ClientError

from handler import lambda_handler


class TestCompleteEventProcessingFlow:
    """Integration tests for complete event processing flow."""

    def setup_method(self):
        """Set up test environment before each test."""
        # Set required environment variable for tests
        os.environ['SNS_TOPIC_ARN'] = 'arn:aws:sns:us-east-1:123456789012:test-topic'

    def teardown_method(self):
        """Clean up test environment after each test."""
        # Clean up environment variables
        if 'SNS_TOPIC_ARN' in os.environ:
            del os.environ['SNS_TOPIC_ARN']

    @patch('src.handler.boto3.client')
    def test_complete_single_file_processing_flow(self, mock_boto3_client):
        """
        Test complete event processing flow for a single file upload.
        
        This test validates the entire pipeline from S3 event reception
        to SNS notification delivery.
        
        Requirements: 1.1, 1.2, 1.3
        """
        # Mock S3 client for content type detection
        mock_s3_client = MagicMock()
        mock_s3_client.head_object.return_value = {
            'ContentType': 'application/pdf',
            'ContentLength': 2048576,
            'LastModified': '2024-01-01T12:00:00Z'
        }
        
        # Mock SNS client for publishing
        mock_sns_client = MagicMock()
        mock_sns_client.publish.return_value = {
            'MessageId': 'test-message-id-12345'
        }
        
        # Configure boto3.client to return appropriate mocks
        def client_side_effect(service_name):
            if service_name == 's3':
                return mock_s3_client
            elif service_name == 'sns':
                return mock_sns_client
            else:
                return MagicMock()
        
        mock_boto3_client.side_effect = client_side_effect
        
        # Create a realistic S3 event
        s3_event = {
            "Records": [
                {
                    "eventVersion": "2.1",
                    "eventSource": "aws:s3",
                    "eventTime": "2024-01-01T12:00:00.000Z",
                    "eventName": "ObjectCreated:Put",
                    "userIdentity": {
                        "principalId": "AWS:AIDACKCEVSQ6C2EXAMPLE"
                    },
                    "requestParameters": {
                        "sourceIPAddress": "203.0.113.5"
                    },
                    "responseElements": {
                        "x-amz-request-id": "C3D13FE58DE4C810",
                        "x-amz-id-2": "FMyUVURIY8/IgAtTv8xRjskZQpcIZ9KG4V5Wp6S7S/JRWeUWerMUE5JgHvANOjpD"
                    },
                    "s3": {
                        "s3SchemaVersion": "1.0",
                        "configurationId": "testConfigRule",
                        "bucket": {
                            "name": "file-uploads-123456789012-us-east-1-dev",
                            "ownerIdentity": {
                                "principalId": "A3NL1KOZZKExample"
                            },
                            "arn": "arn:aws:s3:::file-uploads-123456789012-us-east-1-dev"
                        },
                        "object": {
                            "key": "documents/report.pdf",
                            "size": 2048576,
                            "eTag": "0123456789abcdef0123456789abcdef",
                            "sequencer": "0A1B2C3D4E5F678901"
                        }
                    }
                }
            ]
        }
        
        # Create mock context
        mock_context = Mock()
        mock_context.aws_request_id = 'test-request-id-12345'
        
        # Call the lambda handler
        response = lambda_handler(s3_event, mock_context)
        
        # Verify successful response
        assert response['statusCode'] == 200
        
        # Parse response body
        body = json.loads(response['body'])
        assert body['message'] == 'Processing completed successfully'
        assert body['processed_count'] == 1
        assert body['processed_files'] == ['report.pdf']
        assert body['errors'] == []
        
        # Verify S3 head_object was called for content type detection
        mock_s3_client.head_object.assert_called_once_with(
            Bucket='file-uploads-123456789012-us-east-1-dev',
            Key='documents/report.pdf'
        )
        
        # Verify SNS publish was called
        mock_sns_client.publish.assert_called_once()
        
        # Verify SNS publish parameters
        call_args = mock_sns_client.publish.call_args
        kwargs = call_args.kwargs
        
        assert kwargs['TopicArn'] == 'arn:aws:sns:us-east-1:123456789012:test-topic'
        assert 'Subject' in kwargs
        assert 'Message' in kwargs
        
        # Verify subject contains file name
        subject = kwargs['Subject']
        assert 'report.pdf' in subject
        assert subject.startswith('📁 New File Upload:')
        
        # Verify message contains all required information
        message = kwargs['Message']
        assert 'report.pdf' in message
        assert 'file-uploads-123456789012-us-east-1-dev' in message
        assert '1.95 MB' in message  # Formatted file size (2048576 bytes = 1.95 MB)
        assert 'application/pdf' in message
        assert '2024-01-01T12:00:00.000Z' in message
        assert 'documents/report.pdf' in message

    @patch('src.handler.boto3.client')
    def test_complete_batch_processing_flow(self, mock_boto3_client):
        """
        Test complete event processing flow for multiple files (batch processing).
        
        This test validates batch processing with multiple files in a single
        Lambda invocation, ensuring all files are processed correctly.
        
        Requirements: 1.1, 1.2, 1.3
        """
        # Mock S3 client for content type detection
        mock_s3_client = MagicMock()
        
        # Configure different responses for different files
        def s3_head_object_side_effect(**kwargs):
            object_key = kwargs['Key']
            if 'image.jpg' in object_key:
                return {
                    'ContentType': 'image/jpeg',
                    'ContentLength': 1048576,
                    'LastModified': '2024-01-01T12:00:00Z'
                }
            elif 'document.pdf' in object_key:
                return {
                    'ContentType': 'application/pdf',
                    'ContentLength': 2048576,
                    'LastModified': '2024-01-01T12:01:00Z'
                }
            elif 'data.json' in object_key:
                return {
                    'ContentType': 'application/json',
                    'ContentLength': 4096,
                    'LastModified': '2024-01-01T12:02:00Z'
                }
            else:
                return {
                    'ContentType': 'application/octet-stream',
                    'ContentLength': 1024,
                    'LastModified': '2024-01-01T12:03:00Z'
                }
        
        mock_s3_client.head_object.side_effect = s3_head_object_side_effect
        
        # Mock SNS client for publishing
        mock_sns_client = MagicMock()
        
        # Configure SNS to return different message IDs for each call
        message_ids = ['msg-id-1', 'msg-id-2', 'msg-id-3']
        mock_sns_client.publish.side_effect = [
            {'MessageId': msg_id} for msg_id in message_ids
        ]
        
        # Configure boto3.client to return appropriate mocks
        def client_side_effect(service_name):
            if service_name == 's3':
                return mock_s3_client
            elif service_name == 'sns':
                return mock_sns_client
            else:
                return MagicMock()
        
        mock_boto3_client.side_effect = client_side_effect
        
        # Create S3 event with multiple files
        s3_event = {
            "Records": [
                {
                    "eventVersion": "2.1",
                    "eventSource": "aws:s3",
                    "eventTime": "2024-01-01T12:00:00.000Z",
                    "eventName": "ObjectCreated:Put",
                    "awsRegion": "us-east-1",
                    "s3": {
                        "bucket": {"name": "test-bucket"},
                        "object": {"key": "photos/image.jpg", "size": 1048576}
                    }
                },
                {
                    "eventVersion": "2.1",
                    "eventSource": "aws:s3",
                    "eventTime": "2024-01-01T12:01:00.000Z",
                    "eventName": "ObjectCreated:Post",
                    "awsRegion": "us-east-1",
                    "s3": {
                        "bucket": {"name": "test-bucket"},
                        "object": {"key": "documents/document.pdf", "size": 2048576}
                    }
                },
                {
                    "eventVersion": "2.1",
                    "eventSource": "aws:s3",
                    "eventTime": "2024-01-01T12:02:00.000Z",
                    "eventName": "ObjectCreated:Copy",
                    "awsRegion": "us-east-1",
                    "s3": {
                        "bucket": {"name": "test-bucket"},
                        "object": {"key": "api/data.json", "size": 4096}
                    }
                }
            ]
        }
        
        # Create mock context
        mock_context = Mock()
        mock_context.aws_request_id = 'batch-request-id-12345'
        
        # Call the lambda handler
        response = lambda_handler(s3_event, mock_context)
        
        # Verify successful response
        assert response['statusCode'] == 200
        
        # Parse response body
        body = json.loads(response['body'])
        assert body['message'] == 'Processing completed successfully'
        assert body['processed_count'] == 3
        assert set(body['processed_files']) == {'image.jpg', 'document.pdf', 'data.json'}
        assert body['errors'] == []
        
        # Verify S3 head_object was called for each file
        assert mock_s3_client.head_object.call_count == 3
        
        # Verify all expected S3 calls were made
        expected_s3_calls = [
            {'Bucket': 'test-bucket', 'Key': 'photos/image.jpg'},
            {'Bucket': 'test-bucket', 'Key': 'documents/document.pdf'},
            {'Bucket': 'test-bucket', 'Key': 'api/data.json'}
        ]
        
        actual_s3_calls = [call.kwargs for call in mock_s3_client.head_object.call_args_list]
        for expected_call in expected_s3_calls:
            assert expected_call in actual_s3_calls
        
        # Verify SNS publish was called for each file
        assert mock_sns_client.publish.call_count == 3
        
        # Verify each SNS call has correct topic ARN
        for call in mock_sns_client.publish.call_args_list:
            kwargs = call.kwargs
            assert kwargs['TopicArn'] == 'arn:aws:sns:us-east-1:123456789012:test-topic'
            assert 'Subject' in kwargs
            assert 'Message' in kwargs

    @patch('src.handler.boto3.client')
    def test_mixed_success_and_error_processing(self, mock_boto3_client):
        """
        Test processing flow with mixed success and error scenarios.
        
        This test validates that the system continues processing valid records
        even when some records fail, and accurately reports both successes and errors.
        
        Requirements: 1.1, 1.2, 1.3
        """
        # Mock S3 client
        mock_s3_client = MagicMock()
        
        # Configure S3 to succeed for some files and fail for others
        def s3_head_object_side_effect(**kwargs):
            object_key = kwargs['Key']
            if 'valid-file.txt' in object_key:
                return {
                    'ContentType': 'text/plain',
                    'ContentLength': 1024,
                    'LastModified': '2024-01-01T12:00:00Z'
                }
            elif 'restricted-file.pdf' in object_key:
                # Simulate access denied error
                error_response = {
                    'Error': {
                        'Code': 'AccessDenied',
                        'Message': 'Access Denied'
                    }
                }
                raise ClientError(error_response, 'HeadObject')
            else:
                return {
                    'ContentType': 'application/octet-stream',
                    'ContentLength': 2048,
                    'LastModified': '2024-01-01T12:00:00Z'
                }
        
        mock_s3_client.head_object.side_effect = s3_head_object_side_effect
        
        # Mock SNS client
        mock_sns_client = MagicMock()
        
        # Configure SNS to succeed for most calls but fail for one
        def sns_publish_side_effect(**kwargs):
            subject = kwargs.get('Subject', '')
            if 'failing-notification.txt' in subject:
                # Simulate SNS publishing failure
                error_response = {
                    'Error': {
                        'Code': 'InvalidTopicArn',
                        'Message': 'Invalid topic ARN'
                    }
                }
                raise ClientError(error_response, 'Publish')
            else:
                return {'MessageId': f'msg-id-{len(mock_sns_client.publish.call_args_list)}'}
        
        mock_sns_client.publish.side_effect = sns_publish_side_effect
        
        # Configure boto3.client to return appropriate mocks
        def client_side_effect(service_name):
            if service_name == 's3':
                return mock_s3_client
            elif service_name == 'sns':
                return mock_sns_client
            else:
                return MagicMock()
        
        mock_boto3_client.side_effect = client_side_effect
        
        # Create S3 event with mixed scenarios
        s3_event = {
            "Records": [
                # Valid record that should succeed
                {
                    "eventVersion": "2.1",
                    "eventSource": "aws:s3",
                    "eventTime": "2024-01-01T12:00:00.000Z",
                    "eventName": "ObjectCreated:Put",
                    "awsRegion": "us-east-1",
                    "s3": {
                        "bucket": {"name": "test-bucket"},
                        "object": {"key": "uploads/valid-file.txt", "size": 1024}
                    }
                },
                # Record with S3 access denied (should continue processing)
                {
                    "eventVersion": "2.1",
                    "eventSource": "aws:s3",
                    "eventTime": "2024-01-01T12:01:00.000Z",
                    "eventName": "ObjectCreated:Put",
                    "awsRegion": "us-east-1",
                    "s3": {
                        "bucket": {"name": "test-bucket"},
                        "object": {"key": "restricted/restricted-file.pdf", "size": 2048}
                    }
                },
                # Malformed record (missing required fields)
                {
                    "eventVersion": "2.1",
                    "eventSource": "aws:s3",
                    "eventTime": "2024-01-01T12:02:00.000Z",
                    "eventName": "ObjectCreated:Put",
                    "awsRegion": "us-east-1",
                    "s3": {
                        "bucket": {"name": "test-bucket"},
                        # Missing 'object' field - should be skipped
                    }
                },
                # Valid record but SNS will fail
                {
                    "eventVersion": "2.1",
                    "eventSource": "aws:s3",
                    "eventTime": "2024-01-01T12:03:00.000Z",
                    "eventName": "ObjectCreated:Put",
                    "awsRegion": "us-east-1",
                    "s3": {
                        "bucket": {"name": "test-bucket"},
                        "object": {"key": "uploads/failing-notification.txt", "size": 512}
                    }
                }
            ]
        }
        
        # Create mock context
        mock_context = Mock()
        mock_context.aws_request_id = 'mixed-scenario-request-id'
        
        # Call the lambda handler
        response = lambda_handler(s3_event, mock_context)
        
        # Verify response indicates partial success
        assert response['statusCode'] == 200
        
        # Parse response body
        body = json.loads(response['body'])
        assert body['message'] == 'Processing completed successfully'
        
        # Should have 1 successful file (valid-file.txt)
        # restricted-file.pdf should succeed in processing but use default content type
        # failing-notification.txt should fail at SNS publishing
        # malformed record should be skipped
        
        assert body['processed_count'] == 2  # valid-file.txt and restricted-file.pdf
        assert 'valid-file.txt' in body['processed_files']
        assert 'restricted-file.pdf' in body['processed_files']
        
        # Should have errors for malformed record and SNS failure
        assert len(body['errors']) == 2
        
        # Verify error messages contain relevant information
        error_messages = ' '.join(body['errors'])
        assert 'Failed to process S3 record' in error_messages or 'Failed to send notification' in error_messages


class TestErrorScenariosAndRecovery:
    """Integration tests for error scenarios and recovery mechanisms."""

    def setup_method(self):
        """Set up test environment before each test."""
        os.environ['SNS_TOPIC_ARN'] = 'arn:aws:sns:us-east-1:123456789012:test-topic'

    def teardown_method(self):
        """Clean up test environment after each test."""
        if 'SNS_TOPIC_ARN' in os.environ:
            del os.environ['SNS_TOPIC_ARN']

    def test_missing_environment_variable_error_recovery(self):
        """
        Test error handling when required environment variables are missing.
        
        The system should fail fast with a clear error message and not attempt
        to process any records when configuration is invalid.
        
        Requirements: 1.1, 1.2, 1.3
        """
        # Remove the required environment variable
        if 'SNS_TOPIC_ARN' in os.environ:
            del os.environ['SNS_TOPIC_ARN']
        
        # Create a valid S3 event
        s3_event = {
            "Records": [
                {
                    "eventVersion": "2.1",
                    "eventSource": "aws:s3",
                    "eventTime": "2024-01-01T12:00:00.000Z",
                    "eventName": "ObjectCreated:Put",
                    "awsRegion": "us-east-1",
                    "s3": {
                        "bucket": {"name": "test-bucket"},
                        "object": {"key": "test-file.txt", "size": 1024}
                    }
                }
            ]
        }
        
        # Create mock context
        mock_context = Mock()
        mock_context.aws_request_id = 'config-error-request-id'
        
        # Call the lambda handler
        response = lambda_handler(s3_event, mock_context)
        
        # Verify error response
        assert response['statusCode'] == 500
        
        # Parse response body
        body = json.loads(response['body'])
        assert body['error'] == 'Configuration error'
        assert 'SNS_TOPIC_ARN' in body['message']

    @patch('src.handler.boto3.client')
    @patch('src.handler.time.sleep')  # Mock sleep to speed up tests
    def test_sns_retry_mechanism_integration(self, mock_sleep, mock_boto3_client):
        """
        Test SNS retry mechanism in the context of complete event processing.
        
        This test validates that SNS publishing failures trigger retry logic
        and eventual success or failure is properly handled and reported.
        
        Requirements: 1.1, 1.2, 1.3
        """
        # Mock S3 client
        mock_s3_client = MagicMock()
        mock_s3_client.head_object.return_value = {
            'ContentType': 'text/plain',
            'ContentLength': 1024,
            'LastModified': '2024-01-01T12:00:00Z'
        }
        
        # Mock SNS client with retry scenario
        mock_sns_client = MagicMock()
        
        # Configure SNS to fail first, then succeed on retry
        error_response = {
            'Error': {
                'Code': 'Throttling',
                'Message': 'Rate exceeded'
            }
        }
        mock_sns_client.publish.side_effect = [
            ClientError(error_response, 'Publish'),  # First attempt fails
            {'MessageId': 'retry-success-msg-id'}    # Retry succeeds
        ]
        
        # Configure boto3.client to return appropriate mocks
        def client_side_effect(service_name):
            if service_name == 's3':
                return mock_s3_client
            elif service_name == 'sns':
                return mock_sns_client
            else:
                return MagicMock()
        
        mock_boto3_client.side_effect = client_side_effect
        
        # Create S3 event
        s3_event = {
            "Records": [
                {
                    "eventVersion": "2.1",
                    "eventSource": "aws:s3",
                    "eventTime": "2024-01-01T12:00:00.000Z",
                    "eventName": "ObjectCreated:Put",
                    "awsRegion": "us-east-1",
                    "s3": {
                        "bucket": {"name": "test-bucket"},
                        "object": {"key": "retry-test.txt", "size": 1024}
                    }
                }
            ]
        }
        
        # Create mock context
        mock_context = Mock()
        mock_context.aws_request_id = 'retry-test-request-id'
        
        # Call the lambda handler
        response = lambda_handler(s3_event, mock_context)
        
        # Verify successful response (after retry)
        assert response['statusCode'] == 200
        
        # Parse response body
        body = json.loads(response['body'])
        assert body['message'] == 'Processing completed successfully'
        assert body['processed_count'] == 1
        assert body['processed_files'] == ['retry-test.txt']
        assert body['errors'] == []
        
        # Verify retry mechanism was triggered
        assert mock_sns_client.publish.call_count == 2  # Initial + 1 retry
        mock_sleep.assert_called_once_with(1)  # Verify backoff delay

    def test_empty_event_handling(self):
        """
        Test handling of empty events (no records).
        
        The system should handle empty events gracefully and return
        appropriate success response indicating no processing was needed.
        
        Requirements: 1.1, 1.2, 1.3
        """
        # Create empty S3 event
        s3_event = {"Records": []}
        
        # Create mock context
        mock_context = Mock()
        mock_context.aws_request_id = 'empty-event-request-id'
        
        # Call the lambda handler
        response = lambda_handler(s3_event, mock_context)
        
        # Verify successful response
        assert response['statusCode'] == 200
        
        # Parse response body
        body = json.loads(response['body'])
        assert body['message'] == 'No records to process'
        assert body['processed_count'] == 0
        assert body['processed_files'] == []
        assert body['errors'] == []

    def test_malformed_event_handling(self):
        """
        Test handling of completely malformed events.
        
        The system should handle malformed events gracefully and return
        appropriate success response with error details.
        
        Requirements: 1.1, 1.2, 1.3
        """
        # Create event with no Records field
        malformed_event = {"SomeOtherField": "value"}
        
        # Create mock context
        mock_context = Mock()
        mock_context.aws_request_id = 'malformed-event-request-id'
        
        # Call the lambda handler
        response = lambda_handler(malformed_event, mock_context)
        
        # Verify successful response (graceful handling)
        assert response['statusCode'] == 200
        
        # Parse response body
        body = json.loads(response['body'])
        assert body['message'] == 'No records to process'
        assert body['processed_count'] == 0
        assert body['processed_files'] == []
        assert body['errors'] == []


class TestEndToEndScenarios:
    """Integration tests for realistic end-to-end scenarios."""

    def setup_method(self):
        """Set up test environment before each test."""
        os.environ['SNS_TOPIC_ARN'] = 'arn:aws:sns:us-east-1:123456789012:test-topic'

    def teardown_method(self):
        """Clean up test environment after each test."""
        if 'SNS_TOPIC_ARN' in os.environ:
            del os.environ['SNS_TOPIC_ARN']

    @patch('src.handler.boto3.client')
    def test_realistic_file_upload_scenario(self, mock_boto3_client):
        """
        Test a realistic file upload scenario with various file types.
        
        This test simulates a real-world scenario where different types of files
        are uploaded to different folders in an S3 bucket, validating the complete
        processing pipeline for each file type.
        
        Requirements: 1.1, 1.2, 1.3
        """
        # Mock S3 client with realistic responses
        mock_s3_client = MagicMock()
        
        def s3_head_object_side_effect(**kwargs):
            object_key = kwargs['Key']
            
            # Return appropriate content types based on file extensions
            if object_key.endswith('.pdf'):
                return {
                    'ContentType': 'application/pdf',
                    'ContentLength': 2048576,  # 2MB
                    'LastModified': '2024-01-01T12:00:00Z'
                }
            elif object_key.endswith('.jpg') or object_key.endswith('.jpeg'):
                return {
                    'ContentType': 'image/jpeg',
                    'ContentLength': 1048576,  # 1MB
                    'LastModified': '2024-01-01T12:01:00Z'
                }
            elif object_key.endswith('.docx'):
                return {
                    'ContentType': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
                    'ContentLength': 512000,  # 500KB
                    'LastModified': '2024-01-01T12:02:00Z'
                }
            elif object_key.endswith('.csv'):
                return {
                    'ContentType': 'text/csv',
                    'ContentLength': 102400,  # 100KB
                    'LastModified': '2024-01-01T12:03:00Z'
                }
            else:
                return {
                    'ContentType': 'application/octet-stream',
                    'ContentLength': 1024,
                    'LastModified': '2024-01-01T12:04:00Z'
                }
        
        mock_s3_client.head_object.side_effect = s3_head_object_side_effect
        
        # Mock SNS client
        mock_sns_client = MagicMock()
        mock_sns_client.publish.side_effect = [
            {'MessageId': f'msg-id-{i}'} for i in range(10)
        ]
        
        # Configure boto3.client to return appropriate mocks
        def client_side_effect(service_name):
            if service_name == 's3':
                return mock_s3_client
            elif service_name == 'sns':
                return mock_sns_client
            else:
                return MagicMock()
        
        mock_boto3_client.side_effect = client_side_effect
        
        # Create realistic S3 event with various file types
        s3_event = {
            "Records": [
                {
                    "eventVersion": "2.1",
                    "eventSource": "aws:s3",
                    "eventTime": "2024-01-01T12:00:00.000Z",
                    "eventName": "ObjectCreated:Put",
                    "awsRegion": "us-east-1",
                    "s3": {
                        "bucket": {"name": "company-documents-bucket"},
                        "object": {"key": "reports/quarterly-report-2024-q1.pdf", "size": 2048576}
                    }
                },
                {
                    "eventVersion": "2.1",
                    "eventSource": "aws:s3",
                    "eventTime": "2024-01-01T12:01:00.000Z",
                    "eventName": "ObjectCreated:Put",
                    "awsRegion": "us-east-1",
                    "s3": {
                        "bucket": {"name": "company-documents-bucket"},
                        "object": {"key": "images/team-photo-2024.jpg", "size": 1048576}
                    }
                },
                {
                    "eventVersion": "2.1",
                    "eventSource": "aws:s3",
                    "eventTime": "2024-01-01T12:02:00.000Z",
                    "eventName": "ObjectCreated:Post",
                    "awsRegion": "us-east-1",
                    "s3": {
                        "bucket": {"name": "company-documents-bucket"},
                        "object": {"key": "contracts/client-agreement-draft.docx", "size": 512000}
                    }
                },
                {
                    "eventVersion": "2.1",
                    "eventSource": "aws:s3",
                    "eventTime": "2024-01-01T12:03:00.000Z",
                    "eventName": "ObjectCreated:Copy",
                    "awsRegion": "us-east-1",
                    "s3": {
                        "bucket": {"name": "company-documents-bucket"},
                        "object": {"key": "data/sales-data-january.csv", "size": 102400}
                    }
                }
            ]
        }
        
        # Create mock context
        mock_context = Mock()
        mock_context.aws_request_id = 'realistic-scenario-request-id'
        
        # Call the lambda handler
        response = lambda_handler(s3_event, mock_context)
        
        # Verify successful response
        assert response['statusCode'] == 200
        
        # Parse response body
        body = json.loads(response['body'])
        assert body['message'] == 'Processing completed successfully'
        assert body['processed_count'] == 4
        
        expected_files = [
            'quarterly-report-2024-q1.pdf',
            'team-photo-2024.jpg',
            'client-agreement-draft.docx',
            'sales-data-january.csv'
        ]
        assert set(body['processed_files']) == set(expected_files)
        assert body['errors'] == []
        
        # Verify all S3 head_object calls were made
        assert mock_s3_client.head_object.call_count == 4
        
        # Verify all SNS publish calls were made
        assert mock_sns_client.publish.call_count == 4
        
        # Verify each SNS message contains appropriate content
        for call in mock_sns_client.publish.call_args_list:
            kwargs = call.kwargs
            message = kwargs['Message']
            
            # Each message should contain the bucket name
            assert 'company-documents-bucket' in message
            
            # Each message should contain formatted file sizes
            assert any(size_unit in message for size_unit in ['KB', 'MB', 'GB', 'B'])
            
            # Each message should contain appropriate content types
            content_types = ['application/pdf', 'image/jpeg', 'application/vnd.openxmlformats-officedocument.wordprocessingml.document', 'text/csv']
            assert any(content_type in message for content_type in content_types)

    @patch('src.handler.boto3.client')
    def test_large_batch_processing_performance(self, mock_boto3_client):
        """
        Test processing performance with a large batch of files.
        
        This test validates that the system can handle a large number of files
        in a single Lambda invocation without timing out or failing.
        
        Requirements: 1.1, 1.2, 1.3
        """
        # Mock S3 client
        mock_s3_client = MagicMock()
        mock_s3_client.head_object.return_value = {
            'ContentType': 'text/plain',
            'ContentLength': 1024,
            'LastModified': '2024-01-01T12:00:00Z'
        }
        
        # Mock SNS client
        mock_sns_client = MagicMock()
        mock_sns_client.publish.side_effect = [
            {'MessageId': f'msg-id-{i}'} for i in range(50)
        ]
        
        # Configure boto3.client to return appropriate mocks
        def client_side_effect(service_name):
            if service_name == 's3':
                return mock_s3_client
            elif service_name == 'sns':
                return mock_sns_client
            else:
                return MagicMock()
        
        mock_boto3_client.side_effect = client_side_effect
        
        # Create S3 event with many files (simulating bulk upload)
        records = []
        for i in range(25):  # 25 files - reasonable batch size for Lambda
            record = {
                "eventVersion": "2.1",
                "eventSource": "aws:s3",
                "eventTime": f"2024-01-01T12:{i:02d}:00.000Z",
                "eventName": "ObjectCreated:Put",
                "awsRegion": "us-east-1",
                "s3": {
                    "bucket": {"name": "bulk-upload-bucket"},
                    "object": {"key": f"batch-upload/file-{i:03d}.txt", "size": 1024 + i}
                }
            }
            records.append(record)
        
        s3_event = {"Records": records}
        
        # Create mock context
        mock_context = Mock()
        mock_context.aws_request_id = 'large-batch-request-id'
        
        # Call the lambda handler
        response = lambda_handler(s3_event, mock_context)
        
        # Verify successful response
        assert response['statusCode'] == 200
        
        # Parse response body
        body = json.loads(response['body'])
        assert body['message'] == 'Processing completed successfully'
        assert body['processed_count'] == 25
        assert len(body['processed_files']) == 25
        assert body['errors'] == []
        
        # Verify all files were processed
        expected_files = [f'file-{i:03d}.txt' for i in range(25)]
        assert set(body['processed_files']) == set(expected_files)
        
        # Verify all S3 and SNS calls were made
        assert mock_s3_client.head_object.call_count == 25
        assert mock_sns_client.publish.call_count == 25