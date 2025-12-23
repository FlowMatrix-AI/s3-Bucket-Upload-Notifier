"""
S3 File Upload Notifier Lambda Handler

This module contains the main Lambda handler for processing S3 upload events
and sending notifications via SNS.
"""

import json
import logging
import os
import time
from typing import Dict, Any, List, Optional
from urllib.parse import unquote_plus
import boto3
from botocore.exceptions import ClientError, BotoCoreError


def get_content_type(bucket_name: str, object_key: str) -> str:
    """
    Determine the content type of an S3 object using head_object API.
    
    This function retrieves the content type metadata from S3 for the specified
    object. It handles S3 API errors gracefully and returns appropriate defaults
    for unknown or inaccessible objects.
    
    Args:
        bucket_name (str): Name of the S3 bucket
        object_key (str): S3 object key (path to the file)
        
    Returns:
        str: Content type of the object, or default if unavailable
        
    Examples:
        get_content_type("my-bucket", "document.pdf") -> "application/pdf"
        get_content_type("my-bucket", "image.jpg") -> "image/jpeg"
        get_content_type("my-bucket", "unknown.xyz") -> "application/octet-stream"
    """
    logger = logging.getLogger(__name__)
    
    # Default content type for unknown files
    default_content_type = "application/octet-stream"
    
    try:
        # Initialize S3 client
        s3_client = boto3.client('s3')
        
        # Call head_object to get metadata
        response = s3_client.head_object(Bucket=bucket_name, Key=object_key)
        
        # Extract content type from response
        content_type = response.get('ContentType')
        
        if content_type:
            logger.debug(f"Retrieved content type for {object_key}: {content_type}")
            return content_type
        else:
            logger.warning(f"No content type found for {object_key}, using default")
            return default_content_type
            
    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', 'Unknown')
        
        if error_code == 'NoSuchKey':
            logger.warning(f"Object not found: {object_key}")
        elif error_code == 'NoSuchBucket':
            logger.warning(f"Bucket not found: {bucket_name}")
        elif error_code == 'AccessDenied':
            logger.warning(f"Access denied for object: {object_key}")
        else:
            logger.warning(f"S3 client error ({error_code}) for {object_key}: {e}")
        
        return default_content_type
        
    except BotoCoreError as e:
        logger.warning(f"BotoCore error retrieving content type for {object_key}: {e}")
        return default_content_type
        
    except Exception as e:
        logger.warning(f"Unexpected error retrieving content type for {object_key}: {e}")
        return default_content_type


def format_file_size(size_bytes: int) -> str:
    """
    Format file size in bytes to human-readable units.
    
    Converts file size from bytes to appropriate units (B, KB, MB, GB, TB)
    with proper precision and unit selection. Handles edge cases including
    zero bytes and very large files.
    
    Args:
        size_bytes (int): File size in bytes
        
    Returns:
        str: Formatted file size with appropriate unit
        
    Examples:
        format_file_size(0) -> "0 B"
        format_file_size(1024) -> "1.0 KB"
        format_file_size(1536) -> "1.5 KB"
        format_file_size(1048576) -> "1.0 MB"
    """
    if not isinstance(size_bytes, int) or size_bytes < 0:
        raise ValueError(f"File size must be a non-negative integer, got: {size_bytes}")
    
    # Handle zero bytes case
    if size_bytes == 0:
        return "0 B"
    
    # Define units and their byte values
    units = [
        ('TB', 1024**4),  # 1,099,511,627,776 bytes
        ('GB', 1024**3),  # 1,073,741,824 bytes
        ('MB', 1024**2),  # 1,048,576 bytes
        ('KB', 1024**1),  # 1,024 bytes
        ('B', 1)          # 1 byte
    ]
    
    # Find the appropriate unit
    for unit_name, unit_size in units:
        if size_bytes >= unit_size:
            # Calculate the size in this unit
            size_in_unit = size_bytes / unit_size
            
            # Format with appropriate precision
            if unit_name == 'B':
                # Bytes should always be whole numbers
                return f"{size_bytes} B"
            elif size_in_unit >= 100:
                # For values >= 100, show no decimal places
                return f"{size_in_unit:.0f} {unit_name}"
            elif size_in_unit >= 10:
                # For values >= 10, show 1 decimal place
                return f"{size_in_unit:.1f} {unit_name}"
            else:
                # For values < 10, show 2 decimal places for better precision
                return f"{size_in_unit:.2f} {unit_name}"
    
    # This should never be reached due to the 'B' case, but included for safety
    return f"{size_bytes} B"


def setup_logging() -> logging.Logger:
    """
    Set up logging configuration for the Lambda function.
    
    Returns:
        logging.Logger: Configured logger instance
    """
    # Get log level from environment variable, default to INFO
    log_level = os.environ.get('LOG_LEVEL', 'INFO').upper()
    
    # Configure logging
    logging.basicConfig(
        level=getattr(logging, log_level, logging.INFO),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    logger = logging.getLogger(__name__)
    return logger


def process_s3_record(record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Process a single S3 event record and extract file metadata.
    
    This function extracts file information from an S3 event record,
    including bucket name, object key, file size, and event details.
    It handles URL decoding for object keys and validates the event structure.
    
    Args:
        record (Dict[str, Any]): Single S3 event record from the event payload
        
    Returns:
        Optional[Dict[str, Any]]: Dictionary containing extracted file metadata,
                                 or None if the record is invalid
        
    The returned dictionary contains:
        - bucket_name: Name of the S3 bucket
        - file_name: Original filename (URL decoded)
        - object_key: Full S3 object key (URL decoded)
        - file_size: File size in bytes
        - event_time: ISO timestamp of the event
        - event_type: Type of S3 event (e.g., ObjectCreated:Put)
        - aws_region: AWS region where the event occurred
    """
    logger = logging.getLogger(__name__)
    
    try:
        # Validate that this is an S3 event
        if record.get('eventSource') != 'aws:s3':
            logger.warning(f"Skipping non-S3 event: {record.get('eventSource')}")
            return None
        
        # Extract S3 event data
        s3_data = record.get('s3')
        if not s3_data:
            logger.error("Missing 's3' field in event record")
            return None
        
        # Extract bucket information
        bucket_info = s3_data.get('bucket')
        if not bucket_info or not bucket_info.get('name'):
            logger.error("Missing bucket information in S3 event")
            return None
        
        bucket_name = bucket_info['name']
        
        # Extract object information
        object_info = s3_data.get('object')
        if not object_info:
            logger.error("Missing object information in S3 event")
            return None
        
        # Get object key and handle URL decoding
        object_key = object_info.get('key')
        if not object_key:
            logger.error("Missing object key in S3 event")
            return None
        
        # URL decode the object key (S3 keys are URL encoded in events)
        try:
            decoded_object_key = unquote_plus(object_key)
        except Exception as e:
            logger.warning(f"Failed to URL decode object key '{object_key}': {e}")
            decoded_object_key = object_key  # Use original if decoding fails
        
        # Extract file name from object key (last part after final slash)
        file_name = decoded_object_key.split('/')[-1] if '/' in decoded_object_key else decoded_object_key
        
        # Get file size
        file_size = object_info.get('size')
        if file_size is None:
            logger.error("Missing file size in S3 event")
            return None
        
        # Ensure file_size is an integer
        try:
            file_size = int(file_size)
        except (ValueError, TypeError):
            logger.error(f"Invalid file size format: {file_size}")
            return None
        
        # Extract event metadata
        event_time = record.get('eventTime')
        if not event_time:
            logger.error("Missing event time in S3 event")
            return None
        
        event_name = record.get('eventName', 'Unknown')
        aws_region = record.get('awsRegion', 'Unknown')
        
        # Build and return metadata dictionary
        metadata = {
            'bucket_name': bucket_name,
            'file_name': file_name,
            'object_key': decoded_object_key,
            'file_size': file_size,
            'event_time': event_time,
            'event_type': event_name,
            'aws_region': aws_region
        }
        
        logger.info(f"Successfully processed S3 record for file: {file_name}")
        logger.debug(f"Extracted metadata: {metadata}")
        
        return metadata
        
    except Exception as e:
        logger.error(f"Error processing S3 record: {str(e)}", exc_info=True)
        return None


def validate_environment() -> str:
    """
    Validate required environment variables.
    
    Returns:
        str: SNS topic ARN
        
    Raises:
        ValueError: If required environment variables are missing or invalid
    """
    sns_topic_arn = os.environ.get('SNS_TOPIC_ARN')
    
    if not sns_topic_arn:
        raise ValueError("SNS_TOPIC_ARN environment variable is required but not set")
    
    # Basic validation of SNS ARN format
    # Expected format: arn:aws:sns:region:account-id:topic-name
    if not sns_topic_arn.startswith('arn:aws:sns:'):
        raise ValueError(f"Invalid SNS topic ARN format: {sns_topic_arn}")
    
    # Additional validation - ensure ARN has the minimum required parts
    arn_parts = sns_topic_arn.split(':')
    if len(arn_parts) < 6:  # arn:aws:sns:region:account-id:topic-name
        raise ValueError(f"Invalid SNS topic ARN format: {sns_topic_arn}")
    
    return sns_topic_arn


def send_notification(file_info: Dict[str, Any], sns_topic_arn: str) -> None:
    """
    Send a formatted notification message via SNS for a file upload.
    
    This function creates a structured notification message with file details,
    location information, and timestamp, then publishes it to the specified
    SNS topic. It handles SNS subject length limits, message formatting, and
    includes proper error handling with retry logic.
    
    Args:
        file_info (Dict[str, Any]): File metadata dictionary containing:
            - file_name: Name of the uploaded file
            - file_size: File size in bytes
            - bucket_name: S3 bucket name
            - object_key: Full S3 object key
            - event_time: ISO timestamp of the upload
            - event_type: Type of S3 event
            - aws_region: AWS region
        sns_topic_arn (str): ARN of the SNS topic to publish to
        
    Raises:
        ClientError: If SNS publishing fails after retry
        ValueError: If required file_info fields are missing
    """
    logger = logging.getLogger(__name__)
    
    # Validate required fields in file_info
    required_fields = ['file_name', 'file_size', 'bucket_name', 'object_key', 'event_time', 'aws_region']
    for field in required_fields:
        if field not in file_info:
            raise ValueError(f"Missing required field in file_info: {field}")
    
    # Extract file information
    file_name = file_info['file_name']
    file_size = file_info['file_size']
    bucket_name = file_info['bucket_name']
    object_key = file_info['object_key']
    event_time = file_info['event_time']
    event_type = file_info.get('event_type', 'ObjectCreated')
    aws_region = file_info['aws_region']
    
    # Format file size for display
    formatted_size = format_file_size(file_size)
    
    # Get content type (with error handling built into the function)
    content_type = get_content_type(bucket_name, object_key)
    
    # Create subject line with length limit handling (SNS limit is 100 characters)
    base_subject = f"📁 New File Upload: {file_name}"
    if len(base_subject) > 100:
        # Truncate filename to fit within limit, keeping the emoji and basic text
        max_filename_length = 100 - len("📁 New File Upload: ...")
        truncated_filename = file_name[:max_filename_length] + "..."
        subject = f"📁 New File Upload: {truncated_filename}"
    else:
        subject = base_subject
    
    # Create structured message body
    message_body = f"""📁 FILE UPLOAD NOTIFICATION

📄 FILE DETAILS
   Name: {file_name}
   Size: {formatted_size}
   Type: {content_type}

📍 LOCATION
   Bucket: {bucket_name}
   Region: {aws_region}
   Path: {object_key}

⏰ TIMESTAMP
   Event Time: {event_time}
   Event Type: {event_type}

🔗 S3 CONSOLE LINK
   https://s3.console.aws.amazon.com/s3/object/{bucket_name}?region={aws_region}&prefix={object_key}

This notification was generated automatically by the S3 Upload Notifier system."""
    
    # Initialize SNS client
    sns_client = boto3.client('sns')
    
    # Implement retry logic for SNS publishing
    max_retries = 1  # Single retry as per requirements
    last_exception = None
    
    for attempt in range(max_retries + 1):  # 0 = first attempt, 1 = retry
        try:
            # Publish message to SNS
            response = sns_client.publish(
                TopicArn=sns_topic_arn,
                Subject=subject,
                Message=message_body
            )
            
            # Log successful delivery
            message_id = response.get('MessageId')
            logger.info(f"SNS notification sent successfully. MessageId: {message_id}")
            logger.debug(f"Published to topic: {sns_topic_arn}")
            
            # Success - exit the retry loop
            return
            
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', 'Unknown')
            error_message = e.response.get('Error', {}).get('Message', str(e))
            
            last_exception = e
            
            if attempt < max_retries:
                # Log retry attempt
                logger.warning(f"SNS publishing failed (attempt {attempt + 1}), retrying. Error: {error_code} - {error_message}")
                
                # Simple exponential backoff - wait 1 second before retry
                time.sleep(1)
            else:
                # Final attempt failed - log error and re-raise
                logger.error(f"Failed to send SNS notification after {max_retries + 1} attempts. Final error: {error_code} - {error_message}")
                
        except Exception as e:
            last_exception = e
            
            if attempt < max_retries:
                # Log retry attempt for unexpected errors
                logger.warning(f"Unexpected error sending SNS notification (attempt {attempt + 1}), retrying: {str(e)}")
                
                # Simple exponential backoff - wait 1 second before retry
                time.sleep(1)
            else:
                # Final attempt failed - log error and re-raise
                logger.error(f"Unexpected error sending SNS notification after {max_retries + 1} attempts: {str(e)}")
    
    # If we reach here, all attempts failed - re-raise the last exception
    if last_exception:
        raise last_exception


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Main Lambda handler for processing S3 upload events.
    
    This function is invoked by AWS Lambda when S3 upload events occur.
    It processes the events, extracts file metadata, and sends notifications.
    
    Args:
        event (Dict[str, Any]): S3 event data containing upload information
        context (Any): Lambda context object with runtime information
        
    Returns:
        Dict[str, Any]: Response with status code and processing results
        
    Raises:
        ValueError: If environment variables are invalid
        Exception: For any other processing errors
    """
    # Set up logging
    logger = setup_logging()
    
    try:
        # Log function start
        logger.info(f"Lambda function started. Request ID: {context.aws_request_id}")
        logger.info(f"Received event with {len(event.get('Records', []))} records")
        
        # Validate environment variables
        sns_topic_arn = validate_environment()
        logger.info(f"Using SNS topic: {sns_topic_arn}")
        
        # Initialize processing counters
        processed_files: List[str] = []
        errors: List[str] = []
        
        # Process S3 records
        records = event.get('Records', [])
        
        if not records:
            logger.warning("No records found in event")
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'No records to process',
                    'processed_count': 0,
                    'processed_files': [],
                    'errors': []
                })
            }
        
        # Process each S3 record
        for record in records:
            try:
                # Process the S3 record to extract metadata
                file_metadata = process_s3_record(record)
                
                if file_metadata:
                    # Send notification for the uploaded file
                    try:
                        send_notification(file_metadata, sns_topic_arn)
                        processed_files.append(file_metadata['file_name'])
                        logger.info(f"Successfully processed and notified: {file_metadata['file_name']}")
                    except Exception as e:
                        error_msg = f"Failed to send notification for {file_metadata['file_name']}: {str(e)}"
                        logger.error(error_msg)
                        errors.append(error_msg)
                else:
                    # Failed to process record (logged in process_s3_record)
                    errors.append("Failed to process S3 record - invalid format")
                    
            except Exception as e:
                error_msg = f"Error processing record: {str(e)}"
                logger.error(error_msg, exc_info=True)
                errors.append(error_msg)
        
        # Log successful completion
        logger.info(f"Lambda processing completed successfully")
        logger.info(f"Processed {len(processed_files)} files, {len(errors)} errors")
        
        # Return success response
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Processing completed successfully',
                'processed_count': len(processed_files),
                'processed_files': processed_files,
                'errors': errors
            })
        }
        
    except ValueError as e:
        # Configuration or validation errors
        logger.error(f"Configuration error: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': 'Configuration error',
                'message': str(e)
            })
        }
        
    except Exception as e:
        # Unexpected errors
        logger.error(f"Unexpected error during processing: {str(e)}", exc_info=True)
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': 'Internal server error',
                'message': str(e)
            })
        }