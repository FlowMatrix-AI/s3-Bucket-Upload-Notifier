# Implementation Plan: S3 File Upload Notifier

## Overview

This implementation plan converts the S3 Upload Notifier design into discrete coding tasks. Each task builds incrementally toward a complete serverless notification system using AWS SAM, Python Lambda functions, and SNS integration. The plan includes both core functionality and comprehensive testing with property-based tests.

## Tasks

- [x] 1. Set up project structure and SAM template
  - Create directory structure (src/, infrastructure/)
  - Initialize SAM template with basic configuration
  - Define S3 bucket, Lambda function, and SNS topic resources
  - Configure IAM roles and permissions with least-privilege principle
  - Add CloudFormation outputs for key resource identifiers
  - _Requirements: 6.1, 6.2, 6.3, 6.4, 6.5_

- [ ] 2. Implement core Lambda function structure
  - [x] 2.1 Create Lambda handler entry point
    - Implement `lambda_handler` function with proper signature
    - Add environment variable validation for SNS_TOPIC_ARN
    - Set up basic logging configuration
    - _Requirements: 5.3_

  - [x] 2.2 Write property test for Lambda handler
    - **Property 5: Processing Metrics Accuracy**
    - **Validates: Requirements 5.2, 5.5**

  - [x] 2.3 Implement S3 event record processing
    - Create `process_s3_record` function to extract file metadata
    - Handle URL decoding for object keys
    - Validate S3 event structure and required fields
    - _Requirements: 2.1, 2.4, 5.4_

  - [x] 2.4 Write property test for metadata extraction
    - **Property 2: Complete Metadata Extraction**
    - **Validates: Requirements 2.1, 2.2, 2.3, 2.4**

- [ ] 3. Implement file metadata processing
  - [x] 3.1 Create file size formatting function
    - Implement `format_file_size` with proper unit conversion (B, KB, MB, GB, TB)
    - Handle edge cases (zero bytes, very large files)
    - _Requirements: 2.2, 3.2_

  - [x] 3.2 Write property test for file size formatting
    - **Property 6: File Size Formatting Consistency**
    - **Validates: Requirements 3.2**

  - [x] 3.3 Implement content type detection
    - Create `get_content_type` function using S3 head_object
    - Handle S3 API errors gracefully
    - Return appropriate defaults for unknown types
    - _Requirements: 2.3, 2.5_

  - [x] 3.4 Write unit tests for content type detection
    - Test various file types and extensions
    - Test error handling for inaccessible objects
    - _Requirements: 2.3, 2.5_

- [x] 4. Implement notification message formatting
  - [x] 4.1 Create message formatter
    - Implement `send_notification` function with structured message format
    - Include file details, location, and timestamp sections
    - Handle SNS subject length limits (100 characters)
    - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.5_

  - [x] 4.2 Write property test for message formatting
    - **Property 3: Comprehensive Message Formatting**
    - **Validates: Requirements 3.1, 3.2, 3.3, 3.4**

  - [x] 4.3 Implement SNS publishing
    - Add SNS client initialization and publishing logic
    - Include proper error handling and retry logic
    - Log successful deliveries with message IDs
    - _Requirements: 4.1, 4.2, 4.3, 4.4_

  - [x] 4.4 Write property test for SNS publishing
    - **Property 4: SNS Publishing Success**
    - **Validates: Requirements 4.1, 4.2, 4.4**

- [x] 5. Add comprehensive error handling and logging
  - [x] 5.1 Implement error handling patterns
    - Add try-catch blocks for all AWS API calls
    - Implement retry logic for SNS publishing
    - Handle malformed events gracefully
    - _Requirements: 4.3, 5.1, 5.4_

  - [x] 5.2 Write unit tests for error scenarios
    - Test SNS publishing failures and retries
    - Test malformed S3 event handling
    - Test missing environment variables
    - _Requirements: 4.3, 5.1, 5.3, 5.4_

  - [x] 5.3 Add comprehensive logging
    - Log processing start/completion with metrics
    - Log individual file processing results
    - Log all errors with detailed context
    - _Requirements: 5.1, 5.2_

- [x] 6. Create Lambda dependencies and deployment configuration
  - [x] 6.1 Create requirements.txt
    - Add boto3 dependency with version pinning
    - Include any additional testing dependencies
    - _Requirements: Infrastructure support_

  - [x] 6.2 Configure SAM deployment settings
    - Set up proper Lambda runtime and memory configuration
    - Configure S3 event trigger with ObjectCreated events
    - Add S3 invoke permissions for Lambda
    - _Requirements: 6.1, 6.2_

- [x] 7. Integration and end-to-end validation
  - [x] 7.1 Wire all components together
    - Integrate all functions in the main Lambda handler
    - Ensure proper error propagation and logging
    - Validate environment variable usage
    - _Requirements: All core requirements_

  - [x] 7.2 Write integration tests
    - Test complete event processing flow
    - Test batch processing with multiple files
    - Test error scenarios and recovery
    - _Requirements: 1.1, 1.2, 1.3_

- [x] 8. Final checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- All tasks are required for comprehensive development from the start
- Each task references specific requirements for traceability
- Property tests validate universal correctness properties from the design
- Unit tests validate specific examples and edge cases
- The SAM template handles all infrastructure provisioning
- Lambda function uses Python 3.12 runtime as specified in requirements