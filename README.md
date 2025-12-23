# S3 File Upload Notifier

A serverless notification system that automatically detects file uploads to S3 buckets and sends formatted email notifications via SNS.

## Architecture

- **S3 Bucket**: Monitors file uploads with event notifications
- **Lambda Function**: Processes upload events and formats notifications
- **SNS Topic**: Distributes email notifications to subscribers
- **CloudWatch**: Centralized logging and monitoring

## Prerequisites

- AWS CLI configured with appropriate permissions
- AWS SAM CLI installed
- Python 3.12 or later
- Valid email address for notifications

## Quick Start

1. **Clone and setup:**
   ```bash
   git clone <your-repo-url>
   cd s3-upload-notifier
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   pip install -r src/requirements.txt
   ```

2. **Run tests:**
   ```bash
   cd src && python -m pytest . -v
   ```

3. **Deploy to AWS:**
   ```bash
   # Set your notification email
   export NOTIFICATION_EMAIL="your-email@example.com"
   
   # Deploy using the script
   ./scripts/deploy.sh
   
   # Or deploy manually
   sam build && sam deploy --parameter-overrides NotificationEmail=your-email@example.com
   ```

4. **Test the system:**
   - Upload a file to your S3 bucket
   - Check your email for the notification!

## Deployment

1. **Build the application:**
   ```bash
   sam build
   ```

2. **Deploy with guided setup:**
   ```bash
   sam deploy --guided
   ```

3. **Deploy with parameters:**
   ```bash
   sam deploy --parameter-overrides \
     Environment=dev \
     NotificationEmail=your-email@example.com
   ```

## Configuration

### Environment Variables

- `SNS_TOPIC_ARN`: ARN of the SNS topic for notifications (automatically configured)
- `LOG_LEVEL`: Logging level (default: INFO)

### Parameters

- `Environment`: Deployment environment (dev/staging/prod)
- `NotificationEmail`: Email address to receive notifications

## Usage

1. Upload files to the created S3 bucket
2. Receive email notifications with file details
3. Monitor logs in CloudWatch for troubleshooting

## File Structure

```
├── .kiro/specs/           # Feature specifications and design documents
├── src/                   # Lambda function source code
│   ├── handler.py         # Main Lambda handler
│   ├── requirements.txt   # Python dependencies
│   ├── test_handler_unit.py        # Unit tests
│   ├── test_handler_properties.py  # Property-based tests
│   └── test_handler_integration.py # Integration tests
├── events/                # Sample S3 events for testing
├── scripts/               # Deployment and utility scripts
├── infrastructure/        # Infrastructure configuration
├── template.yaml          # SAM template
├── samconfig.toml         # SAM CLI configuration
├── Makefile              # Build and deployment commands
└── README.md             # This file
```

## Monitoring

- **CloudWatch Logs**: `/aws/lambda/file-upload-processor-{environment}`
- **Metrics**: Lambda invocations, errors, and duration
- **SNS**: Message delivery status and failures

## Security

- S3 bucket with public access blocked
- Least-privilege IAM permissions
- Encrypted SNS topic with AWS managed keys
- VPC endpoints support (optional)

## Development

### Local Testing

```bash
# Validate SAM template
sam validate

# Build and test locally
sam build
sam local invoke FileUploadProcessor --event events/s3-event.json
```

### Testing

The project includes comprehensive testing with three types of tests:

```bash
# Run all tests
cd src && python -m pytest . -v

# Run unit tests only
cd src && python -m pytest test_handler_unit.py -v

# Run property-based tests only  
cd src && python -m pytest test_handler_properties.py -v

# Run integration tests only
cd src && python -m pytest test_handler_integration.py -v
```

**Test Coverage:**
- **Unit Tests (37 tests)**: Specific functionality and edge cases
- **Property-Based Tests (11 tests)**: Universal correctness properties across randomized inputs
- **Integration Tests (9 tests)**: End-to-end workflows and error scenarios
- **Total: 57 tests** validating all requirements and design properties

## Cleanup

To remove all resources:

```bash
sam delete
```

## Support

For issues and questions, refer to the CloudWatch logs and AWS documentation.