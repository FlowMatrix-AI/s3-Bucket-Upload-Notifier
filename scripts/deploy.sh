#!/bin/bash

# S3 Upload Notifier Deployment Script

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Default values
ENVIRONMENT=${ENVIRONMENT:-dev}
NOTIFICATION_EMAIL=${NOTIFICATION_EMAIL:-""}
REGION=${AWS_DEFAULT_REGION:-us-east-1}

# Function to print colored output
print_status() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check prerequisites
check_prerequisites() {
    print_status "Checking prerequisites..."
    
    if ! command -v sam &> /dev/null; then
        print_error "AWS SAM CLI is not installed. Please install it first."
        exit 1
    fi
    
    if ! command -v aws &> /dev/null; then
        print_error "AWS CLI is not installed. Please install it first."
        exit 1
    fi
    
    # Check AWS credentials
    if ! aws sts get-caller-identity &> /dev/null; then
        print_error "AWS credentials not configured. Please run 'aws configure'."
        exit 1
    fi
    
    print_status "Prerequisites check passed."
}

# Validate email parameter
validate_email() {
    if [[ -z "$NOTIFICATION_EMAIL" ]]; then
        print_error "NOTIFICATION_EMAIL environment variable is required."
        print_error "Usage: NOTIFICATION_EMAIL=your-email@example.com ./scripts/deploy.sh"
        exit 1
    fi
    
    # Basic email validation
    if [[ ! "$NOTIFICATION_EMAIL" =~ ^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$ ]]; then
        print_error "Invalid email format: $NOTIFICATION_EMAIL"
        exit 1
    fi
}

# Build the application
build_application() {
    print_status "Building SAM application..."
    sam build
    print_status "Build completed successfully."
}

# Deploy the application
deploy_application() {
    print_status "Deploying to environment: $ENVIRONMENT"
    print_status "Notification email: $NOTIFICATION_EMAIL"
    print_status "Region: $REGION"
    
    sam deploy \
        --parameter-overrides \
            Environment="$ENVIRONMENT" \
            NotificationEmail="$NOTIFICATION_EMAIL" \
        --region "$REGION" \
        --confirm-changeset \
        --capabilities CAPABILITY_IAM
    
    print_status "Deployment completed successfully!"
}

# Get stack outputs
show_outputs() {
    print_status "Retrieving stack outputs..."
    
    STACK_NAME="s3-upload-notifier"
    
    echo ""
    echo "=== Deployment Outputs ==="
    aws cloudformation describe-stacks \
        --stack-name "$STACK_NAME" \
        --region "$REGION" \
        --query 'Stacks[0].Outputs[*].[OutputKey,OutputValue]' \
        --output table
    
    echo ""
    print_status "You can now upload files to the S3 bucket to test the notification system."
    print_warning "Don't forget to confirm your email subscription in SNS!"
}

# Main execution
main() {
    echo "S3 Upload Notifier Deployment Script"
    echo "===================================="
    
    check_prerequisites
    validate_email
    build_application
    deploy_application
    show_outputs
    
    print_status "Deployment process completed!"
}

# Run main function
main "$@"