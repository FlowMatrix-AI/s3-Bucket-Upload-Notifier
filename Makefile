# S3 Upload Notifier Makefile

.PHONY: help build validate deploy test clean logs tail-logs delete

# Default target
help:
	@echo "S3 Upload Notifier - Available Commands:"
	@echo ""
	@echo "  build        - Build the SAM application"
	@echo "  validate     - Validate the SAM template"
	@echo "  deploy       - Deploy the application (requires NOTIFICATION_EMAIL)"
	@echo "  test         - Run all tests"
	@echo "  clean        - Clean build artifacts"
	@echo "  logs         - Show recent Lambda logs"
	@echo "  tail-logs    - Tail Lambda logs in real-time"
	@echo "  delete       - Delete the CloudFormation stack"
	@echo ""
	@echo "Environment Variables:"
	@echo "  ENVIRONMENT      - Deployment environment (default: dev)"
	@echo "  NOTIFICATION_EMAIL - Email for notifications (required for deploy)"
	@echo "  AWS_REGION       - AWS region (default: us-east-1)"

# Build the application
build:
	@echo "Building SAM application..."
	sam build

# Validate the SAM template
validate:
	@echo "Validating SAM template..."
	sam validate --lint

# Deploy the application
deploy:
	@if [ -z "$(NOTIFICATION_EMAIL)" ]; then \
		echo "Error: NOTIFICATION_EMAIL environment variable is required"; \
		echo "Usage: make deploy NOTIFICATION_EMAIL=your-email@example.com"; \
		exit 1; \
	fi
	@echo "Deploying application..."
	./scripts/deploy.sh

# Run tests
test:
	@echo "Running tests..."
	@if [ -d "src" ] && [ -f "src/requirements.txt" ]; then \
		cd src && python -m pytest tests/ -v; \
	else \
		echo "No tests found or source directory missing"; \
	fi

# Clean build artifacts
clean:
	@echo "Cleaning build artifacts..."
	rm -rf .aws-sam/
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true

# Show recent Lambda logs
logs:
	@echo "Fetching recent Lambda logs..."
	sam logs -n FileUploadProcessor --start-time '1 hour ago'

# Tail Lambda logs in real-time
tail-logs:
	@echo "Tailing Lambda logs (Ctrl+C to stop)..."
	sam logs -n FileUploadProcessor --tail

# Delete the CloudFormation stack
delete:
	@echo "Deleting CloudFormation stack..."
	@read -p "Are you sure you want to delete the stack? (y/N): " confirm; \
	if [ "$$confirm" = "y" ] || [ "$$confirm" = "Y" ]; then \
		sam delete; \
	else \
		echo "Deletion cancelled."; \
	fi

# Local development targets
local-build: build
	@echo "Application built for local testing"

local-invoke:
	@echo "Invoking function locally with sample event..."
	sam local invoke FileUploadProcessor --event events/s3-event.json

# Install development dependencies
install-dev:
	@echo "Installing development dependencies..."
	pip install -r src/requirements.txt

# Format code (if using black)
format:
	@if command -v black >/dev/null 2>&1; then \
		echo "Formatting Python code..."; \
		black src/; \
	else \
		echo "black not installed, skipping formatting"; \
	fi

# Lint code (if using flake8)
lint:
	@if command -v flake8 >/dev/null 2>&1; then \
		echo "Linting Python code..."; \
		flake8 src/; \
	else \
		echo "flake8 not installed, skipping linting"; \
	fi