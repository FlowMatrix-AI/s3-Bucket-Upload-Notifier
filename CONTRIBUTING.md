# Contributing to S3 Upload Notifier

Thank you for your interest in contributing to the S3 Upload Notifier project!

## Development Setup

1. **Clone the repository:**
   ```bash
   git clone <your-repo-url>
   cd s3-upload-notifier
   ```

2. **Set up Python environment:**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   pip install -r src/requirements.txt
   ```

3. **Install development dependencies:**
   ```bash
   pip install pytest hypothesis boto3 moto
   ```

## Testing

This project uses comprehensive testing with three types of tests:

### Running Tests

```bash
# Run all tests
cd src && python -m pytest . -v

# Run specific test types
cd src && python -m pytest test_handler_unit.py -v        # Unit tests
cd src && python -m pytest test_handler_properties.py -v  # Property-based tests  
cd src && python -m pytest test_handler_integration.py -v # Integration tests
```

### Test Types

1. **Unit Tests**: Test specific functions and edge cases
2. **Property-Based Tests**: Test universal properties across randomized inputs using Hypothesis
3. **Integration Tests**: Test end-to-end workflows and error scenarios

### Writing Tests

- **Unit tests**: Add to `src/test_handler_unit.py`
- **Property tests**: Add to `src/test_handler_properties.py` 
- **Integration tests**: Add to `src/test_handler_integration.py`

All tests should follow the existing patterns and include proper documentation.

## Code Style

- Follow PEP 8 Python style guidelines
- Use type hints where appropriate
- Include docstrings for all functions
- Keep functions focused and testable

## Submitting Changes

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Run all tests to ensure they pass
5. Submit a pull request with a clear description

## Architecture

The project follows AWS serverless best practices:

- **Lambda Function**: Event-driven processing
- **S3 Events**: Trigger notifications on file uploads
- **SNS**: Reliable message delivery
- **CloudWatch**: Centralized logging and monitoring

## Specification-Driven Development

This project uses specification-driven development with:

- **Requirements**: Formal EARS-pattern requirements in `.kiro/specs/`
- **Design**: Comprehensive design with correctness properties
- **Implementation**: Code that validates against the specifications
- **Testing**: Property-based tests that verify correctness properties

## Questions?

Feel free to open an issue for any questions or suggestions!