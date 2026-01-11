# Contributing to Kafka Pizza Shop Learning Project

Thank you for your interest in contributing to this Kafka learning project! This document provides guidelines for contributing to the project.

## Table of Contents

- [Code of Conduct](#code-of-conduct)
- [How to Report Bugs](#how-to-report-bugs)
- [How to Suggest Features](#how-to-suggest-features)
- [Pull Request Process](#pull-request-process)
- [Code Style Guidelines](#code-style-guidelines)
- [Development Setup](#development-setup)

## Code of Conduct

This project welcomes contributions from everyone. Please be respectful and constructive in all interactions.

## How to Report Bugs

If you find a bug, please create an issue with the following information:

### Bug Report Template

```
**Description**
A clear and concise description of the bug.

**To Reproduce**
Steps to reproduce the behavior:
1. Run command '...'
2. Execute script '...'
3. See error

**Expected Behavior**
What you expected to happen.

**Actual Behavior**
What actually happened.

**Environment**
- OS: [e.g., macOS, Linux, Windows]
- Python version: [e.g., 3.9]
- Docker version: [e.g., 20.10.8]
- Kafka version: [from docker-compose.yml]

**Logs**
```

Paste relevant error messages or logs here

```

**Additional Context**
Any other information about the problem.
```

## How to Suggest Features

We welcome feature suggestions! Please create an issue with:

### Feature Request Template

```
**Feature Description**
A clear description of the feature you'd like to see.

**Use Case**
Explain the use case and why this would be valuable.

**Proposed Implementation**
(Optional) Suggest how this could be implemented.

**Alternatives Considered**
(Optional) Other approaches you've considered.
```

## Pull Request Process

### Before You Start

1. Check existing issues and pull requests to avoid duplicates
2. For major changes, open an issue first to discuss the proposal
3. Fork the repository and create a new branch for your changes

### Making Changes

1. Create a feature branch from `main`:

   ```bash
   git checkout -b feature/your-feature-name
   ```

2. Make your changes following the [Code Style Guidelines](#code-style-guidelines)

3. Test your changes:

   ```bash
   # Run tests
   python -m pytest tests/
   
   # Test manually with Docker
   docker-compose up -d
   python src/producers/simulate_orders.py
   ```

4. Update documentation if needed:
   - Update README.md for user-facing changes
   - Add docstrings to new functions/classes
   - Update relevant comments

### Submitting Pull Request

1. Commit your changes with clear, descriptive messages:

   ```bash
   git commit -m "Add feature: description of change"
   ```

2. Push to your fork:

   ```bash
   git push origin feature/your-feature-name
   ```

3. Create a Pull Request with:
   - **Title**: Clear, concise description of changes
   - **Description**:
     - What changes were made
     - Why these changes were needed
     - How to test the changes
     - Screenshots (if applicable)
   - **Issue Reference**: Link to related issue if applicable

4. Wait for review and address any feedback

### Pull Request Requirements

- [ ] Code follows the style guidelines
- [ ] All tests pass
- [ ] New features include tests
- [ ] Documentation updated
- [ ] Commit messages are clear
- [ ] No merge conflicts with main branch

## Code Style Guidelines

### Python Style

- Follow [PEP 8](https://www.python.org/dev/peps/pep-0008/) style guide
- Use 4 spaces for indentation (no tabs)
- Maximum line length: 100 characters
- Use meaningful variable and function names

### Naming Conventions

```python
# Classes: PascalCase
class PizzaMaker:
    pass

# Functions and variables: snake_case
def process_order():
    order_id = "123"
    
# Constants: UPPER_CASE
MAX_RETRIES = 3
TOPIC_NAME = "orders"
```

### Docstrings

Use Google-style docstrings:

```python
def process_order(order_id: str, priority: int = 1) -> bool:
    """
    Process a pizza order with the given ID.
    
    Args:
        order_id: Unique identifier for the order
        priority: Order priority level (default: 1)
        
    Returns:
        True if order was processed successfully, False otherwise
        
    Raises:
        KafkaException: If Kafka connection fails
    """
    pass
```

### Import Organization

```python
# Standard library imports
import json
import time
from typing import Dict, List

# Third-party imports
from kafka import KafkaProducer, KafkaConsumer

# Local imports
from config import KAFKA_BOOTSTRAP_SERVERS
from models import Order
```

### Error Handling

- Use specific exception types
- Include helpful error messages
- Log errors appropriately

```python
try:
    producer.send(topic, message)
except KafkaError as e:
    logger.error(f"Failed to send message to {topic}: {e}")
    raise
```

### Comments

- Write self-documenting code when possible
- Add comments for complex logic
- Keep comments up-to-date with code changes

```python
# Good: Explains why, not what
# Retry 3 times because Kafka may be temporarily unavailable during startup
for attempt in range(3):
    try:
        connect_to_kafka()
        break
    except Exception:
        time.sleep(1)

# Avoid: States the obvious
# Loop 3 times
for attempt in range(3):
    pass
```

## Development Setup

### Initial Setup

```bash
# Clone the repository
git clone https://github.com/yourusername/kafka-pizza-shop.git
cd kafka-pizza-shop

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Copy environment template
cp .env.example .env

# Start Kafka infrastructure
docker-compose up -d
```

### Testing Workflow

```bash
# Run all tests
python -m pytest tests/

# Run specific test file
python -m pytest tests/test_kafka_connection.py

# Run with verbose output
python -m pytest tests/ -v

# Test a specific component manually
python src/consumers/bookkeeper.py
```

### Common Development Tasks

#### Adding a New Consumer

1. Create file in `src/consumers/`
2. Inherit from base consumer pattern
3. Implement message processing logic
4. Add error handling and logging
5. Create run script if needed
6. Update README with usage instructions

#### Adding a New Producer

1. Create file in `src/producers/`
2. Follow existing producer patterns
3. Add validation for messages
4. Include error handling
5. Update documentation

#### Modifying Message Schema

1. Update [`models.py`](src/models.py)
2. Update affected producers
3. Update affected consumers
4. Document changes in README
5. Consider backward compatibility

## Questions?

If you have questions about contributing:

1. Check existing issues and discussions
2. Review the README.md for project details
3. Open an issue with the "question" label

Thank you for contributing to this learning project! 🍕
