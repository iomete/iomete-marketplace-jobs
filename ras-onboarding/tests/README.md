# Tests for RAS Onboarding

This directory contains the test suite for the RAS (Resource Access Security) onboarding migration tool.

## Structure

```
tests/
├── conftest.py                           # Shared fixtures and pytest configuration
├── namespace/                            # Namespace migration tests
│   ├── __init__.py
│   ├── test_queries.py                  # SQL query tests
│   ├── test_permission_assignment.py    # Permission assignment tests
│   └── test_migration.py                # Migration logic tests
└── README.md                             # This file
```

## Running Tests

### Run all tests
```bash
pytest
```

### Run tests with verbose output
```bash
pytest -v
```

### Run specific test file
```bash
pytest tests/namespace/test_migration.py
```

### Run specific test class
```bash
pytest tests/namespace/test_migration.py::TestMigrateDomain
```

### Run specific test function
```bash
pytest tests/namespace/test_migration.py::TestMigrateDomain::test_migrate_domain_success
```

### Run tests by marker
```bash
# Run only unit tests
pytest -m unit

# Run only integration tests
pytest -m integration

# Skip slow tests
pytest -m "not slow"
```

### Run with coverage
```bash
pytest --cov=ras_onboarding --cov-report=html
```

## Test Categories

### Unit Tests
Fast tests that mock all external dependencies (databases, external services). These should run in milliseconds.

### Integration Tests
Tests that may interact with actual services or require more complex setup. Marked with `@pytest.mark.integration`.

### Database Tests
Tests that require database connections. Marked with `@pytest.mark.database`.

## Writing Tests

### Using Fixtures
Common fixtures are available in `conftest.py`:
- `mock_database_connection`: Mock database connection
- `mock_database_manager`: Mock DatabaseManager instance
- `full_migration_config`: Complete configuration for migrations
- `sample_domain_id`, `sample_namespace`, etc.: Sample test data

Example:
```python
def test_example(mock_database_manager, sample_domain_id):
    # Your test code here
    pass
```

### Test Naming Conventions
- Test files: `test_*.py`
- Test classes: `Test*`
- Test functions: `test_*`

### Organizing Tests
Group related tests in classes:
```python
class TestMigrateDomain:
    """Test migrate_domain method."""

    def test_success_case(self):
        pass

    def test_error_case(self):
        pass
```

## Key Testing Patterns

### Mocking Database Calls
```python
from unittest.mock import Mock

def test_database_query(mock_bundle_db):
    mock_bundle_db.execute_query.return_value = [{"id": "123"}]
    # Your test code
```

### Testing Error Handling
```python
import pytest

def test_error_handling():
    with pytest.raises(ValueError) as exc_info:
        # Code that should raise ValueError
        pass

    assert "expected message" in str(exc_info.value)
```

### Using Side Effects for Multiple Calls
```python
mock_db.execute_query.side_effect = [
    [{"id": "1"}],  # First call result
    [{"id": "2"}],  # Second call result
    Exception("Error")  # Third call raises exception
]
```

## Test Coverage

Current test coverage includes:
- ✅ SQL query structure and formatting
- ✅ Permission assignment logic
- ✅ User discovery from resource tables
- ✅ Namespace bundle creation and management
- ✅ Domain migration orchestration
- ✅ Error handling and edge cases
- ✅ Dry run mode
- ✅ Duplicate bundle handling strategies

## Continuous Integration

These tests are designed to run in CI/CD pipelines without requiring actual database connections by using mocks.

## Troubleshooting

### Import Errors
Ensure the package is installed in development mode:
```bash
pip install -e .
```

### Fixture Not Found
Check that `conftest.py` is in the tests directory and properly configured.

### Tests Not Discovered
Ensure your test files, classes, and functions follow the naming conventions.
