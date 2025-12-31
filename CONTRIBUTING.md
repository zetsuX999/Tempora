# Contributing to Tempora

Thank you for your interest in contributing to Tempora! This document provides guidelines and information for contributors.

## Code of Conduct

By participating in this project, you agree to maintain a respectful and inclusive environment for everyone.

## How to Contribute

### Reporting Bugs

1. **Search existing issues** to avoid duplicates
2. **Use the bug report template** when creating a new issue
3. **Include**:
   - Python version
   - Django version
   - PostgreSQL version
   - Steps to reproduce
   - Expected vs actual behavior
   - Error messages and stack traces

### Suggesting Features

1. **Open a discussion** first to gauge interest
2. **Describe the use case** and why existing features don't solve it
3. **Consider implementation complexity** and maintenance burden

### Pull Requests

1. **Fork the repository** and create a feature branch
2. **Follow the code style** (see below)
3. **Write tests** for new functionality
4. **Update documentation** if needed
5. **Submit a PR** with a clear description

## Development Setup

### Prerequisites

- Python 3.11+
- PostgreSQL 14+
- Git

### Local Setup

```bash
# Clone your fork
git clone https://github.com/YOUR_USERNAME/tempora.git
cd tempora

# Create virtual environment
python -m venv venv
source venv/bin/activate  # or `venv\Scripts\activate` on Windows

# Install development dependencies
pip install -e ".[dev]"

# Set up test database
createdb tempora_test

# Run tests
pytest
```

### Running Tests

```bash
# All tests
pytest

# Specific test file
pytest tests/test_raft.py

# With coverage
pytest --cov=tempora --cov-report=html

# Async tests only
pytest -m asyncio
```

## Code Style

### Python Style

- Follow [PEP 8](https://pep8.org/)
- Use type hints for all public APIs
- Maximum line length: 100 characters
- Use `black` for formatting
- Use `isort` for import sorting

```bash
# Format code
black tempora tests
isort tempora tests

# Check linting
flake8 tempora tests
mypy tempora
```

### Naming Conventions

- Classes: `PascalCase`
- Functions/methods: `snake_case`
- Constants: `UPPER_SNAKE_CASE`
- Private members: `_leading_underscore`

### Documentation

- All public classes and functions must have docstrings
- Use Google-style docstrings
- Include type information in docstrings

```python
def schedule_task(
    name: str,
    func: str,
    args: dict | None = None,
    run_at: datetime | None = None,
) -> Task:
    """Schedule a task for execution.

    Args:
        name: Unique task identifier.
        func: Dotted path to callable (e.g., 'myapp.tasks.send_email').
        args: Arguments to pass to the function.
        run_at: When to execute. Defaults to immediate execution.

    Returns:
        The created Task instance.

    Raises:
        ValueError: If name is already in use.
        ImportError: If func cannot be imported.
    """
```

## Architecture Guidelines

### Core Principles

1. **Consistency over availability** - Tempora uses Raft consensus which prioritizes consistency
2. **No external dependencies** - Keep the core scheduler free of Redis/RabbitMQ requirements
3. **Django-first** - Integrate naturally with Django patterns and conventions
4. **Production-ready** - All features must be hardened for production use

### Package Structure

```
tempora/
├── coordination/     # TCP layer, connection management
├── distributed/      # Raft consensus, replication
├── models.py         # Django ORM models
├── settings.py       # Default settings
└── __init__.py       # Public API exports
```

### Testing Requirements

- **Unit tests** for all new functions/methods
- **Integration tests** for multi-node scenarios
- **Async tests** must use `pytest-asyncio`
- **Mock external I/O** (database, network) in unit tests
- **Minimum 80% coverage** for new code

## Commit Guidelines

### Commit Messages

Follow [Conventional Commits](https://www.conventionalcommits.org/):

```
<type>(<scope>): <description>

[optional body]

[optional footer]
```

Types:
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation only
- `style`: Code style (formatting, semicolons, etc.)
- `refactor`: Code refactoring
- `test`: Adding or updating tests
- `chore`: Maintenance tasks

Examples:
```
feat(raft): add pre-vote optimization for leader election

fix(replication): handle network partition during append entries

docs(readme): add production deployment section
```

### Branch Naming

- `feature/description` - New features
- `fix/description` - Bug fixes
- `docs/description` - Documentation
- `refactor/description` - Code refactoring

## Review Process

1. **All PRs require review** before merging
2. **CI must pass** (tests, linting, type checking)
3. **Documentation must be updated** for user-facing changes
4. **Changelog entry required** for all changes

## Release Process

Releases are managed by maintainers:

1. Update version in `pyproject.toml`
2. Update `CHANGELOG.md`
3. Create a git tag
4. GitHub Actions handles PyPI publishing

## Getting Help

- **Questions**: Open a GitHub Discussion
- **Bugs**: Open a GitHub Issue
- **Security**: Email security@tempora.io (do not open public issues)

## License

By contributing, you agree that your contributions will be licensed under the project's dual MIT/Commercial license.

---

Thank you for contributing to Tempora!
